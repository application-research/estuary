package main

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/ipfs/go-blockservice"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-merkledag"
	"github.com/labstack/echo/v4"
	"github.com/libp2p/go-libp2p-core/peer"
)

type pinningOperation struct {
	obj   cid.Cid
	name  string
	peers []peer.AddrInfo
	meta  map[string]interface{}

	status string

	contId  uint
	replace uint

	started     time.Time
	numFetched  int
	sizeFetched int64
	fetchErr    error
	endTime     time.Time

	lk sync.Mutex
}

func (po *pinningOperation) fail(err error) {
	po.lk.Lock()
	defer po.lk.Unlock()

	po.fetchErr = err
	po.endTime = time.Now()
	po.status = "failed"
}

func (po *pinningOperation) complete() {
	po.lk.Lock()
	defer po.lk.Unlock()

	po.endTime = time.Now()
	po.status = "pinned"
}

func (po *pinningOperation) setStatus(st string) {
	po.lk.Lock()
	defer po.lk.Unlock()

	po.status = st
}

func (s *Server) pinStatus(cont uint) (*ipfsPinStatus, error) {
	s.pinLk.Lock()
	po, ok := s.pinJobs[cont]
	s.pinLk.Unlock()
	if !ok {
		var content Content
		if err := s.DB.First(&content, "id = ?", cont).Error; err != nil {
			return nil, err
		}

		var meta map[string]interface{}
		if content.PinMeta != "" {
			if err := json.Unmarshal([]byte(content.PinMeta), &meta); err != nil {
				log.Warnf("content %d has invalid pinmeta: %s", cont, err)
			}
		}

		ps := &ipfsPinStatus{
			Requestid: fmt.Sprint(cont),
			Status:    "pinning",
			Created:   content.CreatedAt,
			Pin: ipfsPin{
				Cid:  content.Cid.CID.String(),
				Name: content.Name,
				Meta: meta,
			},
			Delegates: s.pinDelegatesForContent(cont),
			Info:      nil, // TODO: all sorts of extra info we could add...
		}

		if content.Active {
			ps.Status = "pinned"
		}

		return ps, nil
	}

	po.lk.Lock()
	defer po.lk.Unlock()

	return &ipfsPinStatus{
		Requestid: fmt.Sprint(cont),
		Status:    po.status,
		Created:   po.started,
		Pin: ipfsPin{
			Cid:  po.obj.String(),
			Name: po.name,
			Meta: po.meta,
		},
		Delegates: s.pinDelegatesForContent(cont),
		Info:      nil,
	}, nil
}

func (s *Server) pinDelegatesForContent(cont uint) []string {
	var out []string
	for _, a := range s.Node.Host.Addrs() {
		out = append(out, fmt.Sprintf("%s/p2p/%s", a, s.Node.Host.ID()))
	}

	return out
}

func (s *Server) doPinning(op *pinningOperation) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ctx, span := s.tracer.Start(ctx, "doPinning")
	defer span.End()

	for _, pi := range op.peers {
		if err := s.Node.Host.Connect(ctx, pi); err != nil {
			log.Warnf("failed to connect to origin node for pinning operation: %s", err)
		}
	}

	bserv := blockservice.New(s.Node.Blockstore, s.Node.Bitswap)
	dserv := merkledag.NewDAGService(bserv)

	dsess := merkledag.NewSession(ctx, dserv)

	op.setStatus("pinning")
	if err := s.addDatabaseTrackingToContent(ctx, op.contId, dsess, s.Node.Blockstore, op.obj); err != nil {
		op.fail(err)
		return err
	}

	op.complete()

	s.CM.ToCheck <- op.contId

	if err := s.Node.Provider.Provide(op.obj); err != nil {
		log.Infof("providing failed: %s", err)
	}

	if op.replace > 0 {
		if err := s.CM.RemoveContent(ctx, op.replace, true); err != nil {
			log.Infof("failed to remove content in replacement: %d", op.replace)
		}
	}

	return nil
}

func (s *Server) pinQueueManager() {
	var next *pinningOperation

	var send chan *pinningOperation

	if len(s.pinQueue) > 0 {
		next = s.pinQueue[0]
		s.pinQueue = s.pinQueue[1:]
		send = s.pinQueueOut
	}

	for {
		select {
		case op := <-s.pinQueueIn:
			if next == nil {
				next = op
				send = s.pinQueueOut
			} else {
				s.pinQueueLk.Lock()
				s.pinQueue = append(s.pinQueue, op)
				s.pinQueueLk.Unlock()
			}
		case send <- next:
			if len(s.pinQueue) > 0 {
				next = s.pinQueue[0]
				s.pinQueue = s.pinQueue[1:]
				send = s.pinQueueOut
			} else {
				next = nil
				send = nil
			}
		}
	}
}

func (s *Server) pinWorker() {
	for op := range s.pinQueueOut {
		if err := s.doPinning(op); err != nil {
			log.Errorf("pinning queue error: %s", err)
		}
	}
}

func (s *Server) refreshPinQueue() error {
	var toPin []Content
	if err := s.DB.Find(&toPin, "active = false and pinning = true").Error; err != nil {
		return err
	}

	// TODO: this doesnt persist the replacement directives, so a queued
	// replacement, if ongoing during a restart of the node, will still
	// complete the pin when the process comes back online, but it wont delete
	// the old pin.
	// Need to fix this, probably best option is just to add a 'replace' field
	// to content, could be interesting to see the graph of replacements
	// anyways
	for _, c := range toPin {
		s.addPinToQueue(c, nil, 0)
	}

	return nil
}

func (s *Server) pinContent(user uint, obj cid.Cid, name string, cols []*Collection, peers []peer.AddrInfo, replace uint, meta map[string]interface{}) (*ipfsPinStatus, error) {
	var metab string
	if meta != nil {
		b, err := json.Marshal(meta)
		if err != nil {
			return nil, err
		}
		metab = string(b)
	}

	cont := Content{
		Cid: dbCID{obj},

		Name:        name,
		UserID:      user,
		Active:      false,
		Replication: defaultReplication,

		Pinning: true,
		PinMeta: metab,

		/*
			Size        int64  `json:"size"`
			Offloaded   bool   `json:"offloaded"`
		*/

	}
	if err := s.DB.Create(&cont).Error; err != nil {
		return nil, err
	}

	s.addPinToQueue(cont, peers, replace)

	return s.pinStatus(cont.ID)
}

// TODO: the queue needs to be a lot smarter than throwing things into a channel...
func (s *Server) addPinToQueue(cont Content, peers []peer.AddrInfo, replace uint) {
	op := &pinningOperation{
		contId:  cont.ID,
		obj:     cont.Cid.CID,
		name:    cont.Name,
		peers:   peers,
		started: cont.CreatedAt,
		status:  "queued",
		replace: replace,
	}

	s.pinLk.Lock()
	// TODO: check if we are overwriting anything here
	s.pinJobs[cont.ID] = op
	s.pinLk.Unlock()

	go func() {
		s.pinQueueIn <- op
	}()
}

type ipfsPin struct {
	Cid     string                 `json:"cid"`
	Name    string                 `json:"name"`
	Origins []string               `json:"origins"`
	Meta    map[string]interface{} `json:"meta"`
}

type ipfsPinStatus struct {
	Requestid string                 `json:"requestid"`
	Status    string                 `json:"status"`
	Created   time.Time              `json:"created"`
	Pin       ipfsPin                `json:"pin"`
	Delegates []string               `json:"delegates"`
	Info      map[string]interface{} `json:"info"`
}

// pinning api /pins endpoint
func (s *Server) handleListPins(e echo.Context, u *User) error {
	_, span := s.tracer.Start(e.Request().Context(), "handleListPins")
	defer span.End()

	qcids := e.QueryParam("cids")
	qname := e.QueryParam("name")
	qstatus := e.QueryParam("status")
	qbefore := e.QueryParam("before")
	qafter := e.QueryParam("after")
	qlimit := e.QueryParam("limit")

	q := s.DB.Model(Content{}).Where("user_id = ?", u.ID).Order("created_at desc")

	if qcids != "" {
		var cids []cid.Cid
		for _, cstr := range strings.Split(qcids, ",") {
			c, err := cid.Decode(cstr)
			if err != nil {
				return err
			}
			cids = append(cids, c)
		}

		q = q.Where("cid in ?", cids)
	}

	if qname != "" {
		q = q.Where("name = ?", qname)
	}

	if qbefore != "" {
		beftime, err := time.Parse(time.RFC3339, qbefore)
		if err != nil {
			return err
		}

		q = q.Where("created_at <= ?", beftime)
	}

	if qafter != "" {
		aftime, err := time.Parse(time.RFC3339, qafter)
		if err != nil {
			return err
		}

		q = q.Where("created_at > ?", aftime)
	}

	var lim int
	if qlimit != "" {
		limit, err := strconv.Atoi(qlimit)
		if err != nil {
			return err
		}
		lim = limit
	}

	var contents []Content
	if err := q.Scan(&contents).Error; err != nil {
		return err
	}

	var allowed map[string]bool

	if qstatus != "" {
		allowed = make(map[string]bool)
		/*
		   - queued     # pinning operation is waiting in the queue; additional info can be returned in info[status_details]
		   - pinning    # pinning in progress; additional info can be returned in info[status_details]
		   - pinned     # pinned successfully
		   - failed     # pinning service was unable to finish pinning operation; additional info can be found in info[status_details]
		*/
		statuses := strings.Split(qstatus, ",")
		for _, s := range statuses {
			switch s {
			case "queued", "pinning", "pinned", "failed":
				allowed[s] = true
			default:
				return fmt.Errorf("unrecognized pin status in query: %q", s)
			}
		}

	}

	var out []*ipfsPinStatus
	for _, c := range contents {
		if lim > 0 && len(out) >= lim {
			break
		}

		st, err := s.pinStatus(c.ID)
		if err != nil {
			return err
		}
		if allowed == nil || allowed[st.Status] {
			out = append(out, st)
		}
	}

	return e.JSON(200, map[string]interface{}{
		"count":   len(contents),
		"results": out,
	})
}

/*
{

    "cid": "QmCIDToBePinned",
    "name": "PreciousData.pdf",
    "origins":

[

    "/ip4/203.0.113.142/tcp/4001/p2p/QmSourcePeerId",
    "/ip4/203.0.113.114/udp/4001/quic/p2p/QmSourcePeerId"

],
"meta":

    {
        "app_id": "99986338-1113-4706-8302-4420da6158aa"
    }

}
*/

func (s *Server) handleAddPin(e echo.Context, u *User) error {
	var pin ipfsPin
	if err := e.Bind(&pin); err != nil {
		return err
	}

	/*
		var col *Collection
		if params.Collection != "" {
			var srchCol Collection
			if err := s.DB.First(&srchCol, "uuid = ? and user_id = ?", params.Collection, u.ID).Error; err != nil {
				return err
			}

			col = &srchCol
		}
	*/

	var addrInfos []peer.AddrInfo
	for _, p := range pin.Origins {
		ai, err := peer.AddrInfoFromString(p)
		if err != nil {
			return err
		}

		addrInfos = append(addrInfos, *ai)
	}

	obj, err := cid.Decode(pin.Cid)
	if err != nil {
		return err
	}

	status, err := s.pinContent(u.ID, obj, pin.Name, nil, addrInfos, 0, pin.Meta)
	if err != nil {
		return err
	}

	return e.JSON(202, status)
}

func (s *Server) handleGetPin(e echo.Context, u *User) error {
	id, err := strconv.Atoi(e.Param("requestid"))
	if err != nil {
		return err
	}

	st, err := s.pinStatus(uint(id))
	if err != nil {
		return err
	}

	return e.JSON(200, st)
}

func (s *Server) handleReplacePin(e echo.Context, u *User) error {
	id, err := strconv.Atoi(e.Param("requestid"))
	if err != nil {
		return err
	}

	var pin ipfsPin
	if err := e.Bind(&pin); err != nil {
		return err
	}

	var content Content
	if err := s.DB.First(&content, "id = ?", id).Error; err != nil {
		return err
	}
	if content.UserID != u.ID {
		return &httpError{
			Code:    401,
			Message: ERR_NOT_AUTHORIZED,
		}
	}

	var addrInfos []peer.AddrInfo
	for _, p := range pin.Origins {
		ai, err := peer.AddrInfoFromString(p)
		if err != nil {
			return err
		}

		addrInfos = append(addrInfos, *ai)
	}

	obj, err := cid.Decode(pin.Cid)
	if err != nil {
		return err
	}

	status, err := s.pinContent(u.ID, obj, pin.Name, nil, addrInfos, uint(id), pin.Meta)
	if err != nil {
		return err
	}

	return e.JSON(200, status)
}

func (s *Server) handleDeletePin(e echo.Context, u *User) error {
	ctx := e.Request().Context()
	id, err := strconv.Atoi(e.Param("requestid"))
	if err != nil {
		return err
	}

	var content Content
	if err := s.DB.First(&content, "id = ?", id).Error; err != nil {
		return err
	}
	if content.UserID != u.ID {
		return &httpError{
			Code:    401,
			Message: ERR_NOT_AUTHORIZED,
		}
	}

	if err := s.CM.RemoveContent(ctx, uint(id), true); err != nil {
		return err
	}

	return nil
}
