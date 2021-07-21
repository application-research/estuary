package main

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"time"

	"github.com/ipfs/go-blockservice"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-merkledag"
	"github.com/labstack/echo/v4"
	"github.com/libp2p/go-libp2p-core/peer"
	drpc "github.com/whyrusleeping/estuary/drpc"
	"github.com/whyrusleeping/estuary/pinner"
	"github.com/whyrusleeping/estuary/types"
	"github.com/whyrusleeping/estuary/util"
	"golang.org/x/xerrors"
)

func (cm *ContentManager) pinStatus(cont uint) (*types.IpfsPinStatus, error) {
	cm.pinLk.Lock()
	po, ok := cm.pinJobs[cont]
	cm.pinLk.Unlock()
	if !ok {
		var content Content
		if err := cm.DB.First(&content, "id = ?", cont).Error; err != nil {
			return nil, err
		}

		var meta map[string]interface{}
		if content.PinMeta != "" {
			if err := json.Unmarshal([]byte(content.PinMeta), &meta); err != nil {
				log.Warnf("content %d has invalid pinmeta: %s", cont, err)
			}
		}

		ps := &types.IpfsPinStatus{
			Requestid: fmt.Sprint(cont),
			Status:    "pinning",
			Created:   content.CreatedAt,
			Pin: types.IpfsPin{
				Cid:  content.Cid.CID.String(),
				Name: content.Name,
				Meta: meta,
			},
			Delegates: cm.pinDelegatesForContent(cont),
			Info:      nil, // TODO: all sorts of extra info we could add...
		}

		if content.Active {
			ps.Status = "pinned"
		}

		return ps, nil
	}

	status := po.PinStatus()
	status.Delegates = cm.pinDelegatesForContent(cont)

	return status, nil
}

func (cm *ContentManager) pinDelegatesForContent(cont uint) []string {
	var out []string
	for _, a := range cm.Host.Addrs() {
		out = append(out, fmt.Sprintf("%s/p2p/%s", a, cm.Host.ID()))
	}

	return out
}

func (s *Server) doPinning(ctx context.Context, op *pinner.PinningOperation) error {
	ctx, span := s.tracer.Start(ctx, "doPinning")
	defer span.End()

	for _, pi := range op.Peers {
		if err := s.Node.Host.Connect(ctx, pi); err != nil {
			log.Warnf("failed to connect to origin node for pinning operation: %s", err)
		}
	}

	bserv := blockservice.New(s.Node.Blockstore, s.Node.Bitswap)
	dserv := merkledag.NewDAGService(bserv)

	dsess := merkledag.NewSession(ctx, dserv)

	if err := s.CM.addDatabaseTrackingToContent(ctx, op.ContId, dsess, s.Node.Blockstore, op.Obj); err != nil {
		return err
	}

	s.CM.ToCheck <- op.ContId

	if op.Replace > 0 {
		if err := s.CM.RemoveContent(ctx, op.Replace, true); err != nil {
			log.Infof("failed to remove content in replacement: %d", op.Replace)
		}
	}

	// this provide call goes out immediately
	if err := s.Node.FullRT.Provide(ctx, op.Obj, true); err != nil {
		log.Infof("provider broadcast failed: %s", err)
	}

	// this one adds to a queue
	if err := s.Node.Provider.Provide(op.Obj); err != nil {
		log.Infof("providing failed: %s", err)
	}

	return nil
}

func (cm *ContentManager) refreshPinQueue() error {
	var toPin []Content
	if err := cm.DB.Find(&toPin, "active = false and pinning = true").Error; err != nil {
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
		cm.addPinToQueue(c, nil, 0)
	}

	return nil
}

func (cm *ContentManager) pinContent(ctx context.Context, user uint, obj cid.Cid, name string, cols []*Collection, peers []peer.AddrInfo, replace uint, meta map[string]interface{}) (*types.IpfsPinStatus, error) {
	loc, err := cm.selectLocationForContent(ctx, obj, user)
	if err != nil {
		return nil, xerrors.Errorf("selecting location for content failed: %w", err)
	}

	var metab string
	if meta != nil {
		b, err := json.Marshal(meta)
		if err != nil {
			return nil, err
		}
		metab = string(b)
	}

	cont := Content{
		Cid: util.DbCID{obj},

		Name:        name,
		UserID:      user,
		Active:      false,
		Replication: defaultReplication,

		Pinning: true,
		PinMeta: metab,

		Location: loc,

		/*
			Size        int64  `json:"size"`
			Offloaded   bool   `json:"offloaded"`
		*/

	}
	if err := cm.DB.Create(&cont).Error; err != nil {
		return nil, err
	}

	if loc == "local" {
		cm.addPinToQueue(cont, peers, replace)
	} else {
		if err := cm.pinContentOnShuttle(ctx, cont, peers, replace, loc); err != nil {
			return nil, err
		}
	}

	return cm.pinStatus(cont.ID)
}

// TODO: the queue needs to be a lot smarter than throwing things into a channel...
func (cm *ContentManager) addPinToQueue(cont Content, peers []peer.AddrInfo, replace uint) {

	op := &pinner.PinningOperation{
		ContId:   cont.ID,
		UserId:   cont.UserID,
		Obj:      cont.Cid.CID,
		Name:     cont.Name,
		Peers:    peers,
		Started:  cont.CreatedAt,
		Status:   "queued",
		Replace:  replace,
		Location: "local",
	}

	cm.pinLk.Lock()
	// TODO: check if we are overwriting anything here
	cm.pinJobs[cont.ID] = op
	cm.pinLk.Unlock()

	cm.pinMgr.Add(op)
}

func (cm *ContentManager) pinContentOnShuttle(ctx context.Context, cont Content, peers []peer.AddrInfo, replace uint, handle string) error {
	if err := cm.sendShuttleCommand(ctx, handle, &drpc.Command{
		Op: drpc.CMD_AddPin,
		Params: drpc.CmdParams{
			AddPin: &drpc.AddPin{
				DBID:   cont.ID,
				UserId: cont.UserID,
				Cid:    cont.Cid.CID,
				Peers:  peers,
			},
		},
	}); err != nil {
		return err
	}

	op := &pinner.PinningOperation{
		ContId:   cont.ID,
		UserId:   cont.UserID,
		Obj:      cont.Cid.CID,
		Name:     cont.Name,
		Peers:    peers,
		Started:  cont.CreatedAt,
		Status:   "queued",
		Replace:  replace,
		Location: handle,
	}

	cm.pinLk.Lock()
	// TODO: check if we are overwriting anything here
	cm.pinJobs[cont.ID] = op
	cm.pinLk.Unlock()

	return nil
}

func (cm *ContentManager) selectLocationForContent(ctx context.Context, obj cid.Cid, uid uint) (string, error) {
	ctx, span := cm.tracer.Start(ctx, "selectLocation")
	defer span.End()

	var user User
	if err := cm.DB.First(&user, "id = ?", uid).Error; err != nil {
		return "", err
	}

	if user.Flags&4 == 0 {
		return "local", nil
	}

	var activeShuttles []string
	cm.shuttlesLk.Lock()
	for d := range cm.shuttles {
		activeShuttles = append(activeShuttles, d)
	}
	cm.shuttlesLk.Unlock()

	var shuttles []Shuttle
	if err := cm.DB.Find(&shuttles, "handle in ? and open", activeShuttles).Error; err != nil {
		return "", err
	}

	if len(shuttles) == 0 {
		log.Warn("no shuttles available for content to be delegated to")
		return "local", nil
	}

	// TODO: take into account existing staging zones and their primary
	// locations while choosing

	n := rand.Intn(len(shuttles))
	return shuttles[n].Handle, nil
}

// pinning api /pins endpoint
func (s *Server) handleListPins(e echo.Context, u *User) error {
	_, span := s.tracer.Start(e.Request().Context(), "handleListPins")
	defer span.End()

	qcids := e.QueryParam("cid")
	qname := e.QueryParam("name")
	qstatus := e.QueryParam("status")
	qbefore := e.QueryParam("before")
	qafter := e.QueryParam("after")
	qlimit := e.QueryParam("limit")

	q := s.DB.Model(Content{}).Where("user_id = ?", u.ID).Order("created_at desc")

	if qcids != "" {
		var cids []util.DbCID
		for _, cstr := range strings.Split(qcids, ",") {
			c, err := cid.Decode(cstr)
			if err != nil {
				return err
			}
			cids = append(cids, util.DbCID{c})
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

	var out []*types.IpfsPinStatus
	for _, c := range contents {
		if lim > 0 && len(out) >= lim {
			break
		}

		st, err := s.CM.pinStatus(c.ID)
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
	ctx := e.Request().Context()

	var pin types.IpfsPin
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

	status, err := s.CM.pinContent(ctx, u.ID, obj, pin.Name, nil, addrInfos, 0, pin.Meta)
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

	st, err := s.CM.pinStatus(uint(id))
	if err != nil {
		return err
	}

	return e.JSON(200, st)
}

func (s *Server) handleReplacePin(e echo.Context, u *User) error {
	ctx := e.Request().Context()
	id, err := strconv.Atoi(e.Param("requestid"))
	if err != nil {
		return err
	}

	var pin types.IpfsPin
	if err := e.Bind(&pin); err != nil {
		return err
	}

	var content Content
	if err := s.DB.First(&content, "id = ?", id).Error; err != nil {
		return err
	}
	if content.UserID != u.ID {
		return &util.HttpError{
			Code:    401,
			Message: util.ERR_NOT_AUTHORIZED,
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

	status, err := s.CM.pinContent(ctx, u.ID, obj, pin.Name, nil, addrInfos, uint(id), pin.Meta)
	if err != nil {
		return err
	}

	return e.JSON(200, status)
}

func (s *Server) handleDeletePin(e echo.Context, u *User) error {
	// TODO: need to cancel any in-progress pinning operation
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
		return &util.HttpError{
			Code:    401,
			Message: util.ERR_NOT_AUTHORIZED,
		}
	}

	if err := s.CM.RemoveContent(ctx, uint(id), true); err != nil {
		return err
	}

	return nil
}

func (cm *ContentManager) UpdatePinStatus(handle string, cont uint, status string) {
	cm.pinLk.Lock()
	op, ok := cm.pinJobs[cont]
	cm.pinLk.Unlock()
	if !ok {
		log.Warnw("got pin status update for unknown content", "content", cont, "status", status, "shuttle", handle)
		return
	}

	op.SetStatus(status)
}

func (cm *ContentManager) handlePinningComplete(ctx context.Context, handle string, pincomp *drpc.PinComplete) error {
	ctx, span := cm.tracer.Start(ctx, "handlePinningComplete")
	defer span.End()

	var cont Content
	if err := cm.DB.First(&cont, "id = ?", pincomp.DBID).Error; err != nil {
		return xerrors.Errorf("got shuttle pin complete for unknown content %d (shuttle = %s): %w", pincomp.DBID, handle, err)
	}

	if cont.Active {
		// content already active, no need to add objects, just update location
		if err := cm.DB.Model(Content{}).Where("id = ?", cont.ID).UpdateColumns(map[string]interface{}{
			"location": handle,
		}).Error; err != nil {
			return err
		}

		// TODO: should we recheck the staging zones?
		return nil
	}

	objects := make([]*Object, 0, len(pincomp.Objects))
	for _, o := range pincomp.Objects {
		objects = append(objects, &Object{
			Cid:  util.DbCID{o.Cid},
			Size: o.Size,
		})
	}

	if err := cm.addObjectsToDatabase(ctx, pincomp.DBID, objects); err != nil {
		return xerrors.Errorf("failed to add objects to database: %w", err)
	}

	return nil
}
