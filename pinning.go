package main

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	drpc "github.com/application-research/estuary/drpc"
	"github.com/application-research/estuary/pinner"
	"github.com/application-research/estuary/types"
	"github.com/application-research/estuary/util"
	"github.com/ipfs/go-blockservice"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-merkledag"
	"github.com/labstack/echo/v4"
	"github.com/libp2p/go-libp2p-core/peer"
	"go.opentelemetry.io/otel/attribute"
	trace "go.opentelemetry.io/otel/trace"
	"golang.org/x/xerrors"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

func (cm *ContentManager) pinStatus(cont Content) (*types.IpfsPinStatus, error) {
	cm.pinLk.Lock()
	po, ok := cm.pinJobs[cont.ID]
	cm.pinLk.Unlock()
	if !ok {
		var meta map[string]interface{}
		if cont.PinMeta != "" {
			if err := json.Unmarshal([]byte(cont.PinMeta), &meta); err != nil {
				log.Warnf("content %d has invalid pinmeta: %s", cont, err)
			}
		}

		ps := &types.IpfsPinStatus{
			Requestid: fmt.Sprintf("%d", cont.ID),
			Status:    "pinning",
			Created:   cont.CreatedAt,
			Pin: types.IpfsPin{
				Cid:  cont.Cid.CID.String(),
				Name: cont.Name,
				Meta: meta,
			},
			Delegates: cm.pinDelegatesForContent(cont),
			Info:      nil, // TODO: all sorts of extra info we could add...
		}

		if cont.Active {
			ps.Status = "pinned"
		}
		if cont.Failed {
			ps.Status = "failed"
		}

		return ps, nil
	}

	status := po.PinStatus()
	status.Delegates = cm.pinDelegatesForContent(cont)

	return status, nil
}

func (cm *ContentManager) pinDelegatesForContent(cont Content) []string {
	if cont.Location == "local" {
		var out []string
		for _, a := range cm.Host.Addrs() {
			out = append(out, fmt.Sprintf("%s/p2p/%s", a, cm.Host.ID()))
		}

		return out
	} else {
		ai, err := cm.addrInfoForShuttle(cont.Location)
		if err != nil {
			log.Errorf("failed to get address info for shuttle %q: %s", cont.Location, err)
			return nil
		}

		if ai == nil {
			log.Warnf("no address info for shuttle %s: %s", cont.Location, err)
			return nil
		}

		var out []string
		for _, a := range ai.Addrs {
			out = append(out, fmt.Sprintf("%s/p2p/%s", a, ai.ID))
		}
		return out
	}
}

func (s *Server) doPinning(ctx context.Context, op *pinner.PinningOperation, cb pinner.PinProgressCB) error {
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

	if err := s.CM.addDatabaseTrackingToContent(ctx, op.ContId, dsess, s.Node.Blockstore, op.Obj, cb); err != nil {
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
	if err := cm.DB.Find(&toPin, "active = false and pinning = true and not aggregate").Error; err != nil {
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
		if c.Location == "local" {
			cm.addPinToQueue(c, nil, 0)
		} else {
			if err := cm.pinContentOnShuttle(context.TODO(), c, nil, 0, c.Location); err != nil {
				log.Errorf("failed to send pin message to shuttle: %s", err)
				time.Sleep(time.Millisecond * 100)
			}
		}
	}

	return nil
}

func (cm *ContentManager) pinContent(ctx context.Context, user uint, obj cid.Cid, name string, cols []*CollectionRef, peers []peer.AddrInfo, replace uint, meta map[string]interface{}) (*types.IpfsPinStatus, error) {
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

	if len(cols) > 0 {
		for _, c := range cols {
			c.Content = cont.ID
		}

		if err := cm.DB.Clauses(clause.OnConflict{
			Columns:   []clause.Column{{Name: "path"}, {Name: "collection"}},
			DoUpdates: clause.AssignmentColumns([]string{"created_at", "content"}),
		}).Create(cols).Error; err != nil {
			return nil, err
		}
	}

	if loc == "local" {
		cm.addPinToQueue(cont, peers, replace)
	} else {
		if err := cm.pinContentOnShuttle(ctx, cont, peers, replace, loc); err != nil {
			return nil, err
		}
	}

	return cm.pinStatus(cont)
}

func (cm *ContentManager) addPinToQueue(cont Content, peers []peer.AddrInfo, replace uint) {
	if cont.Location != "local" {
		log.Errorf("calling addPinToQueue on non-local content")
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
		Location: cont.Location,
	}

	cm.pinLk.Lock()
	// TODO: check if we are overwriting anything here
	cm.pinJobs[cont.ID] = op
	cm.pinLk.Unlock()

	cm.pinMgr.Add(op)
}

func (cm *ContentManager) pinContentOnShuttle(ctx context.Context, cont Content, peers []peer.AddrInfo, replace uint, handle string) error {
	ctx, span := cm.tracer.Start(ctx, "pinContentOnShuttle", trace.WithAttributes(
		attribute.String("handle", handle),
		attribute.String("CID", cont.Cid.CID.String()),
	))
	defer span.End()

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

	allShuttlesLowSpace := true
	lowSpace := make(map[string]bool)
	var activeShuttles []string
	cm.shuttlesLk.Lock()
	for d, sh := range cm.shuttles {
		if !sh.private {
			lowSpace[d] = sh.spaceLow
			activeShuttles = append(activeShuttles, d)
		} else {
			allShuttlesLowSpace = false
		}
	}
	cm.shuttlesLk.Unlock()

	var shuttles []Shuttle
	if err := cm.DB.Order("priority desc").Find(&shuttles, "handle in ? and open", activeShuttles).Error; err != nil {
		return "", err
	}

	// prefer shuttles that are not low on blockstore space
	sort.SliceStable(shuttles, func(i, j int) bool {
		lsI := lowSpace[shuttles[i].Handle]
		lsJ := lowSpace[shuttles[j].Handle]

		if lsI == lsJ {
			return false
		}

		return lsJ
	})

	if len(shuttles) == 0 {
		//log.Info("no shuttles available for content to be delegated to")
		if cm.localContentAddingDisabled {
			return "", fmt.Errorf("no shuttles available and local content adding disabled")
		}

		return "local", nil
	}

	// TODO: take into account existing staging zones and their primary
	// locations while choosing
	ploc, err := cm.primaryStagingLocation(ctx, uid)
	if err != nil {
		return "", err
	}

	if ploc != "" {
		if allShuttlesLowSpace || !lowSpace[ploc] {
			for _, sh := range shuttles {
				if sh.Handle == ploc {
					return ploc, nil
				}
			}
		}

		// TODO: maybe we should just assign the pin to the preferred shuttle
		// anyways, this could be the case where theres a small amount of
		// downtime from rebooting or something
		log.Warnf("preferred shuttle %q not online", ploc)
	}

	// since they are ordered by priority, just take the first
	return shuttles[0].Handle, nil
}

func (cm *ContentManager) selectLocationForRetrieval(ctx context.Context, cont Content) (string, error) {
	ctx, span := cm.tracer.Start(ctx, "selectLocationForRetrieval")
	defer span.End()

	var activeShuttles []string
	cm.shuttlesLk.Lock()
	for d, sh := range cm.shuttles {
		if !sh.private {
			activeShuttles = append(activeShuttles, d)
		}
	}
	cm.shuttlesLk.Unlock()

	var shuttles []Shuttle
	if err := cm.DB.Order("priority desc").Find(&shuttles, "handle in ? and open", activeShuttles).Error; err != nil {
		return "", err
	}

	if len(shuttles) == 0 {
		if cm.localContentAddingDisabled {
			return "", fmt.Errorf("no shuttles available and local content adding disabled")
		}

		return "local", nil
	}

	// prefer the shuttle the content is already on
	for _, sh := range shuttles {
		if sh.Handle == cont.Location {
			return sh.Handle, nil
		}
	}

	// since they are ordered by priority, just take the first
	return shuttles[0].Handle, nil
}

func (cm *ContentManager) primaryStagingLocation(ctx context.Context, uid uint) (string, error) {
	cm.bucketLk.Lock()
	defer cm.bucketLk.Unlock()
	zones, ok := cm.buckets[uid]
	if !ok {
		return "", nil
	}

	// TODO: maybe we could make this more complex, but for now, if we have a
	// staging zone opened in a particular location, just keep using that one
	for _, z := range zones {
		return z.Location, nil
	}

	log.Warnf("empty staging zone set for user %d", uid)
	return "", nil
}

// handleListPins godoc
// @Summary      List all pinned objects
// @Description  List all pinned objects
// @Tags         pinning
// @Produce      json
// @Router       /pinning/pins [get]
func (s *Server) handleListPins(e echo.Context, u *User) error {
	_, span := s.tracer.Start(e.Request().Context(), "handleListPins")
	defer span.End()

	qcids := e.QueryParam("cid")
	qname := e.QueryParam("name")
	qstatus := e.QueryParam("status")
	qbefore := e.QueryParam("before")
	qafter := e.QueryParam("after")
	qlimit := e.QueryParam("limit")
	qreqids := e.QueryParam("requestid")

	q := s.DB.Model(Content{}).Where("user_id = ? and not aggregate", u.ID).Order("created_at desc")

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

	if qreqids != "" {
		var ids []int
		for _, rs := range strings.Split(qreqids, ",") {
			id, err := strconv.Atoi(rs)
			if err != nil {
				return err
			}

			ids = append(ids, id)
		}

		q = q.Where("id in ?", ids)
	}

	var lim int
	if qlimit != "" {
		limit, err := strconv.Atoi(qlimit)
		if err != nil {
			return err
		}
		lim = limit
	}

	if lim == 0 {
		lim = 500
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

	// certain sets of statuses we can use the database to filter for
	oq, dblimit, err := filterForStatusQuery(q, allowed)
	if err != nil {
		return err
	}
	q = oq

	if dblimit {
		q = q.Limit(lim)
	}

	var contents []Content
	if err := q.Scan(&contents).Error; err != nil {
		return err
	}

	var out []*types.IpfsPinStatus
	for _, c := range contents {
		if lim > 0 && len(out) >= lim {
			break
		}

		st, err := s.CM.pinStatus(c)
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

func filterForStatusQuery(q *gorm.DB, statuses map[string]bool) (*gorm.DB, bool, error) {
	if len(statuses) == 0 || len(statuses) == 4 {
		// if not filtering by status, we return *all* pins, in that case we can use the query to limit results
		return q, true, nil
	}

	pinned := statuses["pinned"]
	failed := statuses["failed"]
	pinning := statuses["pinning"]
	queued := statuses["queued"]

	if len(statuses) == 1 {
		switch {
		case pinned:
			return q.Where("active"), true, nil
		case failed:
			return q.Where("failed"), true, nil
		default:
			return q, false, nil
		}
	}

	if len(statuses) == 2 {
		if pinned && failed {
			return q.Where("active or failed"), true, nil
		}

		if pinning && queued {
			return q.Where("not active and not failed"), true, nil
		}
		// fallthrough to the rest of the logic
	}

	var canUseDBLimit bool = true
	// If the query is trying to distinguish between pinning and queued, we cannot do that solely via a database query
	if (statuses["queued"] && !statuses["pinning"]) || (statuses["pinning"] && !statuses["queued"]) {
		canUseDBLimit = false
	}

	if !statuses["failed"] {
		q = q.Where("not failed")
	}

	if !statuses["pinned"] {
		q = q.Where("not active")
	}

	return q, canUseDBLimit, nil
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

// handleAddPin  godoc
// @Summary      Add and pin object
// @Description  Add and pin an object
// @Tags         pinning
// @Produce      json
// @in           200,400,default  string  Token "token"
// @Param        cid    path      string  true  "cid"
// @Param        name   path      string  true  "name"
// @Router       /pinning/pins [post]
func (s *Server) handleAddPin(e echo.Context, u *User) error {
	ctx := e.Request().Context()

	if s.CM.contentAddingDisabled || u.StorageDisabled {
		return &util.HttpError{
			Code:    400,
			Message: util.ERR_CONTENT_ADDING_DISABLED,
		}
	}

	var pin types.IpfsPin
	if err := e.Bind(&pin); err != nil {
		return err
	}

	var cols []*CollectionRef
	if c, ok := pin.Meta["collection"].(string); ok && c != "" {
		var srchCol Collection
		if err := s.DB.First(&srchCol, "uuid = ? and user_id = ?", c, u.ID).Error; err != nil {
			return err
		}

		var colpath *string
		colp, ok := pin.Meta["collectionPath"].(string)
		if ok {
			p, err := sanitizePath(colp)
			if err != nil {
				return err
			}

			colpath = &p
		}

		cols = []*CollectionRef{&CollectionRef{
			Collection: srchCol.ID,
			Path:       colpath,
		}}
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

	status, err := s.CM.pinContent(ctx, u.ID, obj, pin.Name, cols, addrInfos, 0, pin.Meta)
	if err != nil {
		return err
	}

	return e.JSON(202, status)
}

// handleGetPin  godoc
// @Summary      Get a pinned objects
// @Description  Get a pinned objects
// @Tags         pinning
// @Produce      json
// @Param        id   path      string  true  "cid"
// @Router       /pinning/pins/:id [get]
func (s *Server) handleGetPin(e echo.Context, u *User) error {
	id, err := strconv.Atoi(e.Param("requestid"))
	if err != nil {
		return err
	}

	var content Content
	if err := s.DB.First(&content, "id = ?", uint(id)).Error; err != nil {
		return err
	}

	st, err := s.CM.pinStatus(content)
	if err != nil {
		return err
	}

	return e.JSON(200, st)
}

// handleReplacePin godoc
// @Summary      Replace a pinned object
// @Description  Replace a pinned object
// @Tags         pinning
// @Produce      json
// @Param        id   path      string  true  "id"
// @Router       /pinning/pins/:id [post]
func (s *Server) handleReplacePin(e echo.Context, u *User) error {
	if s.CM.contentAddingDisabled || u.StorageDisabled {
		return &util.HttpError{
			Code:    400,
			Message: util.ERR_CONTENT_ADDING_DISABLED,
		}
	}

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

// handleDeletePin godoc
// @Summary      Delete a pinned object
// @Description  Delete a pinned object
// @Tags         pinning
// @Produce      json
// @Param        id   path      string  true  "id"
// @Router       /pinning/pins/:id [delete]
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

	// TODO: what if we delete a pin that was in progress?
	if err := s.CM.unpinContent(ctx, uint(id)); err != nil {
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
	if status == "failed" {
		var c Content
		if err := cm.DB.First(&c, "id = ?", cont).Error; err != nil {
			log.Errorf("failed to look up content: %s", err)
			return
		}

		if c.Active {
			log.Errorf("got failed pin status message from shuttle %s where content(%d) was already active, refusing to do anything", handle, cont)
			return
		}

		if err := cm.DB.Model(Content{}).Where("id = ?", cont).UpdateColumns(map[string]interface{}{
			"active":  false,
			"pinning": false,
			"failed":  true,
		}).Error; err != nil {
			log.Errorf("failed to mark content as failed in database: %s", err)
		}
	}
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

	if cont.Aggregate {
		if err := cm.DB.Model(Content{}).Where("id = ?", cont.ID).UpdateColumns(map[string]interface{}{
			"active":   true,
			"pinning":  false,
			"location": handle,
		}).Error; err != nil {
			return xerrors.Errorf("failed to update content in database: %w", err)
		}
		return nil
	}

	objects := make([]*Object, 0, len(pincomp.Objects))
	for _, o := range pincomp.Objects {
		objects = append(objects, &Object{
			Cid:  util.DbCID{o.Cid},
			Size: o.Size,
		})
	}

	if err := cm.addObjectsToDatabase(ctx, pincomp.DBID, objects, handle); err != nil {
		return xerrors.Errorf("failed to add objects to database: %w", err)
	}

	cm.ToCheck <- cont.ID

	return nil
}
