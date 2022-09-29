package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/application-research/estuary/constants"
	drpc "github.com/application-research/estuary/drpc"
	"github.com/application-research/estuary/pinner"
	"github.com/application-research/estuary/pinner/types"
	"github.com/application-research/estuary/util"
	"github.com/ipfs/go-blockservice"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-merkledag"
	"github.com/labstack/echo/v4"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/pkg/errors"
	"go.opentelemetry.io/otel/attribute"
	trace "go.opentelemetry.io/otel/trace"
	"golang.org/x/xerrors"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

const (
	DEFAULT_IPFS_PIN_LIMIT = 10 // https://github.com/ipfs/pinning-services-api-spec/blob/main/ipfs-pinning-service.yaml#L610
	IPFS_PIN_LIMIT_MIN     = 1
	IPFS_PIN_LIMIT_MAX     = 1000
)

func (cm *ContentManager) pinStatus(cont util.Content, origins []*peer.AddrInfo) (*types.IpfsPinStatusResponse, error) {
	delegates := cm.pinDelegatesForContent(cont)

	cm.pinLk.Lock()
	po, ok := cm.pinJobs[cont.ID]
	cm.pinLk.Unlock()
	if !ok {
		meta := make(map[string]interface{}, 0)
		if cont.PinMeta != "" {
			if err := json.Unmarshal([]byte(cont.PinMeta), &meta); err != nil {
				log.Warnf("content %d has invalid pinmeta: %s", cont, err)
			}
		}

		originStrs := make([]string, 0)
		for _, o := range origins {
			ai, err := peer.AddrInfoToP2pAddrs(o)
			if err == nil {
				for _, a := range ai {
					originStrs = append(originStrs, a.String())
				}
			}
		}

		ps := &types.IpfsPinStatusResponse{
			RequestID: fmt.Sprintf("%d", cont.ID),
			Status:    types.PinningStatusPinning,
			Created:   cont.CreatedAt,
			Pin: types.IpfsPin{
				CID:     cont.Cid.CID.String(),
				Name:    cont.Name,
				Meta:    meta,
				Origins: originStrs,
			},
			Delegates: delegates,
			Info:      make(map[string]interface{}, 0), // TODO: all sorts of extra info we could add...
		}

		if cont.Active {
			ps.Status = types.PinningStatusPinned
		}

		if cont.Failed {
			ps.Status = types.PinningStatusFailed
		}
		return ps, nil
	}

	status := po.PinStatus()
	status.Delegates = delegates
	return status, nil
}

func (cm *ContentManager) pinDelegatesForContent(cont util.Content) []string {
	if cont.Location == constants.ContentLocationLocal {
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
			log.Warnf("no address info for shuttle: %s", cont.Location)
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

	// remove replacement async - move this out
	if op.Replace > 0 {
		go func() {
			if err := s.CM.removeContent(ctx, op.Replace, true); err != nil {
				log.Infof("failed to remove content in replacement: %d with: %d", op.Replace, op.ContId)
			}
		}()
	}

	for _, pi := range op.Peers {
		if err := s.Node.Host.Connect(ctx, *pi); err != nil {
			log.Warnf("failed to connect to origin node for pinning operation: %s", err)
		}
	}

	bserv := blockservice.New(s.Node.Blockstore, s.Node.Bitswap)
	dserv := merkledag.NewDAGService(bserv)
	dsess := dserv.Session(ctx)

	if err := s.CM.addDatabaseTrackingToContent(ctx, op.ContId, dsess, op.Obj, cb); err != nil {
		return err
	}

	if op.MakeDeal {
		s.CM.toCheck(op.ContId)
	}

	// this provide call goes out immediately
	if err := s.Node.FullRT.Provide(ctx, op.Obj, true); err != nil {
		log.Warnf("provider broadcast failed: %s", err)
	}

	// this one adds to a queue
	if err := s.Node.Provider.Provide(op.Obj); err != nil {
		log.Warnf("providing failed: %s", err)
	}
	return nil
}

func (s *Server) PinStatusFunc(contID uint, location string, status types.PinningStatus) error {
	return s.CM.UpdatePinStatus(location, contID, status)
}

func (cm *ContentManager) refreshPinQueue(ctx context.Context, contentLoc string) error {
	log.Infof("trying to refresh pin queue for %s contents", contentLoc)

	var contents []util.Content
	if err := cm.DB.Find(&contents, "pinning and not active and not failed and not aggregate and location=?", contentLoc).Error; err != nil {
		return err
	}

	makeDeal := true
	for _, c := range contents {
		select {
		case <-ctx.Done():
			log.Debugf("refresh pin queue canceled for %s contents", contentLoc)
			return nil
		default:
			var origins []*peer.AddrInfo
			// when refreshing pinning queue, use content origins if available
			if c.Origins != "" {
				_ = json.Unmarshal([]byte(c.Origins), &origins) // no need to handle or log err, its just a nice to have
			}

			if c.Location == constants.ContentLocationLocal {
				cm.addPinToQueue(c, origins, 0, makeDeal)
			} else {
				if err := cm.pinContentOnShuttle(ctx, c, origins, 0, c.Location, makeDeal); err != nil {
					log.Errorf("failed to send pin message to shuttle: %s", err)
					time.Sleep(time.Millisecond * 100)
				}
			}
		}
	}
	return nil
}

func (cm *ContentManager) pinContent(ctx context.Context, user uint, obj cid.Cid, filename string, cols []*CollectionRef, origins []*peer.AddrInfo, replaceID uint, meta map[string]interface{}, makeDeal bool) (*types.IpfsPinStatusResponse, error) {
	loc, err := cm.selectLocationForContent(ctx, obj, user)
	if err != nil {
		return nil, xerrors.Errorf("selecting location for content failed: %w", err)
	}

	if replaceID > 0 {
		// mark as replace since it will removed and so it should not be fetched anymore
		if err := cm.DB.Model(&util.Content{}).Where("id = ?", replaceID).Update("replace", true).Error; err != nil {
			return nil, err
		}
	}

	var metaStr string
	if meta != nil {
		b, err := json.Marshal(meta)
		if err != nil {
			return nil, err
		}
		metaStr = string(b)
	}

	var originsStr string
	if origins != nil {
		b, err := json.Marshal(origins)
		if err != nil {
			return nil, err
		}
		originsStr = string(b)
	}

	cont := util.Content{
		Cid:         util.DbCID{CID: obj},
		Name:        filename,
		UserID:      user,
		Active:      false,
		Replication: cm.Replication,
		Pinning:     true,
		PinMeta:     metaStr,
		Location:    loc,
		Origins:     originsStr,
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

	if loc == constants.ContentLocationLocal {
		cm.addPinToQueue(cont, origins, replaceID, makeDeal)
	} else {
		if err := cm.pinContentOnShuttle(ctx, cont, origins, replaceID, loc, makeDeal); err != nil {
			return nil, err
		}
	}
	return cm.pinStatus(cont, origins)
}

func (cm *ContentManager) addPinToQueue(cont util.Content, peers []*peer.AddrInfo, replaceID uint, makeDeal bool) {
	if cont.Location != constants.ContentLocationLocal {
		log.Errorf("calling addPinToQueue on non-local content")
	}

	op := &pinner.PinningOperation{
		ContId:   cont.ID,
		UserId:   cont.UserID,
		Obj:      cont.Cid.CID,
		Name:     cont.Name,
		Peers:    peers,
		Started:  cont.CreatedAt,
		Status:   types.PinningStatusQueued,
		Replace:  replaceID,
		Location: cont.Location,
		MakeDeal: makeDeal,
		Meta:     cont.PinMeta,
	}

	cm.pinLk.Lock()
	// TODO: check if we are overwriting anything here
	cm.pinJobs[cont.ID] = op
	cm.pinLk.Unlock()

	cm.pinMgr.Add(op)
}

func (cm *ContentManager) pinContentOnShuttle(ctx context.Context, cont util.Content, peers []*peer.AddrInfo, replaceID uint, handle string, makeDeal bool) error {
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
		Status:   types.PinningStatusQueued,
		Replace:  replaceID,
		Location: handle,
		MakeDeal: makeDeal,
		Meta:     cont.PinMeta,
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
		if cm.localContentAddingDisabled {
			return "", fmt.Errorf("no shuttles available and local content adding disabled")
		}
		return constants.ContentLocationLocal, nil
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

func (cm *ContentManager) selectLocationForRetrieval(ctx context.Context, cont util.Content) (string, error) {
	_, span := cm.tracer.Start(ctx, "selectLocationForRetrieval")
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
		return constants.ContentLocationLocal, nil
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
// @Summary      List all pin status objects
// @Description  This endpoint lists all pin status objects
// @Tags         pinning
// @Produce      json
// @Failure      400  {object}  util.HttpError
// @Failure      404  {object}  util.HttpError
// @Failure      500  {object}  util.HttpError
// @Router       /pinning/pins [get]
func (s *Server) handleListPins(e echo.Context, u *User) error {
	_, span := s.tracer.Start(e.Request().Context(), "handleListPins")
	defer span.End()

	qcids := e.QueryParam("cid")
	qname := e.QueryParam("name")
	qmatch := e.QueryParam("match")
	qstatus := e.QueryParam("status")
	qbefore := e.QueryParam("before")
	qafter := e.QueryParam("after")
	qlimit := e.QueryParam("limit")
	qreqids := e.QueryParam("requestid")

	lim := DEFAULT_IPFS_PIN_LIMIT
	if qlimit != "" {
		limit, err := strconv.Atoi(qlimit)
		if err != nil {
			return err
		}
		lim = limit

		if lim > IPFS_PIN_LIMIT_MAX || lim < IPFS_PIN_LIMIT_MIN {
			return &util.HttpError{
				Code:    http.StatusBadRequest,
				Reason:  util.ERR_INVALID_QUERY_PARAM_VALUE,
				Details: fmt.Sprintf("specify a valid LIMIT value between %d and %d", IPFS_PIN_LIMIT_MIN, IPFS_PIN_LIMIT_MAX),
			}
		}
	}

	q := s.DB.Model(util.Content{}).Where("user_id = ? AND not aggregate AND not replace", u.ID).Order("created_at desc")

	if qcids != "" {
		var cids []util.DbCID
		for _, cstr := range strings.Split(qcids, ",") {
			c, err := cid.Decode(cstr)
			if err != nil {
				return err
			}
			cids = append(cids, util.DbCID{CID: c})
		}
		q = q.Where("cid in ?", cids)
	}

	if qname != "" {
		switch strings.ToLower(qmatch) {
		case "ipartial":
			q = q.Where("lower(name) like ?", fmt.Sprintf("%%%s%%", strings.ToLower(qname)))
		case "partial":
			q = q.Where("name like ?", fmt.Sprintf("%%%s%%", qname))
		case "iexact":
			q = q.Where("lower(name) = ?", strings.ToLower(qname))
		default: //exact
			q = q.Where("name = ?", qname)
		}
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

	pinStatuses := make(map[types.PinningStatus]bool)
	if qstatus != "" {
		statuses := strings.Split(qstatus, ",")
		for _, s := range statuses {
			ps := types.PinningStatus(s)
			switch ps {
			case types.PinningStatusQueued, types.PinningStatusPinning, types.PinningStatusPinned, types.PinningStatusFailed:
				pinStatuses[ps] = true
			default:
				return &util.HttpError{
					Code:    http.StatusBadRequest,
					Reason:  util.ERR_INVALID_PINNING_STATUS,
					Details: fmt.Sprintf("unrecognized pin status in query: %q", s),
				}
			}
		}
	}

	q, err := filterForStatusQuery(q, pinStatuses)
	if err != nil {
		return err
	}

	var count int64
	if err := q.Count(&count).Error; err != nil {
		return err
	}

	q.Limit(lim)

	var contents []util.Content
	if err := q.Scan(&contents).Error; err != nil {
		return err
	}

	out := make([]*types.IpfsPinStatusResponse, 0)
	for _, c := range contents {
		st, err := s.CM.pinStatus(c, nil)
		if err != nil {
			return err
		}
		out = append(out, st)
	}

	return e.JSON(http.StatusOK, types.IpfsListPinStatusResponse{
		Count:   int(count),
		Results: out,
	})
}

func filterForStatusQuery(q *gorm.DB, statuses map[types.PinningStatus]bool) (*gorm.DB, error) {
	// TODO maybe we should move all these statuses to a status column in contents
	if len(statuses) == 0 || len(statuses) == 4 {
		return q, nil // if no status filter or all statuses are specified, return all pins
	}

	pinned := statuses[types.PinningStatusPinned]
	failed := statuses[types.PinningStatusFailed]
	pinning := statuses[types.PinningStatusPinning]
	queued := statuses[types.PinningStatusQueued]

	if len(statuses) == 1 {
		switch {
		case pinned:
			return q.Where("active and not failed and not pinning"), nil
		case failed:
			return q.Where("failed and not active and not pinning"), nil
		case pinning:
			return q.Where("pinning and not active and not failed"), nil
		default:
			return q.Where("not active and not pinning and not failed"), nil
		}
	}

	if len(statuses) == 2 {
		if pinned && failed {
			return q.Where("(active or failed) and not pinning"), nil
		}

		if pinned && queued {
			return q.Where("active and not failed and not pinning"), nil
		}

		if pinned && pinning {
			return q.Where("(active or pinning) and not failed"), nil
		}

		if pinning && failed {
			return q.Where("(pinning or failed) and not active"), nil
		}

		if pinning && queued {
			return q.Where("pinning and not active and not failed"), nil
		}

		if failed && queued {
			return q.Where("failed and not active and not pinning"), nil
		}
	}

	if !statuses[types.PinningStatusFailed] {
		return q.Where("not failed and (active or pinning)"), nil
	}

	if !statuses[types.PinningStatusPinned] {
		return q.Where("not active and (failed or pinning"), nil
	}

	if !statuses[types.PinningStatusPinning] {
		return q.Where("not pinning and (active or failed"), nil
	}
	return q.Where("active or pinning or failed"), nil
}

// handleAddPin  godoc
// @Summary      Add and pin object
// @Description  This endpoint adds a pin to the IPFS daemon.
// @Tags         pinning
// @Produce      json
// @in           200,400,default  string  Token "token"
// @Param        cid   path  string  true  "cid"
// @Param        name  path  string  true  "name"
// @Router       /pinning/pins [post]
func (s *Server) handleAddPin(e echo.Context, u *User) error {
	ctx := e.Request().Context()

	if err := util.ErrorIfContentAddingDisabled(s.isContentAddingDisabled(u)); err != nil {
		return err
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
		colp, ok := pin.Meta["colpath"].(string)
		if ok {
			p, err := sanitizePath(colp)
			if err != nil {
				return err
			}

			colpath = &p
		}

		cols = []*CollectionRef{
			{
				Collection: srchCol.ID,
				Path:       colpath,
			},
		}
	}

	var origins []*peer.AddrInfo
	for _, p := range pin.Origins {
		ai, err := peer.AddrInfoFromString(p)
		if err != nil {
			return err
		}
		origins = append(origins, ai)
	}

	obj, err := cid.Decode(pin.CID)
	if err != nil {
		return err
	}

	makeDeal := true
	// TODO pinning should be async
	status, err := s.CM.pinContent(ctx, u.ID, obj, pin.Name, cols, origins, 0, pin.Meta, makeDeal)
	if err != nil {
		return err
	}
	return e.JSON(http.StatusAccepted, status)
}

// handleGetPin  godoc
// @Summary      Get a pin status object
// @Description  This endpoint returns a pin status object.
// @Tags         pinning
// @Produce      json
// @Param        pinid  path  string  true  "cid"
// @Router       /pinning/pins/{pinid} [get]
func (s *Server) handleGetPin(e echo.Context, u *User) error {
	pinID, err := strconv.Atoi(e.Param("pinid"))
	if err != nil {
		return err
	}

	var content util.Content
	if err := s.DB.First(&content, "id = ? AND not replace", pinID).Error; err != nil {
		if xerrors.Is(err, gorm.ErrRecordNotFound) {
			return &util.HttpError{
				Code:    http.StatusNotFound,
				Reason:  util.ERR_CONTENT_NOT_FOUND,
				Details: fmt.Sprintf("content with ID(%d) was not found", pinID),
			}
		}
		return err
	}

	if err := util.IsContentOwner(u.ID, content.UserID); err != nil {
		return err
	}

	st, err := s.CM.pinStatus(content, nil)
	if err != nil {
		return err
	}
	return e.JSON(http.StatusOK, st)
}

// handleReplacePin godoc
// @Summary      Replace a pinned object
// @Description  This endpoint replaces a pinned object.
// @Tags         pinning
// @Produce      json
// @Param        pinid  path  string  true  "Pin ID"
// @Router       /pinning/pins/{pinid} [post]
func (s *Server) handleReplacePin(e echo.Context, u *User) error {

	if err := util.ErrorIfContentAddingDisabled(s.isContentAddingDisabled(u)); err != nil {
		return err
	}

	pinID, err := strconv.Atoi(e.Param("pinid"))
	if err != nil {
		return err
	}

	var pin types.IpfsPin
	if err := e.Bind(&pin); err != nil {
		return err
	}

	var content util.Content
	if err := s.DB.First(&content, "id = ? AND not replace", pinID).Error; err != nil {
		if xerrors.Is(err, gorm.ErrRecordNotFound) {
			return &util.HttpError{
				Code:    http.StatusNotFound,
				Reason:  util.ERR_CONTENT_NOT_FOUND,
				Details: fmt.Sprintf("content with ID(%d) was not found", pinID),
			}
		}
		return err
	}

	if err := util.IsContentOwner(u.ID, content.UserID); err != nil {
		return err
	}

	var origins []*peer.AddrInfo
	for _, p := range pin.Origins {
		ai, err := peer.AddrInfoFromString(p)
		if err != nil {
			return err
		}
		origins = append(origins, ai)
	}

	pinCID, err := cid.Decode(pin.CID)
	if err != nil {
		return err
	}

	makeDeal := true
	status, err := s.CM.pinContent(e.Request().Context(), u.ID, pinCID, pin.Name, nil, origins, uint(pinID), pin.Meta, makeDeal)
	if err != nil {
		return err
	}
	return e.JSON(http.StatusAccepted, status)
}

// handleDeletePin godoc
// @Summary      Delete a pinned object
// @Description  This endpoint deletes a pinned object.
// @Tags         pinning
// @Produce      json
// @Param        pinid  path  string  true  "Pin ID"
// @Router       /pinning/pins/{pinid} [delete]
func (s *Server) handleDeletePin(e echo.Context, u *User) error {
	pinID, err := strconv.Atoi(e.Param("pinid"))
	if err != nil {
		return err
	}

	var content util.Content
	if err := s.DB.First(&content, "id = ? AND not replace", pinID).Error; err != nil {
		if xerrors.Is(err, gorm.ErrRecordNotFound) {
			return &util.HttpError{
				Code:    http.StatusNotFound,
				Reason:  util.ERR_CONTENT_NOT_FOUND,
				Details: fmt.Sprintf("content with ID(%d) was not found", pinID),
			}
		}
		return err
	}

	if err := util.IsContentOwner(u.ID, content.UserID); err != nil {
		return err
	}

	// mark as replace since it will removed and so it should not be fetched anymore
	if err := s.DB.Model(&util.Content{}).Where("id = ?", pinID).Update("replace", true).Error; err != nil {
		return err
	}

	// unpin async
	go func() {
		if err := s.CM.unpinContent(e.Request().Context(), uint(pinID)); err != nil {
			log.Errorf("could not unpinContent(%d): %s", err, pinID)
		}
	}()
	return e.NoContent(http.StatusAccepted)
}

func (cm *ContentManager) UpdatePinStatus(location string, contID uint, status types.PinningStatus) error {
	cm.pinLk.Lock()
	op, ok := cm.pinJobs[contID]

	cm.pinLk.Unlock()
	if !ok {
		return fmt.Errorf("got pin status update for unknown content: %d, status: %s, location: %s", contID, status, location)
	}

	if status == types.PinningStatusFailed {
		var c util.Content
		if err := cm.DB.First(&c, "id = ?", contID).Error; err != nil {
			return errors.Wrap(err, "failed to look up content")
		}

		if c.Active {
			return fmt.Errorf("got failed pin status message from location: %s where content(%d) was already active, refusing to do anything", location, contID)
		}

		if err := cm.DB.Model(util.Content{}).Where("id = ?", contID).UpdateColumns(map[string]interface{}{
			"active":  false,
			"pinning": false,
			"failed":  true,
		}).Error; err != nil {
			log.Errorf("failed to mark content as failed in database: %s", err)
		}
	}
	op.SetStatus(status)
	return nil
}

func (cm *ContentManager) handlePinningComplete(ctx context.Context, handle string, pincomp *drpc.PinComplete) error {
	ctx, span := cm.tracer.Start(ctx, "handlePinningComplete")
	defer span.End()

	var cont util.Content
	if err := cm.DB.First(&cont, "id = ?", pincomp.DBID).Error; err != nil {
		return xerrors.Errorf("got shuttle pin complete for unknown content %d (shuttle = %s): %w", pincomp.DBID, handle, err)
	}

	if cont.Active {
		// content already active, no need to add objects, just update location
		if err := cm.DB.Model(util.Content{}).Where("id = ?", cont.ID).UpdateColumns(map[string]interface{}{
			"pinning":  false,
			"location": handle,
		}).Error; err != nil {
			return err
		}
		// TODO: should we recheck the staging zones?
		return nil
	}

	if cont.Aggregate {
		if err := cm.DB.Model(util.Content{}).Where("id = ?", cont.ID).UpdateColumns(map[string]interface{}{
			"active":   true,
			"pinning":  false,
			"location": handle,
		}).Error; err != nil {
			return xerrors.Errorf("failed to update content in database: %w", err)
		}
		return nil
	}

	objects := make([]*util.Object, 0, len(pincomp.Objects))
	for _, o := range pincomp.Objects {
		objects = append(objects, &util.Object{
			Cid:  util.DbCID{CID: o.Cid},
			Size: o.Size,
		})
	}

	if err := cm.addObjectsToDatabase(ctx, pincomp.DBID, nil, cid.Cid{}, objects, handle); err != nil {
		return xerrors.Errorf("failed to add objects to database: %w", err)
	}

	cm.toCheck(cont.ID)
	return nil
}
