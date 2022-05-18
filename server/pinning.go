package server

import (
	"context"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/application-research/estuary/pinner"
	"github.com/application-research/estuary/types"
	"github.com/application-research/estuary/util"
	"github.com/ipfs/go-blockservice"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-merkledag"
	"github.com/labstack/echo/v4"
	"github.com/libp2p/go-libp2p-core/peer"
	"gorm.io/gorm"
)

func (s *Server) DoPinning(ctx context.Context, op *pinner.PinningOperation, cb pinner.PinProgressCB) error {
	ctx, span := s.Tracer.Start(ctx, "doPinning")
	defer span.End()

	connectedToAtLeastOne := false
	for _, pi := range op.Peers {
		if err := s.Node.Host.Connect(ctx, pi); err != nil && s.Node.Host.ID() != pi.ID {
			log.Warnf("failed to connect to origin node for pinning operation: %s", err)
		} else {
			//	Check if it's trying to connect to itself since we only want to check if the
			//	the connection is between the host and the external/other peers.
			connectedToAtLeastOne = true
		}
	}

	//	If it can't connect to any legitimate provider peers, then we fail the entire operation.
	if !connectedToAtLeastOne {
		log.Errorf("unable to connect to any of the provider peers for pinning operation")
		return nil
	}

	bserv := blockservice.New(s.Node.Blockstore, s.Node.Bitswap)
	dserv := merkledag.NewDAGService(bserv)

	dsess := merkledag.NewSession(ctx, dserv)

	if err := s.CM.AddDatabaseTrackingToContent(ctx, op.ContId, dsess, s.Node.Blockstore, op.Obj, cb); err != nil {
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

// handleListPins godoc
// @Summary      List all pinned objects
// @Description  This endpoint lists all pinned objects
// @Tags         pinning
// @Produce      json
// @Failure      400  {object}  util.HttpError
// @Failure      404  {object}  util.HttpError
// @Failure      500  {object}  util.HttpError
// @Router       /pinning/pins [get]
func (s *Server) handleListPins(e echo.Context, u *util.User) error {
	_, span := s.Tracer.Start(e.Request().Context(), "handleListPins")
	defer span.End()

	qcids := e.QueryParam("cid")
	qname := e.QueryParam("name")
	qstatus := e.QueryParam("status")
	qbefore := e.QueryParam("before")
	qafter := e.QueryParam("after")
	qlimit := e.QueryParam("limit")
	qreqids := e.QueryParam("requestid")

	q := s.DB.Model(util.Content{}).Where("user_id = ? and not aggregate", u.ID).Order("created_at desc")

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

	var contents []util.Content
	if err := q.Scan(&contents).Error; err != nil {
		return err
	}

	var out []*types.IpfsPinStatus
	for _, c := range contents {
		if lim > 0 && len(out) >= lim {
			break
		}

		st, err := s.CM.PinStatus(c)
		if err != nil {
			return err
		}
		if allowed == nil || allowed[st.Status] {
			out = append(out, st)
		}
	}

	if len(out) == 0 {
		out = make([]*types.IpfsPinStatus, 0)
	}
	return e.JSON(http.StatusOK, map[string]interface{}{
		"count":   len(out),
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
// @Description  This endpoint adds a pin to the IPFS daemon.
// @Tags         pinning
// @Produce      json
// @in           200,400,default  string  Token "token"
// @Param        cid   path  string  true  "cid"
// @Param        name  path  string  true  "name"
// @Router       /pinning/pins [post]
func (s *Server) handleAddPin(e echo.Context, u *util.User) error {
	ctx := e.Request().Context()

	if s.CM.ContentAddingDisabled || u.StorageDisabled {
		return &util.HttpError{
			Code:    http.StatusBadRequest,
			Message: util.ERR_CONTENT_ADDING_DISABLED,
		}
	}

	var pin types.IpfsPin
	if err := e.Bind(&pin); err != nil {
		return err
	}

	var cols []*util.CollectionRef
	if c, ok := pin.Meta["collection"].(string); ok && c != "" {
		var srchCol util.Collection
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

		cols = []*util.CollectionRef{&util.CollectionRef{
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

	makeDeal := true
	status, err := s.CM.PinContent(ctx, u.ID, obj, pin.Name, cols, addrInfos, 0, pin.Meta, makeDeal)
	if err != nil {
		return err
	}

	status.Pin.Meta = pin.Meta

	return e.JSON(http.StatusAccepted, status)
}

// handleGetPin  godoc
// @Summary      Get a pinned objects
// @Description  This endpoint returns a pinned object.
// @Tags         pinning
// @Produce      json
// @Param        requestid  path  string  true  "cid"
// @Router       /pinning/pins/{requestid} [get]
func (s *Server) handleGetPin(e echo.Context, u *util.User) error {
	id, err := strconv.Atoi(e.Param("requestid"))
	if err != nil {
		return err
	}

	var content util.Content
	if err := s.DB.First(&content, "id = ?", uint(id)).Error; err != nil {
		return err
	}

	st, err := s.CM.PinStatus(content)
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
// @Param        id  path  string  true  "id"
// @Router       /pinning/pins/{id} [post]
func (s *Server) handleReplacePin(e echo.Context, u *util.User) error {
	if s.CM.ContentAddingDisabled || u.StorageDisabled {
		return &util.HttpError{
			Code:    http.StatusBadRequest,
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

	var content util.Content
	if err := s.DB.First(&content, "id = ?", id).Error; err != nil {
		return err
	}
	if content.UserID != u.ID {
		return &util.HttpError{
			Code:    http.StatusUnauthorized,
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

	makeDeal := true
	status, err := s.CM.PinContent(ctx, u.ID, obj, pin.Name, nil, addrInfos, uint(id), pin.Meta, makeDeal)
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
// @Param        requestid  path  string  true  "requestid"
// @Router       /pinning/pins/{requestid} [delete]
func (s *Server) handleDeletePin(e echo.Context, u *util.User) error {
	// TODO: need to cancel any in-progress pinning operation
	ctx := e.Request().Context()
	id, err := strconv.Atoi(e.Param("requestid"))
	if err != nil {
		return err
	}

	var content util.Content
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
	if err := s.CM.UnpinContent(ctx, uint(id)); err != nil {
		return err
	}

	return e.NoContent(http.StatusAccepted)
}
