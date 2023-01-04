package api

import (
	"errors"
	"fmt"
	"net/http"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/application-research/estuary/collections"
	"github.com/application-research/estuary/model"
	"github.com/application-research/estuary/pinner/types"
	"github.com/application-research/estuary/util"
	"github.com/ipfs/go-cid"
	"github.com/labstack/echo/v4"
	"github.com/libp2p/go-libp2p/core/peer"
	"golang.org/x/xerrors"
	"gorm.io/gorm"
)

const (
	DEFAULT_IPFS_PIN_LIMIT = 10 // https://github.com/ipfs/pinning-services-api-spec/blob/main/ipfs-pinning-service.yaml#L610
	IPFS_PIN_LIMIT_MIN     = 1
	IPFS_PIN_LIMIT_MAX     = 1000
)

// handleListPins godoc
// @Summary      List all pin status objects
// @Description  This endpoint lists all pin status objects
// @Tags         pinning
// @Produce      json
// @Success      200  {object}  types.IpfsListPinStatusResponse
// @Failure      400  {object}  util.HttpError
// @Failure      500  {object}  util.HttpError
// @Router       /pinning/pins [get]
func (s *apiV1) handleListPins(c echo.Context, u *util.User) error {
	_, span := s.tracer.Start(c.Request().Context(), "handleListPins")
	defer span.End()

	qcids := c.QueryParam("cid")
	qname := c.QueryParam("name")
	qmatch := c.QueryParam("match")
	qstatus := c.QueryParam("status")
	qbefore := c.QueryParam("before")
	qafter := c.QueryParam("after")
	qlimit := c.QueryParam("limit")
	qreqids := c.QueryParam("requestid")

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
		st, err := s.CM.PinStatus(c, nil)
		if err != nil {
			return err
		}
		out = append(out, st)
	}

	return c.JSON(http.StatusOK, types.IpfsListPinStatusResponse{
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
// @Accept		 json
// @Produce      json
// @Success      202	{object}  types.IpfsPinStatusResponse
// @Failure      500    {object}  util.HttpError
// @in           202,default  string  Token "token"
// @Param        pin          body      types.IpfsPin  true   "Pin Body {cid:cid, name:name}"
// @Param        ignore-dupes  query     string                   false  "Ignore Dupes"
// @Param        overwrite	   query     string                   false  "Overwrite conflicting files in collections"
// @Router       /pinning/pins [post]
func (s *apiV1) handleAddPin(c echo.Context, u *util.User) error {
	ctx := c.Request().Context()

	if err := util.ErrorIfContentAddingDisabled(s.isContentAddingDisabled(u)); err != nil {
		return err
	}

	overwrite := false
	if c.QueryParam("overwrite") == "true" {
		overwrite = true
	}

	var pin types.IpfsPin
	if err := c.Bind(&pin); err != nil {
		return err
	}

	filename := pin.Name
	if filename == "" {
		filename = pin.CID
	}

	var cols []*collections.CollectionRef
	if c, ok := pin.Meta["collection"].(string); ok && c != "" {
		var srchCol collections.Collection
		if err := s.DB.First(&srchCol, "uuid = ? and user_id = ?", c, u.ID).Error; err != nil {
			return err
		}

		colp, _ := pin.Meta[ColDir].(string)
		path, err := collections.ConstructDirectoryPath(colp)
		if err != nil {
			return err
		}
		fullPath := filepath.Join(path, filename)

		cols = []*collections.CollectionRef{
			{
				Collection: srchCol.ID,
				Path:       &fullPath,
			},
		}

		// see if there's already a file with that name/path on that collection
		pathInCollection := collections.Contains(&srchCol, fullPath, s.DB)
		if pathInCollection && !overwrite {
			return &util.HttpError{
				Code:    http.StatusBadRequest,
				Reason:  util.ERR_CONTENT_IN_COLLECTION,
				Details: "file already exists in collection, specify 'overwrite=true' to overwrite",
			}
		}
	}

	var origins []*peer.AddrInfo
	for _, p := range pin.Origins {
		ai, err := peer.AddrInfoFromString(p)
		if err != nil {
			s.log.Warnf("could not parse origin(%s): %s", p, err)
			continue
		}
		origins = append(origins, ai)
	}

	obj, err := cid.Decode(pin.CID)
	if err != nil {
		return err
	}

	if c.QueryParam("ignore-dupes") == "true" {
		var count int64
		if err := s.DB.Model(util.Content{}).Where("cid = ? and user_id = ?", obj.Bytes(), u.ID).Count(&count).Error; err != nil {
			return err
		}
		if count > 0 {
			return c.JSON(302, map[string]string{"message": "content with given cid already preserved"})
		}
	}

	makeDeal := true
	status, pinOp, err := s.CM.PinContent(ctx, u.ID, obj, pin.Name, cols, origins, 0, pin.Meta, makeDeal)
	if err != nil {
		return err
	}
	s.pinMgr.Add(pinOp)

	return c.JSON(http.StatusAccepted, status)
}

// handleGetPin  godoc
// @Summary      Get a pin status object
// @Description  This endpoint returns a pin status object.
// @Tags         pinning
// @Produce      json
// @Success      200	{object}  types.IpfsPinStatusResponse
// @Failure      404	{object}  util.HttpError
// @Failure      500    {object}  util.HttpError
// @Param        pinid  path      string  true  "cid"
// @Router       /pinning/pins/{pinid} [get]
func (s *apiV1) handleGetPin(c echo.Context, u *util.User) error {
	pinID, err := strconv.Atoi(c.Param("pinid"))
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

	st, err := s.CM.PinStatus(content, nil)
	if err != nil {
		return err
	}
	return c.JSON(http.StatusOK, st)
}

// handleReplacePin godoc
// @Summary      Replace a pinned object
// @Description  This endpoint replaces a pinned object.
// @Tags         pinning
// @Accept		 json
// @Produce      json
// @Success      202	{object}	types.IpfsPinStatusResponse
// @Failure      404	{object}	util.HttpError
// @Failure      500  {object}  util.HttpError
// @Param        pinid		path      string  true  "Pin ID to be replaced"
// @Param        pin          body      types.IpfsPin  true   "New pin"
// @Router       /pinning/pins/{pinid} [post]
func (s *apiV1) handleReplacePin(c echo.Context, u *util.User) error {

	if err := util.ErrorIfContentAddingDisabled(s.isContentAddingDisabled(u)); err != nil {
		return err
	}

	pinID, err := strconv.Atoi(c.Param("pinid"))
	if err != nil {
		return err
	}

	var pin types.IpfsPin
	if err := c.Bind(&pin); err != nil {
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
	status, pinOp, err := s.CM.PinContent(c.Request().Context(), u.ID, pinCID, pin.Name, nil, origins, uint(pinID), pin.Meta, makeDeal)
	if err != nil {
		return err
	}
	s.pinMgr.Add(pinOp)

	return c.JSON(http.StatusAccepted, status)
}

// handleDeletePin godoc
// @Summary      Delete a pinned object
// @Description  This endpoint deletes a pinned object.
// @Tags         pinning
// @Produce      json
// @Success		 202
// @Failure      500  {object}  util.HttpError
// @Param        pinid  path      string  true  "Pin ID"
// @Router       /pinning/pins/{pinid} [delete]
func (s *apiV1) handleDeletePin(c echo.Context, u *util.User) error {
	pinID, err := strconv.Atoi(c.Param("pinid"))
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

	if content.AggregatedIn > 0 {
		var zone *model.StagingZone
		if err := s.DB.First(&zone, "cont_id = ?", content.AggregatedIn).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				s.log.Errorf("content %d's aggregatedIn zone %d not found in DB", content.ID, content.AggregatedIn)
			}
			return err
		}

		if zone.Status != model.ZoneStatusOpen {
			return fmt.Errorf("unable to unpin content while zone is not open (pin: %d, zone: %d)", content.ID, content.AggregatedIn)
		}
	}

	// mark as replace since it will removed and so it should not be fetched anymore
	if err := s.DB.Model(&util.Content{}).Where("id = ?", pinID).Update("replace", true).Error; err != nil {
		return err
	}

	// unpin async
	go func() {
		if err := s.CM.UnpinContent(c.Request().Context(), uint(pinID)); err != nil {
			s.log.Errorf("could not unpinContent(%d): %s", err, pinID)
		}
	}()
	return c.NoContent(http.StatusAccepted)
}
