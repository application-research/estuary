package api

import (
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/application-research/estuary/pinner"

	"github.com/ipfs/go-cid"
	"github.com/labstack/echo/v4"
	"github.com/libp2p/go-libp2p/core/peer"
	"golang.org/x/xerrors"
	"gorm.io/gorm"

	"github.com/application-research/estuary/model"
	pinningstatus "github.com/application-research/estuary/pinner/status"
	"github.com/application-research/estuary/util"
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
// @Success      200  {object}  pinner.IpfsListPinStatusResponse
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

	q := s.db.Model(util.Content{}).Where("user_id = ? AND not aggregate AND not replace", u.ID).Order("created_at desc")

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

	pinStatuses := make(map[pinningstatus.PinningStatus]bool)
	if qstatus != "" {
		statuses := strings.Split(qstatus, ",")
		for _, s := range statuses {
			ps := pinningstatus.PinningStatus(s)
			switch ps {
			case pinningstatus.PinningStatusQueued, pinningstatus.PinningStatusPinning, pinningstatus.PinningStatusPinned, pinningstatus.PinningStatusFailed:
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

	out := make([]*pinner.IpfsPinStatusResponse, 0)
	for _, c := range contents {
		st, err := s.pinMgr.PinStatus(c, nil)
		if err != nil {
			return err
		}
		out = append(out, st)
	}

	return c.JSON(http.StatusOK, pinner.IpfsListPinStatusResponse{
		Count:   int(count),
		Results: out,
	})
}

func filterForStatusQuery(q *gorm.DB, statuses map[pinningstatus.PinningStatus]bool) (*gorm.DB, error) {
	// TODO maybe we should move all these statuses to a status column in contents
	if len(statuses) == 0 || len(statuses) == 4 {
		return q, nil // if no status filter or all statuses are specified, return all pins
	}

	whereSet := false
	for status, ok := range statuses {
		if ok {
			switch status {
			case pinningstatus.PinningStatusPinned:
				if whereSet {
					q = q.Or("active")
				} else {
					q = q.Where("active")
					whereSet = true
				}
			case pinningstatus.PinningStatusFailed:
				if whereSet {
					q = q.Or("failed")
				} else {
					q = q.Where("failed")
					whereSet = true
				}
			case pinningstatus.PinningStatusPinning:
				if whereSet {
					q = q.Or("pinning")
				} else {
					q = q.Where("pinning")
					whereSet = true
				}
			case pinningstatus.PinningStatusQueued:
				if whereSet {
					q = q.Or("not active and not pinning and not failed")
				} else {
					q = q.Where("not active and not pinning and not failed")
					whereSet = true
				}
			}
		}
	}
	return q, nil
}

// handleAddPin  godoc
// @Summary      Add and pin object
// @Description  This endpoint adds a pin to the IPFS daemon.
// @Tags         pinning
// @Accept		 json
// @Produce      json
// @Success      202	{object}  pinner.IpfsPinStatusResponse
// @Failure      500    {object}  util.HttpError
// @in           202,default  string  Token "token"
// @Param        pin          body      pinner.IpfsPin  true   "Pin Body {cid:cid, name:name}"
// @Param        ignore-dupes  query     string                   false  "Ignore Dupes"
// @Param        overwrite	   query     string                   false  "Overwrite conflicting files in collections"
// @Router       /pinning/pins [post]
func (s *apiV1) handleAddPin(c echo.Context, u *util.User) error {
	var pin pinner.IpfsPin
	if err := c.Bind(&pin); err != nil {
		return err
	}

	if err := util.ErrorIfContentAddingDisabled(s.isContentAddingDisabled(u)); err != nil {
		return err
	}

	overwrite := false
	if c.QueryParam("overwrite") == "true" {
		overwrite = true
	}

	ignoreDuplicates := false
	if c.QueryParam("ignore-dupes") == "true" {
		ignoreDuplicates = true
	}

	pinningParam := pinner.PinCidParam{
		User:             u,                // the user
		CidToPin:         pin,              // the pin object
		Overwrite:        overwrite,        // the overwrite flag
		IgnoreDuplicates: ignoreDuplicates, // the ignore duplicates flag
		Replication:      s.cfg.Replication,
		MakeDeal:         true,
	}

	status, err := s.pinMgr.PinCid(c, pinningParam)
	if err != nil {
		return err
	}

	return c.JSON(http.StatusAccepted, status)
}

// handleGetPin  godoc
// @Summary      Get a pin status object
// @Description  This endpoint returns a pin status object.
// @Tags         pinning
// @Produce      json
// @Success      200	{object}  pinner.IpfsPinStatusResponse
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
	if err := s.db.First(&content, "id = ? AND not replace", pinID).Error; err != nil {
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

	st, err := s.pinMgr.PinStatus(content, nil)
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
// @Success      202	{object}	pinner.IpfsPinStatusResponse
// @Failure      404	{object}	util.HttpError
// @Failure      500  {object}  util.HttpError
// @Param        pinid		path      string  true  "Pin ID to be replaced"
// @Param        pin          body      pinner.IpfsPin  true   "New pin"
// @Router       /pinning/pins/{pinid} [post]
func (s *apiV1) handleReplacePin(c echo.Context, u *util.User) error {
	if err := util.ErrorIfContentAddingDisabled(s.isContentAddingDisabled(u)); err != nil {
		return err
	}

	pinID, err := strconv.Atoi(c.Param("pinid"))
	if err != nil {
		return err
	}

	var pin pinner.IpfsPin
	if err := c.Bind(&pin); err != nil {
		return err
	}

	var content util.Content
	if err := s.db.First(&content, "id = ? AND not replace", pinID).Error; err != nil {
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
	status, err := s.pinMgr.PinContent(c.Request().Context(), u.ID, pinCID, pin.Name, nil, origins, uint(pinID), pin.Meta, s.cfg.Replication, makeDeal)
	if err != nil {
		return err
	}

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
	if err := s.db.First(&content, "id = ? AND not replace", pinID).Error; err != nil {
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
		if err := s.db.First(&zone, "cont_id = ?", content.AggregatedIn).Error; err != nil {
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
	if err := s.db.Model(&util.Content{}).Where("id = ?", pinID).Update("replace", true).Error; err != nil {
		return err
	}

	// unpin async
	go func() {
		if err := s.cm.UnpinContent(c.Request().Context(), uint(pinID)); err != nil {
			s.log.Errorf("could not unpinContent(%d): %s", err, pinID)
		}
	}()
	return c.NoContent(http.StatusAccepted)
}
