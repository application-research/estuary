package api

import (
	"net/http"

	"github.com/application-research/estuary/pinner"
	"github.com/application-research/estuary/util"
	"github.com/labstack/echo/v4"
)

type BatchedPinRequest struct {
	ContentIdToPin string `json:"content_id"`
}

// handleGetBatchedPins  godoc
// @Summary      Get the pin statuses of a given list of cids
// @Description  This endpoint returns a pin status object.
// @Tags         pinning
// @Produce      json
// @Success      200	{object}  []pinner.IpfsPinStatusResponse
// @Failure      404	{object}  util.HttpError
// @Failure      500    {object}  util.HttpError
// @Param        pin           body      []api.BatchedPinRequest  true   "Pin Body {[content_id:"content_id_to_pin"]}"
// @Router       /v2/pinning/batched-pins/ [get]
func (s *apiV2) handleGetBatchedPins(c echo.Context, u *util.User) error {
	var pins []BatchedPinRequest
	if err := c.Bind(&pins); err != nil {
		return err
	}

	var pinStatuses []*pinner.IpfsPinStatusResponse
	for _, pin := range pins {
		paramCidsToGet := pinner.GetPinParam{
			User:     u,                  // the user
			CidToGet: pin.ContentIdToPin, // the pin object
		}
		st, err := s.pinMgr.GetPin(paramCidsToGet)
		if err != nil {
			return err
		}

		pinStatuses = append(pinStatuses, st)
		if err != nil {
			return err
		}
	}

	return c.JSON(http.StatusOK, pinStatuses)
}

// handleAddBatchedPins  godoc
// @Summary      Add and pin objects in batches
// @Description  This endpoint adds a pin to the IPFS daemon.
// @Tags         pinning
// @Accept		 json
// @Produce      json
// @Success      202	{object}  []pinner.IpfsPinStatusResponse
// @Failure      500    {object}  util.HttpError
// @in           202,default  string  Token "token"
// @Param        pin           body      []types.IpfsPin  true   "Pin Body {[cid:cid, name:name]}"
// @Param        ignore-dupes  query     string                   false  "Ignore Dupes"
// @Param        overwrite	   query     string                   false  "Overwrite conflicting files in collections"
// @Router       /v2/pinning/batched-pins [post]
func (s *apiV2) handleAddBatchedPins(c echo.Context, u *util.User) error {
	var pins []pinner.IpfsPin
	if err := c.Bind(&pins); err != nil {
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

	var pinStatuses []*pinner.IpfsPinStatusResponse
	for _, pin := range pins {
		pinningParam := pinner.PinCidParam{
			User:             u,                // the user
			CidToPin:         pin,              // the pin object
			Overwrite:        overwrite,        // the overwrite flag
			IgnoreDuplicates: ignoreDuplicates, // the ignore duplicates flag
			Replication:      s.cfg.Replication,
		}

		pinnerAddStatus, err := s.pinMgr.PinCidAndRequestMakeDeal(c, pinningParam)
		if err != nil {
			return err
		}

		pinStatuses = append(pinStatuses, pinnerAddStatus) // collect the status
		if err != nil {
			return err
		}
	}
	return c.JSON(http.StatusAccepted, pinStatuses)
}
