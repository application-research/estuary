package api

import (
	"fmt"
	"github.com/application-research/estuary/api/v1"
	"github.com/application-research/estuary/collections"
	"github.com/application-research/estuary/pinner/types"
	"github.com/application-research/estuary/util"
	"github.com/ipfs/go-cid"
	"github.com/labstack/echo/v4"
	"github.com/libp2p/go-libp2p/core/peer"
	"golang.org/x/xerrors"
	"gorm.io/gorm"
	"net/http"
	"path/filepath"
)

type BatchedPinRequest struct {
	ContentIdToPin string `json:"content_id"`
}

// handleGetBatchedPins  godoc
// @Summary      Get a pin status object
// @Description  This endpoint returns a pin status object.
// @Tags         pinning
// @Produce      json
// @Success      200	{object}  []types.IpfsPinStatusResponse
// @Failure      404	{object}  util.HttpError
// @Failure      500    {object}  util.HttpError
// @Param        pin           body      []api.BatchedPinRequest  true   "Pin Body {[content_id:"content_id_to_pin"]}"
// @Router       /v2/pinning/batched-pins/ [get]
func (s *apiV2) handleGetBatchedPins(c echo.Context, u *util.User) error {

	var pins []BatchedPinRequest
	err := c.Bind(&pins)
	if err != nil {
		return err
	}

	var pinStatuses []*types.IpfsPinStatusResponse
	for _, pinId := range pins {
		contentId := pinId.ContentIdToPin
		var content util.Content
		if err := s.DB.First(&content, "id = ? AND not replace", contentId).Error; err != nil {
			if xerrors.Is(err, gorm.ErrRecordNotFound) {
				return &util.HttpError{
					Code:    http.StatusNotFound,
					Reason:  util.ERR_CONTENT_NOT_FOUND,
					Details: fmt.Sprintf("Content with id %s not found", contentId),
				}
			}
			return err
		}

		if err := util.IsContentOwner(u.ID, content.UserID); err != nil {
			return err
		}

		st, err := s.CM.PinStatus(content, nil)
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
// @Success      202	{object}  []types.IpfsPinStatusResponse
// @Failure      500    {object}  util.HttpError
// @in           202,default  string  Token "token"
// @Param        pin           body      []types.IpfsPin  true   "Pin Body {[cid:cid, name:name]}"
// @Param        ignore-dupes  query     string                   false  "Ignore Dupes"
// @Param        overwrite	   query     string                   false  "Overwrite conflicting files in collections"
// @Router       /v2/pinning/batched-pins [post]
func (s *apiV2) handleAddBatchedPins(c echo.Context, u *util.User) error {
	ctx := c.Request().Context()

	if err := util.ErrorIfContentAddingDisabled(s.isContentAddingDisabled(u)); err != nil {
		return err
	}

	overwrite := false
	if c.QueryParam("overwrite") == "true" {
		overwrite = true
	}

	var pins []types.IpfsPin
	if err := c.Bind(&pins); err != nil {
		return err
	}

	var pinStatuses []*types.IpfsPinStatusResponse
	for _, pin := range pins {
		filename := pin.Name
		if filename == "" {
			filename = pin.CID
		}

		var cols []*collections.CollectionRef
		if c, ok := pin.Meta["collection"].(string); ok && c != "" {
			srchCol, err := collections.GetCollection(c, s.DB, u)
			if err != nil {
				return err
			}

			colp, _ := pin.Meta[api.ColDir].(string)
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
		pinStatuses = append(pinStatuses, status)
		if err != nil {
			return err
		}
		s.pinMgr.Add(pinOp)
	}

	return c.JSON(http.StatusAccepted, pinStatuses)
}
