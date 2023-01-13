package api

import (
	"github.com/application-research/estuary/api/v1"
	"github.com/application-research/estuary/collections"
	"github.com/application-research/estuary/pinner/types"
	"github.com/application-research/estuary/util"
	"github.com/ipfs/go-cid"
	"github.com/labstack/echo/v4"
	"github.com/libp2p/go-libp2p/core/peer"
	"net/http"
	"path/filepath"
)

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
// @Router       /pinning/batched-pins [post]
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
