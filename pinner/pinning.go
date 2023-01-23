package pinner

import (
	"fmt"
	"github.com/application-research/estuary/collections"
	contentmgr "github.com/application-research/estuary/content"
	"github.com/application-research/estuary/pinner/operation"
	"github.com/application-research/estuary/pinner/types"
	"github.com/application-research/estuary/util"
	"github.com/ipfs/go-cid"
	"github.com/labstack/echo/v4"
	"github.com/libp2p/go-libp2p/core/peer"
	"golang.org/x/xerrors"
	"gorm.io/gorm"
	"path/filepath"
)

const (
	ColDir string = "dir"
)

type PinCidParam struct {
	Ctx              echo.Context
	CM               *contentmgr.ContentManager
	Db               *gorm.DB
	User             *util.User
	CidToPin         types.IpfsPin
	Overwrite        bool
	IgnoreDuplicates bool
	Replication      int
	MakeDeal         bool `default:"true"`
}

type GetPinParam struct {
	Ctx      echo.Context
	CM       *contentmgr.ContentManager
	Db       *gorm.DB
	User     *util.User
	CidToGet string
}

type PinningHelperError struct {
	error
	Reason  string
	Details string
}

// PinCidAndQueue adds a cid to the pin queue, and pins it if possible
func PinCidAndRequestMakeDeal(param PinCidParam) (*types.IpfsPinStatusResponse, *operation.PinningOperation, error) {
	ctx := param.Ctx.Request().Context()

	// get the filename and set it to the cid if it's not set
	filename := param.CidToPin.Name
	if filename == "" {
		filename = param.CidToPin.CID
	}

	// get the related collections.
	var cols []*collections.CollectionRef
	if c, ok := param.CidToPin.Meta["collection"].(string); ok && c != "" {
		srchCol, err := collections.GetCollection(c, param.Db, param.User)
		colp, _ := param.CidToPin.Meta[ColDir].(string)
		path, err := collections.ConstructDirectoryPath(colp)
		if err != nil {
			return nil, nil, err
		}
		fullPath := filepath.Join(path, filename)

		cols = []*collections.CollectionRef{
			{
				Collection: srchCol.ID,
				Path:       &fullPath,
			},
		}

		// see if there's already a file with that name/path on that collection
		pathInCollection := collections.Contains(&srchCol, fullPath, param.Db)

		// 	this helper is not in the context of HTTP request so we need to respect that
		//	by returning a generic struct which can be casted by the caller.
		if pathInCollection && !param.Overwrite {
			return nil, nil, &PinningHelperError{
				Reason:  util.ERR_CONTENT_IN_COLLECTION,
				Details: "file already exists in collection, specify 'overwrite=true' to overwrite",
			}
		}
	}

	//  this is really important. As soon as we pin the cid, we need to make sure that
	//	the node is aware of the origins (peered).
	var origins []*peer.AddrInfo
	for _, p := range param.CidToPin.Origins {
		ai, err := peer.AddrInfoFromString(p)
		if err != nil {
			log.Warn("could not parse origin(%s): %s", p, err)
			continue
		}
		origins = append(origins, ai)
	}

	//	 decode the cid
	obj, err := cid.Decode(param.CidToPin.CID)
	if err != nil {
		return nil, nil, err
	}

	//	ignore duplicates can be set as a parameter, or if the cid is already pinned
	//	we should ignore it.
	if param.IgnoreDuplicates {
		var count int64
		if err := param.Db.Model(util.Content{}).Where("cid = ? and user_id = ?", obj.Bytes(), param.User.ID).Count(&count).Error; err != nil {
			return nil, nil, err
		}
		if count > 0 {
			return nil, nil, param.Ctx.JSON(302, map[string]string{"message": "content with given cid already preserved"})
		}
	}

	//	 this is set to true by default but the param object has a way to override it.
	status, pinOp, err := param.CM.PinContent(ctx, param.User.ID, obj, param.CidToPin.Name, cols, origins, 0, param.CidToPin.Meta, param.Replication, param.MakeDeal)
	if err != nil {
		return nil, nil, err
	}
	return status, pinOp, nil
}

// GetPinStatus returns the status of a pin operation
func GetPin(param GetPinParam) (*types.IpfsPinStatusResponse, error) {
	contentId := param.CidToGet
	var content util.Content
	if err := param.Db.First(&content, "id = ? AND not replace", contentId).Error; err != nil {
		if xerrors.Is(err, gorm.ErrRecordNotFound) {
			return nil, &PinningHelperError{
				Reason:  util.ERR_CONTENT_NOT_FOUND,
				Details: fmt.Sprintf("Content with id %s not found", contentId),
			}
		}
		return nil, err
	}

	if err := util.IsContentOwner(param.User.ID, content.UserID); err != nil {
		return nil, err
	}

	st, err := param.CM.PinStatus(content, nil)
	if err != nil {
		return nil, err
	}
	return st, nil
}
