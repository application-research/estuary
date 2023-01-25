package pinner

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"path/filepath"

	"github.com/application-research/estuary/collections"
	"github.com/application-research/estuary/constants"
	"github.com/application-research/estuary/pinner/operation"
	"github.com/application-research/estuary/pinner/status"
	"github.com/application-research/estuary/util"
	"github.com/ipfs/go-blockservice"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-merkledag"
	"github.com/labstack/echo/v4"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/pkg/errors"
	"golang.org/x/xerrors"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

const (
	ColDir string = "dir"
)

type PinCidParam struct {
	User             *util.User
	CidToPin         IpfsPin
	Overwrite        bool
	IgnoreDuplicates bool
	Replication      int
	MakeDeal         bool `default:"true"`
}

type GetPinParam struct {
	User     *util.User
	CidToGet string
}

// PinCidAndQueue adds a cid to the pin queue, and pins it if possible
func (pm *EstuaryPinManager) PinCidAndRequestMakeDeal(eCtx echo.Context, param PinCidParam) (*IpfsPinStatusResponse, error) {
	ctx := eCtx.Request().Context()

	// get the filename and set it to the cid if it's not set
	filename := param.CidToPin.Name
	if filename == "" {
		filename = param.CidToPin.CID
	}

	// get the related collections.
	var cols []*collections.CollectionRef
	if c, ok := param.CidToPin.Meta["collection"].(string); ok && c != "" {
		srchCol, err := collections.GetCollection(c, pm.db, param.User)
		colp, _ := param.CidToPin.Meta[ColDir].(string)
		path, err := collections.ConstructDirectoryPath(colp)
		if err != nil {
			return nil, err
		}
		fullPath := filepath.Join(path, filename)

		cols = []*collections.CollectionRef{
			{
				Collection: srchCol.ID,
				Path:       &fullPath,
			},
		}

		// see if there's already a file with that name/path on that collection
		pathInCollection := collections.Contains(&srchCol, fullPath, pm.db)
		// 	this helper is not in the context of HTTP request so we need to respect that
		//	by returning a generic struct which can be casted by the caller.
		if pathInCollection && !param.Overwrite {
			return nil, &util.HttpError{
				Code:    http.StatusBadRequest,
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
		return nil, err
	}

	//	ignore duplicates can be set as a parameter, or if the cid is already pinned
	//	we should ignore it.
	if param.IgnoreDuplicates {
		var count int64
		if err := pm.db.Model(util.Content{}).Where("cid = ? and user_id = ?", obj.Bytes(), param.User.ID).Count(&count).Error; err != nil {
			return nil, err
		}
		if count > 0 {
			return nil, eCtx.JSON(302, map[string]string{"message": "content with given cid already preserved"})
		}
	}

	// this is set to true by default but the param object has a way to override it.
	status, err := pm.PinContent(ctx, param.User.ID, obj, param.CidToPin.Name, cols, origins, 0, param.CidToPin.Meta, param.Replication, param.MakeDeal)
	if err != nil {
		return nil, err
	}
	return status, nil
}

// GetPinStatus returns the status of a pin operation
func (pm *EstuaryPinManager) GetPin(param GetPinParam) (*IpfsPinStatusResponse, error) {
	contentId := param.CidToGet
	var content util.Content
	if err := pm.db.First(&content, "id = ? AND not replace", contentId).Error; err != nil {
		if xerrors.Is(err, gorm.ErrRecordNotFound) {
			return nil, &util.HttpError{
				Code:    http.StatusBadRequest,
				Reason:  util.ERR_CONTENT_NOT_FOUND,
				Details: fmt.Sprintf("Content with id %s not found", contentId),
			}
		}
		return nil, err
	}

	if err := util.IsContentOwner(param.User.ID, content.UserID); err != nil {
		return nil, err
	}

	st, err := pm.PinStatus(content, nil)
	if err != nil {
		return nil, err
	}
	return st, nil
}

func (m *EstuaryPinManager) PinContent(ctx context.Context, user uint, obj cid.Cid, filename string, cols []*collections.CollectionRef, origins []*peer.AddrInfo, replaceID uint, meta map[string]interface{}, replication int, makeDeal bool) (*IpfsPinStatusResponse, error) {
	if replaceID > 0 {
		// mark as replace since it will removed and so it should not be fetched anymore
		if err := m.db.Model(&util.Content{}).Where("id = ?", replaceID).Update("replace", true).Error; err != nil {
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

	loc, err := m.shuttleMgr.GetLocationForStorage(ctx, obj, user)
	if err != nil {
		return nil, xerrors.Errorf("selecting location for content failed: %w", err)
	}

	cont := util.Content{
		Cid:         util.DbCID{CID: obj},
		Name:        filename,
		UserID:      user,
		Active:      false,
		Pinning:     false,
		Replication: replication,
		PinMeta:     metaStr,
		Location:    loc,
		Origins:     originsStr,
	}
	if err := m.db.Create(&cont).Error; err != nil {
		return nil, err
	}

	if len(cols) > 0 {
		for _, c := range cols {
			c.Content = cont.ID
		}

		if err := m.db.Clauses(clause.OnConflict{
			Columns:   []clause.Column{{Name: "path"}, {Name: "collection"}},
			DoUpdates: clause.AssignmentColumns([]string{"created_at", "content"}),
		}).Create(cols).Error; err != nil {
			return nil, err
		}
	}

	var pinOp *operation.PinningOperation
	if loc == constants.ContentLocationLocal {
		pinOp = m.getPinOperation(cont, origins, replaceID, makeDeal)
		m.Add(pinOp)
	} else {
		if err := m.shuttleMgr.PinContent(ctx, loc, cont, origins); err != nil {
			return nil, err
		}
	}

	ipfsRes, err := m.PinStatus(cont, origins)
	if err != nil {
		return nil, err
	}
	return ipfsRes, nil
}

func (m *EstuaryPinManager) getPinOperation(cont util.Content, peers []*peer.AddrInfo, replaceID uint, makeDeal bool) *operation.PinningOperation {
	if cont.Location != constants.ContentLocationLocal {
		m.log.Errorf("calling addPinToQueue on non-local content")
	}

	return &operation.PinningOperation{
		ContId:   cont.ID,
		UserId:   cont.UserID,
		Obj:      cont.Cid.CID,
		Name:     cont.Name,
		Peers:    operation.SerializePeers(peers),
		Started:  cont.CreatedAt,
		Status:   status.GetContentPinningStatus(cont),
		Replace:  replaceID,
		Location: cont.Location,
		MakeDeal: makeDeal,
		Meta:     cont.PinMeta,
	}
}

func (m *EstuaryPinManager) PinStatus(cont util.Content, origins []*peer.AddrInfo) (*IpfsPinStatusResponse, error) {
	delegates := m.PinDelegatesForContent(cont)

	meta := make(map[string]interface{}, 0)
	if cont.PinMeta != "" {
		if err := json.Unmarshal([]byte(cont.PinMeta), &meta); err != nil {
			m.log.Warnf("content %d has invalid pinmeta: %s", cont, err)
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

	ps := &IpfsPinStatusResponse{
		RequestID: fmt.Sprintf("%d", cont.ID),
		Status:    status.GetContentPinningStatus(cont),
		Created:   cont.CreatedAt,
		Pin: IpfsPin{
			CID:     cont.Cid.CID.String(),
			Name:    cont.Name,
			Meta:    meta,
			Origins: originStrs,
		},
		Content:   cont,
		Delegates: delegates,
		Info:      make(map[string]interface{}, 0), // TODO: all sorts of extra info we could add...
	}
	return ps, nil
}

func (m *EstuaryPinManager) PinDelegatesForContent(cont util.Content) []string {
	out := make([]string, 0)
	if cont.Location == constants.ContentLocationLocal {
		for _, a := range m.nd.Host.Addrs() {
			out = append(out, fmt.Sprintf("%s/p2p/%s", a, m.nd.Host.ID()))
		}
		return out
	}

	ai, err := m.addrInfoForContentLocation(cont.Location)
	if err != nil {
		m.log.Warnf("failed to get address info for shuttle %q: %s", cont.Location, err)
		return out
	}

	if ai == nil {
		m.log.Warnf("no address info for shuttle: %s", cont.Location)
		return out
	}

	for _, a := range ai.Addrs {
		out = append(out, fmt.Sprintf("%s/p2p/%s", a, ai.ID))
	}
	return out
}

func (m *EstuaryPinManager) addrInfoForContentLocation(handle string) (*peer.AddrInfo, error) {
	if handle == constants.ContentLocationLocal {
		return &peer.AddrInfo{
			ID:    m.nd.Host.ID(),
			Addrs: m.nd.Host.Addrs(),
		}, nil
	}
	return m.shuttleMgr.AddrInfo(handle)
}

func (m *EstuaryPinManager) doPinning(ctx context.Context, op *operation.PinningOperation) error {
	ctx, span := m.tracer.Start(ctx, "doPinning")
	defer span.End()

	// remove replacement async - move this out
	if op.Replace > 0 {
		go func() {
			if err := m.cm.RemoveContent(ctx, op.Replace, true); err != nil {
				m.log.Infof("failed to remove content in replacement: %d with: %d", op.Replace, op.ContId)
			}
		}()
	}

	var c *util.Content
	if err := m.db.First(&c, "id = ?", op.ContId).Error; err != nil {
		return errors.Wrap(err, "failed to look up content for dopinning")
	}

	prs := operation.UnSerializePeers(op.Peers)
	for _, pi := range prs {
		if err := m.nd.Host.Connect(ctx, *pi); err != nil {
			m.log.Warnf("failed to connect to origin node for pinning operation: %s", err)
		}
	}

	bserv := blockservice.New(m.nd.Blockstore, m.nd.Bitswap)
	dserv := merkledag.NewDAGService(bserv)
	dsess := dserv.Session(ctx)

	if err := m.blockMgr.WalkAndSaveBlocks(ctx, c, dsess, op.Obj); err != nil {
		return err
	}

	// this provide call goes out immediately
	if err := m.nd.FullRT.Provide(ctx, op.Obj, true); err != nil {
		m.log.Warnf("provider broadcast failed: %s", err)
	}

	// this one adds to a queue
	if err := m.nd.Provider.Provide(op.Obj); err != nil {
		m.log.Warnf("providing failed: %s", err)
	}
	return nil
}
