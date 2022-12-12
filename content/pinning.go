package contentmgr

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/application-research/estuary/collections"
	"github.com/application-research/estuary/constants"
	"github.com/application-research/estuary/pinner/operation"
	"github.com/application-research/estuary/pinner/progress"
	"github.com/application-research/estuary/pinner/types"
	"github.com/application-research/estuary/util"
	"github.com/ipfs/go-blockservice"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-merkledag"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/pkg/errors"
	"golang.org/x/xerrors"
	"gorm.io/gorm/clause"
)

func (cm *ContentManager) PinStatus(cont util.Content, origins []*peer.AddrInfo) (*types.IpfsPinStatusResponse, error) {
	delegates := cm.PinDelegatesForContent(cont)

	meta := make(map[string]interface{}, 0)
	if cont.PinMeta != "" {
		if err := json.Unmarshal([]byte(cont.PinMeta), &meta); err != nil {
			cm.log.Warnf("content %d has invalid pinmeta: %s", cont, err)
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
		Status:    types.GetContentPinningStatus(cont),
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
	return ps, nil
}

func (cm *ContentManager) PinDelegatesForContent(cont util.Content) []string {
	if cont.Location == constants.ContentLocationLocal {
		var out []string
		for _, a := range cm.node.Host.Addrs() {
			out = append(out, fmt.Sprintf("%s/p2p/%s", a, cm.node.Host.ID()))
		}
		return out

	} else {
		ai, err := cm.addrInfoForContentLocation(cont.Location)
		if err != nil {
			cm.log.Errorf("failed to get address info for shuttle %q: %s", cont.Location, err)
			return nil
		}

		if ai == nil {
			cm.log.Warnf("no address info for shuttle: %s", cont.Location)
			return nil
		}

		var out []string
		for _, a := range ai.Addrs {
			out = append(out, fmt.Sprintf("%s/p2p/%s", a, ai.ID))
		}
		return out
	}
}

func (cm *ContentManager) PinContent(ctx context.Context, user uint, obj cid.Cid, filename string, cols []*collections.CollectionRef, origins []*peer.AddrInfo, replaceID uint, meta map[string]interface{}, makeDeal bool) (*types.IpfsPinStatusResponse, *operation.PinningOperation, error) {
	loc, err := cm.shuttleMgr.GetLocationForStorage(ctx, obj, user)
	if err != nil {
		return nil, nil, xerrors.Errorf("selecting location for content failed: %w", err)
	}

	if replaceID > 0 {
		// mark as replace since it will removed and so it should not be fetched anymore
		if err := cm.db.Model(&util.Content{}).Where("id = ?", replaceID).Update("replace", true).Error; err != nil {
			return nil, nil, err
		}
	}

	var metaStr string
	if meta != nil {
		b, err := json.Marshal(meta)
		if err != nil {
			return nil, nil, err
		}
		metaStr = string(b)
	}

	var originsStr string
	if origins != nil {
		b, err := json.Marshal(origins)
		if err != nil {
			return nil, nil, err
		}
		originsStr = string(b)
	}

	cont := util.Content{
		Cid:         util.DbCID{CID: obj},
		Name:        filename,
		UserID:      user,
		Active:      false,
		Replication: cm.cfg.Replication,
		Pinning:     true,
		PinMeta:     metaStr,
		Location:    loc,
		Origins:     originsStr,
	}
	if err := cm.db.Create(&cont).Error; err != nil {
		return nil, nil, err
	}

	if len(cols) > 0 {
		for _, c := range cols {
			c.Content = cont.ID
		}

		if err := cm.db.Clauses(clause.OnConflict{
			Columns:   []clause.Column{{Name: "path"}, {Name: "collection"}},
			DoUpdates: clause.AssignmentColumns([]string{"created_at", "content"}),
		}).Create(cols).Error; err != nil {
			return nil, nil, err
		}
	}

	var pinOp *operation.PinningOperation
	if loc == constants.ContentLocationLocal {
		pinOp = cm.GetPinOperation(cont, origins, replaceID, makeDeal)
	} else {
		if err := cm.shuttleMgr.PinContent(ctx, loc, cont, origins); err != nil {
			return nil, nil, err
		}
	}

	ipfsRes, err := cm.PinStatus(cont, origins)
	if err != nil {
		return nil, nil, err
	}
	return ipfsRes, pinOp, nil
}

func (cm *ContentManager) GetPinOperation(cont util.Content, peers []*peer.AddrInfo, replaceID uint, makeDeal bool) *operation.PinningOperation {
	if cont.Location != constants.ContentLocationLocal {
		cm.log.Errorf("calling addPinToQueue on non-local content")
	}

	return &operation.PinningOperation{
		ContId:   cont.ID,
		UserId:   cont.UserID,
		Obj:      cont.Cid.CID,
		Name:     cont.Name,
		Peers:    operation.SerializePeers(peers),
		Started:  cont.CreatedAt,
		Status:   types.GetContentPinningStatus(cont),
		Replace:  replaceID,
		Location: cont.Location,
		MakeDeal: makeDeal,
		Meta:     cont.PinMeta,
	}
}

// even though there are 4 pin statuses, queued, pinning, pinned and failed
// the UpdatePinStatus only changes DB state for failed status
// when the content was added, status = pinning
// when the pin process is complete, status = pinned
func (cm *ContentManager) UpdatePinStatus(contID uint, location string, status types.PinningStatus) error {
	if status == types.PinningStatusFailed {
		cm.log.Debugf("updating pin: %d, status: %s, loc: %s", contID, status, location)

		var c util.Content
		if err := cm.db.First(&c, "id = ?", contID).Error; err != nil {
			return errors.Wrap(err, "failed to look up content")
		}

		// if content is already active, ignore it
		if c.Active {
			return nil
		}

		// if an aggregate zone is failing, zone is stuck
		// TODO - not sure if this is happening, but we should look (next pr will have a zone status), ignore for now
		if c.Aggregate {
			cm.log.Errorf("a zone is stuck, as an aggregate zone: %d, failed to aggregate(pin) on location: %s", c.ID, location)
			return nil
		}

		if err := cm.db.Model(util.Content{}).Where("id = ?", contID).UpdateColumns(map[string]interface{}{
			"active":        false,
			"pinning":       false,
			"failed":        true,
			"aggregated_in": 0, // remove from staging zone so the zone can consolidate without it
		}).Error; err != nil {
			cm.log.Errorf("failed to mark content as failed in database: %s", err)
			return err
		}
	}
	return nil
}

func (cm *ContentManager) DoPinning(ctx context.Context, op *operation.PinningOperation, cb progress.PinProgressCB) error {
	ctx, span := cm.tracer.Start(ctx, "doPinning")
	defer span.End()

	// remove replacement async - move this out
	if op.Replace > 0 {
		go func() {
			if err := cm.RemoveContent(ctx, op.Replace, true); err != nil {
				cm.log.Infof("failed to remove content in replacement: %d with: %d", op.Replace, op.ContId)
			}
		}()
	}

	prs := operation.UnSerializePeers(op.Peers)
	for _, pi := range prs {
		if err := cm.node.Host.Connect(ctx, *pi); err != nil {
			cm.log.Warnf("failed to connect to origin node for pinning operation: %s", err)
		}
	}

	bserv := blockservice.New(&cm.node.Blockstore, cm.node.Bitswap)
	dserv := merkledag.NewDAGService(bserv)
	dsess := dserv.Session(ctx)

	if err := cm.AddDatabaseTrackingToContent(ctx, op.ContId, dsess, op.Obj, cb); err != nil {
		return err
	}

	if op.MakeDeal {
		cm.ToCheck(op.ContId)
	}

	// this provide call goes out immediately
	if err := cm.node.FullRT.Provide(ctx, op.Obj, true); err != nil {
		cm.log.Warnf("provider broadcast failed: %s", err)
	}

	// this one adds to a queue
	if err := cm.node.Provider.Provide(op.Obj); err != nil {
		cm.log.Warnf("providing failed: %s", err)
	}
	return nil
}
