package contentmgr

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/application-research/estuary/collections"
	"github.com/application-research/estuary/constants"
	"github.com/application-research/estuary/model"
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
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

func (m *manager) PinStatus(cont util.Content, origins []*peer.AddrInfo) (*types.IpfsPinStatusResponse, error) {
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

func (m *manager) PinDelegatesForContent(cont util.Content) []string {
	out := make([]string, 0)

	if cont.Location == constants.ContentLocationLocal {
		for _, a := range m.node.Host.Addrs() {
			out = append(out, fmt.Sprintf("%s/p2p/%s", a, m.node.Host.ID()))
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

func (m *manager) PinContent(ctx context.Context, user uint, obj cid.Cid, filename string, cols []*collections.CollectionRef, origins []*peer.AddrInfo, replaceID uint, meta map[string]interface{}, makeDeal bool) (*types.IpfsPinStatusResponse, *operation.PinningOperation, error) {
	loc, err := m.shuttleMgr.GetLocationForStorage(ctx, obj, user)
	if err != nil {
		return nil, nil, xerrors.Errorf("selecting location for content failed: %w", err)
	}

	if replaceID > 0 {
		// mark as replace since it will removed and so it should not be fetched anymore
		if err := m.db.Model(&util.Content{}).Where("id = ?", replaceID).Update("replace", true).Error; err != nil {
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
		Replication: m.cfg.Replication,
		Pinning:     true,
		PinMeta:     metaStr,
		Location:    loc,
		Origins:     originsStr,
	}
	if err := m.db.Create(&cont).Error; err != nil {
		return nil, nil, err
	}

	if len(cols) > 0 {
		for _, c := range cols {
			c.Content = cont.ID
		}

		if err := m.db.Clauses(clause.OnConflict{
			Columns:   []clause.Column{{Name: "path"}, {Name: "collection"}},
			DoUpdates: clause.AssignmentColumns([]string{"created_at", "content"}),
		}).Create(cols).Error; err != nil {
			return nil, nil, err
		}
	}

	var pinOp *operation.PinningOperation
	if loc == constants.ContentLocationLocal {
		pinOp = m.GetPinOperation(cont, origins, replaceID, makeDeal)
	} else {
		if err := m.shuttleMgr.PinContent(ctx, loc, cont, origins); err != nil {
			return nil, nil, err
		}
	}

	ipfsRes, err := m.PinStatus(cont, origins)
	if err != nil {
		return nil, nil, err
	}
	return ipfsRes, pinOp, nil
}

func (m *manager) GetPinOperation(cont util.Content, peers []*peer.AddrInfo, replaceID uint, makeDeal bool) *operation.PinningOperation {
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
func (m *manager) UpdatePinStatus(contID uint, location string, status types.PinningStatus) error {
	if status == types.PinningStatusFailed {
		m.log.Debugf("updating pin: %d, status: %s, loc: %s", contID, status, location)

		var c util.Content
		if err := m.db.First(&c, "id = ?", contID).Error; err != nil {
			if !xerrors.Is(err, gorm.ErrRecordNotFound) {
				return xerrors.Errorf("failed to look up content: %d (location = %s): %w", contID, location, err)
			}
			m.log.Warnf("content: %d not found for pin update from location: %s", contID, location)
			return nil
		}

		// if content is already active, ignore it
		if c.Active {
			return nil
		}

		// if an aggregate zone is failing, zone is stuck
		// TODO - revisit this later if it is actually happening
		if c.Aggregate {
			m.log.Warnf("zone: %d is stuck, failed to aggregate(pin) on location: %s", c.ID, location)

			return m.db.Model(model.StagingZone{}).Where("id = ?", contID).UpdateColumns(map[string]interface{}{
				"status":  model.ZoneStatusStuck,
				"message": model.ZoneMessageStuck,
			}).Error
		}

		return m.db.Transaction(func(tx *gorm.DB) error {
			if err := m.db.Model(util.Content{}).Where("id = ?", contID).UpdateColumns(map[string]interface{}{
				"active":        false,
				"pinning":       false,
				"failed":        true,
				"aggregated_in": 0, // reset, so if it was in a staging zone, the zone can consolidate without it
			}).Error; err != nil {
				m.log.Errorf("failed to mark content as failed in database: %s", err)
				return err
			}

			// deduct from the zone, so new content can be added, this way we get consistent size for aggregation
			// we did not reset the flag so that consolidation will not be reattempted by the worker
			if c.AggregatedIn > 0 {
				return tx.Raw("UPDATE staging_zones SET size = size - ? WHERE cont_id = ? ", c.Size, contID).Error
			}
			return nil
		})
	}
	return nil
}

func (m *manager) DoPinning(ctx context.Context, op *operation.PinningOperation, cb progress.PinProgressCB) error {
	ctx, span := m.tracer.Start(ctx, "doPinning")
	defer span.End()

	// remove replacement async - move this out
	if op.Replace > 0 {
		go func() {
			if err := m.RemoveContent(ctx, op.Replace, true); err != nil {
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
		if err := m.node.Host.Connect(ctx, *pi); err != nil {
			m.log.Warnf("failed to connect to origin node for pinning operation: %s", err)
		}
	}

	bserv := blockservice.New(m.node.Blockstore, m.node.Bitswap)
	dserv := merkledag.NewDAGService(bserv)
	dsess := dserv.Session(ctx)

	if err := m.AddDatabaseTrackingToContent(ctx, c, dsess, op.Obj, cb); err != nil {
		return err
	}

	// this provide call goes out immediately
	if err := m.node.FullRT.Provide(ctx, op.Obj, true); err != nil {
		m.log.Warnf("provider broadcast failed: %s", err)
	}

	// this one adds to a queue
	if err := m.node.Provider.Provide(op.Obj); err != nil {
		m.log.Warnf("providing failed: %s", err)
	}
	return nil
}
