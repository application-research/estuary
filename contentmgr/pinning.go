package contentmgr

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"

	"github.com/application-research/estuary/collections"
	"github.com/application-research/estuary/constants"
	"github.com/application-research/estuary/drpc"
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
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
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
	loc, err := cm.selectLocationForContent(ctx, obj, user)
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
		if err := cm.PinContentOnShuttle(ctx, cont, origins, replaceID, loc, makeDeal); err != nil {
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

func (cm *ContentManager) PinContentOnShuttle(ctx context.Context, cont util.Content, peers []*peer.AddrInfo, replaceID uint, handle string, makeDeal bool) error {
	ctx, span := cm.tracer.Start(ctx, "pinContentOnShuttle", trace.WithAttributes(
		attribute.String("handle", handle),
		attribute.String("CID", cont.Cid.CID.String()),
	))
	defer span.End()

	return cm.SendShuttleCommand(ctx, handle, &drpc.Command{
		Op: drpc.CMD_AddPin,
		Params: drpc.CmdParams{
			AddPin: &drpc.AddPin{
				DBID:   cont.ID,
				UserId: cont.UserID,
				Cid:    cont.Cid.CID,
				Peers:  peers,
			},
		},
	})
}

func (cm *ContentManager) selectLocationForContent(ctx context.Context, obj cid.Cid, uid uint) (string, error) {
	ctx, span := cm.tracer.Start(ctx, "selectLocation")
	defer span.End()

	allShuttlesLowSpace := true
	lowSpace := make(map[string]bool)
	var activeShuttles []string
	cm.ShuttlesLk.Lock()
	for d, sh := range cm.Shuttles {
		if !sh.private && !sh.ContentAddingDisabled {
			lowSpace[d] = sh.spaceLow
			activeShuttles = append(activeShuttles, d)
		} else {
			allShuttlesLowSpace = false
		}
	}
	cm.ShuttlesLk.Unlock()

	var shuttles []model.Shuttle
	if err := cm.db.Order("priority desc").Find(&shuttles, "handle in ? and open", activeShuttles).Error; err != nil {
		return "", err
	}

	// prefer shuttles that are not low on blockstore space
	sort.SliceStable(shuttles, func(i, j int) bool {
		lsI := lowSpace[shuttles[i].Handle]
		lsJ := lowSpace[shuttles[j].Handle]

		if lsI == lsJ {
			return false
		}

		return lsJ
	})

	if len(shuttles) == 0 {
		if cm.cfg.Content.DisableLocalAdding {
			return "", fmt.Errorf("no shuttles available and local content adding disabled")
		}
		return constants.ContentLocationLocal, nil
	}

	// TODO: take into account existing staging zones and their primary
	// locations while choosing
	ploc := cm.primaryStagingLocation(ctx, uid)
	if ploc == "" {
		cm.log.Warnf("empty staging zone set for user %d", uid)
	}

	if ploc != "" {
		if allShuttlesLowSpace || !lowSpace[ploc] {
			for _, sh := range shuttles {
				if sh.Handle == ploc {
					return ploc, nil
				}
			}
		}

		// TODO: maybe we should just assign the pin to the preferred shuttle
		// anyways, this could be the case where theres a small amount of
		// downtime from rebooting or something
		cm.log.Warnf("preferred shuttle %q not online", ploc)
	}
	// since they are ordered by priority, just take the first
	return shuttles[0].Handle, nil
}

func (cm *ContentManager) selectLocationForRetrieval(ctx context.Context, cont util.Content) (string, error) {
	_, span := cm.tracer.Start(ctx, "selectLocationForRetrieval")
	defer span.End()

	var activeShuttles []string
	cm.ShuttlesLk.Lock()
	for d, sh := range cm.Shuttles {
		if !sh.private {
			activeShuttles = append(activeShuttles, d)
		}
	}
	cm.ShuttlesLk.Unlock()

	var shuttles []model.Shuttle
	if err := cm.db.Order("priority desc").Find(&shuttles, "handle in ? and open", activeShuttles).Error; err != nil {
		return "", err
	}

	if len(shuttles) == 0 {
		if cm.cfg.Content.DisableLocalAdding {
			return "", fmt.Errorf("no shuttles available and local content adding disabled")
		}
		return constants.ContentLocationLocal, nil
	}

	// prefer the shuttle the content is already on
	for _, sh := range shuttles {
		if sh.Handle == cont.Location {
			return sh.Handle, nil
		}
	}
	// since they are ordered by priority, just take the first
	return shuttles[0].Handle, nil
}

// even though there are 4 pin statuses, queued, pinning, pinned and failed
// the UpdatePinStatus only changes DB state for failed status
// when the content was added, status = pinning
// when the pin process is complete, status = pinned
func (cm *ContentManager) UpdatePinStatus(location string, contID uint, status types.PinningStatus) error {
	if status == types.PinningStatusFailed {
		var c util.Content
		if err := cm.db.First(&c, "id = ?", contID).Error; err != nil {
			return errors.Wrap(err, "failed to look up content")
		}

		if c.Active {
			return fmt.Errorf("got failed pin status message from location: %s where content(%d) was already active, refusing to do anything", location, contID)
		}

		if c.AggregatedIn > 0 {
			// unmark zone as consolidating if a staged content fails to pin
			cm.MarkFinishedConsolidating(c.AggregatedIn)
		}

		if err := cm.db.Model(util.Content{}).Where("id = ?", contID).UpdateColumns(map[string]interface{}{
			"active":  false,
			"pinning": false,
			"failed":  true,
			// TODO: consider if we should not clear aggregated_in, but instead filter for not failed contents when aggregating
			"aggregated_in": 0, // remove from staging zone so the zone can consolidate without it
		}).Error; err != nil {
			cm.log.Errorf("failed to mark content as failed in database: %s", err)
		}
	}
	return nil
}

func (cm *ContentManager) handlePinningComplete(ctx context.Context, handle string, pincomp *drpc.PinComplete) error {
	ctx, span := cm.tracer.Start(ctx, "handlePinningComplete")
	defer span.End()

	var cont util.Content
	if err := cm.db.First(&cont, "id = ?", pincomp.DBID).Error; err != nil {
		return xerrors.Errorf("got shuttle pin complete for unknown content %d (shuttle = %s): %w", pincomp.DBID, handle, err)
	}

	if cont.Active {
		// content already active, no need to add objects, just update location
		// this is used by consolidated contents
		if err := cm.db.Model(util.Content{}).Where("id = ?", cont.ID).UpdateColumns(map[string]interface{}{
			"pinning":  false,
			"location": handle,
		}).Error; err != nil {
			return err
		}
		return nil
	}

	if cont.Aggregate {
		// this is used by staging content aggregate
		if len(pincomp.Objects) != 1 {
			return fmt.Errorf("aggregate has more than 1 objects")
		}

		obj := &util.Object{
			Cid:  util.DbCID{CID: pincomp.Objects[0].Cid},
			Size: pincomp.Objects[0].Size,
		}
		if err := cm.db.Create(obj).Error; err != nil {
			return xerrors.Errorf("failed to create Object: %w", err)
		}

		if err := cm.db.Create(&util.ObjRef{
			Content: cont.ID,
			Object:  obj.ID,
		}).Error; err != nil {
			return xerrors.Errorf("failed to create Object reference: %w", err)
		}

		if err := cm.db.Model(util.Content{}).Where("id = ?", cont.ID).UpdateColumns(map[string]interface{}{
			"active":   true,
			"pinning":  false,
			"location": handle,
			"cid":      util.DbCID{CID: pincomp.CID},
			"size":     pincomp.Size,
		}).Error; err != nil {
			return xerrors.Errorf("failed to update content in database: %w", err)
		}

		// if the content is a consolidated aggregate, it means aggregation has been completed and we can mark as finished
		cm.MarkFinishedAggregating(cont.ID)

		// after aggregate is done, make deal for it
		cm.ToCheck(cont.ID)
		return nil
	}

	objects := make([]*util.Object, 0, len(pincomp.Objects))
	for _, o := range pincomp.Objects {
		objects = append(objects, &util.Object{
			Cid:  util.DbCID{CID: o.Cid},
			Size: o.Size,
		})
	}

	if err := cm.addObjectsToDatabase(ctx, pincomp.DBID, objects, handle); err != nil {
		return xerrors.Errorf("failed to add objects to database: %w", err)
	}

	cm.ToCheck(cont.ID)
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

func (cm *ContentManager) PinStatusFunc(contID uint, location string, status types.PinningStatus) error {
	if status == types.PinningStatusFailed {
		cm.log.Debugf("updating pin: %d, status: %s, loc: %s", contID, status, location)

		return cm.UpdatePinStatus(location, contID, status)
	}
	return nil
}
