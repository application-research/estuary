package contentmgr

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/application-research/estuary/constants"
	"github.com/application-research/estuary/util"
	"github.com/ipfs/go-blockservice"
	ipld "github.com/ipfs/go-ipld-format"
	"github.com/ipfs/go-merkledag"
	uio "github.com/ipfs/go-unixfs/io"
	"github.com/pkg/errors"
	"golang.org/x/xerrors"
	"gorm.io/gorm"
)

type ContentStagingZone struct {
	ZoneOpened      time.Time      `json:"zoneOpened"`
	Contents        []util.Content `json:"contents"`
	MinSize         int64          `json:"minSize"`
	MaxSize         int64          `json:"maxSize"`
	CurSize         int64          `json:"curSize"`
	User            uint           `json:"user"`
	ContID          uint           `json:"contentID"`
	Location        string         `json:"location"`
	IsConsolidating bool           `json:"isConsolidating"`
	lk              sync.Mutex
}

func (cm *ContentManager) newContentStagingZone(user uint, loc string) (*util.Content, error) {
	content := &util.Content{
		Size:        0,
		Name:        "aggregate",
		Active:      false,
		Pinning:     true,
		UserID:      user,
		Replication: cm.cfg.Replication,
		Aggregate:   true,
		Location:    loc,
	}

	if err := cm.db.Create(content).Error; err != nil {
		return nil, err
	}
	return content, nil
}

func (cm *ContentManager) tryAddContent(zone util.Content, c util.Content) (bool, error) {
	// if this bucket is being consolidated or aggregated, do not add anymore content
	if cm.IsZoneConsolidating(zone.ID) || cm.IsZoneAggregating(zone.ID) {
		return false, nil
	}

	err := cm.db.Transaction(func(tx *gorm.DB) error {
		if err := tx.Model(util.Content{}).
			Where("id = ?", c.ID).
			UpdateColumn("aggregated_in", zone.ID).Error; err != nil {
			return err
		}

		if err := tx.Model(util.Content{}).
			Where("id = ?", zone.ID).
			UpdateColumn("size", gorm.Expr("size + ?", c.Size)).Error; err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return false, err
	}
	return true, nil
}

// tryRemoveContent Removes content from in-memory buckets
// Assumes content is already removed from DB
func (cm *ContentManager) tryRemoveContent(cb *ContentStagingZone, c util.Content) (bool, error) {
	cb.lk.Lock()
	defer cb.lk.Unlock()

	// if this bucket is being consolidated, do not remove content
	if cb.IsConsolidating {
		return false, nil
	}

	newContents := make([]util.Content, 0)
	newSize := int64(0)
	for _, cont := range cb.Contents {
		if cont.ID != c.ID {
			newContents = append(newContents, cont)
			newSize += cont.Size
		}
	}
	cb.Contents = newContents
	cb.CurSize = newSize

	return true, nil
}

func (cm *ContentManager) runStagingBucketWorker(ctx context.Context) {
	timer := time.NewTicker(cm.cfg.StagingBucket.AggregateInterval)
	for {
		select {
		case <-timer.C:
			cm.log.Debugw("content check queue", "length", cm.queueMgr.Len(), "nextEvent", cm.queueMgr.NextEvent())

			readyZones, err := cm.getReadyStagingZones()
			if err != nil {
				cm.log.Errorf("failed to get ready staging zones: %s", err)
				continue
			}

			cm.log.Debugf("found ready staging zones: %d", len(readyZones))
			for _, z := range readyZones {
				// if this zone is no longer pinning(meaning it has been pinned), activate it
				if !z.Pinning && !z.Active {
					if err := cm.db.Model(util.Content{}).Where("id = ?", z.ID).UpdateColumn("active", true).Error; err != nil {
						cm.log.Errorf("failed to get ready staging zones: %s", err)
					} else {
						// aggregation has been completed and we can mark as finished
						cm.MarkFinishedAggregating(z.ID)
						// after aggregate is done, make deal for it
						cm.queueMgr.ToCheck(z.ID)
					}
					continue
				}

				if err := cm.processStagingZone(ctx, z); err != nil {
					cm.log.Errorf("content aggregation failed (zone %d): %s", z.ID, err)
					continue
				}
			}
		}
	}
}

// getReadyStagingZones gets zones that are not activated yet (but could have been pinned)
func (cm *ContentManager) getReadyStagingZones() ([]util.Content, error) {
	var readyZones []util.Content
	var readyZonesBatch []util.Content
	if err := cm.db.Model(&util.Content{}).
		Where("not active and aggregate and size >= ?", constants.MinDealContentSize).
		FindInBatches(&readyZonesBatch, 500, func(tx *gorm.DB, batch int) error {
			readyZones = append(readyZones, readyZonesBatch...)
			return nil
		}).Error; err != nil {
		return nil, err
	}
	return readyZones, nil
}

// if content is not already staged, if it is below min deal content size, and staging zone is enabled
func (cm *ContentManager) canStageContent(cont util.Content) bool {
	return !cont.Aggregate && cont.Size < cm.cfg.Content.MinSize && cm.cfg.StagingBucket.Enabled
}

func (cm *ContentManager) setUpStaging(ctx context.Context) {
	// if staging buckets are enabled, run the bucket aggregate worker
	if cm.cfg.StagingBucket.Enabled {
		// recomputing staging zone sizes
		if err := cm.recomputeStagingZoneSizes(); err != nil {
			cm.log.Errorf("failed to recompute staging zone sizes: %s", err)
		}
		// run the staging bucket aggregator worker
		go cm.runStagingBucketWorker(ctx)
		cm.log.Infof("spun up staging bucket worker")
	}
}

func (cm *ContentManager) IsZoneConsolidating(zoneID uint) bool {
	cm.consolidatingZonesLk.Lock()
	defer cm.consolidatingZonesLk.Unlock()
	return cm.consolidatingZones[zoneID]
}

func (cm *ContentManager) IsZoneAggregating(zoneID uint) bool {
	cm.aggregatingZonesLk.Lock()
	defer cm.aggregatingZonesLk.Unlock()
	return cm.aggregatingZones[zoneID]
}

func (cm *ContentManager) MarkStartedConsolidating(zoneID uint) bool {
	cm.consolidatingZonesLk.Lock()
	defer cm.consolidatingZonesLk.Unlock()
	if cm.consolidatingZones[zoneID] {
		// skip since it is already processing
		return false
	}
	cm.consolidatingZones[zoneID] = true
	return true
}

func (cm *ContentManager) MarkFinishedConsolidating(zoneID uint) {
	cm.consolidatingZonesLk.Lock()
	delete(cm.consolidatingZones, zoneID)
	cm.consolidatingZonesLk.Unlock()
}

func (cm *ContentManager) MarkStartedAggregating(zoneID uint) bool {
	cm.aggregatingZonesLk.Lock()
	defer cm.aggregatingZonesLk.Unlock()
	if cm.aggregatingZones[zoneID] {
		// skip since it is already processing
		return false
	}
	cm.aggregatingZones[zoneID] = true
	return true
}

func (cm *ContentManager) MarkFinishedAggregating(zoneID uint) {
	cm.aggregatingZonesLk.Lock()
	delete(cm.aggregatingZones, zoneID)
	cm.aggregatingZonesLk.Unlock()
}

func (cm *ContentManager) processStagingZone(ctx context.Context, zone util.Content) error {
	ctx, span := cm.tracer.Start(ctx, "aggregateContent")
	defer span.End()

	var grpLocs []string
	if err := cm.db.Model(&util.Content{}).Where("active and aggregated_in = ?", zone.ID).Distinct().Pluck("location", &grpLocs).Error; err != nil {
		return err
	}

	if len(grpLocs) == 0 {
		return fmt.Errorf("no location for staged contents")
	}

	// if the staged contents of this bucket are in different locations (more than 1 group)
	// Need to migrate/consolidate the contents to the same location
	// TODO - we should avoid doing this, best we have staging by location - this process is just to expensive
	if len(grpLocs) > 1 {
		// Need to migrate content all to the same shuttle
		// Only attempt consolidation on a zone if one is not ongoing, prevents re-consolidation request

		// should never be aggregating here but check anyways
		if cm.IsZoneAggregating(zone.ID) || !cm.MarkStartedConsolidating(zone.ID) {
			// skip if it is aggregating or already consolidating
			return nil
		}

		go func() {
			// sometimes this call just ends in sending the take content cmd
			if err := cm.consolidateStagedContent(ctx, zone); err != nil {
				cm.log.Errorf("failed to consolidate staged content: %s", err)
			}
		}()
		return nil
	}

	// if we reached here, consolidation is done and we can move to aggregating
	cm.MarkFinishedConsolidating(zone.ID)

	loc := grpLocs[0]

	if !cm.MarkStartedAggregating(zone.ID) {
		// skip if zone is consolidating or already aggregating
		return nil
	}
	// if all contents are already in one location, proceed to aggregate them
	return cm.AggregateStagingZone(ctx, zone, loc)
}

func (cm *ContentManager) consolidateStagedContent(ctx context.Context, zone util.Content) error {
	var dstLocation string
	var curMax int64
	dataByLoc := make(map[string]int64)
	contentByLoc, err := cm.getStagedContentsGroupedByLocation(ctx, zone.ID)
	if err != nil {
		return err
	}

	for loc, contents := range contentByLoc {
		var ntot int64
		for _, c := range contents {
			ntot = dataByLoc[loc] + c.Size
			dataByLoc[loc] = ntot
		}

		// temp: dont ever migrate content back to primary instance for aggregation, always prefer elsewhere
		if loc == constants.ContentLocationLocal {
			continue
		}

		if ntot > curMax && cm.shuttleMgr.CanAddContent(loc) {
			curMax = ntot
			dstLocation = loc
		}
	}

	//TODO (next pr) - update zone status
	if dstLocation == "" {
		return fmt.Errorf("zone: %d failed to consolidate as no destination location could be determined", zone.ID)
	}

	// okay, move everything to 'primary'
	var toMove []util.Content
	for loc, conts := range contentByLoc {
		if loc != dstLocation {
			toMove = append(toMove, conts...)
		}
	}

	cm.log.Debugw("consolidating content to single location for aggregation", "user", zone.UserID, "dstLocation", dstLocation, "numItems", len(toMove), "primaryWeight", curMax)
	if dstLocation == constants.ContentLocationLocal {
		return cm.migrateContentsToLocalNode(ctx, toMove)
	}
	return cm.shuttleMgr.ConsolidateContent(ctx, dstLocation, toMove)
}

// AggregateStagingZone assumes zone is already in consolidatingZones
func (cm *ContentManager) AggregateStagingZone(ctx context.Context, zone util.Content, loc string) error {
	ctx, span := cm.tracer.Start(ctx, "aggregateStagingZone")
	defer span.End()

	cm.log.Debugf("aggregating zone: %d", zone.ID)

	var zoneContents []util.Content
	var zoneContentsBatch []util.Content
	if err := cm.db.Where("aggregated_in = ?", zone.ID).FindInBatches(&zoneContentsBatch, 500, func(tx *gorm.DB, batch int) error {
		zoneContents = append(zoneContents, zoneContentsBatch...)
		return nil
	}).Error; err != nil {
		return err
	}

	if loc == constants.ContentLocationLocal {
		dir, err := cm.CreateAggregate(ctx, zoneContents)
		if err != nil {
			return xerrors.Errorf("failed to create aggregate: %w", err)
		}

		ncid := dir.Cid()
		size, err := dir.Size()
		if err != nil {
			return err
		}

		if size == 0 {
			cm.log.Warnf("content %d aggregate dir apparent size is zero", zone.ID)
		}

		obj := &util.Object{
			Cid:  util.DbCID{CID: ncid},
			Size: int(size),
		}
		if err := cm.db.Create(obj).Error; err != nil {
			return err
		}

		if err := cm.db.Create(&util.ObjRef{
			Content: zone.ID,
			Object:  obj.ID,
		}).Error; err != nil {
			return err
		}

		if err := cm.blockstore.Put(ctx, dir); err != nil {
			return err
		}

		if err := cm.db.Model(util.Content{}).Where("id = ?", zone.ID).UpdateColumns(map[string]interface{}{
			"active":   true,
			"pinning":  false,
			"cid":      util.DbCID{CID: ncid},
			"size":     size,
			"location": loc,
		}).Error; err != nil {
			return err
		}

		go func() {
			cm.queueMgr.ToCheck(zone.ID)
		}()

		cm.MarkFinishedAggregating(zone.ID)
		return nil
	}

	// handle aggregate on shuttle
	return cm.shuttleMgr.AggregateContent(ctx, loc, zone, zoneContents)
}

func (cm *ContentManager) CreateAggregate(ctx context.Context, conts []util.Content) (ipld.Node, error) {
	cm.log.Debug("aggregating contents in staging zone into new content")

	bserv := blockservice.New(cm.blockstore, cm.node.Bitswap)
	dserv := merkledag.NewDAGService(bserv)

	sort.Slice(conts, func(i, j int) bool {
		return conts[i].ID < conts[j].ID
	})

	dir := uio.NewDirectory(dserv)
	for _, c := range conts {
		nd, err := dserv.Get(ctx, c.Cid.CID)
		if err != nil {
			return nil, err
		}

		// TODO: consider removing the "<cid>-" prefix on the content name
		err = dir.AddChild(ctx, fmt.Sprintf("%d-%s", c.ID, c.Name), nd)
		if err != nil {
			return nil, err
		}
	}
	dirNd, err := dir.GetNode()
	if err != nil {
		return nil, err
	}
	return dirNd, nil
}

type zoneSize struct {
	ID   uint
	Size int64
}

func (cm *ContentManager) recomputeStagingZoneSizes() error {
	cm.log.Info("recomputing staging zone sizes .......")

	var storedZoneSizes []zoneSize
	var storedZoneSizesBatch []zoneSize
	if err := cm.db.Model(&util.Content{}).
		Where("not active and pinning and aggregate and size = 0").
		Select("id, size").
		FindInBatches(&storedZoneSizesBatch, 500, func(tx *gorm.DB, batch int) error {
			storedZoneSizes = append(storedZoneSizes, storedZoneSizesBatch...)
			return nil
		}).Error; err != nil {
		return err
	}

	if len(storedZoneSizes) > 0 {
		var zoneIDs []uint
		for _, zone := range storedZoneSizes {
			zoneIDs = append(zoneIDs, zone.ID)
		}

		var actualZoneSizes []zoneSize
		if err := cm.db.Model(&util.Content{}).
			Where("aggregated_in IN ?", zoneIDs).
			Select("aggregated_in, sum(size) as zoneSize").
			Group("aggregated_in").
			Select("aggregated_in as id, sum(size) as size").
			Find(&actualZoneSizes).Error; err != nil {
			return err
		}

		var zoneToStoredSize = make(map[uint]int64)
		var toUpdate = make(map[uint]int64)
		for _, zone := range storedZoneSizes {
			zoneToStoredSize[zone.ID] = zone.Size
		}

		for _, zone := range actualZoneSizes {
			storedSize := zoneToStoredSize[zone.ID]
			if zone.Size != storedSize {
				toUpdate[zone.ID] = zone.Size
			}
		}

		for id, size := range toUpdate {
			if err := cm.db.Model(util.Content{}).
				Where("id = ?", id).
				UpdateColumn("size", size).Error; err != nil {
				return err
			}
		}
		cm.log.Infof("completed recomputing staging zone sizes, %d updates made", len(toUpdate))
	} else {
		cm.log.Infof("no staging zones to recompute")
	}
	return nil
}

// getStagedContentsGroupedByLocation gets the active(pin) contents only for aggregation and consolidation
func (cm *ContentManager) getStagedContentsGroupedByLocation(ctx context.Context, zoneID uint) (map[string][]util.Content, error) {
	var conts []util.Content
	var contsBatch []util.Content
	if err := cm.db.Where("active and aggregated_in = ?", zoneID, 5, nil).FindInBatches(&contsBatch, 500, func(tx *gorm.DB, batch int) error {
		conts = append(conts, contsBatch...)
		return nil
	}).Error; err != nil {
		return nil, err
	}

	out := make(map[string][]util.Content)
	for _, c := range conts {
		out[c.Location] = append(out[c.Location], c)
	}

	if len(out) == 0 {
		return nil, fmt.Errorf("no location for staged contents")
	}
	return out, nil
}

func (cm *ContentManager) buildStagingZoneFromContent(zone util.Content) (*ContentStagingZone, error) {
	var contents []util.Content
	var contentsBatch []util.Content
	if err := cm.db.Where("active and aggregated_in = ?", zone.ID).FindInBatches(&contentsBatch, 500, func(tx *gorm.DB, batch int) error {
		contents = append(contents, contentsBatch...)
		return nil
	}).Error; err != nil {
		return nil, errors.Wrapf(err, "could not build ContentStagingZone struct from content id")
	}

	var zSize int64
	for _, c := range contents {
		zSize += c.Size
	}

	return &ContentStagingZone{
		ZoneOpened: zone.CreatedAt,
		Contents:   contents,
		MinSize:    constants.MinDealContentSize,
		MaxSize:    constants.MaxDealContentSize,
		CurSize:    zSize,
		User:       zone.UserID,
		ContID:     zone.ID,
		Location:   zone.Location,
	}, nil
}

func (cm *ContentManager) GetStagingZonesForUser(ctx context.Context, user uint) []*ContentStagingZone {
	var zones []util.Content
	var zonesBatch []util.Content
	if err := cm.db.Where("not active and pinning and aggregate and user_id = ?", user).FindInBatches(&zonesBatch, 500, func(tx *gorm.DB, batch int) error {
		zones = append(zones, zonesBatch...)
		return nil
	}).Error; err != nil {
		return nil
	}

	var out []*ContentStagingZone
	for _, zone := range zones {
		stagingZone, err := cm.buildStagingZoneFromContent(zone)
		if err != nil {
			continue
		}
		out = append(out, stagingZone)
	}
	return out
}

func (cm *ContentManager) GetStagingZoneSnapshot(ctx context.Context) map[uint][]*ContentStagingZone {
	var zones []util.Content
	var zonesBatch []util.Content
	if err := cm.db.Where("not active and pinning and aggregate").FindInBatches(&zonesBatch, 500, func(tx *gorm.DB, batch int) error {
		zones = append(zones, zonesBatch...)
		return nil
	}).Error; err != nil {
		return nil
	}

	out := make(map[uint][]*ContentStagingZone)
	for _, zone := range zones {
		stagingZone, err := cm.buildStagingZoneFromContent(zone)
		if err != nil {
			continue
		}
		out[zone.UserID] = append(out[zone.UserID], stagingZone)
	}
	return out
}

func (cm *ContentManager) addContentToStagingZone(ctx context.Context, content util.Content) error {
	_, span := cm.tracer.Start(ctx, "stageContent")
	defer span.End()

	cm.addStagingContentLk.Lock()
	defer cm.addStagingContentLk.Unlock()

	if content.AggregatedIn > 0 {
		cm.log.Warnf("attempted to add content to staging zone that was already staged: %d (is in %d)", content.ID, content.AggregatedIn)
		return nil
	}

	cm.log.Debugf("adding content to staging zone: %d", content.ID)

	// TODO: move processing state into DB, use FirstOrInit here, also filter for not processing
	// theoretically any user only needs to have up to one non-processing zone at a time
	var zones []util.Content
	var zonesBatch []util.Content
	if err := cm.db.Where("not active and pinning and aggregate and user_id = ? and size + ? <= ?", content.UserID, content.Size, constants.MaxDealContentSize).FindInBatches(&zonesBatch, 500, func(tx *gorm.DB, batch int) error {
		zones = append(zones, zonesBatch...)
		return nil
	}).Error; err != nil {
		return nil
	}

	if len(zones) == 0 {
		zone, err := cm.newContentStagingZone(content.UserID, content.Location)
		if err != nil {
			return fmt.Errorf("failed to create new staging zone content: %w", err)
		}

		_, err = cm.tryAddContent(*zone, content)
		if err != nil {
			return fmt.Errorf("failed to add content to staging zone: %w", err)
		}
		return nil
	}

	for _, zone := range zones {
		ok, err := cm.tryAddContent(zone, content)
		if err != nil {
			return err
		}

		if ok {
			return nil
		}
	}

	zone, err := cm.newContentStagingZone(content.UserID, content.Location)
	if err != nil {
		return err
	}

	_, err = cm.tryAddContent(*zone, content)
	if err != nil {
		return err
	}
	return nil
}
