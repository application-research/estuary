package contentmgr

import (
	"context"
	"fmt"
	"sort"
	"time"

	pinningtypes "github.com/application-research/estuary/pinner/types"

	"github.com/application-research/estuary/constants"
	"github.com/application-research/estuary/model"
	"github.com/application-research/estuary/util"
	"github.com/ipfs/go-blockservice"
	ipld "github.com/ipfs/go-ipld-format"
	"github.com/ipfs/go-merkledag"
	uio "github.com/ipfs/go-unixfs/io"
	"github.com/pkg/errors"
	"golang.org/x/xerrors"
	"gorm.io/gorm"
)

var ErrZoneCouldNotBeClaimed = fmt.Errorf("zone could not be claimed")

func (cm *ContentManager) getStagingZoneTrackerLastContentID() (*model.StagingZoneTracker, error) {
	var trks []*model.StagingZoneTracker
	if err := cm.db.Find(&trks).Error; err != nil {
		return nil, err
	}

	if len(trks) == 0 {
		// for the first time it will be empty
		trk := &model.StagingZoneTracker{LastContID: 0}
		if err := cm.db.Create(&trk).Error; err != nil {
			return nil, err
		}
		return trk, nil
	}
	return trks[0], nil
}

func (cm *ContentManager) addContentsToStagingZones(ctx context.Context, tracker *model.StagingZoneTracker) error {
	var zoneContents []util.Content
	return cm.db.Where("id > ? and size <= ?", tracker.LastContID, cm.cfg.Content.MinSize).Order("id asc").FindInBatches(&zoneContents, 2000, func(tx *gorm.DB, batch int) error {

		cm.log.Infof("trying to stage the next 2000 contents: %d - %d", zoneContents[0].ID, zoneContents[len(zoneContents)-1].ID)

		for _, c := range zoneContents {
			if c.Size == 0 {
				// size = 0 are shuttle contents yet to be updated with their objects sizes, avoid them
				continue
			}

			// for backward compatibility
			if c.Aggregate {
				if c.Active {
					if err := cm.db.Model(model.StagingZone{}).Where("cont_id = ?", c.ID).UpdateColumns(map[string]interface{}{
						"status":     model.ZoneStatusDone,
						"message":    model.ZoneMessageDone,
						"created_at": c.CreatedAt,
					}).Error; err != nil {
						return err
					}
				}
				continue
			}

			// for backward compatibilty
			if c.AggregatedIn > 0 {
				if err := cm.tryAddOldContentToStagingZone(ctx, c, tracker.ID); err != nil {
					return err
				}
				continue
			}

			// after backfill, all new contents going forward
			if err := cm.tryAddNewContentToStagingZone(ctx, c, tracker.ID); err != nil {
				return err
			}
		}
		return nil
	}).Error
}

func (cm *ContentManager) runStagingZoneCreationWorker(ctx context.Context) {
	timer := time.NewTicker(cm.cfg.StagingBucket.CreationInterval)
	for {
		select {
		case <-timer.C:
			cm.log.Debug("running staging zone creation worker")

			lastContID, err := cm.getStagingZoneTrackerLastContentID()
			if err != nil {
				cm.log.Warnf("failed to get staginng zone tracker last content id - %s", err)
				continue
			}

			if err := cm.addContentsToStagingZones(ctx, lastContID); err != nil {
				cm.log.Warnf("failed to add contents to staging zones - %s", err)
				continue
			}
		}
	}
}

func (cm *ContentManager) runStagingZoneAggregationWorker(ctx context.Context) {
	timer := time.NewTicker(cm.cfg.StagingBucket.AggregateInterval)
	for {
		select {
		case <-timer.C:
			cm.log.Debug("running staging zone aggregation worker")

			readyZones, err := cm.getReadyStagingZones()
			if err != nil {
				cm.log.Errorf("failed to get ready staging zones: %s", err)
				continue
			}

			cm.log.Debugf("found: %d ready staging zones", len(readyZones))

			for _, z := range readyZones {
				var zc util.Content
				if err := cm.db.First(&zc, "id = ?", z.ContID).Error; err != nil {
					cm.log.Warnf("zone %d aggregation failed to get zone content %d for processing - %s", z.ID, z.ContID, err)
					continue
				}

				if err := cm.processStagingZone(ctx, zc, z); err != nil {
					cm.log.Errorf("zone aggregation worker failed to process zone: %d - %s", z.ID, err)
					continue
				}
			}
		}
	}
}

// getReadyStagingZones gets zones that are done but have reasonable sizes
func (cm *ContentManager) getReadyStagingZones() ([]*model.StagingZone, error) {
	notReadyStatuses := []model.ZoneStatus{model.ZoneStatusDone, model.ZoneStatusStuck}
	var readyZones []*model.StagingZone
	if err := cm.db.Model(&model.StagingZone{}).Where("size >= ? and status not in ? ", cm.cfg.Content.MinSize, notReadyStatuses).Limit(500).Find(&readyZones).Error; err != nil {
		return nil, err
	}
	return readyZones, nil
}

func (cm *ContentManager) setUpStaging(ctx context.Context) {
	// if staging buckets are enabled, run the bucket aggregate worker
	if cm.cfg.StagingBucket.Enabled {
		cm.log.Infof("starting up staging zone workers")

		// run staging zone creation worker
		go cm.runStagingZoneCreationWorker(ctx)

		// run staging zone aggregation/consoliation worker
		go cm.runStagingZoneAggregationWorker(ctx)

		cm.log.Infof("spun up staging zone workers")
	}
}

func (cm *ContentManager) markZoneStatus(zone *model.StagingZone, upSts model.ZoneStatus, upMgs model.ZoneMessage) (bool, error) {
	result := cm.db.Exec("UPDATE staging_zones SET status = ?, message = ? WHERE id = ? and status <> ?", upSts, upMgs, zone.ID, upSts)
	if result.Error != nil {
		return false, result.Error
	}
	// claim state - a naive lock, using db conditinal write
	return result.RowsAffected == 1, nil
}

func (cm *ContentManager) processStagingZone(ctx context.Context, zoneCont util.Content, zone *model.StagingZone) error {
	ctx, span := cm.tracer.Start(ctx, "aggregateContent")
	defer span.End()

	var grpLocs []string
	if err := cm.db.Model(&util.Content{}).Where("active and aggregated_in = ?", zoneCont.ID).Distinct().Pluck("location", &grpLocs).Error; err != nil {
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

		if canProceed, err := cm.markZoneStatus(zone, model.ZoneStatusConsolidating, model.ZoneMessageConsolidating); err != nil || !canProceed {
			return err
		}

		if err := cm.consolidateStagedContent(ctx, zone.ID, zoneCont); err != nil {
			cm.log.Errorf("failed to consolidate staged content: %s", err)
		}
		return nil
	}
	// if all contents are already in one location, proceed to aggregate them
	return cm.AggregateStagingZone(ctx, zone, zoneCont, grpLocs[0])
}

func (cm *ContentManager) consolidateStagedContent(ctx context.Context, zoneID uint, zoneContent util.Content) error {
	dstLocation, toMove, curMax, err := cm.getContentsAndDestinationLocationForConsolidation(ctx, zoneID, zoneContent.ID)
	if err != nil {
		return err
	}

	cm.log.Debugw("consolidating content to single location for aggregation", "user", zoneContent.UserID, "dstLocation", dstLocation, "numItems", len(toMove), "primaryWeight", curMax)

	if dstLocation == constants.ContentLocationLocal {
		return cm.migrateContentsToLocalNode(ctx, toMove)
	}

	if err := cm.db.Transaction(func(tx *gorm.DB) error {
		// point zone content location to dstLocation
		if err := cm.db.Model(util.Content{}).
			Where("id = ?", zoneContent.ID).
			UpdateColumn("location", dstLocation).Error; err != nil {
			return err
		}

		// point staging zone location to dstLocation
		if err := tx.Model(model.StagingZone{}).
			Where("cont_id = ?", zoneContent.ID).
			UpdateColumn("location", dstLocation).Error; err != nil {
			return err
		}
		return nil
	}); err != nil {
		return err
	}
	return cm.shuttleMgr.ConsolidateContent(ctx, dstLocation, toMove)
}

// AggregateStagingZone assumes zone is already in consolidatingZones
func (cm *ContentManager) AggregateStagingZone(ctx context.Context, zone *model.StagingZone, zoneCont util.Content, loc string) error {
	ctx, span := cm.tracer.Start(ctx, "aggregateStagingZone")
	defer span.End()

	cm.log.Debugf("aggregating zone: %d", zoneCont.ID)

	// skip if zone is already beign aggregated by another process
	if canProceed, err := cm.markZoneStatus(zone, model.ZoneStatuAggregating, model.ZoneMessageAggregating); err != nil || !canProceed {
		cm.log.Debugf("could not proceed to aggregate zone: %d", zone.ID)
		return err
	}

	var zoneContents []util.Content
	var zoneContentsBatch []util.Content
	if err := cm.db.Where("aggregated_in = ?", zoneCont.ID).FindInBatches(&zoneContentsBatch, 500, func(tx *gorm.DB, batch int) error {
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
			cm.log.Warnf("content %d aggregate dir apparent size is zero", zoneCont.ID)
		}

		return cm.db.Transaction(func(tx *gorm.DB) error {
			obj := &util.Object{
				Cid:  util.DbCID{CID: ncid},
				Size: int(size),
			}
			if err := cm.db.Create(obj).Error; err != nil {
				return err
			}

			if err := cm.db.Create(&util.ObjRef{
				Content: zoneCont.ID,
				Object:  obj.ID,
			}).Error; err != nil {
				return err
			}

			if err := cm.blockstore.Put(ctx, dir); err != nil {
				return err
			}

			// mark zone content active
			if err := cm.db.Model(util.Content{}).Where("id = ?", zoneCont.ID).UpdateColumns(map[string]interface{}{
				"active":   true,
				"pinning":  false,
				"cid":      util.DbCID{CID: ncid},
				"location": loc,
			}).Error; err != nil {
				return err
			}

			// mark zone done and ready for deal-making
			if err := tx.Model(model.StagingZone{}).
				Where("cont_id = ?", zoneCont.ID).
				UpdateColumns(map[string]interface{}{
					"status":   model.ZoneStatusDone,
					"message":  model.ZoneMessageDone,
					"location": loc,
				}).Error; err != nil {
				return err
			}

			// for now keep pushing to content queue
			cm.queueMgr.ToCheck(zoneCont.ID, zoneCont.Size)
			return nil
		})
	}
	// handle aggregate on shuttle
	return cm.shuttleMgr.AggregateContent(ctx, loc, zoneCont, zoneContents)
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

func (cm *ContentManager) getContentsAndDestinationLocationForConsolidation(ctx context.Context, zoneID uint, zoneContID uint) (string, []util.Content, int64, error) {
	// first determine the location(destination) to move contents to, that are not in that location over.
	// Do this by checking what location has the largest contents.
	var dstLocation string
	var curMax int64

	dataByLoc := make(map[string]int64)
	var contsBatch []util.Content
	if err := cm.db.Where("active and aggregated_in = ?", zoneContID).FindInBatches(&contsBatch, 500, func(tx *gorm.DB, batch int) error {
		for _, c := range contsBatch {
			dataByLoc[c.Location] = dataByLoc[c.Location] + c.Size

			// temp: dont ever migrate content back to primary instance for aggregation, always prefer elsewhere
			if c.Location == constants.ContentLocationLocal {
				continue
			}

			canAddContent, err := cm.shuttleMgr.CanAddContent(c.Location)
			if err != nil {
				return err
			}

			if dataByLoc[c.Location] > curMax && canAddContent {
				curMax = dataByLoc[c.Location]
				dstLocation = c.Location
			}
		}
		return nil
	}).Error; err != nil {
		return "", nil, 0, err
	}

	if dstLocation == "" {
		return "", nil, 0, fmt.Errorf("zone %d failed to consolidate as no destination location could be determined", zoneID)
	}

	var toMove []util.Content
	var toMoveBatch []util.Content
	if err := cm.db.Where("active and aggregated_in = ?", zoneContID).FindInBatches(&toMoveBatch, 500, func(tx *gorm.DB, batch int) error {
		for _, c := range toMoveBatch {
			if c.Location != dstLocation {
				toMove = append(toMove, c)
			}
		}
		return nil
	}).Error; err != nil {
		return "", nil, 0, err
	}
	return dstLocation, toMove, curMax, nil
}

func (cm *ContentManager) GetStagingZonesForUser(ctx context.Context, userID uint, limit int, offset int) ([]*model.StagingZone, error) {
	var zones []*model.StagingZone
	if err := cm.db.Limit(limit).Offset(offset).Order("created_at desc").Find(&zones, "user_id = ?", userID, userID).Error; err != nil {
		return nil, err
	}
	return zones, nil
}

func (cm *ContentManager) GetStagingZoneWithoutContents(ctx context.Context, userID uint, zoneID uint) (*model.StagingZone, error) {
	var zone *model.StagingZone
	if err := cm.db.First(&zone, "id = ? and user_id = ?", zoneID, userID).Error; err != nil {
		return nil, errors.Wrapf(err, "zone not found or does not belong to user: %d", zoneID)
	}
	return zone, nil
}

func (cm *ContentManager) GetStagingZoneContents(ctx context.Context, user uint, zoneID uint, limit int, offset int) ([]util.Content, error) {
	var zc model.StagingZone
	if err := cm.db.First(&zc, "id = ?", zoneID).Error; err != nil {
		if xerrors.Is(err, gorm.ErrRecordNotFound) {
			return make([]util.Content, 0), nil
		}
		return nil, err
	}

	var zoneConts []util.Content
	if err := cm.db.Limit(limit).Offset(offset).Order("created_at desc").Find(&zoneConts, "active and user_id = ? and aggregated_in = ?", user, zc.ContID).Error; err != nil {
		return nil, errors.Wrapf(err, "could not get contents for staging zone: %d", zc.ID)
	}

	for i, c := range zoneConts {
		zoneConts[i].PinningStatus = string(pinningtypes.GetContentPinningStatus(c))
	}
	return zoneConts, nil
}

func (cm *ContentManager) tryAddOldContentToStagingZone(ctx context.Context, c util.Content, trackerID uint) error {
	_, span := cm.tracer.Start(ctx, "tryAddOldContentToStagingZone")
	defer span.End()

	cm.log.Debugf("adding old content: %d to its staging zone: %d", c.ID, c.AggregatedIn)

	err := cm.db.Transaction(func(tx *gorm.DB) error {
		result := tx.Exec("UPDATE staging_zones SET size = size + ? WHERE cont_id = ?", c.Size, c.AggregatedIn)
		if result.Error != nil {
			return result.Error
		}

		if result.RowsAffected == 1 {
			// we added to a zone, track zone creation last content counter
			return tx.Model(model.StagingZoneTracker{}).Where("id = ?", trackerID).UpdateColumn("last_cont_id", c.ID).Error
		}
		return ErrZoneCouldNotBeClaimed // old zone is probably not created yet
	})
	if err != nil {
		if err == ErrZoneCouldNotBeClaimed { // since we could not claim a zone, create a new one
			return cm.newContentStagingZoneFromContent(c, trackerID)
		}
		return err
	}
	return nil
}

func (cm *ContentManager) tryAddNewContentToStagingZone(ctx context.Context, c util.Content, trackerID uint) error {
	_, span := cm.tracer.Start(ctx, "tryAddNewContentToStagingZones")
	defer span.End()

	cm.log.Debugf("adding new content: %d to a staging zone", c.ID)

	// find available zones
	var openZones []*model.StagingZone
	var openZonesBatch []*model.StagingZone
	if err := cm.db.Where("user_id = ? and size + ? <= ? and status = ?", c.UserID, c.Size, cm.cfg.Content.MaxSize, model.ZoneStatusOpen).FindInBatches(&openZonesBatch, 500, func(tx *gorm.DB, batch int) error {
		openZones = append(openZones, openZonesBatch...)
		return nil
	}).Error; err != nil {
		return err
	}

	for _, zone := range openZones {
		err := cm.db.Transaction(func(tx *gorm.DB) error {
			if err := tx.Model(util.Content{}).
				Where("id = ?", c.ID).
				UpdateColumn("aggregated_in", zone.ContID).Error; err != nil {
				return err
			}

			// if a zone is in consolidating and the size is below min size, it means a content was removed after the content
			// failed to be consolidated. Try to add any content for the same location to move the size above the min size
			// we we make consistent deal content size for all contents even those that previous consolidatione errors
			if zone.Status == model.ZoneStatusConsolidating && c.Location == zone.Location {
				result := tx.Exec("UPDATE staging_zones SET size = size + ? WHERE id = ? and size < ? and status = ?", c.Size, zone.ID, cm.cfg.Content.MaxSize, model.ZoneStatusConsolidating)
				if result.Error != nil {
					return result.Error
				}

				if result.RowsAffected == 1 {
					// update the aggregate content size
					if err := tx.Exec("UPDATE contents SET size = size + ? WHERE id = ?", c.Size, zone.ContID).Error; err != nil {
						return err
					}
					// we added to a zone, track zone creation last content counter
					return tx.Model(model.StagingZoneTracker{}).Where("id = ?", trackerID).UpdateColumn("last_cont_id", c.ID).Error
				}
				// if we could not add a content, proceed normally
			}

			// try to add a content to a zone using the db conditional write, so long as the where clause matches and a row is hit, we are good
			result := tx.Exec("UPDATE staging_zones SET size = size + ? WHERE id = ? and size < ? and status = ?", c.Size, zone.ID, cm.cfg.Content.MaxSize, model.ZoneStatusOpen)
			if result.Error != nil {
				return result.Error
			}

			// if a row is not hit, that zone is propably in processing (aggregation or consolidation) at the time we tried updating
			// or other process added more contents before this process and size is out of bound
			// that is why the update did not affect any row, try next zone
			if result.RowsAffected == 0 {
				return ErrZoneCouldNotBeClaimed
			}

			// update the aggregate content size
			if err := tx.Exec("UPDATE contents SET size = size + ? WHERE id = ?", c.Size, zone.ContID).Error; err != nil {
				return err
			}
			// we added to a zone, track zone creation last content counter
			return tx.Model(model.StagingZoneTracker{}).Where("id = ?", trackerID).UpdateColumn("last_cont_id", c.ID).Error
		})
		if err != nil {
			if err == ErrZoneCouldNotBeClaimed { // since we could not claim a zone, try next
				continue
			}
			return err
		}
		return nil
	}
	// if no zones could be claimed, create a new one and add content
	return cm.newContentStagingZoneFromContent(c, trackerID)
}

func (cm *ContentManager) newContentStagingZoneFromContent(cont util.Content, trackerID uint) error {
	// create an aggregate content and a staging zone for this content
	return cm.db.Transaction(func(tx *gorm.DB) error {
		zoneContID := cont.AggregatedIn

		// bakward compatibility feature
		// only new contents will cont.AggregatedIn = 0, so create a zone content for it
		// old contents already have zone contents
		if zoneContID == 0 {
			zoneCont := &util.Content{
				Size:        cont.Size,
				Name:        "aggregate",
				Active:      false,
				Pinning:     true,
				UserID:      cont.UserID,
				Replication: cm.cfg.Replication,
				Aggregate:   true,
				Location:    cont.Location,
			}
			if err := cm.db.Create(zoneCont).Error; err != nil {
				return err
			}

			// aggregate the content into the staging zone content ID
			if err := tx.Model(util.Content{}).
				Where("id = ?", cont.ID).
				UpdateColumn("aggregated_in", zoneCont.ID).Error; err != nil {
				return err
			}

			zoneContID = zoneCont.ID
		}

		// create staging zone for both old and new contents
		zone := &model.StagingZone{
			CreatedAt: cont.CreatedAt,
			MinSize:   cm.cfg.Content.MinSize,
			MaxSize:   cm.cfg.Content.MaxSize,
			Size:      cont.Size,
			UserID:    cont.UserID,
			ContID:    zoneContID,
			Location:  cont.Location,
			Status:    model.ZoneStatusOpen,
			Message:   model.ZoneMessageOpen,
		}
		if err := cm.db.Create(zone).Error; err != nil {
			return err
		}
		// update staging zone creation tracker
		return tx.Model(model.StagingZoneTracker{}).Where("id = ?", trackerID).UpdateColumn("last_cont_id", cont.ID).Error
	})
}
