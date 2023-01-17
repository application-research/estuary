package stagingzone

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/application-research/estuary/constants"
	"github.com/application-research/estuary/model"
	"github.com/application-research/estuary/util"
	"github.com/ipfs/go-blockservice"
	"github.com/ipfs/go-cid"
	ipld "github.com/ipfs/go-ipld-format"
	"github.com/ipfs/go-merkledag"
	uio "github.com/ipfs/go-unixfs/io"
	"golang.org/x/xerrors"
	"gorm.io/gorm"
)

func (m *manager) runStagingZoneAggregationWorker(ctx context.Context) {
	timer := time.NewTicker(m.cfg.StagingBucket.AggregateInterval)
	for {
		select {
		case <-timer.C:
			m.log.Debug("running staging zone aggregation worker")

			readyZones, err := m.getReadyStagingZones()
			if err != nil {
				m.log.Errorf("failed to get ready staging zones: %s", err)
				continue
			}

			m.log.Debugf("found: %d ready staging zones", len(readyZones))

			for _, z := range readyZones {
				var zc util.Content
				if err := m.db.First(&zc, "id = ?", z.ContID).Error; err != nil {
					m.log.Warnf("zone %d aggregation failed to get zone content %d for processing - %s", z.ID, z.ContID, err)
					continue
				}

				if err := m.processStagingZone(ctx, zc, z); err != nil {
					m.log.Errorf("zone aggregation worker failed to process zone: %d - %s", z.ID, err)
					continue
				}
			}
		}
	}
}

// getReadyStagingZones gets zones that are done but have reasonable sizes
func (m *manager) getReadyStagingZones() ([]*model.StagingZone, error) {
	notReadyStatuses := []model.ZoneStatus{model.ZoneStatusDone, model.ZoneStatusStuck}
	var readyZones []*model.StagingZone
	if err := m.db.Model(&model.StagingZone{}).Where("size >= ? and status not in ? ", m.cfg.Content.MinSize, notReadyStatuses).Limit(500).Find(&readyZones).Error; err != nil {
		return nil, err
	}
	return readyZones, nil
}

func (m *manager) markZoneStatus(zone *model.StagingZone, upSts model.ZoneStatus, upMgs model.ZoneMessage) (bool, error) {
	result := m.db.Exec("UPDATE staging_zones SET status = ?, message = ? WHERE id = ? and status <> ?", upSts, upMgs, zone.ID, upSts)
	if result.Error != nil {
		return false, result.Error
	}
	// claim state - a naive lock, using db conditinal write
	return result.RowsAffected == 1, nil
}

func (m *manager) processStagingZone(ctx context.Context, zoneCont util.Content, zone *model.StagingZone) error {
	ctx, span := m.tracer.Start(ctx, "aggregateContent")
	defer span.End()

	var grpLocs []string
	if err := m.db.Model(&util.Content{}).Where("active and aggregated_in = ?", zoneCont.ID).Distinct().Pluck("location", &grpLocs).Error; err != nil {
		return err
	}

	if len(grpLocs) == 0 {
		return fmt.Errorf("no location for staged contents")
	}

	// if the staged contents of this bucket are in different locations (more than 1 group)
	// Need to migrate/consolidate the contents to the same location
	// NB - we should avoid doing this, best we have staging by location - this process is just to expensive
	if len(grpLocs) > 1 {
		// Need to migrate content all to the same shuttle
		// Only attempt consolidation on a zone if one is not ongoing, prevents re-consolidation request

		if canProceed, err := m.markZoneStatus(zone, model.ZoneStatusConsolidating, model.ZoneMessageConsolidating); err != nil || !canProceed {
			return err
		}

		if err := m.consolidateStagedContent(ctx, zone.ID, zoneCont); err != nil {
			m.log.Errorf("failed to consolidate staged content: %s", err)
		}
		return nil
	}
	// if all contents are already in one location, proceed to aggregate them
	return m.AggregateStagingZone(ctx, zone, zoneCont, grpLocs[0])
}

func (m *manager) consolidateStagedContent(ctx context.Context, zoneID uint, zoneContent util.Content) error {
	dstLocation, toMove, curMax, err := m.getContentsAndDestinationLocationForConsolidation(ctx, zoneID, zoneContent.ID)
	if err != nil {
		return err
	}

	m.log.Debugw("consolidating content to single location for aggregation", "user", zoneContent.UserID, "dstLocation", dstLocation, "numItems", len(toMove), "primaryWeight", curMax)

	if dstLocation == constants.ContentLocationLocal {
		return m.migrateContentsToLocalNode(ctx, toMove)
	}

	if err := m.db.Transaction(func(tx *gorm.DB) error {
		// point zone content location to dstLocation
		if err := tx.Model(util.Content{}).
			Where("id = ?", zoneContent.ID).
			UpdateColumn("location", dstLocation).Error; err != nil {
			return err
		}

		// point staging zone location to dstLocation
		if err := tx.Model(util.Content{}).
			Where("cont_id = ?", zoneContent.ID).
			UpdateColumn("location", dstLocation).Error; err != nil {
			return err
		}
		return nil
	}); err != nil {
		return err
	}
	return m.shuttleMgr.ConsolidateContent(ctx, dstLocation, toMove)
}

func (m *manager) migrateContentsToLocalNode(ctx context.Context, toMove []util.Content) error {
	for _, c := range toMove {
		if err := m.migrateContentToLocalNode(ctx, c); err != nil {
			return err
		}
	}
	return nil
}

func (m *manager) migrateContentToLocalNode(ctx context.Context, cont util.Content) error {
	done, err := m.safeFetchData(ctx, cont.Cid.CID)
	if err != nil {
		return fmt.Errorf("failed to fetch data: %w", err)
	}

	defer done()

	if err := m.db.Model(util.ObjRef{}).Where("id = ?", cont.ID).UpdateColumns(map[string]interface{}{
		"offloaded": 0,
	}).Error; err != nil {
		return err
	}

	if err := m.db.Model(util.Content{}).Where("id = ?", cont.ID).UpdateColumns(map[string]interface{}{
		"offloaded": false,
		"location":  constants.ContentLocationLocal,
	}).Error; err != nil {
		return err
	}
	// TODO: send unpin command to where the content was migrated from
	return nil
}

func (m *manager) safeFetchData(ctx context.Context, c cid.Cid) (func(), error) {
	// TODO: this method should mark each object it fetches as 'needed' before pulling the data so that
	// any concurrent deletion tasks can avoid deleting our data as we fetch it
	bserv := blockservice.New(m.blockstore, m.node.Bitswap)
	dserv := merkledag.NewDAGService(bserv)

	deref := func() {
		m.log.Warnf("TODO: implement safe fetch data protections")
	}

	cset := cid.NewSet()
	err := merkledag.Walk(ctx, func(ctx context.Context, c cid.Cid) ([]*ipld.Link, error) {
		node, err := dserv.Get(ctx, c)
		if err != nil {
			return nil, err
		}

		if c.Type() == cid.Raw {
			return nil, nil
		}

		return util.FilterUnwalkableLinks(node.Links()), nil
	}, c, cset.Visit, merkledag.Concurrent())
	if err != nil {
		return deref, err
	}
	return deref, nil
}

// AggregateStagingZone assumes zone is already in consolidatingZones
func (m *manager) AggregateStagingZone(ctx context.Context, zone *model.StagingZone, zoneCont util.Content, loc string) error {
	ctx, span := m.tracer.Start(ctx, "aggregateStagingZone")
	defer span.End()

	m.log.Debugf("aggregating zone: %d", zoneCont.ID)

	// skip if zone is already beign aggregated by another process
	if canProceed, err := m.markZoneStatus(zone, model.ZoneStatuAggregating, model.ZoneMessageAggregating); err != nil || !canProceed {
		m.log.Debugf("could not proceed to aggregate zone: %d", zone.ID)
		return err
	}

	var zoneContents []util.Content
	var zoneContentsBatch []util.Content
	if err := m.db.Where("aggregated_in = ?", zoneCont.ID).FindInBatches(&zoneContentsBatch, 500, func(tx *gorm.DB, batch int) error {
		zoneContents = append(zoneContents, zoneContentsBatch...)
		return nil
	}).Error; err != nil {
		return err
	}

	if loc == constants.ContentLocationLocal {
		dir, err := m.CreateAggregate(ctx, zoneContents)
		if err != nil {
			return xerrors.Errorf("failed to create aggregate: %w", err)
		}

		ncid := dir.Cid()
		size, err := dir.Size()
		if err != nil {
			return err
		}

		if size == 0 {
			m.log.Warnf("content %d aggregate dir apparent size is zero", zoneCont.ID)
		}

		return m.db.Transaction(func(tx *gorm.DB) error {
			obj := &util.Object{
				Cid:  util.DbCID{CID: ncid},
				Size: int(size),
			}
			if err := tx.Create(obj).Error; err != nil {
				return err
			}

			if err := tx.Create(&util.ObjRef{
				Content: zoneCont.ID,
				Object:  obj.ID,
			}).Error; err != nil {
				return err
			}

			if err := m.blockstore.Put(ctx, dir); err != nil {
				return err
			}

			// mark zone content active
			if err := tx.Model(util.Content{}).Where("id = ?", zoneCont.ID).UpdateColumns(map[string]interface{}{
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
			return nil
		})
	}
	// handle aggregate on shuttle
	return m.shuttleMgr.AggregateContent(ctx, loc, zoneCont, zoneContents)
}

func (m *manager) CreateAggregate(ctx context.Context, conts []util.Content) (ipld.Node, error) {
	m.log.Debug("aggregating contents in staging zone into new content")

	bserv := blockservice.New(m.blockstore, m.node.Bitswap)
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

func (m *manager) getContentsAndDestinationLocationForConsolidation(ctx context.Context, zoneID uint, zoneContID uint) (string, []util.Content, int64, error) {
	// first determine the location(destination) to move contents to, that are not in that location over.
	// Do this by checking what location has the largest contents.
	var dstLocation string
	var curMax int64

	dataByLoc := make(map[string]int64)
	var contsBatch []util.Content
	if err := m.db.Where("active and aggregated_in = ?", zoneContID).FindInBatches(&contsBatch, 500, func(tx *gorm.DB, batch int) error {
		for _, c := range contsBatch {
			dataByLoc[c.Location] = dataByLoc[c.Location] + c.Size

			// temp: dont ever migrate content back to primary instance for aggregation, always prefer elsewhere
			if c.Location == constants.ContentLocationLocal {
				continue
			}

			canAddContent, err := m.shuttleMgr.CanAddContent(c.Location)
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
	if err := m.db.Where("active and aggregated_in = ?", zoneContID).FindInBatches(&toMoveBatch, 500, func(tx *gorm.DB, batch int) error {
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
