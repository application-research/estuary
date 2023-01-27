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

var ErrWaitForRemoteAggregate = fmt.Errorf("waiting for remote content aggregation")

// getReadyStagingZones gets zones that are done but have reasonable sizes
func (m *manager) getReadyStagingZones() ([]*model.StagingZone, error) {
	var readyZones []*model.StagingZone
	if err := m.db.Model(&model.StagingZone{}).Where("size >= ? and status <> ? and attempted < 3 and next_attempt_at < ? ", m.cfg.Content.MinSize, model.ZoneStatusDone, time.Now().UTC()).Limit(500).Find(&readyZones).Error; err != nil {
		return nil, err
	}
	return readyZones, nil
}

func (m *manager) processStagingZone(ctx context.Context, zoneCont *util.Content, zone *model.StagingZone) error {
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
		// Need to migrate all content the same shuttle
		return m.consolidateStagedContent(ctx, zone.ID, zoneCont)
	}
	// if all contents are already in one location, proceed to aggregate them
	return m.AggregateStagingZone(ctx, zone, zoneCont, grpLocs[0])
}

func (m *manager) consolidateStagedContent(ctx context.Context, zoneID uint64, zoneContent *util.Content) error {
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
	if err := m.shuttleMgr.ConsolidateContent(ctx, dstLocation, toMove); err != nil {
		return err
	}
	return ErrWaitForRemoteAggregate
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
func (m *manager) AggregateStagingZone(ctx context.Context, zone *model.StagingZone, zoneCont *util.Content, loc string) error {
	ctx, span := m.tracer.Start(ctx, "aggregateStagingZone")
	defer span.End()

	m.log.Debugf("aggregating zone: %d", zoneCont.ID)

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
				Size: uint64(size),
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
			// queue aggregate content for deal making
			return m.dealQueueMgr.QueueContent(zoneCont, tx)
		})
	}
	// handle aggregate on shuttle
	if err := m.shuttleMgr.AggregateContent(ctx, loc, zoneCont, zoneContents); err != nil {
		return err
	}
	return ErrWaitForRemoteAggregate
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

func (m *manager) getContentsAndDestinationLocationForConsolidation(ctx context.Context, zoneID uint64, zoneContID uint64) (string, []util.Content, int64, error) {
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

func (m *manager) processZoneFailed(zoneID uint64) {
	if err := m.db.Exec("UPDATE staging_zones SET attempted = attempted + 1, next_attempt_at = ?, status = ?, message = ? WHERE id = ?", time.Now().Add(2*time.Hour), model.ZoneStatusStuck, model.ZoneMessageStuck, zoneID).Error; err != nil {
		m.log.Errorf("failed to update staging zone (processZoneFailed) for zone %d - %s", zoneID, err)
	}
}

func (m *manager) processZoneRequested(zoneID uint64) {
	if err := m.db.Exec("UPDATE staging_zones SET next_attempt_at = ? WHERE id = ?", time.Now().Add(9*time.Hour), zoneID).Error; err != nil {
		m.log.Errorf("failed to update staging zone (processZoneRequested) for zone %d - %s", zoneID, err)
	}
}
