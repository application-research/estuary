package stagingzonecreation

import (
	"context"
	"fmt"
	"time"

	"github.com/application-research/estuary/config"
	"github.com/application-research/estuary/model"
	"github.com/application-research/estuary/util"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

var ErrZoneCouldNotBeClaimed = fmt.Errorf("zone could not be claimed")

type IManager interface {
	Run(ctx context.Context)
	TryAddNewContentToStagingZone(ctx context.Context, cont *util.Content, contSize int64, tracker *model.StagingZoneTracker) error
}

type manager struct {
	db     *gorm.DB
	cfg    *config.Estuary
	log    *zap.SugaredLogger
	tracer trace.Tracer
}

func NewManager(db *gorm.DB, cfg *config.Estuary, log *zap.SugaredLogger) IManager {
	return &manager{
		db:     db,
		cfg:    cfg,
		log:    log,
		tracer: otel.Tracer("stagingzone"),
	}
}

func (m *manager) getStagingZoneTrackerLastContentID() (*model.StagingZoneTracker, error) {
	var trks []*model.StagingZoneTracker
	if err := m.db.Find(&trks).Error; err != nil {
		return nil, err
	}

	if len(trks) == 0 {
		// for the first time it will be empty
		trk := &model.StagingZoneTracker{LastContID: 0}
		if err := m.db.Create(&trk).Error; err != nil {
			return nil, err
		}
		return trk, nil
	}
	return trks[0], nil
}

func (m *manager) Run(ctx context.Context) {
	// if staging zone is enabled, run the worker
	if m.cfg.StagingBucket.Enabled {
		m.log.Infof("starting up staging zone creation worker")

		go m.runCreationWorker(ctx)

		m.log.Infof("spun up staging zone creation worker")
	}
}

func (m *manager) runCreationWorker(ctx context.Context) {
	m.log.Debug("running staging zone creation worker")

	timer := time.NewTicker(m.cfg.StagingBucket.CreationInterval)
	for {
		select {
		case <-timer.C:
			lastContID, err := m.getStagingZoneTrackerLastContentID()
			if err != nil {
				m.log.Warnf("failed to get staging zone tracker last content id - %s", err)
				continue
			}

			if err := m.addNextContentToStagingZone(ctx, lastContID); err != nil {
				m.log.Warnf("failed to add contents to staging zones - %s", err)
			}
		}
	}
}

func (m *manager) addNextContentToStagingZone(ctx context.Context, tracker *model.StagingZoneTracker) error {
	m.log.Infof("trying to stage the next content after: %d", tracker.LastContID)

	// we do one content at a time
	var contents []*util.Content
	if err := m.db.Where("id > ? and size <= ?", tracker.LastContID, m.cfg.Content.MinSize).Order("id asc").Limit(1).Find(&contents).Error; err != nil {
		return err
	}

	for _, cont := range contents {
		// size = 0 are shuttle/cid pins contents, that are yet to be updated with their objects sizes, avoid them,
		// pincomplete will handle them
		if cont.Size == 0 {
			// move tracker forward
			if err := m.db.Model(model.StagingZoneTracker{}).Where("id = ?", tracker.ID).UpdateColumn("last_cont_id", cont.ID).Error; err != nil {
				return err
			}
			continue
		}

		// if content is an aggregate content, update status and message
		if cont.Aggregate {
			if err := m.db.Transaction(func(tx *gorm.DB) error {
				if cont.Active {
					if err := tx.Model(model.StagingZone{}).Where("cont_id = ?", cont.ID).UpdateColumns(map[string]interface{}{
						"status":     model.ZoneStatusDone,
						"message":    model.ZoneMessageDone,
						"created_at": cont.CreatedAt,
					}).Error; err != nil {
						return err
					}
				}

				// move tracker forward
				if err := tx.Model(model.StagingZoneTracker{}).Where("id = ?", tracker.ID).UpdateColumn("last_cont_id", cont.ID).Error; err != nil {
					return err
				}
				return nil
			}); err != nil {
				return err
			}
			continue
		}

		// for backward compatibilty
		//todo verify this for case when loop happens atfer pin complete
		if cont.AggregatedIn > 0 {
			if err := m.tryAddOldContentToStagingZone(ctx, cont, tracker); err != nil {
				return err
			}
			continue
		}

		// after backfill, all new contents going forward
		if err := m.TryAddNewContentToStagingZone(ctx, cont, cont.Size, tracker); err != nil {
			return err
		}
	}
	return nil
}

func (m *manager) tryAddOldContentToStagingZone(ctx context.Context, cont *util.Content, tracker *model.StagingZoneTracker) error {
	_, span := m.tracer.Start(ctx, "tryAddOldContentToStagingZone")
	defer span.End()

	m.log.Debugf("adding old content: %d to its staging zone: %d", cont.ID, cont.AggregatedIn)

	err := m.db.Transaction(func(tx *gorm.DB) error {
		result := tx.Exec("UPDATE staging_zones SET size = size + ? WHERE cont_id = ?", cont.Size, cont.AggregatedIn)
		if result.Error != nil {
			return result.Error
		}

		if result.RowsAffected == 1 {
			// we added to a zone, track zone creation last content counter
			return tx.Model(model.StagingZoneTracker{}).Where("id = ?", tracker.ID).UpdateColumn("last_cont_id", cont.ID).Error
		}
		return ErrZoneCouldNotBeClaimed // old zone is probably not created yet
	})
	if err != nil {
		if err == ErrZoneCouldNotBeClaimed { // since we could not claim a zone, create a new one
			return m.newStagingZoneFromContent(cont, cont.Size, tracker)
		}
		return err
	}
	return nil
}

func (m *manager) TryAddNewContentToStagingZone(ctx context.Context, cont *util.Content, contSize int64, tracker *model.StagingZoneTracker) error {
	_, span := m.tracer.Start(ctx, "tryAddNewContentToStagingZones")
	defer span.End()

	m.log.Debugf("adding new content: %d to a staging zone", cont.ID)

	// find available zones
	var openZones []*model.StagingZone
	var openZonesBatch []*model.StagingZone
	if err := m.db.Where("user_id = ? and size + ? <= ? and status = ?", cont.UserID, contSize, m.cfg.Content.MaxSize, model.ZoneStatusOpen).FindInBatches(&openZonesBatch, 500, func(tx *gorm.DB, batch int) error {
		openZones = append(openZones, openZonesBatch...)
		return nil
	}).Error; err != nil {
		return err
	}

	for _, zone := range openZones {
		err := m.db.Transaction(func(tx *gorm.DB) error {
			if err := tx.Model(util.Content{}).
				Where("id = ?", cont.ID).
				UpdateColumn("aggregated_in", zone.ContID).Error; err != nil {
				return err
			}

			// if a zone is in consolidating and the size is below min size, it means a content was removed after the content
			// failed to be consolidated. Try to add any content for the same location to move the size above the min size
			// we we make consistent deal content size for all contents even those that previous consolidatione errors
			if zone.Status == model.ZoneStatusConsolidating && cont.Location == zone.Location {
				result := tx.Exec("UPDATE staging_zones SET size = size + ? WHERE id = ? and size < ? and status = ?", cont.Size, zone.ID, m.cfg.Content.MaxSize, model.ZoneStatusConsolidating)
				if result.Error != nil {
					return result.Error
				}

				if result.RowsAffected == 1 {
					// update the aggregate content size
					if err := tx.Exec("UPDATE contents SET size = size + ? WHERE id = ?", contSize, zone.ContID).Error; err != nil {
						return err
					}
					// we added to a zone, track zone creation last content counter
					if tracker != nil {
						return tx.Model(model.StagingZoneTracker{}).Where("id = ?", tracker.ID).UpdateColumn("last_cont_id", cont.ID).Error
					}
					return nil
				}
				// if we could not add a content, proceed normally
			}

			// try to add a content to a zone using the db conditional write, so long as the where clause matches and a row is hit, we are good
			result := tx.Exec("UPDATE staging_zones SET size = size + ? WHERE id = ? and size < ? and status = ?", contSize, zone.ID, m.cfg.Content.MaxSize, model.ZoneStatusOpen)
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
			if err := tx.Exec("UPDATE contents SET size = size + ? WHERE id = ?", contSize, zone.ContID).Error; err != nil {
				return err
			}

			// we added to a zone, track zone creation last content counter
			if tracker != nil {
				return tx.Model(model.StagingZoneTracker{}).Where("id = ?", tracker.ID).UpdateColumn("last_cont_id", cont.ID).Error
			}
			return nil
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
	return m.newStagingZoneFromContent(cont, contSize, tracker)
}

func (m *manager) newStagingZoneFromContent(cont *util.Content, contSize int64, tracker *model.StagingZoneTracker) error {
	// create an aggregate content and a staging zone for this content
	return m.db.Transaction(func(tx *gorm.DB) error {
		zoneContID := cont.AggregatedIn

		// backward compatibility feature
		// only new contents will cont.AggregatedIn = 0, so create a zone content for it
		// old contents already have zone contents
		if zoneContID == 0 {
			zoneCont := &util.Content{
				Size:        contSize,
				Name:        "aggregate",
				Active:      false,
				Pinning:     true,
				UserID:      cont.UserID,
				Replication: m.cfg.Replication,
				Aggregate:   true,
				Location:    cont.Location,
			}
			if err := tx.Create(zoneCont).Error; err != nil {
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
			MinSize:   m.cfg.Content.MinSize,
			MaxSize:   m.cfg.Content.MaxSize,
			Size:      contSize,
			UserID:    cont.UserID,
			ContID:    zoneContID,
			Location:  cont.Location,
			Status:    model.ZoneStatusOpen,
			Message:   model.ZoneMessageOpen,
		}
		if err := tx.Create(zone).Error; err != nil {
			return err
		}

		// update staging zone creation tracker
		if tracker != nil {
			return tx.Model(model.StagingZoneTracker{}).Where("id = ?", tracker.ID).UpdateColumn("last_cont_id", cont.ID).Error
		}
		return nil
	})
}
