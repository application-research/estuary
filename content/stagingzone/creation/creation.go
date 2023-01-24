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
	RunBackFillWorker(ctx context.Context)
	StageNewContent(ctx context.Context, cont *util.Content, contSize int64) error
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

func (m *manager) RunBackFillWorker(ctx context.Context) {
	// if staging zone is enabled, run the worker
	if m.cfg.StagingBucket.Enabled {
		m.log.Infof("starting up staging zone backfill worker")

		go m.backfillStagingZones(ctx)

		m.log.Infof("spun up staging zone backfill worker")
	}
}

func (m *manager) backfillStagingZones(ctx context.Context) {
	m.log.Debug("running staging zone backfill worker")

	timer := time.NewTicker(m.cfg.StagingBucket.CreationInterval)
	for {
		select {
		case <-timer.C:
			lastContID, err := m.getStagingZoneTrackerLastContentID()
			if err != nil {
				m.log.Warnf("failed to get staging zone tracker last content id - %s", err)
				continue
			}

			if err := m.stageNextOldContent(ctx, lastContID); err != nil {
				m.log.Warnf("failed to add contents to staging zones - %s", err)
			}
		}
	}
}

func (m *manager) stageNextOldContent(ctx context.Context, tracker *model.StagingZoneTracker) error {
	_, span := m.tracer.Start(ctx, "stageOldContent")
	defer span.End()

	// we do one content at a time
	var contents []*util.Content
	if err := m.db.Where("id > ? and size <= ?", tracker.LastContID, m.cfg.Content.MinSize).Order("id asc").Limit(1).Find(&contents).Error; err != nil {
		return err
	}

	for _, cont := range contents {
		// size = 0 are shuttle/cid pins contents, that are yet to be updated with their objects sizes, avoid them,
		// pincomplete will handle them and they will be treated a new contents to add to current open zone
		// if zone_id is not zero, it's a new content
		if (cont.Size == 0 || cont.ZoneID > 0) && !cont.Aggregate {
			// move tracker forward
			if err := m.db.Model(model.StagingZoneTracker{}).Where("id = ?", tracker.ID).UpdateColumn("last_cont_id", cont.ID).Error; err != nil {
				return err
			}
			continue
		}

		// if content is an aggregate content, update status and message
		// since one child content would have created its zone, simply update it
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

		// only contents already assigned to an aggregate - old contents
		// new content will be staged by when its created
		if cont.AggregatedIn > 0 {
			m.log.Debugf("adding old content: %d to its staging zone: %d", cont.ID, cont.AggregatedIn)

			err := m.db.Transaction(func(tx *gorm.DB) error {
				result := tx.Exec("UPDATE staging_zones SET size = size + ? WHERE cont_id = ?", cont.Size, cont.AggregatedIn)
				if result.Error != nil {
					return result.Error
				}

				if result.RowsAffected == 1 {
					// we added to a zone, move tracker forward
					return tx.Model(model.StagingZoneTracker{}).Where("id = ?", tracker.ID).UpdateColumn("last_cont_id", cont.ID).Error
				}
				return ErrZoneCouldNotBeClaimed // old zone is probably not created yet
			})

			if err != nil && err == ErrZoneCouldNotBeClaimed { // since we could not claim a zone, create a new one
				return m.newStagingZoneFromContent(cont, cont.Size, tracker, false)
			}
			return err
		}
	}
	return nil
}

func (m *manager) StageNewContent(ctx context.Context, cont *util.Content, contSize int64) error {
	_, span := m.tracer.Start(ctx, "tryAddNewContentToStagingZones")
	defer span.End()

	m.log.Debugf("adding new content: %d to a staging zone", cont.ID)

	// find most recent available zones
	var openZones []*model.StagingZone
	var openZonesBatch []*model.StagingZone
	if err := m.db.Where("user_id = ? and size + ? <= ? and status = ? order by id desc", cont.UserID, contSize, m.cfg.Content.MaxSize, model.ZoneStatusOpen).FindInBatches(&openZonesBatch, 500, func(tx *gorm.DB, batch int) error {
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

			// if a zone is in consolidation state and the size is below min size, it means a content was removed after the content
			// failed to be consolidated. Try to add any content from the same location to move the size above the min size,
			// we want consistent deal content size even for those that have consolidation errors
			if zone.Status == model.ZoneStatusConsolidating {
				// if content is in same location as zone, try to add it
				if cont.Location == zone.Location {
					result := tx.Exec("UPDATE staging_zones SET size = size + ? WHERE id = ? and size < ? and status = ?", cont.Size, zone.ID, m.cfg.Content.MaxSize, model.ZoneStatusConsolidating)
					if result.Error != nil {
						return result.Error
					}

					if result.RowsAffected == 1 {
						// update the aggregate content size if a zone was claimed
						return tx.Exec("UPDATE contents SET size = size + ? WHERE id = ?", contSize, zone.ContID).Error
					}
				}
				// zone could not be claimed either becuase content is not in same location or size is too large
				// try another zone
				return ErrZoneCouldNotBeClaimed
			}

			// if zone is not consolidating, proceed normally
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
			return tx.Exec("UPDATE contents SET size = size + ? WHERE id = ?", contSize, zone.ContID).Error
		})
		if err != nil {
			if err == ErrZoneCouldNotBeClaimed { // since we could not claim a zone, try next
				continue
			}
			return err
		}
		return nil
	}
	// if no zones could be claimed, create a new one and add content,
	// new contents do not need tracking
	return m.newStagingZoneFromContent(cont, contSize, nil, true)
}

func (m *manager) newStagingZoneFromContent(cont *util.Content, contSize int64, backFillTracker *model.StagingZoneTracker, isNewContentZone bool) error {
	// create an aggregate content and a staging zone for this content
	return m.db.Transaction(func(tx *gorm.DB) error {
		zoneContID := cont.AggregatedIn
		// backward compatibility feature
		// only new contents will have cont.AggregatedIn = 0, so create a zone content for it
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
			CreatedAt:        cont.CreatedAt,
			MinSize:          m.cfg.Content.MinSize,
			MaxSize:          m.cfg.Content.MaxSize,
			Size:             contSize,
			UserID:           cont.UserID,
			ContID:           zoneContID,
			Location:         cont.Location,
			Status:           model.ZoneStatusOpen,
			Message:          model.ZoneMessageOpen,
			IsNewContentZone: isNewContentZone,
		}
		if err := tx.Create(zone).Error; err != nil {
			return err
		}

		// update staging zone backfill tracker
		if backFillTracker != nil {
			return tx.Model(model.StagingZoneTracker{}).Where("id = ?", backFillTracker.ID).UpdateColumn("last_cont_id", cont.ID).Error
		}
		return nil
	})
}
