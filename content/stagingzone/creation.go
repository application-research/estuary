package stagingzone

import (
	"context"
	"fmt"

	"github.com/application-research/estuary/model"
	"github.com/application-research/estuary/util"
	"gorm.io/gorm"
)

var ErrZoneCouldNotBeClaimed = fmt.Errorf("zone could not be claimed")

func (m *manager) stageBackfilledContent(ctx context.Context, cont *util.Content) error {
	_, span := m.tracer.Start(ctx, "stageBackfilledContent")
	defer span.End()

	// if content is an aggregate content, update status and message
	// since one child content would have created its zone, simply update it
	if cont.Aggregate {
		return m.db.Transaction(func(tx *gorm.DB) error {
			if cont.Active {
				if err := tx.Model(model.StagingZone{}).Where("cont_id = ?", cont.ID).UpdateColumns(map[string]interface{}{
					"status":     model.ZoneStatusDone,
					"message":    model.ZoneMessageDone,
					"created_at": cont.CreatedAt,
				}).Error; err != nil {
					return err
				}
			}
			return m.queueMgr.StageComplete(cont.ID)
		})
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
				// we added to a zone, move on
				return m.queueMgr.StageComplete(cont.ID)
			}
			return ErrZoneCouldNotBeClaimed // old zone is probably not created yet
		})

		if err != nil && err == ErrZoneCouldNotBeClaimed { // since we could not claim a zone, create a new one
			return m.newStagingZoneFromContent(cont, cont.Size)
		}
		return err
	}
	return m.queueMgr.StageComplete(cont.ID)
}

func (m *manager) stageNewContent(ctx context.Context, cont *util.Content, contSize int64) error {
	_, span := m.tracer.Start(ctx, "tryAddNewContentToStagingZones")
	defer span.End()

	m.log.Debugf("adding new content: %d to a staging zone", cont.ID)

	// find most recent available zones
	var openZones []*model.StagingZone
	var openZonesBatch []*model.StagingZone
	if err := m.db.Where("user_id = ? and size + ? <= ? and status = ?", cont.UserID, contSize, m.cfg.Content.MaxSize, model.ZoneStatusOpen).Order("id desc").FindInBatches(&openZonesBatch, 500, func(tx *gorm.DB, batch int) error {
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
						if err := tx.Exec("UPDATE contents SET size = size + ? WHERE id = ?", contSize, zone.ContID).Error; err != nil {
							return err
						}
						return m.queueMgr.StageComplete(cont.ID)
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
			if err := tx.Exec("UPDATE contents SET size = size + ? WHERE id = ?", contSize, zone.ContID).Error; err != nil {
				return err
			}
			return m.queueMgr.StageComplete(cont.ID)
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
	return m.newStagingZoneFromContent(cont, contSize)
}

func (m *manager) newStagingZoneFromContent(cont *util.Content, contSize int64) error {
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
		return m.queueMgr.StageComplete(cont.ID)
	})
}
