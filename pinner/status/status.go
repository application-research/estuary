package status

import (
	"github.com/application-research/estuary/model"
	"github.com/application-research/estuary/util"
	"github.com/pkg/errors"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

type PinningStatus string

const (
	/*
	   - queued     # pinning operation is waiting in the queue; additional info can be returned in info[status_details]
	   - pinning    # pinning in progress; additional info can be returned in info[status_details]
	   - pinned     # pinned successfully
	   - failed     # pinning service was unable to finish pinning operation; additional info can be found in info[status_details]
	   - offloaded  # content has been offloaded
	*/
	PinningStatusPinning   PinningStatus = "pinning"
	PinningStatusPinned    PinningStatus = "pinned"
	PinningStatusFailed    PinningStatus = "failed"
	PinningStatusQueued    PinningStatus = "queued"
	PinningStatusOffloaded PinningStatus = "offloaded"
)

type IUpdater interface {
	UpdateContentPinStatus(contID uint64, location string, status PinningStatus) error
}

type updater struct {
	db  *gorm.DB
	log *zap.SugaredLogger
}

func NewUpdater(db *gorm.DB, log *zap.SugaredLogger) IUpdater {
	return &updater{
		db:  db,
		log: log,
	}
}

// UpdateContentPinStatus updates content pinning statuses in DB and removes the content from its zone if failed
func (up *updater) UpdateContentPinStatus(contID uint64, location string, status PinningStatus) error {
	up.log.Debugf("updating pin: %d, status: %s, loc: %s", contID, status, location)

	var c util.Content
	if err := up.db.First(&c, "id = ?", contID).Error; err != nil {
		return errors.Wrap(err, "failed to look up content")
	}

	// if an aggregate zone is failing, zone is stuck
	// TODO - revisit this later if it is actually happening
	if c.Aggregate && status == PinningStatusFailed {
		up.log.Warnf("zone: %d is stuck, failed to aggregate(pin) on location: %s", c.ID, location)

		return up.db.Model(model.StagingZone{}).Where("id = ?", contID).UpdateColumns(map[string]interface{}{
			"status":  model.ZoneStatusStuck,
			"message": model.ZoneMessageStuck,
		}).Error
	}

	updates := map[string]interface{}{
		"active":  status == PinningStatusPinned,
		"pinning": status == PinningStatusPinning,
		"failed":  status == PinningStatusFailed,
	}
	if status == PinningStatusFailed {
		updates["aggregated_in"] = 0 // remove from staging zone so the zone can consolidate without it
	}

	return up.db.Transaction(func(tx *gorm.DB) error {
		if err := tx.Model(util.Content{}).Where("id = ?", contID).UpdateColumns(updates).Error; err != nil {
			return errors.Wrapf(err, "failed to update content status as %s in database: %s", status, err)
		}

		// deduct from the zone, so new content can be added, this way we get consistent size for aggregation
		// we did not reset the flag so that consolidation will not be reattempted by the worker
		if c.AggregatedIn > 0 {
			return tx.Raw("UPDATE staging_zones SET size = size - ? WHERE cont_id = ? ", c.Size, contID).Error
		}
		return nil
	})
}

func GetContentPinningStatus(cont util.Content) PinningStatus {
	status := PinningStatusQueued
	if cont.Active {
		status = PinningStatusPinned
	} else if cont.Failed {
		status = PinningStatusFailed
	} else if cont.Pinning {
		status = PinningStatusPinning
	} else if cont.Offloaded {
		status = PinningStatusOffloaded
	}
	return status
}
