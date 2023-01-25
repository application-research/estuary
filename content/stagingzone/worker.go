package stagingzone

import (
	"context"
	"time"

	"github.com/application-research/estuary/model"
	"github.com/application-research/estuary/util"
	"github.com/jinzhu/gorm"
	"gorm.io/gorm/clause"
)

func (m *manager) runWorkers(ctx context.Context) {
	// if staging zone is enabled, run the workers
	if m.cfg.StagingBucket.Enabled {
		m.log.Infof("starting up staging zone workers")

		// run staging zone backfill worker
		go m.runBackFillWorker(ctx)

		// run staging zone creation worker
		go m.runCreationWorker(ctx)

		// run staging zone aggregation/consoliation worker
		go m.runAggregationWorker(ctx)

		m.log.Infof("spun up staging zone workers")
	}
}

func (m *manager) runBackFillWorker(ctx context.Context) {
	m.log.Debug("running staging zone backfill worker")

	timer := time.NewTicker(m.cfg.WorkerIntervals.StagingZoneInterval)
	for {
		select {
		case <-timer.C:
			tracker, err := m.getQueueTracker()
			if err != nil {
				m.log.Warnf("failed to get staging zone tracker - %s", err)
				continue
			}

			if tracker.LastContID > tracker.StopAt {
				m.log.Info("staging queue backfill is done")
				return
			}

			// we do one content at a time
			var contents []*util.Content
			if err := m.db.Where("id > ? and size <= ?", tracker.LastContID, m.cfg.Content.MinSize).Order("id asc").Limit(1).Find(&contents).Error; err != nil {
				m.log.Warnf("failed to get staging zone contents to backfill - %s", err)
				continue
			}

			for _, cont := range contents {
				// size = 0 are shuttle/cid pins contents, that are yet to be updated with their objects sizes, avoid them,
				// pincomplete will queue them
				if cont.Size > 0 {
					if err := m.queueMgr.QueueContent(cont, true); err != nil {
						m.log.Warnf("failed to queue backfill content - %s", err)
						break
					}
				}

				// move tracker forward
				if err := m.db.Model(model.StagingZoneQueueTracker{}).Where("id = ?", tracker.ID).UpdateColumn("last_cont_id", cont.ID).Error; err != nil {
					m.log.Warnf("failed to move backfill staging zone tracker - %s", err)
					break
				}
			}
		}
	}
}

func (m *manager) runCreationWorker(ctx context.Context) {
	timer := time.NewTicker(m.cfg.WorkerIntervals.StagingZoneInterval)
	for {
		select {
		case <-timer.C:
			m.log.Debug("running staging zone creation worker")
			var tasks []*model.StagingZoneQueue
			if err := m.db.Where("next_attempt_at < ?", time.Now().UTC()).Order("id asc").Limit(1).Find(&tasks).Error; err != nil {
				m.log.Warnf("failed to get staging zone contents to backfill - %s", err)
				continue
			}

			for _, t := range tasks {
				var cont *util.Content
				if err := m.db.First(&cont, "id = ?", t.ContID).Error; err != nil {
					if err != gorm.ErrRecordNotFound {
						m.log.Warnf("failed to get staging zone contents to backfill - %s", err)
						break
					} else {
						m.log.Warnf("failed to get staging zone contents to backfill - %s", err)
						continue
					}
				}

				var err error
				if t.IsBackFilled {
					err = m.stageBackfilledContent(ctx, cont)
				} else {
					err = m.stageNewContent(ctx, cont, cont.Size)
				}

				if err != nil {
					m.log.Warnf("failed to stage content - %s", err)
					if err = m.queueMgr.StageFailed(cont.ID); err != nil {
						m.log.Warnf("failed to get update staging queue - %s", err)
					}
				}
			}
		}
	}
}

func (m *manager) runAggregationWorker(ctx context.Context) {
	timer := time.NewTicker(m.cfg.WorkerIntervals.StagingZoneInterval)
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
				var zc *util.Content
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

func (m *manager) getQueueTracker() (*model.StagingZoneQueueTracker, error) {
	var trks []*model.StagingZoneQueueTracker
	if err := m.db.Find(&trks).Error; err != nil {
		return nil, err
	}

	if len(trks) == 0 || trks[0].StopAt == 0 {
		// for the first time it will be empty
		var contents []*util.Content
		if err := m.db.Order("id asc").Limit(1).Find(&contents).Error; err != nil {
			return nil, err
		}

		trk := &model.StagingZoneQueueTracker{LastContID: 0, StopAt: contents[0].ID}
		if err := m.db.Clauses(&clause.OnConflict{UpdateAll: true}).Create(&trk).Error; err != nil {
			return nil, err
		}
		return trk, nil
	}
	return trks[0], nil
}
