package stagingzone

import (
	"context"
	"time"

	"github.com/application-research/estuary/model"
	"github.com/application-research/estuary/util"
	"github.com/jinzhu/gorm"
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
	timer := time.NewTicker(m.cfg.WorkerIntervals.StagingZoneInterval)
	for {
		select {
		case <-ctx.Done():
			m.log.Info("shutting staging zone backfill worker")
			return
		case <-timer.C:
			m.log.Debug("running staging zone backfill worker")

			tracker, err := m.getQueueTracker()
			if err != nil {
				m.log.Warnf("failed to get staging zone tracker - %s", err)
				continue
			}

			if tracker.LastContID >= tracker.StopAt {
				m.log.Info("staging queue backfill is done")
				return
			}

			m.log.Debugf("trying to start staging zone backfill, starting from content: %d", tracker.LastContID)

			var contents []*util.Content
			if err := m.db.Where("id > ? and size < ?", tracker.LastContID, m.cfg.Content.MinSize).Order("id asc").Limit(2000).Find(&contents).Error; err != nil {
				m.log.Warnf("failed to get staging zone contents to backfill - %s", err)
				continue
			}

			for _, cont := range contents {
				m.log.Debugf("trying to backfill cont %d to staging zone queue", cont.ID)
				// size = 0 are shuttle/cid pins contents, that are yet to be updated with their objects sizes, avoid them,
				// pincomplete will queue them
				if cont.Size > 0 {
					if err := m.queueMgr.QueueContent(cont, true); err != nil {
						m.log.Warnf("failed to queue content: %d in staging zone queue for backfill - %s", cont.ID, err)
						break
					}
				}

				// move tracker forward
				if err := m.db.Model(model.StagingZoneTracker{}).Where("id = ?", tracker.ID).UpdateColumn("last_cont_id", cont.ID).Error; err != nil {
					m.log.Warnf("failed to move backfill stagingzone tracker - %s", err)
					break
				}
			}

			// if there are no more to backfill set stop
			if len(contents) == 0 {
				if err := m.db.Model(model.StagingZoneTracker{}).Where("id = ?", tracker.ID).UpdateColumn("stop_at", tracker.LastContID).Error; err != nil {
					m.log.Warnf("failed to set stop_at for stagingzone queue tracker - %s", err)
				}
			}
		}
	}
}

func (m *manager) runCreationWorker(ctx context.Context) {
	timer := time.NewTicker(m.cfg.WorkerIntervals.StagingZoneInterval)
	for {
		select {
		case <-ctx.Done():
			m.log.Info("shutting staging zone creation worker")
			return
		case <-timer.C:
			m.log.Debug("running staging zone creation worker")

			var tasks []*model.StagingZoneQueue
			if err := m.db.Where("next_attempt_at < ?", time.Now().UTC()).Order("id asc").Limit(2000).Find(&tasks).Error; err != nil {
				m.log.Warnf("failed to get staging zone tasks to stage - %s", err)
				continue
			}

			for _, t := range tasks {
				var cont *util.Content
				if err := m.db.First(&cont, "id = ?", t.ContID).Error; err != nil {
					if err != gorm.ErrRecordNotFound {
						m.log.Warnf("failed to get content: %d for staging - %s", t.ContID, err)
						break
					} else {
						m.log.Warnf("content: %d not found for staging - %s", t.ContID, err)
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
						m.log.Warnf("failed to update staging queue - %s", err)
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
		case <-ctx.Done():
			m.log.Info("shutting staging aggregation worker")
			return
		case <-timer.C:
			m.log.Debug("running staging zone aggregation worker")

			readyZones, err := m.getReadyStagingZones()
			if err != nil {
				m.log.Errorf("failed to get ready staging zones: %s", err)
				continue
			}

			m.log.Debugf("found: %d ready staging zones, for aggregation", len(readyZones))

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

func (m *manager) getQueueTracker() (*model.StagingZoneTracker, error) {
	var trks []*model.StagingZoneTracker
	if err := m.db.Find(&trks).Error; err != nil {
		return nil, err
	}

	if len(trks) == 0 || trks[0].StopAt == 0 {
		// for the first time it will be empty
		var contents []*util.Content
		if err := m.db.Order("id desc").Limit(1).Find(&contents).Error; err != nil {
			return nil, err
		}

		if len(trks) == 0 {
			trk := &model.StagingZoneTracker{LastContID: 0, StopAt: contents[0].ID}
			if err := m.db.Create(&trk).Error; err != nil {
				return nil, err
			}
			return trk, nil
		}

		trk := trks[0]
		if err := m.db.Model(&trk).Update("stop_at", contents[0].ID).Error; err != nil {
			return nil, err
		}
		return trk, nil
	}
	return trks[0], nil
}
