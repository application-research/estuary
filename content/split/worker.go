package split

import (
	"context"
	"time"

	"github.com/application-research/estuary/model"
	"github.com/application-research/estuary/util"

	"gorm.io/gorm"
)

func (m *manager) runWorkers(ctx context.Context) {
	m.log.Infof("starting up split workers")

	go m.runSplitWorker(ctx)

	go m.runSplitBackFillWorker(ctx)

	m.log.Infof("spun up split workers")
}

func (m *manager) getQueueTracker() (*model.SplitQueueTracker, error) {
	var trks []*model.SplitQueueTracker
	if err := m.db.Find(&trks).Error; err != nil {
		return nil, err
	}

	if len(trks) > 0 && trks[0].BackfillDone {
		return trks[0], nil
	}

	var trk *model.SplitQueueTracker
	if len(trks) == 0 {
		// for the first time it will be empty
		var contents []*util.Content
		if err := m.db.Where("size > 0").Order("id desc").Limit(1).Find(&contents).Error; err != nil {
			return nil, err
		}

		stopAt := uint64(0)
		if len(contents) > 0 {
			stopAt = contents[0].ID
		}

		trk = &model.SplitQueueTracker{LastContID: 0, StopAt: stopAt}
		if err := m.db.Create(&trk).Error; err != nil {
			return nil, err
		}
	} else {
		trk = trks[0]
	}

	if trk.LastContID >= trk.StopAt {
		m.log.Info("split queue backfill is done")
		if err := m.db.Model(model.SplitQueueTracker{}).Where("id = ?", trk.ID).UpdateColumn("backfill_done", true).Error; err != nil {
			m.log.Warnf("failed to mark split queue backfill as done - %s", err)
			return nil, err
		}
		trk.BackfillDone = true
	}
	return trk, nil
}

func (m *manager) runSplitBackFillWorker(ctx context.Context) {
	// init tracker before work starts
	tracker, err := m.getQueueTracker()
	if err != nil {
		m.log.Warnf("failed to get split queue tracker - %s", err)
	}

	if tracker.BackfillDone {
		return
	}

	timer := time.NewTicker(m.cfg.WorkerIntervals.SplitInterval)
	for {
		select {
		case <-ctx.Done():
			m.log.Info("shutting down split backfill worker")
			return
		case <-timer.C:
			m.log.Debug("running split backfill worker")

			tracker, err = m.getQueueTracker()
			if err != nil {
				m.log.Warnf("failed to get split queue tracker - %s", err)
				continue
			}

			if tracker.BackfillDone {
				m.log.Info("split queue backfill is done")
				return
			}

			m.log.Debugf("trying to start split queue backfill, starting from content: %d", tracker.LastContID)

			var largeContents []*util.Content
			if err := m.db.Where("size > ? and not dag_split", m.cfg.Content.MaxSize).Order("id asc").Limit(2000).Find(&largeContents).Error; err != nil {
				m.log.Warnf("failed to get contents for split queue backfill - %s", err)
				continue
			}

			m.log.Debugf("trying to backfill split queue for total of %d contents", len(largeContents))
			for _, c := range largeContents {
				if err := m.backfill(ctx, c, tracker); err != nil {
					m.log.Warnf("failed to backfill split queue for cont: %d - %s", c.ID, err)
					break
				}
			}

			// if there are no more to backfill set stop
			if len(largeContents) == 0 {
				if err = m.db.Model(model.SplitQueueTracker{}).Where("id = ?", tracker.ID).UpdateColumn("stop_at", tracker.LastContID).Error; err != nil {
					m.log.Warnf("failed to set stop_at for split queue tracker - %s", err)
				}
			}
		}
	}
}

func (m *manager) backfill(ctx context.Context, cont *util.Content, tracker *model.SplitQueueTracker) error {
	m.log.Debugf("trying to backfill split queue for content: %d", cont.ID)

	if err := m.splitQueueMgr.QueueContent(cont.ID, cont.UserID, m.db); err != nil {
		return err
	}
	return m.db.Model(model.SplitQueueTracker{}).Where("id = ?", tracker.ID).UpdateColumn("last_cont_id", cont.ID).Error
}

func (m *manager) runSplitWorker(ctx context.Context) {
	timer := time.NewTicker(m.cfg.WorkerIntervals.SplitInterval)
	for {
		select {
		case <-ctx.Done():
			m.log.Info("shutting split worker")
			return
		case <-timer.C:
			m.log.Debug("running split worker")
			if err := m.FindAndSplitLargeContents(ctx); err != nil {
				m.log.Warnf("failed to split contents - %s", err)
			}
		}
	}
}

func (m *manager) FindAndSplitLargeContents(ctx context.Context) error {
	var tasks []*model.SplitQueue
	return m.db.Where("attempted < 3 and next_attempt_at < ?", time.Now().UTC()).Order("id asc").FindInBatches(&tasks, 2000, func(tx *gorm.DB, batch int) error {
		m.log.Debugf("trying to split total of %d contents", len(tasks))
		for _, tsk := range tasks {
			var cont util.Content
			if err := m.db.First(&cont, "id = ?", tsk.ContID).Error; err != nil {
				return err
			}

			if err := m.SplitContent(ctx, cont, m.cfg.Content.MaxSize); err != nil {
				return err
			}
		}
		return nil
	}).Error
}
