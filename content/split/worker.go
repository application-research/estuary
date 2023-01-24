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

func (m *manager) getSplitQueueBackFillTracker() (*model.SplitQueueTracker, error) {
	var trackers []*model.SplitQueueTracker
	if err := m.db.Find(&trackers).Error; err != nil {
		return nil, err
	}

	if len(trackers) == 0 {
		// for the first time it will be empty
		trk := &model.SplitQueueTracker{LastContID: 0}
		if err := m.db.Create(&trk).Error; err != nil {
			return nil, err
		}
		return trk, nil
	}
	return trackers[0], nil
}

func (m *manager) runSplitBackFillWorker(ctx context.Context) {
	timer := time.NewTicker(m.cfg.StagingBucket.CreationInterval)
	for {
		select {
		case <-timer.C:
			tracker, err := m.getSplitQueueBackFillTracker()
			if err != nil {
				m.log.Warnf("failed to get staging zone tracker last content id - %s", err)
				continue
			}

			var largeContents []*util.Content
			if err := m.db.Where("size > ? and not dag_split", m.cfg.Content.MaxSize).Order("id asc").FindInBatches(&largeContents, 2000, func(tx *gorm.DB, batch int) error {
				m.log.Debugf("trying to backfill split queue for the next sets of contents: %d - %d", largeContents[0].ID, largeContents[len(largeContents)-1].ID)

				for _, c := range largeContents {
					if err := m.backfill(ctx, c, tracker); err != nil {
						m.log.Errorf("", err)
						continue
					}
				}
				return nil
			}).Error; err != nil {
				m.log.Errorf("", err)
			}
		}
	}
}

func (m *manager) backfill(ctx context.Context, cont *util.Content, tracker *model.SplitQueueTracker) error {
	return m.db.Transaction(func(tx *gorm.DB) error {
		if err := m.splitQueueMgr.QueueContent(ctx, cont.ID, cont.UserID); err != nil {
			return err
		}
		return tx.Model(model.SplitQueueTracker{}).Where("id = ?", tracker.ID).UpdateColumn("last_cont_id", cont.ID).Error
	})
}

func (m *manager) runSplitWorker(ctx context.Context) {
	timer := time.NewTicker(m.cfg.StagingBucket.CreationInterval)
	for {
		select {
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
	return m.db.Where("size > ? and not dag_split", m.cfg.Content.MaxSize).Order("id asc").FindInBatches(&tasks, 2000, func(tx *gorm.DB, batch int) error {
		m.log.Debugf("trying to split the next sets of contents: %d - %d", tasks[0].ID, tasks[len(tasks)-1].ID)

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
