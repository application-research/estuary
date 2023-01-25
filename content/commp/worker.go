package commp

import (
	"context"
	"time"

	"github.com/application-research/estuary/model"
	"github.com/ipfs/go-cid"
	"gorm.io/gorm"
)

func (m *manager) runWorker(ctx context.Context) {
	m.log.Infof("starting up commp worker")

	go m.runCommpForContents(ctx)

	m.log.Infof("spun up commp worker")
}

func (m *manager) runCommpForContents(ctx context.Context) {
	timer := time.NewTicker(m.cfg.WorkerIntervals.CommpInterval)
	for {
		select {
		case <-timer.C:
			// get contents grouped by cid, so one commp can work for all contents with same cid
			var jobs []*model.DealQueue
			if err := m.db.Where("not commp_done and commp_attempted < 3 and commp_next_attempt_at < ?", time.Now().UTC()).Order("id asc").Limit(10).Group("cont_cid").Find(&jobs).Error; err != nil {
				m.log.Warnf("failed to contents to commp - %s", err)
				continue
			}

			for _, job := range jobs {
				_, _, _, err := m.GetPieceCommitment(ctx, job.ContCID.CID, m.blockstore)
				if err != nil && err != gorm.ErrRecordNotFound {
					m.log.Warnf("failed to get commp record - %s", err)
					continue
				}

				if err != nil && err == gorm.ErrRecordNotFound {
					m.runCommpForContent(context.Background(), job.ContID, job.ContCID.CID)
					continue
				}

				// if commp already exist, update all contents where cid
				if err := m.commpStatusUpdater.CommpExist(job.ContCID.CID); err != nil {
					m.log.Warnf("failed to update commp queue - %s", err)
					continue
				}
			}
		}
	}
}

func (m *manager) runCommpForContent(ctx context.Context, contID uint64, data cid.Cid) {
	m.log.Debugf("generating commp for cont: %d", contID)

	pc, carSize, size, err := m.RunPieceCommCompute(ctx, data, m.blockstore)
	if err != nil && err != ErrWaitForRemoteCompute {
		m.log.Errorf("", err)
		if err := m.commpStatusUpdater.ComputeFailed(data); err != nil {
			m.log.Errorf("", err)
		}
		return
	}

	if err != nil && err == ErrWaitForRemoteCompute {
		if err := m.commpStatusUpdater.ComputeRequested(data); err != nil {
			m.log.Errorf("", err)
		}
		return
	}

	if err := m.commpStatusUpdater.ComputeCompleted(data, pc, size, carSize); err != nil {
		m.log.Errorf("", err)
	}
}
