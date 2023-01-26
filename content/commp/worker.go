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
		case <-ctx.Done():
			m.log.Info("shutting down commp worker")
			return
		case <-timer.C:
			m.log.Debugf("running commp worker")

			// get contents grouped by cid, so one commp can work for all contents with same cid
			var tasks []*model.DealQueue
			if err := m.db.Where("not commp_done and commp_attempted < 3 and commp_next_attempt_at < ?", time.Now().UTC()).Distinct("cont_cid, id").Order("id asc").Limit(10).Find(&tasks).Error; err != nil {
				m.log.Warnf("failed to get contents to commp - %s", err)
				continue
			}

			for _, t := range tasks {
				m.log.Debugf("trying to generate commp for cid: %s", t.ContCid)

				_, _, _, err := m.GetPieceCommitment(ctx, t.ContCid.CID, m.blockstore)
				if err != nil && err != gorm.ErrRecordNotFound {
					m.log.Warnf("failed to get commp record for cid: %s - %s", t.ContCid.CID, err)
					continue
				}

				if err != nil && err == gorm.ErrRecordNotFound {
					m.runCommpForContent(context.Background(), t.ContCid.CID)
					continue
				}
				// if commp already exist, update all contents where cid
				m.commpStatusUpdater.CommpExist(t.ContCid.CID)
			}
		}
	}
}

func (m *manager) runCommpForContent(ctx context.Context, data cid.Cid) {
	m.log.Debugf("generating commp for cid: %d", data)

	pc, carSize, size, err := m.RunPieceCommCompute(ctx, data, m.blockstore)
	if err != nil && err != ErrWaitForRemoteCompute {
		m.log.Errorf("failed to run commp for cid: %s - %s", data, err)
		m.commpStatusUpdater.ComputeFailed(data)
		return
	}

	if err != nil && err == ErrWaitForRemoteCompute {
		m.commpStatusUpdater.ComputeRequested(data)
		return
	}
	m.commpStatusUpdater.ComputeCompleted(data, pc, size, carSize)
}
