package main

import (
	"context"
	"time"

	"github.com/application-research/estuary/util"
)

// RunPinningRetryWorker re-attempt pinning contents that have not yet been pinned after a period of time
func (cm *ContentManager) RunPinningRetryWorker(ctx context.Context) {
	log.Info("running pinning retry worker .......")

	timer := time.NewTicker(cm.cfg.Pinning.RetryWorker.Interval)
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			startContentID := 0
			batchDate := time.Now().Add(-cm.cfg.Pinning.RetryWorker.BatchSelectionDuration)
			for {
				var contents []util.Content
				if err := cm.DB.Limit(cm.cfg.Pinning.RetryWorker.BatchSelectionLimit).Order("id ASC").Find(&contents, "pinning and not active and not failed and not aggregate and id > ? and created_at > ?", startContentID, batchDate).Error; err != nil {
					log.Errorf("failed to get contents for pinning monitor: %s", err)
					return
				}

				if len(contents) == 0 {
					break
				}

				go cm.pinContents(ctx, contents)
				startContentID = int(contents[len(contents)-1].ID)
			}
		}
	}
}
