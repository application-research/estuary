package pinner

import (
	"context"
	"encoding/json"
	"time"

	"github.com/application-research/estuary/config"
	"github.com/application-research/estuary/constants"
	"github.com/application-research/estuary/util"
	"github.com/libp2p/go-libp2p/core/peer"
	"gorm.io/gorm"
)

// RunPinningRetryWorker re-attempt pinning contents that have not yet been pinned after a period of time
func (pm *EstuaryPinManager) RunPinningRetryWorker(ctx context.Context, db *gorm.DB, cfg *config.Estuary) {
	log.Info("running pinning retry worker .......")

	timer := time.NewTicker(cfg.Pinning.RetryWorker.Interval)
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			startContentID := 0
			batchDate := time.Now().Add(-cfg.Pinning.RetryWorker.BatchSelectionDuration)
			for {
				var contents []util.Content
				if err := db.Limit(cfg.Pinning.RetryWorker.BatchSelectionLimit).Order("id ASC").Find(&contents, "pinning and not active and not failed and not aggregate and id > ? and created_at > ?", startContentID, batchDate).Error; err != nil {
					log.Errorf("failed to get contents for pinning monitor: %s", err)
					return
				}

				if len(contents) == 0 {
					break
				}

				go pm.pinContents(ctx, contents, cfg)
				startContentID = int(contents[len(contents)-1].ID)
			}
		}
	}
}

func (pm *EstuaryPinManager) pinContents(ctx context.Context, contents []util.Content, cfg *config.Estuary) {
	makeDeal := true
	for _, c := range contents {
		select {
		case <-ctx.Done():
			return
		default:
			var origins []*peer.AddrInfo
			// when refreshing pinning queue, use content origins if available
			if c.Origins != "" {
				_ = json.Unmarshal([]byte(c.Origins), &origins) // no need to handle or log err, its just a nice to have
			}

			if c.Location == constants.ContentLocationLocal {
				// if local content adding is enabled, retry local pin
				if !cfg.Content.DisableLocalAdding {
					pinOp := pm.cm.GetPinOperation(c, origins, 0, makeDeal)
					pm.Add(pinOp)
				}
			} else {
				if err := pm.shuttleMgr.PinContent(ctx, c.Location, c, origins); err != nil {
					log.Errorf("failed to send pin message to shuttle: %s", err)
					time.Sleep(time.Millisecond * 100)
				}
			}
		}
	}
}
