package main

import (
	"context"
	"time"
)

func (cm *ContentManager) RunReprovider() {
	timer := time.NewTimer(time.Minute * 10)

	for {
		select {
		case <-timer.C:
			if err := cm.runReprovide(context.TODO()); err != nil {
				log.Warnf("failed to run reprovider: %s", err)
			}

			timer.Reset(time.Hour * 12)
		}
	}
}

func (cm *ContentManager) runReprovide(ctx context.Context) error {
	var contents []Content
	if err := cm.DB.Find(&contents, "active").Error; err != nil {
		return err
	}

	for _, c := range contents {
		nctx, cancel := context.WithTimeout(ctx, time.Second*20)
		if err := cm.Dht.Provide(nctx, c.Cid.CID, true); err != nil {
			log.Warnw("providing failed", "cid", c.Cid.CID, "error", err)
		}
		cancel()
	}

	return nil
}
