package main

import (
	"context"

	"github.com/application-research/estuary/config"
	"github.com/ipfs/go-cid"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	"gorm.io/gorm"
)

type Initializer struct {
	cfg *config.Config
	db  *gorm.DB
}

func (init Initializer) Config() *config.Node {
	return init.cfg
}

func (init Initializer) BlockstoreWrap(blk blockstore.Blockstore) (blockstore.Blockstore, error) {
	return blk, nil
}

func (init Initializer) KeyProviderFunc(ctx context.Context) (<-chan cid.Cid, error) {
	log.Infof("running key provider func")
	out := make(chan cid.Cid)
	go func() {
		defer close(out)

		var pins []Pin
		if err := init.db.Find(&pins, "active").Error; err != nil {
			log.Errorf("failed to load pins for reproviding: %s", err)
			return
		}
		log.Infof("key provider func returning %d values", len(pins))

		for _, c := range pins {
			select {
			case out <- c.Cid.CID:
			case <-ctx.Done():
				return
			}
		}
	}()
	return out, nil
}
