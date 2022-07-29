package main

import (
	"context"

	"github.com/application-research/estuary/config"
	"github.com/application-research/estuary/util"
	"github.com/ipfs/go-cid"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	"gorm.io/gorm"
)

type Initializer struct {
	cfg            *config.Node
	db             *gorm.DB
	trackingBstore *TrackingBlockstore
}

func (init *Initializer) Config() *config.Node {
	return init.cfg
}

func (init *Initializer) BlockstoreWrap(bs blockstore.Blockstore) (blockstore.Blockstore, error) {
	init.trackingBstore = NewTrackingBlockstore(bs, init.db)
	return init.trackingBstore, nil
}

func (init *Initializer) KeyProviderFunc(rpctx context.Context) (<-chan cid.Cid, error) {
	log.Infof("running key provider func")
	out := make(chan cid.Cid)
	go func() {
		defer close(out)

		var contents []util.Content
		if err := init.db.Find(&contents, "active").Error; err != nil {
			log.Errorf("failed to load contents for reproviding: %s", err)
			return
		}
		log.Infof("key provider func returning %d values", len(contents))

		for _, c := range contents {
			select {
			case out <- c.Cid.CID:
			case <-rpctx.Done():
				return
			}
		}
	}()
	return out, nil
}
