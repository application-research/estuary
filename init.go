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
	trackingBstore *util.TrackingBlockstore
}

func (init *Initializer) Config() *config.Node {
	return init.cfg
}

func (init *Initializer) BlockstoreWrap(bs blockstore.Blockstore) (blockstore.Blockstore, error) {
	init.trackingBstore = util.NewTrackingBlockstore(bs, init.db)
	return init.trackingBstore, nil
}

func (init *Initializer) KeyProviderFunc(rpctx context.Context) (<-chan cid.Cid, error) {
	out := make(chan cid.Cid)
	go func() {
		defer close(out)
		var contents []util.Content
		util.FindAndProcessLargeRequests(init.db, func(tx *gorm.DB, batch int) error {
			for _, c := range contents {
				out <- c.Cid.CID
			}
			return nil
		}, &contents, "active = ?", true)
	}()
	return out, nil
}
