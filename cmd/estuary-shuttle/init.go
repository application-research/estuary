package main

import (
	"context"

	"github.com/application-research/estuary/config"
	"github.com/ipfs/go-cid"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	"gorm.io/gorm"
)

type Initializer struct {
	cfg *config.Node
	db  *gorm.DB
}

func (init Initializer) Config() *config.Node {
	return init.cfg
}

func (init Initializer) BlockstoreWrap(blk blockstore.Blockstore) (blockstore.Blockstore, error) {
	return blk, nil
}

func (init *Initializer) KeyProviderFunc(rpctx context.Context) (<-chan cid.Cid, error) {
	out := make(chan cid.Cid)
	go func() {
		defer close(out)
		var pins []Pin
		init.db.Where("active = ?", true).FindInBatches(&pins, 500, func(tx *gorm.DB, batch int) error {
			for _, c := range pins {
				out <- c.Cid.CID
			}
			return nil
		})
	}()
	return out, nil
}
