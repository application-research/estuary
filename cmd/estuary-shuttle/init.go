package main

import (
	"context"

	"github.com/application-research/estuary/config"
	"github.com/ipfs/go-cid"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
)

type Initializer struct {
	cfg *config.Config
}

func (init Initializer) Config() *config.Config {
	return init.cfg
}

func (init Initializer) BlockstoreWrap(blk blockstore.Blockstore) (blockstore.Blockstore, error) {
	return blk, nil
}

func (init Initializer) KeyProviderFunc(context.Context) (<-chan cid.Cid, error) {
	return nil, nil
}
