package node

import (
	"context"

	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
)

type sanityCheckFn func(cid cid.Cid, err error)

type SanityCheckBlockstore struct {
	blockstore.Blockstore
	checkFn func(cid cid.Cid, err error)
}

func newSanityCheckBlockstoreWrapper(bs blockstore.Blockstore) SanityCheckBlockstore {
	return SanityCheckBlockstore{
		checkFn:    func(cid cid.Cid, err error) {},
		Blockstore: bs,
	}
}

func (sc SanityCheckBlockstore) Get(ctx context.Context, cid cid.Cid) (blocks.Block, error) {
	blk, err := sc.Get(ctx, cid)
	if err != nil {
		go sc.checkFn(cid, err)
	}
	return blk, err
}

func (sc SanityCheckBlockstore) SetSanityCheckFn(fn sanityCheckFn) {
	sc.checkFn = fn
}
