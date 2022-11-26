package node

import (
	"context"

	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
)

type sanityCheckFn func(cid cid.Cid, errMsg string)

type SanityCheckBlockstore struct {
	blockstore.Blockstore
	checkFn sanityCheckFn
}

func newSanityCheckBlockstoreWrapper(bs blockstore.Blockstore) SanityCheckBlockstore {
	return SanityCheckBlockstore{
		checkFn:    func(cid cid.Cid, errMsg string) {},
		Blockstore: bs,
	}
}

func (sc SanityCheckBlockstore) Get(ctx context.Context, cid cid.Cid) (blocks.Block, error) {
	blk, err := sc.Blockstore.Get(ctx, cid)
	if err != nil {
		// anytime a block reads fail, do sanity check
		go sc.checkFn(cid, err.Error())
	}
	return blk, err
}

func (sc SanityCheckBlockstore) SetSanityCheckFn(fn sanityCheckFn) {
	sc.checkFn = fn
}
