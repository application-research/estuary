package sanitycheck

import (
	"context"

	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
)

type CheckFn func(cid cid.Cid, errMsg string)

type Blockstore struct {
	blockstore.Blockstore
	checkFn CheckFn
}

func NewBlockstoreWrapper(bs blockstore.Blockstore, fn CheckFn) *Blockstore {
	return &Blockstore{
		checkFn:    fn,
		Blockstore: bs,
	}
}

func (bs Blockstore) Get(ctx context.Context, cid cid.Cid) (blocks.Block, error) {
	blk, err := bs.Blockstore.Get(ctx, cid)
	if err != nil {
		// anytime a block reads fail, do sanity check
		bs.checkFn(cid, err.Error())
	}
	return blk, err
}
