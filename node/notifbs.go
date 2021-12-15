package node

import (
	"context"
	"sync"

	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
)

type NotifyBlockstore struct {
	EstuaryBlockstore

	subLk sync.Mutex
	subs  map[cid.Cid][]chan blocks.Block
}

var _ blockstore.Blockstore = (*NotifyBlockstore)(nil)

func NewNotifBs(bstore EstuaryBlockstore) *NotifyBlockstore {
	return &NotifyBlockstore{
		EstuaryBlockstore: bstore,
		subs:              make(map[cid.Cid][]chan blocks.Block),
	}

}

func (nb *NotifyBlockstore) WaitFor(ctx context.Context, c cid.Cid) <-chan blocks.Block {
	nch := make(chan blocks.Block, 1)
	nb.subLk.Lock()
	nb.subs[c] = append(nb.subs[c], nch)
	nb.subLk.Unlock()

	// now handle the race condition where the block might have been added
	// right before calling this method
	blk, err := nb.Get(ctx, c)
	if err == nil {
		nb.subLk.Lock()
		chs, ok := nb.subs[c]
		if ok {
			for _, ch := range chs {
				ch <- blk
				close(ch)
			}
			delete(nb.subs, c)
		}
		nb.subLk.Unlock()
	}
	return nch
}

func (nb *NotifyBlockstore) Put(ctx context.Context, blk blocks.Block) error {
	c := blk.Cid()
	nb.subLk.Lock()
	chs, ok := nb.subs[c]
	if ok {
		for _, ch := range chs {
			ch <- blk
			close(ch)
		}
		delete(nb.subs, c)
	}
	nb.subLk.Unlock()

	return nb.EstuaryBlockstore.Put(ctx, blk)
}

func (nb *NotifyBlockstore) PutMany(ctx context.Context, blks []blocks.Block) error {
	nb.subLk.Lock()
	for _, blk := range blks {
		c := blk.Cid()
		chs, ok := nb.subs[c]
		if ok {
			for _, ch := range chs {
				ch <- blk
				close(ch)
			}
			delete(nb.subs, c)
		}
	}
	nb.subLk.Unlock()

	return nb.EstuaryBlockstore.PutMany(ctx, blks)
}
