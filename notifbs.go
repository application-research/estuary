package main

import (
	"sync"

	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
)

type notifyBlockstore struct {
	EstuaryBlockstore

	subLk sync.Mutex
	subs  map[cid.Cid][]chan blocks.Block
}

func NewNotifBs(bstore EstuaryBlockstore) *notifyBlockstore {
	return &notifyBlockstore{
		EstuaryBlockstore: bstore,
		subs:              make(map[cid.Cid][]chan blocks.Block),
	}

}

func (nb *notifyBlockstore) WaitFor(c cid.Cid) <-chan blocks.Block {
	nch := make(chan blocks.Block, 1)
	nb.subLk.Lock()
	nb.subs[c] = append(nb.subs[c], nch)
	nb.subLk.Unlock()

	// now handle the race condition where the block might have been added
	// right before calling this method
	blk, err := nb.Get(c)
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

func (nb *notifyBlockstore) Put(blk blocks.Block) error {
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

	return nb.EstuaryBlockstore.Put(blk)
}

func (nb *notifyBlockstore) PutMany(blks []blocks.Block) error {
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

	return nb.EstuaryBlockstore.PutMany(blks)
}
