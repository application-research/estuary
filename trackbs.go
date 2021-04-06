package main

import (
	"context"
	"fmt"
	"time"

	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	"gorm.io/gorm"
)

type TrackingBlockstore struct {
	bs blockstore.Blockstore

	db *gorm.DB

	buffer    map[cid.Cid]accesses
	getCh     chan cid.Cid
	hasCh     chan cid.Cid
	countsReq chan getCountsReq
}

type accesses struct {
	Last time.Time
	Get  int
	Has  int
}

func NewTrackingBlockstore(bs blockstore.Blockstore, db *gorm.DB) *TrackingBlockstore {
	tbs := &TrackingBlockstore{
		bs:        bs,
		db:        db,
		buffer:    make(map[cid.Cid]accesses),
		getCh:     make(chan cid.Cid, 32),
		hasCh:     make(chan cid.Cid, 32),
		countsReq: make(chan getCountsReq, 32),
	}

	go tbs.coalescer()

	return tbs
}

var _ (blockstore.Blockstore) = (*TrackingBlockstore)(nil)

type getCountsReq struct {
	req  []Object
	resp chan []int
}

func (tbs *TrackingBlockstore) Under() blockstore.Blockstore {
	return tbs.bs
}

func (tbs *TrackingBlockstore) GetCounts(objects []Object) ([]int, error) {
	req := getCountsReq{
		req:  objects,
		resp: make(chan []int),
	}

	tbs.countsReq <- req

	resp := <-req.resp

	return resp, nil
}

func (tbs *TrackingBlockstore) coalescer() {
	for {
		select {
		case c := <-tbs.getCh:
			acc := tbs.buffer[c]
			acc.Get++
			acc.Last = time.Now()
			tbs.buffer[c] = acc
		case c := <-tbs.hasCh:
			acc := tbs.buffer[c]
			acc.Has++
			tbs.buffer[c] = acc
		case req := <-tbs.countsReq:
			resp := make([]int, len(req.req))
			for i, o := range req.req {
				resp[i] = tbs.buffer[o.Cid.CID].Get
			}
			req.resp <- resp
		}
	}
}

func (tbs *TrackingBlockstore) AllKeysChan(context.Context) (<-chan cid.Cid, error) {
	return nil, fmt.Errorf("not supported")
}

func (tbs *TrackingBlockstore) DeleteBlock(_ cid.Cid) error {
	return fmt.Errorf("deleting blocks not supported through this interface")
}

func (tbs *TrackingBlockstore) Get(c cid.Cid) (blocks.Block, error) {
	tbs.getCh <- c
	return tbs.bs.Get(c)
}

func (tbs *TrackingBlockstore) GetSize(c cid.Cid) (int, error) {
	return tbs.bs.GetSize(c)
}

func (tbs *TrackingBlockstore) Has(c cid.Cid) (bool, error) {
	has, err := tbs.bs.Has(c)
	if err != nil {
		return false, err
	}
	if has {
		tbs.hasCh <- c
	}
	return has, nil
}

func (tbs *TrackingBlockstore) HashOnRead(hashOnRead bool) {
	tbs.bs.HashOnRead(hashOnRead)
}

func (tbs *TrackingBlockstore) Put(blk blocks.Block) error {
	return fmt.Errorf("should not be writing blocks through this blockstore")
}

func (tbs *TrackingBlockstore) PutMany(blks []blocks.Block) error {
	return fmt.Errorf("should not be writing blocks through this blockstore")
}
