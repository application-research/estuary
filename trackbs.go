package main

import (
	"context"
	"errors"
	"fmt"
	ipld "github.com/ipfs/go-ipld-format"
	"time"

	"github.com/application-research/estuary/util"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	"golang.org/x/xerrors"
	"gorm.io/gorm"
)

type TrackingBlockstore struct {
	bs blockstore.Blockstore

	db *gorm.DB

	cidReq func(context.Context, cid.Cid) (blocks.Block, error)

	buffer    map[cid.Cid]accesses
	getCh     chan cid.Cid
	hasCh     chan cid.Cid
	countsReq chan getCountsReq
	accessReq chan accessReq
}

type accesses struct {
	Last time.Time
	Get  int
	Has  int
}

func NewTrackingBlockstore(bs blockstore.Blockstore, db *gorm.DB) *TrackingBlockstore {
	cidReq := func(context.Context, cid.Cid) (blocks.Block, error) {
		return nil, errors.New("NewTrackingBlockstore: cidReq not set")
	}

	tbs := &TrackingBlockstore{
		bs:        bs,
		db:        db,
		cidReq:    cidReq,
		buffer:    make(map[cid.Cid]accesses),
		getCh:     make(chan cid.Cid, 32),
		hasCh:     make(chan cid.Cid, 32),
		countsReq: make(chan getCountsReq, 32),
		accessReq: make(chan accessReq, 32),
	}

	go tbs.coalescer()

	return tbs
}

var _ (blockstore.Blockstore) = (*TrackingBlockstore)(nil)

func (tbs *TrackingBlockstore) SetCidReqFunc(f func(context.Context, cid.Cid) (blocks.Block, error)) {
	tbs.cidReq = f
}

func (tbs *TrackingBlockstore) Under() blockstore.Blockstore {
	return tbs.bs
}

type getCountsReq struct {
	req  []util.Object
	resp chan []int
}

func (tbs *TrackingBlockstore) GetCounts(objects []util.Object) ([]int, error) {
	req := getCountsReq{
		req:  objects,
		resp: make(chan []int),
	}

	tbs.countsReq <- req

	resp := <-req.resp

	return resp, nil
}

func (tbs *TrackingBlockstore) coalescer() {
	ticker := time.Tick(time.Minute * 10)

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
		case req := <-tbs.accessReq:
			acc := tbs.buffer[req.c]
			req.resp <- acc
		case <-ticker:
			oldbuffer := tbs.buffer
			tbs.buffer = make(map[cid.Cid]accesses)
			go tbs.persistAccessCounts(oldbuffer)
		}
	}
}

func (tbs *TrackingBlockstore) persistAccessCounts(buf map[cid.Cid]accesses) {
	for c, acc := range buf {
		if acc.Get > 0 {
			err := tbs.db.Model(&util.Object{}).Where("cid = ?", c.Bytes()).Updates(map[string]interface{}{
				"reads":       gorm.Expr("reads + ?", acc.Get),
				"last_access": acc.Last,
			}).Error
			if err != nil {
				log.Errorf("failed to update object in db: %s", err)
			}
		}
	}
}

type accessReq struct {
	c    cid.Cid
	resp chan accesses
}

func (tbs *TrackingBlockstore) LastAccess(ctx context.Context, c cid.Cid) (time.Time, error) {
	req := accessReq{
		c:    c,
		resp: make(chan accesses),
	}
	select {
	case tbs.accessReq <- req:
	case <-ctx.Done():
		return time.Time{}, ctx.Err()
	}

	select {
	case resp := <-req.resp:
		return resp.Last, nil
	case <-ctx.Done():
		return time.Time{}, ctx.Err()
	}
}

func (tbs *TrackingBlockstore) AllKeysChan(context.Context) (<-chan cid.Cid, error) {
	return nil, fmt.Errorf("not supported")
}

func (tbs *TrackingBlockstore) DeleteBlock(ctx context.Context, _ cid.Cid) error {
	return fmt.Errorf("deleting blocks not supported through this interface")
}

func (tbs *TrackingBlockstore) Get(ctx context.Context, c cid.Cid) (blocks.Block, error) {
	tbs.getCh <- c // TODO: should we be tracking all requests? or all successful servings?
	blk, err := tbs.bs.Get(ctx, c)
	if err != nil {
		if ipld.IsNotFound(err) {
			var obj util.Object
			if dberr := tbs.db.First(&obj, "where cid = ?", c.Bytes()).Error; dberr != nil {
				if xerrors.Is(dberr, gorm.ErrRecordNotFound) {
					// explicitly return original error
					return nil, err
				}
				return nil, dberr
			}

			// having the object here in our database implies we are tracking it
			// So since we don't have it, and are tracking it, we need to retrieve it

			// TODO: this will wait for the retrieval to complete, which *might* take a while.
			// maybe we return not found now, and get back to it later?
			return tbs.cidReq(context.TODO(), c)
			// TODO: We should very explicitly record failures here, this is
			// one of the most critical services estuary performs
		}
		return nil, err
	}

	return blk, nil
}

func (tbs *TrackingBlockstore) GetSize(ctx context.Context, c cid.Cid) (int, error) {
	return tbs.bs.GetSize(ctx, c)
}

func (tbs *TrackingBlockstore) Has(ctx context.Context, c cid.Cid) (bool, error) {
	has, err := tbs.bs.Has(ctx, c)
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

func (tbs *TrackingBlockstore) Put(ctx context.Context, blk blocks.Block) error {
	// TODO:
	// return fmt.Errorf("should not be writing blocks through this blockstore")
	return tbs.bs.Put(ctx, blk)
}

func (tbs *TrackingBlockstore) PutMany(ctx context.Context, blks []blocks.Block) error {
	// TODO:
	// return fmt.Errorf("should not be writing blocks through this blockstore")
	return tbs.bs.PutMany(ctx, blks)
}
