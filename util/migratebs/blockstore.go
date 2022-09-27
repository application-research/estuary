package migratebs

import (
	"context"
	ipld "github.com/ipfs/go-ipld-format"
	"time"

	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	logging "github.com/ipfs/go-log/v2"
)

var log = logging.Logger("bs-migrate")

type Blockstore struct {
	dest blockstore.Blockstore

	src blockstore.Blockstore

	del bool
}

func NewBlockstore(from, to blockstore.Blockstore, del bool) (*Blockstore, error) {
	bs := &Blockstore{
		dest: to,
		src:  from,
		del:  del,
	}

	go bs.migrateData(context.Background())

	return bs, nil
}

func (bs *Blockstore) migrateData(ctx context.Context) {
	ch, err := bs.src.AllKeysChan(ctx)
	if err != nil {
		log.Errorf("failed to get keys chan: %s", err)
		return
	}

	log.Infof("starting blockstore migration...")
	var count int
	var fails int
	for c := range ch {
		count++
		if count%20 == 0 {
			log.Infof("migration progress: %d (%d)", count, fails)
		}
		blk, err := bs.src.Get(ctx, c)
		if err != nil {
			log.Errorf("failed to read from source blockstore: %s", err)
			fails++
			time.Sleep(time.Millisecond * 100)
			continue
		}

		if err := bs.dest.Put(ctx, blk); err != nil {
			log.Errorf("failed to write to target blockstore: %s", err)
			fails++
			time.Sleep(time.Millisecond * 100)
			continue
		}

		if bs.del {
			if err := bs.src.DeleteBlock(ctx, blk.Cid()); err != nil {
				fails++
				log.Errorf("failed to delete block from source blockstore: %s", err)
			}
		}
	}
	log.Infof("Migration complete! (count=%d, fails=%d)", count, fails)
}

func (bs *Blockstore) DeleteBlock(ctx context.Context, c cid.Cid) error {
	if err := bs.src.DeleteBlock(ctx, c); err != nil {
		return err
	}

	if err := bs.dest.DeleteBlock(ctx, c); err != nil {
		return err
	}

	return nil
}

type batchDeleter interface {
	DeleteMany([]cid.Cid) error
}

func (bs *Blockstore) DeleteMany(ctx context.Context, cids []cid.Cid) error {
	if dm, ok := bs.src.(batchDeleter); ok {
		return dm.DeleteMany(cids)
	}

	if dm, ok := bs.dest.(batchDeleter); ok {
		return dm.DeleteMany(cids)
	}

	for _, c := range cids {
		if err := bs.src.DeleteBlock(ctx, c); err != nil {
			return err
		}
		if err := bs.dest.DeleteBlock(ctx, c); err != nil {
			return err
		}
	}

	return nil
}

func (bs *Blockstore) Has(ctx context.Context, c cid.Cid) (bool, error) {
	has, err := bs.dest.Has(ctx, c)
	if err != nil {
		return false, err
	}

	if has {
		return true, nil
	}

	return bs.src.Has(ctx, c)
}

func (bs *Blockstore) Get(ctx context.Context, c cid.Cid) (blocks.Block, error) {
	blk, err := bs.dest.Get(ctx, c)
	if err == nil {
		return blk, nil
	}
	if err != nil {
		if !ipld.IsNotFound(err) {
			return nil, err
		}
	}

	return bs.src.Get(ctx, c)
}

// GetSize returns the CIDs mapped BlockSize
func (bs *Blockstore) GetSize(ctx context.Context, c cid.Cid) (int, error) {
	s, err := bs.dest.GetSize(ctx, c)
	if err == nil {
		return s, nil
	}
	if err != nil {
		if !ipld.IsNotFound(err) {
			return 0, err
		}
	}

	return bs.src.GetSize(ctx, c)
}

// Put puts a given block to the underlying datastore
func (bs *Blockstore) Put(ctx context.Context, blk blocks.Block) error {
	return bs.dest.Put(ctx, blk)
}

// PutMany puts a slice of blocks at the same time using batching
// capabilities of the underlying datastore whenever possible.
func (bs *Blockstore) PutMany(ctx context.Context, blks []blocks.Block) error {
	return bs.dest.PutMany(ctx, blks)
}

// AllKeysChan returns a channel from which
// the CIDs in the Blockstore can be read. It should respect
// the given context, closing the channel if it becomes Done.
func (bs *Blockstore) AllKeysChan(ctx context.Context) (<-chan cid.Cid, error) {
	return bs.dest.AllKeysChan(ctx)
}

// HashOnRead specifies if every read block should be
// rehashed to make sure it matches its CID.
func (bs *Blockstore) HashOnRead(enabled bool) {
	bs.dest.HashOnRead(enabled)
}

func (bs *Blockstore) View(ctx context.Context, c cid.Cid, f func([]byte) error) error {
	if cview, ok := bs.dest.(blockstore.Viewer); ok {
		err := cview.View(ctx, c, f)
		if err == nil {
			return nil
		}
		if !ipld.IsNotFound(err) {
			return err
		}
		// explicitly fall through to backup logic...
	}

	// reusing the Get method here to reuse the error handling logic from there
	blk, err := bs.Get(ctx, c)
	if err != nil {
		return err
	}

	return f(blk.RawData())
}
