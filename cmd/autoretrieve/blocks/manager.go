package blocks

import (
	"errors"
	"path/filepath"
	"sync"

	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	flatfs "github.com/ipfs/go-ds-flatfs"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
)

const blockstoreSubdir = "blockstore"

type Block blocks.Block

type ManagerConfig struct {
	DataDir string
}

// Manager is a blockstore with thread safe notification hooking for put
// events.
type Manager struct {
	blockstore.Blockstore
	waitList   map[cid.Cid][]chan<- Block
	waitListLk sync.Mutex
}

func NewManager(config ManagerConfig) (*Manager, error) {
	parseShardFunc, err := flatfs.ParseShardFunc("/repo/flatfs/shard/v1/next-to-last/3")
	if err != nil {
		return nil, err
	}

	blockstoreDatastore, err := flatfs.CreateOrOpen(filepath.Join(config.DataDir, blockstoreSubdir), parseShardFunc, false)
	if err != nil {
		return nil, err
	}

	return &Manager{
		Blockstore: blockstore.NewBlockstoreNoPrefix(blockstoreDatastore),
		waitList:   make(map[cid.Cid][]chan<- Block),
	}, nil
}

func (mgr *Manager) GetAwait(cid cid.Cid, waitChan chan<- Block) error {
	block, err := mgr.Get(cid)

	// If we couldn't get the block, we add it to the waitlist - the block will
	// be populated later during a Put or PutMany event
	if err != nil {
		if !errors.Is(err, blockstore.ErrNotFound) {
			return err
		}

		mgr.waitListLk.Lock()
		mgr.waitList[cid] = append(mgr.waitList[cid], waitChan)
		mgr.waitListLk.Unlock()

		return nil
	}

	// Otherwise, we can immediately populate the channel
	waitChan <- block

	return nil
}

func (mgr *Manager) Put(block blocks.Block) error {
	// We do this first since it should catch any errors with block being nil
	if err := mgr.Blockstore.Put(block); err != nil {
		return err
	}

	mgr.notifyWaitChans(block)

	return nil
}

func (mgr *Manager) PutMany(blocks []blocks.Block) error {
	if err := mgr.Blockstore.PutMany(blocks); err != nil {
		return err
	}

	for _, block := range blocks {
		mgr.notifyWaitChans(block)
	}

	return nil
}

func (mgr *Manager) notifyWaitChans(block Block) {
	cid := block.Cid()

	mgr.waitListLk.Lock()
	if blockChans, ok := mgr.waitList[cid]; ok {
		mgr.waitListLk.Unlock()

		// I'm unlocking the inside of this loop to avoid potential deadlock if
		// ch is full - is this possible?

		delete(mgr.waitList, cid)
		for _, ch := range blockChans {
			ch <- block
		}

		mgr.waitListLk.Lock()
	}
	mgr.waitListLk.Unlock()
}
