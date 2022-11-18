package stagingbs

import (
	"fmt"
	"io/ioutil"
	"os"
	"strings"
	"sync"

	lmdb "github.com/filecoin-project/go-bs-lmdb"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
)

type StagingBSMgr struct {
	RootDir string

	olk  sync.Mutex
	open map[BSID]*lmdb.Blockstore
}

func NewStagingBSMgr(dir string) (*StagingBSMgr, error) {
	if err := os.MkdirAll(dir, 0750); err != nil {
		return nil, err
	}

	return &StagingBSMgr{
		RootDir: dir,
		open:    make(map[BSID]*lmdb.Blockstore),
	}, nil
}

type BSID string

func (sbmgr *StagingBSMgr) AllocNew() (BSID, blockstore.Blockstore, error) {
	dir, err := ioutil.TempDir(sbmgr.RootDir, "bs-*")
	if err != nil {

	}

	bstore, err := lmdb.Open(&lmdb.Options{
		Path:   dir,
		NoSync: false,
	})
	if err != nil {
		return "", nil, err
	}

	sbmgr.olk.Lock()
	sbmgr.open[BSID(dir)] = bstore
	sbmgr.olk.Unlock()

	return BSID(dir), bstore, nil
}

func (sbmgr *StagingBSMgr) CleanUp(bsid BSID) error {
	if bsid == "" {
		return fmt.Errorf("refusing to cleanup empty BSID")
	}

	if !strings.HasPrefix(string(bsid), sbmgr.RootDir) {
		return fmt.Errorf("given bsid not managed by this instance")
	}

	sbmgr.olk.Lock()
	bs, ok := sbmgr.open[bsid]
	sbmgr.olk.Unlock()
	if ok {
		err := bs.Close()
		if err != nil {
			return err
		}
	}

	return os.RemoveAll(string(bsid))
}
