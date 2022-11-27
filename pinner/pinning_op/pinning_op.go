package pinning_op

import (
	"sync"
	"time"

	"github.com/application-research/estuary/pinner/types"
	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p-core/peer"
)

// TODO: some of these fields are overkill for the generalized pin manager
// thing, but are still in use by the primary estuary node. Should probably
// find a way to decouple this better
type PinningOperation struct {
	Obj   cid.Cid
	Name  string
	Peers []*peer.AddrInfo
	Meta  string

	Status types.PinningStatus

	UserId  uint
	ContId  uint
	Replace uint

	LastUpdate time.Time

	Started     time.Time
	NumFetched  int
	SizeFetched int64
	FetchErr    error
	EndTime     time.Time

	Location string

	SkipLimiter bool

	lk sync.Mutex

	MakeDeal bool
}

func (po *PinningOperation) Fail(err error) {
	po.lk.Lock()
	defer po.lk.Unlock()

	po.FetchErr = err
	po.EndTime = time.Now()
	po.Status = types.PinningStatusFailed
	po.LastUpdate = time.Now()
}

func (po *PinningOperation) SetStatus(st types.PinningStatus) {
	po.lk.Lock()
	defer po.lk.Unlock()

	po.Status = st
	po.LastUpdate = time.Now()
}

func (po *PinningOperation) Complete() {
	po.lk.Lock()
	defer po.lk.Unlock()

	po.EndTime = time.Now()
	po.LastUpdate = time.Now()
	po.Status = types.PinningStatusPinned
}

func (po *PinningOperation) UpdateProgress(size int64) {
	po.lk.Lock()
	defer po.lk.Unlock()

	po.NumFetched++
	po.SizeFetched += size
}
