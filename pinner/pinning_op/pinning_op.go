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

	Lk sync.Mutex

	MakeDeal bool
}

func (po *PinningOperation) Fail(err error) {
	po.Lk.Lock()
	po.FetchErr = err
	po.EndTime = time.Now()
	po.Status = types.PinningStatusFailed
	po.LastUpdate = time.Now()
	po.Lk.Unlock()
}

func (po *PinningOperation) SetStatus(st types.PinningStatus) {
	po.Lk.Lock()
	defer po.Lk.Unlock()

	po.Status = st
	po.LastUpdate = time.Now()
}
