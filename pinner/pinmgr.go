package pinner

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/application-research/estuary/types"
	"github.com/ipfs/go-cid"
	logging "github.com/ipfs/go-log"
	"github.com/libp2p/go-libp2p-core/peer"
)

var log = logging.Logger("pinner")

type PinFunc func(context.Context, *PinningOperation, PinProgressCB) error

type PinProgressCB func(int64)
type PinStatusFunc func(uint, string)

func NewPinManager(pinfunc PinFunc, scf PinStatusFunc) *PinManager {
	if scf == nil {
		scf = func(uint, string) {}
	}
	return &PinManager{
		pinQueue:         make(map[uint][]*PinningOperation),
		activePins:       make(map[uint]int),
		pinQueueIn:       make(chan *PinningOperation, 64),
		pinQueueOut:      make(chan *PinningOperation),
		pinComplete:      make(chan *PinningOperation, 64),
		RunPinFunc:       pinfunc,
		StatusChangeFunc: scf,
	}
}

type PinManager struct {
	pinQueueIn  chan *PinningOperation
	pinQueueOut chan *PinningOperation
	pinComplete chan *PinningOperation
	pinQueue    map[uint][]*PinningOperation
	activePins  map[uint]int
	pinQueueLk  sync.Mutex

	RunPinFunc       PinFunc
	StatusChangeFunc func(uint, string)
}

// TODO: some of these fields are overkill for the generalized pin manager
// thing, but are still in use by the primary estuary node. Should probably
// find a way to decouple this better
type PinningOperation struct {
	Obj   cid.Cid
	Name  string
	Peers []peer.AddrInfo
	Meta  map[string]interface{}

	Status string

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
}

func (po *PinningOperation) fail(err error) {
	po.lk.Lock()
	po.FetchErr = err
	po.EndTime = time.Now()
	po.Status = "failed"
	po.LastUpdate = time.Now()
	po.lk.Unlock()
}

func (po *PinningOperation) complete() {
	po.lk.Lock()
	defer po.lk.Unlock()

	po.EndTime = time.Now()
	po.LastUpdate = time.Now()
	po.Status = "pinned"
}

func (po *PinningOperation) SetStatus(st string) {
	po.lk.Lock()
	defer po.lk.Unlock()

	po.Status = st
	po.LastUpdate = time.Now()
}

func (po *PinningOperation) PinStatus() *types.IpfsPinStatus {
	po.lk.Lock()
	defer po.lk.Unlock()

	return &types.IpfsPinStatus{
		Requestid: fmt.Sprint(po.ContId),
		Status:    po.Status,
		Created:   po.Started,
		Pin: types.IpfsPin{
			Cid:  po.Obj.String(),
			Name: po.Name,
			Meta: po.Meta,
		},
		Info: map[string]interface{}{
			"obj_fetched":  po.NumFetched,
			"size_fetched": po.SizeFetched,
		},
	}
}

const maxActivePerUser = 15

func (pm *PinManager) PinQueueSize() int {
	var count int
	pm.pinQueueLk.Lock()
	defer pm.pinQueueLk.Unlock()
	for _, pq := range pm.pinQueue {
		count += len(pq)
	}

	return count
}

func (pm *PinManager) Add(op *PinningOperation) {
	go func() {
		pm.pinQueueIn <- op
	}()

}

var maxTimeout = 24 * time.Hour

func (pm *PinManager) doPinning(op *PinningOperation) error {
	ctx, cancel := context.WithTimeout(context.Background(), maxTimeout)
	defer cancel()

	op.SetStatus("pinning")
	pm.StatusChangeFunc(op.ContId, "pinning")

	if err := pm.RunPinFunc(ctx, op, func(size int64) {
		op.lk.Lock()
		defer op.lk.Unlock()
		op.NumFetched++
		op.SizeFetched += size
	}); err != nil {
		op.fail(err)
		pm.StatusChangeFunc(op.ContId, "failed")
		return err
	}
	op.complete()
	pm.StatusChangeFunc(op.ContId, "pinned")
	return nil
}

func (pm *PinManager) popNextPinOp() *PinningOperation {
	if len(pm.pinQueue) == 0 {
		return nil
	}

	var minCount int = 10000
	var user uint
	for u := range pm.pinQueue {
		active := pm.activePins[u]
		if active < minCount {
			minCount = active
			user = u
		}
	}

	_, ok := pm.pinQueue[0]
	if ok {
		user = 0
	}

	if minCount >= maxActivePerUser && user != 0 {
		return nil
	}

	pq := pm.pinQueue[user]

	next := pq[0]

	if len(pq) == 1 {
		delete(pm.pinQueue, user)
	} else {
		pm.pinQueue[user] = pq[1:]
	}

	return next
}

func (pm *PinManager) enqueuePinOp(po *PinningOperation) {
	u := po.UserId
	if po.SkipLimiter {
		u = 0
	}

	q := pm.pinQueue[u]
	pm.pinQueue[u] = append(q, po)
}

func (pm *PinManager) Run(workers int) {
	for i := 0; i < workers; i++ {
		go pm.pinWorker()
	}

	var next *PinningOperation

	var send chan *PinningOperation

	next = pm.popNextPinOp()
	if next != nil {
		send = pm.pinQueueOut
	}

	for {
		select {
		case op := <-pm.pinQueueIn:
			if next == nil {
				next = op
				send = pm.pinQueueOut
			} else {
				pm.pinQueueLk.Lock()
				pm.enqueuePinOp(op)
				pm.pinQueueLk.Unlock()
			}
		case send <- next:
			pm.pinQueueLk.Lock()
			pm.activePins[next.UserId]++

			next = pm.popNextPinOp()
			if next == nil {
				send = nil
			}
			pm.pinQueueLk.Unlock()
		case op := <-pm.pinComplete:
			pm.pinQueueLk.Lock()
			pm.activePins[op.UserId]--

			if next == nil {
				next = pm.popNextPinOp()
				if next != nil {
					send = pm.pinQueueOut
				}
			}
			pm.pinQueueLk.Unlock()

		}
	}
}

func (pm *PinManager) pinWorker() {
	for op := range pm.pinQueueOut {
		if err := pm.doPinning(op); err != nil {
			log.Errorf("pinning queue error: %s", err)
		}
		pm.pinComplete <- op
	}
}
