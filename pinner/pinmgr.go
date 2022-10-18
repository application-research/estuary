package pinner

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/application-research/estuary/pinner/types"
	"github.com/beeker1121/goque"
	"github.com/ipfs/go-cid"
	logging "github.com/ipfs/go-log/v2"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/pkg/errors"
)

var log = logging.Logger("pinner")

type PinFunc func(context.Context, *PinningOperation, PinProgressCB) error

type PinProgressCB func(int64)
type PinStatusFunc func(contID uint, location string, status types.PinningStatus) error

func NewPinManager(pinfunc PinFunc, scf PinStatusFunc, opts *PinManagerOpts) *PinManager {
	if scf == nil {
		scf = func(contID uint, location string, status types.PinningStatus) error {
			return nil
		}
	}
	if opts == nil {
		opts = DefaultOpts
	}
	if opts.QueueDataDir == "" {
		log.Fatal("Deque needs queue data dir")
	}

	return &PinManager{
		pinQueue:         createDQue(opts.QueueDataDir),
		pinQueuePriority: createDQue(opts.QueueDataDir),
		pinQueueIn:       make(chan *PinningOperation, 64),
		pinQueueOut:      make(chan *PinningOperation),
		pinComplete:      make(chan *PinningOperation, 64),
		RunPinFunc:       pinfunc,
		StatusChangeFunc: scf,
		maxActivePerUser: opts.MaxActivePerUser,
		QueueDataDir:     opts.QueueDataDir,
	}
}

var DefaultOpts = &PinManagerOpts{
	MaxActivePerUser: 15,
	QueueDataDir:     "/tmp/",
}

type PinManagerOpts struct {
	MaxActivePerUser int
	QueueDataDir     string
}

type PinManager struct {
	pinQueueIn       chan *PinningOperation
	pinQueueOut      chan *PinningOperation
	pinComplete      chan *PinningOperation
	pinQueue         *goque.Queue
	pinQueuePriority *goque.Queue
	pinQueueLk       sync.Mutex
	RunPinFunc       PinFunc
	StatusChangeFunc PinStatusFunc
	maxActivePerUser int
	QueueDataDir     string
}

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

// PinningOperationBuilder creates a new PinningOperation and returns a pointer to it.
// This is used when we load a segment of the queue from disk for the Dque
func PinningOperationBuilder() interface{} {
	return &PinningOperation{}
}

func (po *PinningOperation) fail(err error) {
	po.lk.Lock()
	po.FetchErr = err
	po.EndTime = time.Now()
	po.Status = types.PinningStatusFailed
	po.LastUpdate = time.Now()
	po.lk.Unlock()
}

func (po *PinningOperation) complete() {
	po.lk.Lock()
	defer po.lk.Unlock()

	po.EndTime = time.Now()
	po.LastUpdate = time.Now()
	po.Status = types.PinningStatusPinned
}

func (po *PinningOperation) SetStatus(st types.PinningStatus) {
	po.lk.Lock()
	defer po.lk.Unlock()

	po.Status = st
	po.LastUpdate = time.Now()
}

func (po *PinningOperation) PinStatus() *types.IpfsPinStatusResponse {
	po.lk.Lock()
	defer po.lk.Unlock()

	meta := make(map[string]interface{}, 0)
	if po.Meta != "" {
		if err := json.Unmarshal([]byte(po.Meta), &meta); err != nil {
			log.Warnf("content %d has invalid meta: %s", po.ContId, err)
		}
	}

	originStrs := make([]string, 0)
	for _, o := range po.Peers {
		ai, err := peer.AddrInfoToP2pAddrs(o)
		if err == nil {
			for _, a := range ai {
				originStrs = append(originStrs, a.String())
			}
		}
	}

	return &types.IpfsPinStatusResponse{
		RequestID: fmt.Sprint(po.ContId),
		Status:    po.Status,
		Created:   po.Started,
		Pin: types.IpfsPin{
			CID:     po.Obj.String(),
			Name:    po.Name,
			Origins: originStrs,
			Meta:    meta,
		},
		Info: make(map[string]interface{}, 0),
		/* Ref: https://github.com/ipfs/go-pinning-service-http-client/issues/12
		Info: map[string]interface{}{
			"obj_fetched":  po.NumFetched,
			"size_fetched": po.SizeFetched,
		},
		*/
	}
}

func (pm *PinManager) PinQueueSize() int {
	pm.pinQueueLk.Lock()
	defer pm.pinQueueLk.Unlock()
	return int(pm.pinQueuePriority.Length() + pm.pinQueue.Length())
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

	op.SetStatus(types.PinningStatusPinning)
	if err := pm.StatusChangeFunc(op.ContId, op.Location, types.PinningStatusPinning); err != nil {
		return err
	}

	if err := pm.RunPinFunc(ctx, op, func(size int64) {
		op.lk.Lock()
		defer op.lk.Unlock()
		op.NumFetched++
		op.SizeFetched += size
	}); err != nil {
		op.fail(err)
		if err2 := pm.StatusChangeFunc(op.ContId, op.Location, types.PinningStatusFailed); err2 != nil {
			return err2
		}
		return errors.Wrap(err, "shuttle RunPinFunc failed")
	}
	op.complete()
	return pm.StatusChangeFunc(op.ContId, op.Location, types.PinningStatusPinned)
}

func (pm *PinManager) popNextPinOp() *PinningOperation {

	var pq *goque.Queue
	if pm.pinQueuePriority.Length() > 0 {
		pq = pm.pinQueuePriority
	} else {
		if pm.pinQueue.Length() == 0 {
			return nil // no content in queue or priority queue
		}
		pq = pm.pinQueue
	}

	item, err := pq.Dequeue()
	// Dequeue the next item in the queue
	if err != nil {
		log.Fatal("Error dequeuing item ", err)
	}
	// Assert type of the response to an Item pointer so we can work with it
	var next *PinningOperation
	err = item.ToObject(&next)

	if err != nil {
		log.Fatal("Dequeued object is not a PinningOperation pointer")
	}
	return next

}

func createDQue(QueueDataDir string) *goque.Queue {

	//TODO figure out if we want to make this persistent or continue to use mkdirtemp and if so clean up the file

	dname, err := os.MkdirTemp(QueueDataDir, "pinqueue")
	//fmt.Println("make", dname)
	q, err := goque.OpenQueue(dname)
	if err != nil {
		log.Fatal("Unable to create Queue. Out of disk? Too many open files? try ulimit -n 50000")
	}
	return q
}

func (pm *PinManager) enqueuePinOp(po *PinningOperation) {
	u := po.UserId
	if po.SkipLimiter {
		u = 0
	}
	var dq *goque.Queue
	if u == 0 {
		dq = pm.pinQueuePriority
	} else {
		dq = pm.pinQueue
	}
	_, err := dq.EnqueueObject(po)
	if err != nil {
		log.Fatal("Unable to add pin to queue.")
	}

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
			next = pm.popNextPinOp()
			if next == nil {
				send = nil
			}
			pm.pinQueueLk.Unlock()
		case <-pm.pinComplete:
			pm.pinQueueLk.Lock()
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
			log.Errorf("pinning queue error: %+v", err)
		}
		pm.pinComplete <- op
	}
}
