package pinner

import (
	"bytes"
	"context"
	"encoding/gob"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/application-research/estuary/pinner/types"
	"github.com/beeker1121/goque"
	"github.com/ipfs/go-cid"
	logging "github.com/ipfs/go-log/v2"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/pkg/errors"
	"github.com/syndtr/goleveldb/leveldb"
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
		activePins:       make(map[uint]int),
		pinQueueCount:    make(map[uint]int),
		pinQueueIn:       make(chan *PinningOperation, 64),
		pinQueueOut:      make(chan *PinningOperation),
		pinComplete:      make(chan *PinningOperation, 64),
		duplicateGuard:   createLevelDB(opts.QueueDataDir),
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
	duplicateGuard   *leveldb.DB
	activePins       map[uint]int // used to limit the number of pins per user
	pinQueueCount    map[uint]int // keep track of queue count per user
	pinQueue         *goque.PrefixQueue
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

//TODO put this as a subfield inside PinningOperation
type PinningOperationData struct {
	Obj  cid.Cid
	Name string
	//Peers       []*peer.AddrInfo
	Meta        string
	Status      types.PinningStatus
	UserId      uint
	ContId      uint
	Replace     uint
	Location    string
	SkipLimiter bool
	MakeDeal    bool
}

func getPinningData(po *PinningOperation) PinningOperationData {
	return PinningOperationData{
		Obj:  po.Obj,
		Name: po.Name,
		//Peers:       po.Peers,
		Meta:        po.Meta,
		Status:      po.Status,
		UserId:      po.UserId,
		ContId:      po.ContId,
		Replace:     po.Replace,
		Location:    po.Location,
		SkipLimiter: po.SkipLimiter,
		MakeDeal:    po.MakeDeal,
	}
}

func (po *PinningOperation) fail(err error) {
	po.lk.Lock()
	po.FetchErr = err
	po.EndTime = time.Now()
	po.Status = types.PinningStatusFailed
	po.LastUpdate = time.Now()
	po.lk.Unlock()
}

func (pm *PinManager) complete(po *PinningOperation) {
	pm.pinQueueLk.Lock()
	po.lk.Lock()
	defer pm.pinQueueLk.Unlock()
	defer po.lk.Unlock()

	opdata := getPinningData(po)

	pm.duplicateGuard.Delete(createLevelDBKey(opdata), nil)

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

func (pm *PinManager) PinQueueSize() int {
	pm.pinQueueLk.Lock()
	defer pm.pinQueueLk.Unlock()
	return int(pm.pinQueue.Length())
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
	pm.complete(op)
	return nil
}

func (pm *PinManager) popNextPinOp() *PinningOperation {

	if pm.pinQueue.Length() == 0 {
		return nil // no content in queue
	}

	var minCount int = 10000
	var user uint

	//if user id = 0 has any pins to work on, use that
	if pm.pinQueueCount[0] > 0 {
		user = 0
	} else {
		//if not find user with least number of active workers and use that
		for u := range pm.pinQueueCount {
			active := pm.activePins[u]
			if active < minCount {
				minCount = active
				user = u
			}
		}
	}
	if minCount >= pm.maxActivePerUser && user != 0 {
		//return nil if the min count is greater than the limit and user is not 0
		return nil
	}

	item, err := pm.pinQueue.Dequeue(getUserForQueue(user))
	pm.pinQueueCount[user]--
	if pm.pinQueueCount[user] == 0 {
		delete(pm.pinQueueCount, user)
	}

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

func createLevelDBKey(value PinningOperationData) []byte {
	var buffer bytes.Buffer
	enc := gob.NewEncoder(&buffer)
	if err := enc.Encode(value); err != nil {
		log.Fatal("Unable to encode value")
	}
	return buffer.Bytes()
}

func createLevelDB(QueueDataDir string) *leveldb.DB {
	//todo make tmpfile
	dname, _ := os.MkdirTemp(QueueDataDir, "duplicateGuard")

	db, err := leveldb.OpenFile(dname, nil)
	if err != nil {
		log.Fatal("Unable to create LevelDB. Out of disk? Too many open files? try ulimit -n 50000")
	}
	return db
}

func createDQue(QueueDataDir string) *goque.PrefixQueue {

	//TODO figure out if we want to make this persistent or continue to use mkdirtemp and if so how often should we clean up the file
	dname, err := os.MkdirTemp(QueueDataDir, "pinqueue")
	q, err := goque.OpenPrefixQueue(dname)
	if err != nil {
		log.Fatal("Unable to create Queue. Out of disk? Too many open files? try ulimit -n 50000")
	}
	return q
}

func getUserForQueue(UserId uint) []byte {
	return []byte(strconv.Itoa(int(UserId)))
}

func (pm *PinManager) enqueuePinOp(po *PinningOperation) {

	opdata := getPinningData(po)

	_, err := pm.duplicateGuard.Get(createLevelDBKey(opdata), nil)

	if err != leveldb.ErrNotFound {
		//work already exists in the queue not adding duplicate
		return
	}

	u := po.UserId
	if po.SkipLimiter {
		u = 0
	}

	_, err = pm.pinQueue.EnqueueObject(getUserForQueue(u), po)
	pm.pinQueueCount[u]++
	if err != nil {
		log.Fatal("Unable to add pin to queue.")
	}

	err = pm.duplicateGuard.Put(createLevelDBKey(opdata), []byte{255}, nil)
	// Add it to the queue.
	if err != nil {
		log.Fatal("Unable to add to duplicate guard.")
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
