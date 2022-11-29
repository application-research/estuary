package pinner

import (
	"bytes"
	"context"
	"encoding/gob"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"sync"
	"time"

	"github.com/application-research/estuary/pinner/types"
	"github.com/application-research/goque"

	"github.com/ipfs/go-cid"
	logging "github.com/ipfs/go-log/v2"
	"github.com/libp2p/go-libp2p/core/peer"
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

	pinQueue := createDQue(opts.QueueDataDir)
	//we need to have a variable pinQueueCount which keeps track in memory count in the queue
	//Since the disk dequeue is durable
	//we initialize pinQueueCount on boot by iterating through the queue
	pinQueueCount := buildPinQueueCount(pinQueue)

	return &PinManager{
		pinQueue:         pinQueue,
		activePins:       make(map[uint]int),
		pinQueueCount:    pinQueueCount,
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

type PinningOperationData struct {
	ContId uint
}

func getPinningData(po *PinningOperation) PinningOperationData {
	return PinningOperationData{
		ContId: po.ContId,
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
	err := pm.duplicateGuard.Delete(createLevelDBKey(opdata), nil)
	if err != nil {
		//Delete will not returns error if key doesn't exist
		log.Errorf("Error deleting item from duplicate guard ", err)
	}

	pm.activePins[po.UserId]--
	if pm.activePins[po.UserId] == 0 {
		delete(pm.activePins, po.UserId)
	}

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
	var minCount = 10000
	var user uint
	success := false
	//if user id = 0 has any pins to work on, use that
	if pm.pinQueueCount[0] > 0 {
		user = 0
		success = true
	} else {
		//if not find user with least number of active workers and use that
		for u := range pm.pinQueueCount {
			active := pm.activePins[u]
			if active < minCount {
				minCount = active
				user = u
				success = true

			}
		}
	}
	if minCount >= pm.maxActivePerUser && user != 0 {
		//return nil if the min count is greater than the limit and user is not 0
		//TODO investigate whether we should pop the work off anyway and not return nil
		return nil
	}
	if !success {
		//no valid pin found
		return nil
	}

	item, err := pm.pinQueue.Dequeue(getUserForQueue(user))
	pm.pinQueueCount[user]--
	if pm.pinQueueCount[user] == 0 {
		delete(pm.pinQueueCount, user)
	}

	pm.activePins[user]++

	// Dequeue the next item in the queue
	if err != nil {
		log.Errorf("Error dequeuing item ", err)
		return nil
	}
	// Assert type of the response to an Item pointer so we can work with it
	var next *PinningOperation
	var dequeObjectNotPinningOperationErr = "dequeued object is not a PinningOperation"
	if reflect.TypeOf(item).String() != "*goque.Item" {
		err = item.ToObject(&next) // this should be a *PinningOperation type
		if err != nil {
			log.Errorf(dequeObjectNotPinningOperationErr)
			return nil
		}
	} else { // we'll definitely have to return an error if it's *goque.Item (we're expecting *PinningOperation)
		log.Errorf(dequeObjectNotPinningOperationErr)
		return nil
	}

	if err != nil {
		log.Errorf("Dequeued object is not a PinningOperation pointer")
		return nil
	}
	return next

}

//currently only used for the tests since the tests need to open and close multiple dbs
//handling errors paritally for gosec security scanner
func (pm *PinManager) closeQueueDataStructures() {
	err := pm.pinQueue.Close()
	if err != nil {
		log.Fatal(err)
	}
	err = pm.duplicateGuard.Close()
	if err != nil {
		log.Fatal(err)
	}
}

func createLevelDBKey(value PinningOperationData) []byte {
	var buffer bytes.Buffer
	enc := gob.NewEncoder(&buffer)
	if err := enc.Encode(value.ContId); err != nil {
		log.Fatal("Unable to encode value")
	}
	return buffer.Bytes()
}

func createLevelDB(QueueDataDir string) *leveldb.DB {

	dname := filepath.Join(QueueDataDir, "duplicateGuard")
	err := os.MkdirAll(dname, os.ModePerm)
	if err != nil {
		log.Fatal("Unable to create directory for LevelDB. Out of disk? Too many open files? try ulimit -n 50000")
	}
	db, err := leveldb.OpenFile(dname, nil)
	if err != nil {
		log.Fatal("Unable to create LevelDB. Out of disk? Too many open files? try ulimit -n 50000")
	}
	return db
}

// queue defines the unique queue for a prefix.
type queue struct {
	Head uint64
	Tail uint64
}

func buildPinQueueCount(q *goque.PrefixQueue) map[uint]int {
	mapString, err := q.PrefixQueueCount()
	if err != nil {
		log.Fatal(err)
	}

	mapUint := make(map[uint]int)
	for key, element := range mapString {
		keyU, err := strconv.ParseUint(key, 10, 32)
		if err != nil {
			log.Fatal(err)
		}
		mapUint[uint(keyU)] = int(element)
	}
	return mapUint

}

func createDQue(QueueDataDir string) *goque.PrefixQueue {

	dname := filepath.Join(QueueDataDir, "pinQueue")
	err := os.MkdirAll(dname, os.ModePerm)
	if err != nil {
		log.Fatal("Unable to create directory for LevelDB. Out of disk? Too many open files? try ulimit -n 50000")
	}
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

	pm.pinQueueLk.Lock()
	next = pm.popNextPinOp()
	pm.pinQueueLk.Unlock()

	for {
		select {
		case op := <-pm.pinQueueIn:
			if next == nil {
				next = op
			} else {
				pm.pinQueueLk.Lock()
				pm.enqueuePinOp(op)
				pm.pinQueueLk.Unlock()
			}
		case pm.pinQueueOut <- next:
			pm.pinQueueLk.Lock()
			next = pm.popNextPinOp()
			pm.pinQueueLk.Unlock()
		case <-pm.pinComplete:
			pm.pinQueueLk.Lock()
			if next == nil {
				next = pm.popNextPinOp()
			}
			pm.pinQueueLk.Unlock()
		}
	}
}

func (pm *PinManager) pinWorker() {
	for op := range pm.pinQueueOut {
		if op != nil {
			if err := pm.doPinning(op); err != nil {
				log.Errorf("pinning queue error: %+v", err)
			}
			pm.pinComplete <- op
		}
	}
}
