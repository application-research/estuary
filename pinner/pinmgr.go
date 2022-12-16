package pinner

import (
	"bytes"
	"context"
	"encoding/gob"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	contentmgr "github.com/application-research/estuary/content"
	"github.com/application-research/estuary/pinner/operation"
	"github.com/application-research/estuary/pinner/progress"
	"github.com/application-research/estuary/pinner/types"
	"github.com/application-research/estuary/shuttle"
	"github.com/application-research/goque"
	"github.com/vmihailenco/msgpack/v5"

	logging "github.com/ipfs/go-log/v2"
	"github.com/pkg/errors"
	"github.com/syndtr/goleveldb/leveldb"
)

var log = logging.Logger("pinner")

type PinFunc func(context.Context, *operation.PinningOperation, progress.PinProgressCB) error
type PinStatusFunc func(contID uint, location string, status types.PinningStatus) error

func NewEstuaryPinManager(pinfunc PinFunc, scf PinStatusFunc, opts *PinManagerOpts, cm *contentmgr.ContentManager, shuttleMgr shuttle.IManager) *EstuaryPinManager {
	return &EstuaryPinManager{
		PinManager: newPinManager(pinfunc, scf, opts),
		cm:         cm,
		shuttleMgr: shuttleMgr,
	}
}

func NewShuttlePinManager(pinfunc PinFunc, scf PinStatusFunc, opts *PinManagerOpts) *PinManager {
	return newPinManager(pinfunc, scf, opts)
}

func newPinManager(pinfunc PinFunc, scf PinStatusFunc, opts *PinManagerOpts) *PinManager {
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
		pinQueueIn:       make(chan *operation.PinningOperation, 64),
		pinQueueOut:      make(chan *operation.PinningOperation),
		pinComplete:      make(chan *operation.PinningOperation, 64),
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
	pinQueueIn       chan *operation.PinningOperation
	pinQueueOut      chan *operation.PinningOperation
	pinComplete      chan *operation.PinningOperation
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

type ShuttleManager struct {
	*PinManager
}

type EstuaryPinManager struct {
	*PinManager
	cm         *contentmgr.ContentManager
	shuttleMgr shuttle.IManager
}

type PinningOperationData struct {
	ContId uint
}

func getPinningData(po *operation.PinningOperation) PinningOperationData {
	return PinningOperationData{
		ContId: po.ContId,
	}
}

func (pm *PinManager) complete(po *operation.PinningOperation) {
	pm.pinQueueLk.Lock()
	defer pm.pinQueueLk.Unlock()

	opData := getPinningData(po)
	err := pm.duplicateGuard.Delete(createLevelDBKey(opData), nil)
	if err != nil {
		//Delete will not returns error if key doesn't exist
		log.Errorf("Error deleting item from duplicate guard ", err)
	}

	pm.activePins[po.UserId]--
	if pm.activePins[po.UserId] == 0 {
		delete(pm.activePins, po.UserId)
	}
	po.Complete()
}

func (pm *PinManager) PinQueueSize() int {
	pm.pinQueueLk.Lock()
	defer pm.pinQueueLk.Unlock()
	return int(pm.pinQueue.Length())
}

func (pm *PinManager) Add(op *operation.PinningOperation) {
	if op != nil && pm != nil && op.ContId != 0 {
		go func() {
			pm.pinQueueIn <- op
		}()
	}
}

var maxTimeout = 24 * time.Hour

func (pm *PinManager) doPinning(op *operation.PinningOperation) error {
	ctx, cancel := context.WithTimeout(context.Background(), maxTimeout)
	defer cancel()

	op.SetStatus(types.PinningStatusPinning)

	if err := pm.RunPinFunc(ctx, op, func(size int64) {
		op.UpdateProgress(size)
	}); err != nil {
		op.Fail(err)
		if err2 := pm.StatusChangeFunc(op.ContId, op.Location, types.PinningStatusFailed); err2 != nil {
			return err2
		}
		return errors.Wrap(err, "shuttle RunPinFunc failed")
	}
	pm.complete(op)
	return nil
}

func (pm *PinManager) popNextPinOp() *operation.PinningOperation {
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

	// Dequeue the next item in the queue
	item, err := pm.pinQueue.Dequeue(getUserForQueue(user))
	pm.pinQueueCount[user]--
	if pm.pinQueueCount[user] == 0 {
		delete(pm.pinQueueCount, user)
	}
	pm.activePins[user]++

	// no item in the queue for that query
	if err == goque.ErrOutOfBounds {
		return nil
	}

	if err != nil {
		log.Errorf("Error dequeuing item ", err)
		return nil
	}

	// Assert type of the response to an Item pointer so we can work with it
	next, err := decodeMsgPack(item.Value)
	if err != nil {
		log.Errorf("Cannot decode PinningOperation pointer")
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
	dname := filepath.Join(QueueDataDir, "pinQueueMsgPack")
	if err := os.MkdirAll(dname, os.ModePerm); err != nil {
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

func encodeMsgPack(po *operation.PinningOperation) ([]byte, error) {
	return msgpack.Marshal(&po)
}

func decodeMsgPack(po_bytes []byte) (*operation.PinningOperation, error) {
	var next *operation.PinningOperation
	return next, msgpack.Unmarshal(po_bytes, &next)
}

func (pm *PinManager) enqueuePinOp(po *operation.PinningOperation) {
	poData := getPinningData(po)
	if _, err := pm.duplicateGuard.Get(createLevelDBKey(poData), nil); err != leveldb.ErrNotFound {
		//work already exists in the queue not adding duplicate
		return
	}

	u := po.UserId
	if po.SkipLimiter {
		u = 0
	}

	opBytes, err := encodeMsgPack(po)
	if err != nil {
		log.Fatal("Unable to encode data to add to queue.")
	}

	// Add it to the queue.
	_, err = pm.pinQueue.Enqueue(getUserForQueue(u), opBytes)
	if err != nil {
		log.Fatal("Unable to add pin to queue.", err)
	}

	err = pm.duplicateGuard.Put(createLevelDBKey(poData), []byte{255}, nil)
	if err != nil {
		log.Fatal("Unable to add to duplicate guard.")
	}
	pm.pinQueueCount[u]++
}

func (pm *PinManager) Run(workers int) {
	for i := 0; i < workers; i++ {
		go pm.pinWorker()
	}

	var next *operation.PinningOperation

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
