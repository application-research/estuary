package contentqueue

import (
	"container/heap"
	"context"
	"sync"
	"time"

	"github.com/ipfs/go-metrics-interface"
)

type IQueueManager interface {
	ToCheck(contID uint64, contSize int64)
	Add(content uint64, size int64, wait time.Duration)
	NextContent() chan uint64
	Len() int
	NextEvent() time.Time
}

type queueManager struct {
	queue *entryQueue
	cb    func(uint64, int64)
	qlk   sync.Mutex

	nextEvent time.Time
	evtTimer  *time.Timer

	qsizeMetr metrics.Gauge
	qnextMetr metrics.Gauge

	toCheck       chan uint64
	isDisabled    bool
	dealSizeLimit int64
}

type queueEntry struct {
	content   uint64
	size      int64
	checkTime time.Time
}

type entryQueue struct {
	elems []*queueEntry
}

func (eq *entryQueue) Len() int {
	return len(eq.elems)
}

func (eq *entryQueue) Less(i, j int) bool {
	return eq.elems[i].checkTime.Before(eq.elems[j].checkTime)
}

func (eq *entryQueue) Swap(i, j int) {
	eq.elems[i], eq.elems[j] = eq.elems[j], eq.elems[i]
}

func (eq *entryQueue) Push(e interface{}) {
	eq.elems = append(eq.elems, e.(*queueEntry))
}

func (eq *entryQueue) Pop() interface{} {
	out := eq.elems[len(eq.elems)-1]
	eq.elems = eq.elems[:len(eq.elems)-1]
	return out
}

func (eq *entryQueue) PopEntry() *queueEntry {
	return heap.Pop(eq).(*queueEntry)
}

func NewQueueManager(isDisabled bool, dealSizeLimit int64) *queueManager {
	metCtx := metrics.CtxScope(context.Background(), "content_manager")
	qsizeMetr := metrics.NewCtx(metCtx, "queue_size", "number of items in the replicator queue").Gauge()
	qnextMetr := metrics.NewCtx(metCtx, "queue_next", "next event time for queue").Gauge()

	qm := &queueManager{
		queue: new(entryQueue),

		qsizeMetr: qsizeMetr,
		qnextMetr: qnextMetr,

		toCheck:       make(chan uint64, 100000),
		isDisabled:    isDisabled,
		dealSizeLimit: dealSizeLimit,
	}
	qm.cb = qm.ToCheck

	heap.Init(qm.queue)
	return qm
}

func (qm *queueManager) Add(content uint64, size int64, wait time.Duration) {
	qm.qlk.Lock()
	defer qm.qlk.Unlock()

	at := time.Now().Add(wait)

	heap.Push(qm.queue, &queueEntry{
		content:   content,
		size:      size,
		checkTime: at,
	})

	qm.qsizeMetr.Add(1)

	if qm.nextEvent.IsZero() || at.Before(qm.nextEvent) {
		qm.nextEvent = at
		qm.qnextMetr.Set(float64(at.Unix()))

		if qm.evtTimer != nil {
			qm.evtTimer.Reset(wait)
		} else {
			qm.evtTimer = time.AfterFunc(wait, func() {
				qm.processQueue()
			})
		}
	}
}

func (qm *queueManager) processQueue() {
	qm.qlk.Lock()
	defer qm.qlk.Unlock()

	for qm.queue.Len() > 0 {
		qe := qm.queue.PopEntry()
		if time.Now().After(qe.checkTime) {
			qm.qsizeMetr.Add(-1)
			go qm.cb(qe.content, qe.size)
		} else {
			heap.Push(qm.queue, qe)
			qm.nextEvent = qe.checkTime
			qm.qnextMetr.Set(float64(qe.checkTime.Unix()))
			qm.evtTimer.Reset(time.Until(qe.checkTime))
			return
		}
	}
	qm.nextEvent = time.Time{}
}

func (qm *queueManager) ToCheck(contID uint64, contSize int64) {
	// if DisableFilecoinStorage is not enabled, queue content for deal making
	if !qm.isDisabled {
		go func() {
			qm.toCheck <- contID
		}()
	}
}

func (qm *queueManager) NextContent() chan uint64 {
	return qm.toCheck
}

func (qm *queueManager) Len() int {
	return len(qm.queue.elems)
}

func (qm *queueManager) NextEvent() time.Time {
	return qm.nextEvent
}
