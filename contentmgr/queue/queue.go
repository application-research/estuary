package contentqueue

import (
	"container/heap"
	"context"
	"sync"
	"time"

	"github.com/ipfs/go-metrics-interface"
)

type IQueueManager interface {
	ToCheck(contID uint)
	Add(content uint, wait time.Duration)
	NextContent() chan uint
	Len() int
	NextEvent() time.Time
}

type queueManager struct {
	queue *entryQueue
	cb    func(uint)
	qlk   sync.Mutex

	nextEvent time.Time
	evtTimer  *time.Timer

	qsizeMetr metrics.Gauge
	qnextMetr metrics.Gauge

	toCheck    chan uint
	isDisabled bool
}

type queueEntry struct {
	content   uint
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

func NewQueueManager(isDisabled bool) *queueManager {
	metCtx := metrics.CtxScope(context.Background(), "content_manager")
	qsizeMetr := metrics.NewCtx(metCtx, "queue_size", "number of items in the replicator queue").Gauge()
	qnextMetr := metrics.NewCtx(metCtx, "queue_next", "next event time for queue").Gauge()

	qm := &queueManager{
		queue: new(entryQueue),

		qsizeMetr: qsizeMetr,
		qnextMetr: qnextMetr,

		toCheck:    make(chan uint, 100000),
		isDisabled: isDisabled,
	}
	qm.cb = qm.ToCheck

	heap.Init(qm.queue)
	return qm
}

func (qm *queueManager) Add(content uint, wait time.Duration) {
	qm.qlk.Lock()
	defer qm.qlk.Unlock()

	at := time.Now().Add(wait)

	heap.Push(qm.queue, &queueEntry{
		content:   content,
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
			go qm.cb(qe.content)
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

func (qm *queueManager) ToCheck(contID uint) {
	// if DisableFilecoinStorage is not enabled, queue content for deal making
	if !qm.isDisabled {
		qm.toCheck <- contID
	}
}

func (qm *queueManager) NextContent() chan uint {
	return qm.toCheck
}

func (qm *queueManager) Len() int {
	return len(qm.queue.elems)
}

func (qm *queueManager) NextEvent() time.Time {
	return qm.nextEvent
}
