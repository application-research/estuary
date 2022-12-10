package contentmgr

import (
	"container/heap"
	"context"
	"sync"
	"time"

	"github.com/application-research/estuary/util"
	"github.com/ipfs/go-metrics-interface"
	"golang.org/x/xerrors"
)

type queueManager struct {
	queue *entryQueue
	cb    func(uint)
	qlk   sync.Mutex

	nextEvent time.Time
	evtTimer  *time.Timer

	qsizeMetr metrics.Gauge
	qnextMetr metrics.Gauge
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

func newQueueManager(cb func(c uint)) *queueManager {
	metCtx := metrics.CtxScope(context.Background(), "content_manager")
	qsizeMetr := metrics.NewCtx(metCtx, "queue_size", "number of items in the replicator queue").Gauge()
	qnextMetr := metrics.NewCtx(metCtx, "queue_next", "next event time for queue").Gauge()

	qm := &queueManager{
		queue: new(entryQueue),
		cb:    cb,

		qsizeMetr: qsizeMetr,
		qnextMetr: qnextMetr,
	}

	heap.Init(qm.queue)
	return qm
}

func (qm *queueManager) add(content uint, wait time.Duration) {
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

func (cm *ContentManager) ToCheck(contID uint) {
	// if DisableFilecoinStorage is not enabled, queue content for deal making
	if !cm.cfg.DisableFilecoinStorage {
		cm.toCheck <- contID
	}
}

func (cm *ContentManager) rebuildToCheckQueue() error {
	cm.log.Info("rebuilding contents queue .......")

	var allcontent []util.Content
	if err := cm.db.Find(&allcontent, "active AND NOT aggregated_in > 0").Error; err != nil {
		return xerrors.Errorf("finding all content in database: %w", err)
	}

	go func() {
		for i, c := range allcontent {
			// every 100 contents re-queued, wait 5 seconds to avoid over-saturating queues
			// time to requeue all: 10m / 100 * 5 seconds = 5.78 days
			if i%100 == 0 {
				time.Sleep(time.Second * 5)
			}
			cm.ToCheck(c.ID)
		}
	}()
	return nil
}
