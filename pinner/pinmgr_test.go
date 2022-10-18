package pinner

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/application-research/estuary/pinner/types"
	"github.com/stretchr/testify/assert"
)

var count_lock sync.Mutex

func onPinStatusUpdate(cont uint, location string, status types.PinningStatus) error {
	//fmt.Println("onPinStatusUpdate", status, cont)
	return nil
}
func newManager(count *int) *PinManager {

	return NewPinManager(
		func(ctx context.Context, op *PinningOperation, cb PinProgressCB) error {
			go cb(1)
			count_lock.Lock()
			*count += 1
			count_lock.Unlock()
			return nil
		}, onPinStatusUpdate, &PinManagerOpts{
			MaxActivePerUser: 30,
		})
}

func newPinData(name string, userid int) PinningOperation {
	return PinningOperation{
		Name:   name,
		UserId: uint(userid),
	}
}

var N int = 20
var sleeptime time.Duration = 100

func TestSend1Pin1worker(t *testing.T) {
	//run 1 worker

	i := 1
	var count int = 0
	mgr := newManager(&count)
	go mgr.Run(1)
	pin := newPinData("name"+fmt.Sprint(i), i)
	go mgr.Add(&pin)

	sleepWhileWork(mgr, 0)
	assert.Equal(t, 0, mgr.PinQueueSize(), "first pin doesn't enter queue")
	assert.Equal(t, 1, count, "DoPin called once")

}

func TestSend1Pin0workers(t *testing.T) {

	//run 0 workers
	i := 1
	var count int = 0
	mgr := newManager(&count)
	go mgr.Run(0)
	pin := newPinData("name"+fmt.Sprint(i), i)
	go mgr.Add(&pin)

	sleepWhileWork(mgr, 0)
	assert.Equal(t, 0, mgr.PinQueueSize(), "first pin doesn't enter queue")
	assert.Equal(t, 0, count, "DoPin called once")
}

func TestNUniqueNames(t *testing.T) {
	var count int = 0
	mgr := newManager(&count)

	go mgr.Run(0)
	for i := 0; i < N; i++ {
		pin := newPinData("name"+fmt.Sprint(i), i)
		go mgr.Add(&pin)
	}
	sleepWhileWork(mgr, N-1)
	assert.Equal(t, N-1, mgr.PinQueueSize(), "queue should have N pins in it")
}

func TestNUniqueNamesWorker(t *testing.T) {
	var count int = 0
	mgr := newManager(&count)
	go mgr.Run(5)
	for i := 0; i < N; i++ {
		pin := newPinData("name"+fmt.Sprint(i), i)
		go mgr.Add(&pin)
	}
	sleepWhileWork(mgr, 0)
	assert.Equal(t, 0, mgr.PinQueueSize(), "queue should be empty")
	assert.Equal(t, N, count, "work should be done N times")
}

func TestNUniqueNamesSameUserWorker(t *testing.T) {

	var count int = 0
	mgr := newManager(&count)

	for j := 0; j < N; j++ {

		for i := 0; i < N; i++ {
			pin := newPinData("name"+fmt.Sprint(i), i)
			go mgr.Add(&pin)
		}
	}

	go mgr.Run(1)

	sleepWhileWork(mgr, 0)
	assert.Equal(t, 0, mgr.PinQueueSize(), "queue should be empty")
	assert.Equal(t, N+1, count, "Should do N work + 1 for the first item")
}

func TestNUniqueNamesSameUser(t *testing.T) {
	var count int = 0
	mgr := newManager(&count)
	go mgr.Run(0)
	for j := 0; j < N; j++ {

		for i := 0; i < N; i++ {
			pin := newPinData("name"+fmt.Sprint(i), i)
			go mgr.Add(&pin)
		}
	}
	sleepWhileWork(mgr, N)
	assert.Equal(t, N, mgr.PinQueueSize(), "queue should have N pins in it")
	assert.Equal(t, count, 0, "no work done")
}

func TestNDuplicateNamesWorker(t *testing.T) {
	var count int = 0
	mgr := newManager(&count)
	for i := 0; i < N; i++ {
		pin := newPinData("name", 0)
		go mgr.Add(&pin)
	}
	time.Sleep(100 * time.Millisecond)
	go mgr.Run(5)
	time.Sleep(100 * time.Millisecond)

	sleepWhileWork(mgr, 0)
	assert.Equal(t, 0, mgr.PinQueueSize(), "queue should have 0 pins in it")
	assert.Equal(t, 2, count, "work should have N pins in it")
}
func TestNDuplicateNames(t *testing.T) {
	var count int = 0
	mgr := newManager(&count)
	go mgr.Run(0)

	for i := 0; i < N; i++ {
		pin := newPinData("name", 0)
		go mgr.Add(&pin)
	}
	sleepWhileWork(mgr, N-1)
	assert.Equal(t, N-1, mgr.PinQueueSize(), "queue should have N pins in it")
	assert.Equal(t, 0, count, "no work")
}

func TestNDuplicateNamesNDuplicateUsersNTimeWork5Workers(t *testing.T) {
	var count int = 0
	mgr := newManager(&count)
	go mgr.Run(5)
	for k := 0; k < N; k++ {
		for j := 0; j < N; j++ {
			for i := 0; i < N; i++ {
				pin := newPinData("name"+fmt.Sprint(i), j)
				go mgr.Add(&pin)
			}
		}
	}
	sleepWhileWork(mgr, 0)
	assert.Equal(t, 0, mgr.PinQueueSize(), "queue should have 0 pins in it")
	assert.Equal(t, N*N*N, count, "work should have N pins in it")
}

func TestNDuplicateNamesNDuplicateUsersNTimeWork(t *testing.T) {
	var count int = 0
	mgr := newManager(&count)
	go mgr.Run(1)
	for k := 0; k < N; k++ {
		for j := 0; j < N; j++ {
			for i := 0; i < N; i++ {
				pin := newPinData("name"+fmt.Sprint(i), j)
				go mgr.Add(&pin)
			}
		}
	}
	sleepWhileWork(mgr, 0)
	assert.Equal(t, 0, mgr.PinQueueSize(), "queue should have 0 pins in it")
	assert.Equal(t, N*N*N, count, "work should have N pins in it")
}

func sleepWhileWork(mgr *PinManager, SIZE int) {
	for i := 0; i < N; i++ {
		time.Sleep(sleeptime * time.Millisecond)
		if mgr.PinQueueSize() == SIZE {
			time.Sleep(sleeptime * time.Millisecond)
			return
		}
	}
	fmt.Println("Waking up before finding correct result")
}

func TestNDuplicateNamesNDuplicateUsersNTimes(t *testing.T) {

	var count int = 0
	mgr := newManager(&count)
	go mgr.Run(0)
	for k := 0; k < N; k++ {
		for j := 0; j < N; j++ {
			for i := 0; i < N; i++ {
				pin := newPinData("name"+fmt.Sprint(i), j)
				go mgr.Add(&pin)
			}
		}
	}

	sleepWhileWork(mgr, N*N*N-1)
	assert.Equal(t, N*N*N-1, mgr.PinQueueSize(), "queue should have N pins in it")
	assert.Equal(t, 0, count, "no work")
}

/*

test run that iterates above and makes the above code redundant.

func test_run(worker_count int, repeat_count int, user_id_count int, name_count int, t *testing.T) {

	var count int = 0
	var work_completed_count int = 0
	var queue_end_count int = repeat_count*user_id_count*name_count - 1 // total work done minus 1 because first entry is stored as "next" and doesnot enter queue
	if worker_count > 0 {
		work_completed_count = repeat_count * user_id_count * name_count
		queue_end_count = 0 // queue shoud be empty at end
		mgr := newManager(&count)
		go mgr.Run(worker_count)

		for k := 0; k < repeat_count; k++ {
			for j := 0; j < user_id_count; j++ {
				for i := 0; i < name_count; i++ {
					pin := newPinData("name"+fmt.Sprint(i), j)
					go mgr.Add(&pin)
				}
			}
		}
		time.Sleep(sleeptime * time.Millisecond)
		assert.Equal(t, queue_end_count, mgr.PinQueueSize(), "queue has wrong number of pins in it")
		assert.Equal(t, work_completed_count, count, "incorrect amount of work done")
	}

}

func TestPinMgr(t *testing.T) {
	t.Parallel() // marks TLog as capable of running in parallel with other tests

	for worker_count := 0; worker_count < N; worker_count++ {

		for repeat_count := 0; repeat_count < N; repeat_count++ {
			for user_id_count := 0; user_id_count < N; user_id_count++ {
				t.Run(fmt.Sprint("Test_%i_%i_%i_%i", worker_count, repeat_count, user_id_count, user_id_count), func(t *testing.T) {
					t.Parallel() // marks each test case as capable of running in parallel with each other
					test_run(worker_count, repeat_count, user_id_count, user_id_count, t)
				})
			}
		}
	}
}
*/
