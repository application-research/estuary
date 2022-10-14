package pinner

import (
	"context"
	"fmt"
	"time"

	"testing"

	"github.com/application-research/estuary/pinner/types"
	"github.com/stretchr/testify/assert"
)

func onPinStatusUpdate(cont uint, location string, status types.PinningStatus) error {
	//fmt.Println("onPinStatusUpdate", status, cont)
	return nil
}
func newManager(count *int) *PinManager {

	return NewPinManager(
		func(ctx context.Context, op *PinningOperation, cb PinProgressCB) error {
			*count += 1
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

var N int = 10
var sleeptime time.Duration = 100

func TestSend1Pin1worker(t *testing.T) {
	//run 1 worker

	i := 1
	var count int = 0
	mgr := newManager(&count)
	go mgr.Run(1)
	pin := newPinData("name"+fmt.Sprint(i), i)
	go mgr.Add(&pin)

	time.Sleep(sleeptime * time.Millisecond)
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

	time.Sleep(sleeptime * time.Millisecond)
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
	time.Sleep(sleeptime * time.Millisecond)
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
	time.Sleep(sleeptime * time.Millisecond)
	assert.Equal(t, 0, mgr.PinQueueSize(), "queue should be empty")
	assert.Equal(t, N, count, "work should be done N times")
}

func TestNUniqueNamesSameUserWorker(t *testing.T) {

	var count int = 0
	mgr := newManager(&count)
	go mgr.Run(5)

	for j := 0; j < N; j++ {

		for i := 0; i < N; i++ {
			pin := newPinData("name"+fmt.Sprint(i), i)
			go mgr.Add(&pin)
		}
	}
	time.Sleep(sleeptime * time.Millisecond)
	assert.Equal(t, 0, mgr.PinQueueSize(), "queue should be empty")
	assert.Equal(t, N*N, count, "queue should have N pins in it")
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
	time.Sleep(sleeptime * time.Millisecond)
	assert.Equal(t, N*N-1, mgr.PinQueueSize(), "queue should have N pins in it")
	assert.Equal(t, count, 0, "no work done")
}

func TestNDuplicateNamesWorker(t *testing.T) {
	var count int = 0
	mgr := newManager(&count)
	go mgr.Run(5)
	for i := 0; i < N; i++ {
		pin := newPinData("name", 0)
		go mgr.Add(&pin)
	}
	time.Sleep(sleeptime * time.Millisecond)
	assert.Equal(t, 0, mgr.PinQueueSize(), "queue should have 0 pins in it")
	assert.Equal(t, N, count, "work should have N pins in it")
}
func TestNDuplicateNames(t *testing.T) {
	var count int = 0
	mgr := newManager(&count)
	go mgr.Run(0)

	for i := 0; i < N; i++ {
		pin := newPinData("name", 0)
		go mgr.Add(&pin)
	}
	time.Sleep(sleeptime * time.Millisecond)
	assert.Equal(t, N-1, mgr.PinQueueSize(), "queue should have N pins in it")
	assert.Equal(t, 0, count, "no work")
}

func TestNDuplicateNamesNDuplicateUsersNTimeWork(t *testing.T) {
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
	time.Sleep(sleeptime * time.Millisecond)
	assert.Equal(t, 0, mgr.PinQueueSize(), "queue should have 0 pins in it")
	assert.Equal(t, N*N*N, count, "work should have N pins in it")
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
	time.Sleep(sleeptime * time.Millisecond)
	assert.Equal(t, N*N*N-1, mgr.PinQueueSize(), "queue should have N pins in it")
	assert.Equal(t, 0, count, "no work")
}
