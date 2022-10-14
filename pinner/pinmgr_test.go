package pinner

import (
	"context"
	"fmt"
	"time"

	"testing"

	"github.com/application-research/estuary/pinner/types"
	"github.com/stretchr/testify/assert"
)

func doPinning(ctx context.Context, op *PinningOperation, cb PinProgressCB) error {
	fmt.Println("doPinning")
	return nil
}

func onPinStatusUpdate(cont uint, location string, status types.PinningStatus) error {
	fmt.Println("onPinStatusUpdate")
	return nil
}
func newManager() *PinManager {
	return NewPinManager(doPinning, onPinStatusUpdate, &PinManagerOpts{
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

func TestSend1Pin(t *testing.T) {
	i := 1
	mgr := newManager()
	go mgr.Run(0)
	pin := newPinData("name"+fmt.Sprint(i), i)
	go mgr.Add(&pin)

	time.Sleep(sleeptime * time.Millisecond)
	assert.Equal(t, 0, mgr.PinQueueSize(), "first pin doesn't enter queue")
}

func TestNUniqueNames(t *testing.T) {
	mgr := newManager()
	go mgr.Run(0)
	for i := 0; i < N; i++ {
		pin := newPinData("name"+fmt.Sprint(i), i)
		go mgr.Add(&pin)
	}
	time.Sleep(sleeptime * time.Millisecond)
	assert.Equal(t, N-1, mgr.PinQueueSize(), "queue should have N pins in it")
}

func TestNUniqueNamesSameUser(t *testing.T) {
	mgr := newManager()
	go mgr.Run(0)
	for j := 0; j < N; j++ {

		for i := 0; i < N; i++ {
			pin := newPinData("name"+fmt.Sprint(i), i)
			go mgr.Add(&pin)
		}
	}
	time.Sleep(sleeptime * time.Millisecond)
	assert.Equal(t, N*N-1, mgr.PinQueueSize(), "queue should have N pins in it")
}

func TestNDuplicateNames(t *testing.T) {
	mgr := newManager()
	go mgr.Run(0)
	for i := 0; i < N; i++ {
		pin := newPinData("name", 0)
		go mgr.Add(&pin)
	}
	time.Sleep(sleeptime * time.Millisecond)
	assert.Equal(t, N-1, mgr.PinQueueSize(), "queue should have N pins in it")
}

func TestNDuplicateNamesNDuplicateUsersNTimes(t *testing.T) {
	mgr := newManager()
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
}
