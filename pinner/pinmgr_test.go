package pinner

import (
	"context"

	"testing"

	"github.com/application-research/estuary/pinner/types"
)

func doPinning(ctx context.Context, op *PinningOperation, cb PinProgressCB) error {
	return nil
}

func onPinStatusUpdate(cont uint, location string, status types.PinningStatus) error {
	return nil
}
func newManager() *PinManager {
	return NewPinManager(doPinning, onPinStatusUpdate, &PinManagerOpts{
		MaxActivePerUser: 30,
	})
}

func newPinData(name string) PinningOperation {
	return PinningOperation{
		Name:   name,
		UserId: 4,
	}
}

// TestHelloName calls greetings.Hello with a name, checking
// for a valid return value.
func TestHelloName(t *testing.T) {
	mgr := newManager()
	go mgr.Run(1)
	t.Log("Say bye")
	mike := newPinData("mike")
	go mgr.Add(&mike)
	go mgr.Add(&mike)
	go mgr.Add(&mike)
	go mgr.Add(&mike)
	go mgr.Add(&mike)
	go mgr.Add(&mike)
	t.Log("queue size", mgr.PinQueueSize())

}
