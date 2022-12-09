package pinner

import (
	"context"

	"github.com/application-research/estuary/drpc"
	"github.com/application-research/estuary/pinner/types"
)

func (pm *EstuaryPinManager) HandlePinningComplete(ctx context.Context, handle string, pincomp *drpc.PinComplete) error {
	return pm.cm.HandlePinningComplete(ctx, handle, pincomp)
}

func (pm *EstuaryPinManager) UpdatePinStatus(location string, contID uint, status types.PinningStatus) error {
	return pm.cm.UpdatePinStatus(location, contID, status)
}
