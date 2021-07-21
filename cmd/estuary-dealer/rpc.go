package main

import (
	"context"

	"github.com/whyrusleeping/estuary/drpc"
	"github.com/whyrusleeping/estuary/pinner"
	"github.com/whyrusleeping/estuary/util"
)

func (d *Dealer) sendRpcMessage(ctx context.Context, msg *drpc.Message) error {
	select {
	case d.outgoing <- msg:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (d *Dealer) handleRpcAddPin(apo *drpc.AddPinOp) error {
	pin := &Pin{
		Content: apo.DBID,
		Cid:     util.DbCID{apo.Cid},
		UserID:  apo.UserId,

		Active:  false,
		Pinning: true,
	}

	if err := d.DB.Create(pin).Error; err != nil {
		return err
	}

	op := &pinner.PinningOperation{
		Obj:    apo.Cid,
		ContId: apo.DBID,
		UserId: apo.UserId,
		Status: "queued",
	}

	d.PinMgr.Add(op)
	return nil
}

func (d *Dealer) sendPinCompleteMessage(ctx context.Context, cont uint, size int64, objects []Object) {
	objs := make([]drpc.PinObj, 0, len(objects))
	for _, o := range objects {
		objs = append(objs, PinObj{
			Cid:  o.Cid.CID,
			Size: o.Size,
		})
	}

	if err := d.sendRpcMessage(ctx, &drpc.Message{
		Op: drpc.OP_PinComplete,
		Params: drpc.MsgParams{
			PinComplete: &drpc.PinComplete{
				DBID:    cont,
				Size:    size,
				Objects: objs,
			},
		},
	}); err != nil {
		log.Errorf("failed to send pin complete message for content %d: %s", cont, err)
	}
}
