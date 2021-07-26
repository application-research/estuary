package main

import (
	"context"
	"fmt"

	datatransfer "github.com/filecoin-project/go-data-transfer"
	"github.com/filecoin-project/go-state-types/abi"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	"github.com/whyrusleeping/estuary/drpc"
	"github.com/whyrusleeping/estuary/pinner"
	"github.com/whyrusleeping/estuary/util"
	"golang.org/x/xerrors"
	"gorm.io/gorm"
)

func (d *Shuttle) handleRpcCmd(cmd *drpc.Command) error {
	ctx := context.TODO()

	log.Infof("handling rpc command: %s", cmd.Op)
	switch cmd.Op {
	case drpc.CMD_AddPin:
		return d.handleRpcAddPin(ctx, cmd.Params.AddPin)
	case drpc.CMD_ComputeCommP:
		return d.handleRpcComputeCommP(ctx, cmd.Params.ComputeCommP)
	case drpc.CMD_TakeContent:
		return d.handleRpcTakeContent(ctx, cmd.Params.TakeContent)
	case drpc.CMD_AggregateContent:
		return d.handleRpcAggregateContent(ctx, cmd.Params.AggregateContent)
	case drpc.CMD_StartTransfer:
		return d.handleRpcStartTransfer(ctx, cmd.Params.StartTransfer)
	default:
		return fmt.Errorf("unrecognized command op: %q", cmd.Op)
	}
}

func (d *Shuttle) sendRpcMessage(ctx context.Context, msg *drpc.Message) error {
	log.Infof("sending rpc message: %s", msg.Op)
	select {
	case d.outgoing <- msg:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (d *Shuttle) handleRpcAddPin(ctx context.Context, apo *drpc.AddPin) error {
	d.addPinLk.Lock()
	defer d.addPinLk.Unlock()
	return d.addPin(ctx, apo.DBID, apo.Cid, apo.UserId)
}

func (d *Shuttle) addPin(ctx context.Context, contid uint, data cid.Cid, user uint) error {
	var existing Pin
	err := d.DB.First(&existing, "content = ?", contid).Error
	switch {
	default:
		return err
	case err == nil:
		// already have a pin with this content id
		if !existing.Pinning && existing.Active {
			// we already finished pinning this one
			// This implies that the pin complete message got lost, need to resend all the objects

			var objects []*Object
			if err := d.DB.Model(ObjRef{}).Where("pin = ?", existing.ID).
				Joins("left join objects on obj_refs.object = objects.id").
				Scan(&objects).Error; err != nil {
				return err
			}

			go d.sendPinCompleteMessage(ctx, contid, existing.Size, objects)
			return nil
		}

		if !existing.Active {
			if err := d.DB.Model(Pin{}).Where("id = ?", existing.ID).UpdateColumn("pinning", true).Error; err != nil {
				return xerrors.Errorf("failed to update pin pinning state to true: %s", err)
			}
		}
	case xerrors.Is(err, gorm.ErrRecordNotFound):
		// good, no pin found with this content id, lets create it
		pin := &Pin{
			Content: contid,
			Cid:     util.DbCID{data},
			UserID:  user,

			Active:  false,
			Pinning: true,
		}

		if err := d.DB.Create(pin).Error; err != nil {
			return err
		}
	}

	op := &pinner.PinningOperation{
		Obj:    data,
		ContId: contid,
		UserId: user,
		Status: "queued",
	}

	d.PinMgr.Add(op)
	return nil
}

type commpResult struct {
	CommP cid.Cid
	Size  abi.UnpaddedPieceSize
}

func (d *Shuttle) handleRpcComputeCommP(ctx context.Context, cmd *drpc.ComputeCommP) error {
	ctx, span := Tracer.Start(ctx, "handleComputeCommP")
	defer span.End()

	res, err := d.commpMemo.Do(ctx, cmd.Data.String())
	if err != nil {
		return xerrors.Errorf("failed to compute commP: %w", err)
	}

	commpRes, ok := res.(*commpResult)
	if !ok {
		return xerrors.Errorf("result from commp memoizer was of wrong type: %T", res)
	}

	return d.sendRpcMessage(ctx, &drpc.Message{
		Op: drpc.OP_CommPComplete,
		Params: drpc.MsgParams{
			CommPComplete: &drpc.CommPComplete{
				Data:  cmd.Data,
				CommP: commpRes.CommP,
				Size:  commpRes.Size,
			},
		},
	})
}

func (d *Shuttle) sendPinCompleteMessage(ctx context.Context, cont uint, size int64, objects []*Object) {
	objs := make([]drpc.PinObj, 0, len(objects))
	for _, o := range objects {
		objs = append(objs, drpc.PinObj{
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

func (d *Shuttle) handleRpcTakeContent(ctx context.Context, cmd *drpc.TakeContent) error {
	d.addPinLk.Lock()
	defer d.addPinLk.Unlock()

	for _, c := range cmd.Contents {
		var p Pin
		err := d.DB.First(&p, "content = ?", c.ID).Error
		switch err {
		case nil:
			// already have a pin for this, no need to do anything
			continue
		default:
			// some other db error, bail
			return err
		case gorm.ErrRecordNotFound:
			// ok
		}

		if err := d.addPin(ctx, c.ID, c.Cid, c.UserID); err != nil {
			return err
		}
	}

	return nil
}

func (d *Shuttle) handleRpcAggregateContent(ctx context.Context, cmd *drpc.AggregateContent) error {
	d.addPinLk.Lock()
	defer d.addPinLk.Unlock()

	var p Pin
	err := d.DB.First(&p, "content = ?", cmd.DBID).Error
	switch err {
	default:
		return err
	case nil:
		// exists already
		return nil
	case gorm.ErrRecordNotFound:
		// normal case
	}

	totalSize := int64(len(cmd.ObjData))
	for _, c := range cmd.Contents {
		var aggr Pin
		if err := d.DB.First(&aggr, "content = ?", c).Error; err != nil {
			// TODO: implies we dont have all the content locally we are being
			// asked to aggregate, this is an important error to handle
			return err
		}

		totalSize += aggr.Size
	}

	pin := &Pin{
		Content: cmd.DBID,
		Cid:     util.DbCID{cmd.Root},
		UserID:  cmd.UserID,
		Size:    totalSize,

		Active:    false,
		Pinning:   true,
		Aggregate: true,
	}
	if err := d.DB.Create(pin).Error; err != nil {
		return err
	}

	blk, err := blocks.NewBlockWithCid(cmd.ObjData, cmd.Root)
	if err != nil {
		return err
	}
	if err := d.Node.Blockstore.Put(blk); err != nil {
		return err
	}

	if err := d.DB.Model(Pin{}).Where("id = ?", pin.ID).UpdateColumns(map[string]interface{}{
		"active": true,
	}).Error; err != nil {
		return err
	}

	go d.sendPinCompleteMessage(ctx, cmd.DBID, totalSize, nil)
	return nil
}

func (s *Shuttle) trackTransfer(chanid *datatransfer.ChannelID, dealdbid uint) {
	s.tcLk.Lock()
	defer s.tcLk.Unlock()

	s.trackingChannels[chanid.String()] = &chanTrack{
		dbid: dealdbid,
	}
}

func (d *Shuttle) handleRpcStartTransfer(ctx context.Context, cmd *drpc.StartTransfer) error {
	go func() {
		chanid, err := d.Filc.StartDataTransfer(ctx, cmd.Miner, cmd.PropCid, cmd.DataCid)
		if err != nil {
			log.Errorf("failed to start requested data transfer: %s", err)
			d.sendTransferStatusUpdate(ctx, &drpc.TransferStatus{
				DealDBID: cmd.DealDBID,
				Failed:   true,
				Message:  fmt.Sprintf("failed to start data transfer: %s", err),
			})
			return
		}

		d.trackTransfer(chanid, cmd.DealDBID)

		if err := d.sendRpcMessage(ctx, &drpc.Message{
			Op: drpc.OP_TransferStarted,
			Params: drpc.MsgParams{
				TransferStarted: &drpc.TransferStarted{
					DealDBID: cmd.DealDBID,
					Chanid:   chanid.String(),
				},
			},
		}); err != nil {
			log.Errorf("failed to nofity estuary primary node about transfer start: %s", err)
		}
	}()
	return nil
}

func (d *Shuttle) sendTransferStatusUpdate(ctx context.Context, st *drpc.TransferStatus) {
	log.Infof("sending transfer status update: %d %d %s", st.DealDBID, st.State.Status, st.State.Message)
	if err := d.sendRpcMessage(ctx, &drpc.Message{
		Op: drpc.OP_TransferStatus,
		Params: drpc.MsgParams{
			TransferStatus: st,
		},
	}); err != nil {
		log.Errorf("failed to send transfer status update: %s", err)
	}
}
