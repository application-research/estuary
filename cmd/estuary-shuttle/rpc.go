package main

import (
	"context"
	"fmt"

	"github.com/application-research/estuary/drpc"
	"github.com/application-research/estuary/pinner"
	"github.com/application-research/estuary/util"
	dagsplit "github.com/application-research/estuary/util/dagsplit"
	datatransfer "github.com/filecoin-project/go-data-transfer"
	"github.com/filecoin-project/go-state-types/abi"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-blockservice"
	"github.com/ipfs/go-cid"
	cbor "github.com/ipfs/go-ipld-cbor"
	"github.com/ipfs/go-merkledag"
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
	case drpc.CMD_ReqTxStatus:
		return d.handleRpcReqTxStatus(ctx, cmd.Params.ReqTxStatus)
	case drpc.CMD_RetrieveContent:
		return d.handleRpcRetrieveContent(ctx, cmd.Params.RetrieveContent)
	case drpc.CMD_UnpinContent:
		return d.handleRpcUnpinContent(ctx, cmd.Params.UnpinContent)
	case drpc.CMD_SplitContent:
		return d.handleRpcSplitContent(ctx, cmd.Params.SplitContent)
	case drpc.CMD_RestartTransfer:
		return d.handleRpcRestartTransfer(ctx, cmd.Params.RestartTransfer)
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
	return d.addPin(ctx, apo.DBID, apo.Cid, apo.UserId, false)
}

func (d *Shuttle) addPin(ctx context.Context, contid uint, data cid.Cid, user uint, skipLimiter bool) error {
	var search []Pin
	if err := d.DB.Find(&search, "content = ?", contid).Error; err != nil {
		return err
	}

	if len(search) > 0 {
		// already have a pin with this content id
		if len(search) > 1 {
			log.Errorf("have multiple pins for same content id: %d", contid)
		}
		existing := search[0]

		if existing.Failed {
			// being asked to pin a thing we have marked as failed means the
			// primary node isnt aware that this pin failed, we need to resend
			// that notification

			if err := d.sendRpcMessage(ctx, &drpc.Message{
				Op: "UpdatePinStatus",
				Params: drpc.MsgParams{
					UpdatePinStatus: &drpc.UpdatePinStatus{
						DBID:   contid,
						Status: "failed",
					},
				},
			}); err != nil {
				log.Errorf("failed to send pin status update: %s", err)
			}
			return fmt.Errorf("tried to add pin for content we failed to pin previously")
		}

		if !existing.Pinning && existing.Active {
			// we already finished pinning this one
			// This implies that the pin complete message got lost, need to resend all the objects

			go func() {
				if err := d.resendPinComplete(ctx, existing); err != nil {
					log.Error(err)
				}
			}()
			return nil
		}

		if !existing.Active && !existing.Pinning {
			if err := d.DB.Model(Pin{}).Where("id = ?", existing.ID).UpdateColumn("pinning", true).Error; err != nil {
				return xerrors.Errorf("failed to update pin pinning state to true: %s", err)
			}
		}
	} else {
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

		SkipLimiter: skipLimiter,
	}

	d.PinMgr.Add(op)
	return nil
}

func (s *Shuttle) resendPinComplete(ctx context.Context, pin Pin) error {
	objects, err := s.objectsForPin(ctx, pin.ID)
	if err != nil {
		return fmt.Errorf("failed to get objects for pin: %s", err)
	}

	s.sendPinCompleteMessage(ctx, pin.Content, pin.Size, objects)
	return nil
}

func (s *Shuttle) objectsForPin(ctx context.Context, pin uint) ([]*Object, error) {
	_, span := Tracer.Start(ctx, "objectsForPin")
	defer span.End()

	var objects []*Object
	if err := s.DB.Model(ObjRef{}).Where("pin = ?", pin).
		Joins("left join objects on obj_refs.object = objects.id").
		Select("objects.*").
		Scan(&objects).Error; err != nil {
		return nil, err
	}

	return objects, nil
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
		return xerrors.Errorf("failed to compute commP for %s: %w", cmd.Data, err)
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

func (s *Shuttle) sendSplitContentComplete(ctx context.Context, cont uint) {

	panic("NYI")
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
		var count int64
		err := d.DB.Model(Pin{}).Where("content = ?", c.ID).Limit(1).Count(&count).Error
		if err != nil {
			return err
		}
		if count > 0 {
			if count > 1 {
				log.Errorf("have multiple pins for same content: %d", c.ID)
			}
			continue
		}

		if err := d.addPin(ctx, c.ID, c.Cid, c.UserID, true); err != nil {
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

		if !aggr.Active || aggr.Failed {
			return fmt.Errorf("content i am being asked to aggregate is not pinned: %d", c)
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

	obj := &Object{
		Cid:  util.DbCID{blk.Cid()},
		Size: len(blk.RawData()),
	}
	if err := d.DB.Create(obj).Error; err != nil {
		return err
	}
	ref := &ObjRef{
		Pin:    pin.ID,
		Object: obj.ID,
	}
	if err := d.DB.Create(ref).Error; err != nil {
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
	var extra string
	if st.State != nil {
		extra = fmt.Sprintf("%d %s", st.State.Status, st.State.Message)
	}
	log.Infof("sending transfer status update: %d %s", st.DealDBID, extra)
	if err := d.sendRpcMessage(ctx, &drpc.Message{
		Op: drpc.OP_TransferStatus,
		Params: drpc.MsgParams{
			TransferStatus: st,
		},
	}); err != nil {
		log.Errorf("failed to send transfer status update: %s", err)
	}
}

func (s *Shuttle) handleRpcReqTxStatus(ctx context.Context, req *drpc.ReqTxStatus) error {
	go func() {
		ctx := context.TODO()
		st, err := s.Filc.TransferStatus(ctx, &req.ChanID)
		if err != nil {
			log.Errorf("failed to get requested transfer status: %s", err)
			return
		}

		s.sendTransferStatusUpdate(ctx, &drpc.TransferStatus{
			Chanid: req.ChanID.String(),

			// NB: this parameter is only here because
			//its wanted on the other side. We could probably just look up by
			//channel ID instead.
			DealDBID: req.DealDBID,

			State: st,
		})
	}()
	return nil
}

func (s *Shuttle) handleRpcRetrieveContent(ctx context.Context, req *drpc.RetrieveContent) error {
	go func() {
		if err := s.retrieveContent(ctx, req.Content, req.Cid, req.Deals); err != nil {
			log.Errorf("failed to retrieve content: %s", err)
		}
	}()
	return nil
}

func (s *Shuttle) handleRpcUnpinContent(ctx context.Context, req *drpc.UnpinContent) error {
	for _, c := range req.Contents {
		if err := s.Unpin(ctx, c); err != nil {
			return err
		}
	}

	return nil
}

func (s *Shuttle) handleRpcSplitContent(ctx context.Context, req *drpc.SplitContent) error {
	var pin Pin
	if err := s.DB.First(&pin, "content = ?", req.Content).Error; err != nil {
		return xerrors.Errorf("no pin with content %d found for split content request: %w", err)
	}

	// Check if we've done this already...
	if pin.Aggregate && pin.DagSplit {
		// This is only set once we've completed the splitting, so this should be fine

		// However, this might mean that the other side missed receiving some
		// messages from us, so we need to resend everything
		var children []Pin
		if err := s.DB.Find(&children, "aggregated_in = ?", req.Content).Error; err != nil {
			return err
		}

		for _, c := range children {
			if err := s.resendPinComplete(ctx, c); err != nil {
				return err
			}
		}

		s.sendSplitContentComplete(ctx, pin.Content)
	}

	dserv := merkledag.NewDAGService(blockservice.New(s.Node.Blockstore, nil))
	b := dagsplit.NewBuilder(dserv, uint64(req.Size), 0)
	if err := b.Pack(ctx, pin.Cid.CID); err != nil {
		return err
	}

	cst := cbor.NewCborStore(s.Node.Blockstore)

	var boxCids []cid.Cid
	for _, box := range b.Boxes() {
		cc, err := cst.Put(ctx, box)
		if err != nil {
			return err
		}

		boxCids = append(boxCids, cc)
	}

	for i, c := range boxCids {
		fname := fmt.Sprintf("split-%d", i)

		panic("need to figure out how to get user auth...")
		contid, err := s.createContent(ctx, nil, c, fname, "", pin.Content)
		if err != nil {
			return err
		}

		pin := &Pin{
			Cid:          util.DbCID{c},
			Content:      contid,
			Active:       false,
			Pinning:      true,
			UserID:       pin.UserID,
			AggregatedIn: pin.Content,
		}

		if err := s.DB.Create(pin).Error; err != nil {
			return xerrors.Errorf("failed to track new content in database: %w", err)
		}

		if err := s.addDatabaseTrackingToContent(ctx, pin.Content, dserv, s.Node.Blockstore, c, func(int64) {}); err != nil {
			return err
		}
	}

	s.sendSplitContentComplete(ctx, pin.Content)

	return nil
}

func (s *Shuttle) handleRpcRestartTransfer(ctx context.Context, req *drpc.RestartTransfer) error {
	log.Infof("restarting data transfer: %s", req.ChanID)
	st, err := s.Filc.TransferStatus(ctx, &req.ChanID)
	if err != nil {
		return err
	}

	if util.TransferTerminated(st) {
		s.sendTransferStatusUpdate(ctx, &drpc.TransferStatus{
			Chanid: req.ChanID.String(),
			State:  st,
		})
		return fmt.Errorf("cannot restart transfer with status: %d", st.Status)
	}

	return s.Filc.RestartTransfer(ctx, &req.ChanID)
}
