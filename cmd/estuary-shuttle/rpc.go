package main

import (
	"context"
	"fmt"

	"github.com/application-research/estuary/drpc"
	"github.com/application-research/estuary/pinner"
	"github.com/application-research/estuary/util"
	datatransfer "github.com/filecoin-project/go-data-transfer"
	"github.com/filecoin-project/go-state-types/abi"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/xerrors"
	"gorm.io/gorm"
)

func (s *Shuttle) handleRpcCmd(cmd *drpc.Command) error {
	ctx := context.TODO()

	// If the command contains a trace continue it here.
	if cmd.HasTraceCarrier() {
		if sc := cmd.TraceCarrier.AsSpanContext(); sc.IsValid() {
			ctx = trace.ContextWithRemoteSpanContext(ctx, sc)
		}
	}

	log.Infof("handling rpc command: %s", cmd.Op)
	switch cmd.Op {
	case drpc.CMD_AddPin:
		return s.handleRpcAddPin(ctx, cmd.Params.AddPin)
	case drpc.CMD_ComputeCommP:
		return s.handleRpcComputeCommP(ctx, cmd.Params.ComputeCommP)
	case drpc.CMD_TakeContent:
		return s.handleRpcTakeContent(ctx, cmd.Params.TakeContent)
	case drpc.CMD_AggregateContent:
		return s.handleRpcAggregateContent(ctx, cmd.Params.AggregateContent)
	case drpc.CMD_StartTransfer:
		return s.handleRpcStartTransfer(ctx, cmd.Params.StartTransfer)
	case drpc.CMD_ReqTxStatus:
		return s.handleRpcReqTxStatus(ctx, cmd.Params.ReqTxStatus)
	case drpc.CMD_RetrieveContent:
		return s.handleRpcRetrieveContent(ctx, cmd.Params.RetrieveContent)
	default:
		return fmt.Errorf("unrecognized command op: %q", cmd.Op)
	}
}

func (s *Shuttle) sendRpcMessage(ctx context.Context, msg *drpc.Message) error {
	log.Infof("sending rpc message: %s", msg.Op)
	select {
	case s.outgoing <- msg:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *Shuttle) handleRpcAddPin(ctx context.Context, apo *drpc.AddPin) error {
	s.addPinLk.Lock()
	defer s.addPinLk.Unlock()
	return s.addPin(ctx, apo.DBID, apo.Cid, apo.UserId, false)
}

func (s *Shuttle) addPin(ctx context.Context, contid uint, data cid.Cid, user uint, skipLimiter bool) error {
	ctx, span := s.Tracer.Start(ctx, "addPin", trace.WithAttributes(
		attribute.Int64("contID", int64(contid)),
		attribute.Int64("userID", int64(user)),
		attribute.String("data", data.String()),
		attribute.Bool("skipLimiter", skipLimiter),
	))
	defer span.End()

	var search []Pin
	if err := s.DB.Find(&search, "content = ?", contid).Error; err != nil {
		return err
	}

	if len(search) > 0 {
		// already have a pin with this content id
		if len(search) > 1 {
			log.Errorf("have multiple pins for same content id: %d", contid)
		}
		existing := search[0]
		span.AddEvent("previous found a pin locally", trace.WithAttributes(attribute.Int64("ID", int64(existing.ID))))

		if existing.Failed {
			span.AddEvent("detected previous pin failure, resend notification")
			// being asked to pin a thing we have marked as failed means the
			// primary node isnt aware that this pin failed, we need to resend
			// that notification

			if err := s.sendRpcMessage(ctx, &drpc.Message{
				TraceCarrier: drpc.NewTraceCarrier(span.SpanContext()),
				Op:           "UpdatePinStatus",
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
			span.AddEvent("detected pin with lost message, resend all objects", trace.WithAttributes(attribute.Int64("ID", int64(existing.ID))))
			// we already finished pinning this one
			// This implies that the pin complete message got lost, need to resend all the objects

			go func() {
				objects, err := s.objectsForPin(ctx, existing.ID)
				if err != nil {
					log.Errorf("failed to get objects for pin: %s", err)
					return
				}

				s.sendPinCompleteMessage(ctx, contid, existing.Size, objects)
			}()
			return nil
		}

		if !existing.Active && !existing.Pinning {
			if err := s.DB.Model(Pin{}).Where("id = ?", existing.ID).UpdateColumn("pinning", true).Error; err != nil {
				return xerrors.Errorf("failed to update pin pinning state to true: %s", err)
			}
		}
	} else {
		span.AddEvent("detected no pin locally, create pin")
		// good, no pin found with this content id, lets create it
		pin := &Pin{
			Content: contid,
			Cid:     util.DbCID{data},
			UserID:  user,

			Active:  false,
			Pinning: true,
		}

		if err := s.DB.Create(pin).Error; err != nil {
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

	s.PinMgr.Add(op)
	return nil
}

func (s *Shuttle) objectsForPin(ctx context.Context, pin uint) ([]*Object, error) {
	var objects []*Object
	if err := s.DB.Model(ObjRef{}).Where("pin = ?", pin).
		Joins("left join objects on obj_refs.object = objects.id").
		Scan(&objects).Error; err != nil {
		return nil, err
	}

	return objects, nil
}

type commpResult struct {
	CommP cid.Cid
	Size  abi.UnpaddedPieceSize
}

func (s *Shuttle) handleRpcComputeCommP(ctx context.Context, cmd *drpc.ComputeCommP) error {
	ctx, span := s.Tracer.Start(ctx, "handleComputeCommP", trace.WithAttributes(
		attribute.String("data", cmd.Data.String()),
	))
	defer span.End()

	res, err := s.commpMemo.Do(ctx, cmd.Data.String())
	if err != nil {
		return xerrors.Errorf("failed to compute commP for %s: %w", cmd.Data, err)
	}

	commpRes, ok := res.(*commpResult)
	if !ok {
		return xerrors.Errorf("result from commp memoizer was of wrong type: %T", res)
	}

	return s.sendRpcMessage(ctx, &drpc.Message{
		TraceCarrier: drpc.NewTraceCarrier(span.SpanContext()),
		Op:           drpc.OP_CommPComplete,
		Params: drpc.MsgParams{
			CommPComplete: &drpc.CommPComplete{
				Data:  cmd.Data,
				CommP: commpRes.CommP,
				Size:  commpRes.Size,
			},
		},
	})
}

func (s *Shuttle) sendPinCompleteMessage(ctx context.Context, cont uint, size int64, objects []*Object) {
	ctx, span := s.Tracer.Start(ctx, "sendPinCompleteMessage")
	defer span.End()

	objs := make([]drpc.PinObj, 0, len(objects))
	for _, o := range objects {
		objs = append(objs, drpc.PinObj{
			Cid:  o.Cid.CID,
			Size: o.Size,
		})
	}

	if err := s.sendRpcMessage(ctx, &drpc.Message{
		TraceCarrier: drpc.NewTraceCarrier(span.SpanContext()),
		Op:           drpc.OP_PinComplete,
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

func (s *Shuttle) handleRpcTakeContent(ctx context.Context, cmd *drpc.TakeContent) error {
	ctx, span := s.Tracer.Start(ctx, "handleTakeContent")
	defer span.End()

	s.addPinLk.Lock()
	defer s.addPinLk.Unlock()

	for _, c := range cmd.Contents {
		var count int64
		err := s.DB.Model(Pin{}).Where("content = ?", c.ID).Count(&count).Error
		if err != nil {
			return err
		}
		if count > 0 {
			if count > 1 {
				log.Errorf("have multiple pins for same content: %d", c.ID)
			}
			continue
		}

		if err := s.addPin(ctx, c.ID, c.Cid, c.UserID, true); err != nil {
			return err
		}
	}

	return nil
}

func (s *Shuttle) handleRpcAggregateContent(ctx context.Context, cmd *drpc.AggregateContent) error {
	ctx, span := s.Tracer.Start(ctx, "handleAggregateContent", trace.WithAttributes(
		attribute.Int64("dbID", int64(cmd.DBID)),
		attribute.Int64("userId", int64(cmd.UserID)),
		attribute.String("root", cmd.Root.String()),
	))
	defer span.End()

	s.addPinLk.Lock()
	defer s.addPinLk.Unlock()

	var p Pin
	err := s.DB.First(&p, "content = ?", cmd.DBID).Error
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
		if err := s.DB.First(&aggr, "content = ?", c).Error; err != nil {
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
	if err := s.DB.Create(pin).Error; err != nil {
		return err
	}

	blk, err := blocks.NewBlockWithCid(cmd.ObjData, cmd.Root)
	if err != nil {
		return err
	}
	if err := s.Node.Blockstore.Put(blk); err != nil {
		return err
	}

	if err := s.DB.Model(Pin{}).Where("id = ?", pin.ID).UpdateColumns(map[string]interface{}{
		"active": true,
	}).Error; err != nil {
		return err
	}

	obj := &Object{
		Cid:  util.DbCID{blk.Cid()},
		Size: len(blk.RawData()),
	}
	if err := s.DB.Create(obj).Error; err != nil {
		return err
	}
	ref := &ObjRef{
		Pin:    pin.ID,
		Object: obj.ID,
	}
	if err := s.DB.Create(ref).Error; err != nil {
		return err
	}

	go s.sendPinCompleteMessage(ctx, cmd.DBID, totalSize, nil)
	return nil
}

func (s *Shuttle) trackTransfer(chanid *datatransfer.ChannelID, dealdbid uint) {
	s.tcLk.Lock()
	defer s.tcLk.Unlock()

	s.trackingChannels[chanid.String()] = &chanTrack{
		dbid: dealdbid,
	}
}

func (s *Shuttle) handleRpcStartTransfer(ctx context.Context, cmd *drpc.StartTransfer) error {
	ctx, span := s.Tracer.Start(ctx, "handleStartTransfer", trace.WithAttributes(
		attribute.Int64("contentID", int64(cmd.ContentID)),
		attribute.Int64("dealDbID", int64(cmd.DealDBID)),
		attribute.String("miner", cmd.Miner.String()),
		attribute.String("dataCID", cmd.DataCid.String()),
		attribute.String("propCID", cmd.PropCid.String()),
	))
	defer span.End()

	go func() {
		chanid, err := s.Filc.StartDataTransfer(ctx, cmd.Miner, cmd.PropCid, cmd.DataCid)
		if err != nil {
			errMsg := fmt.Sprintf("failed to start data transfer: %s", err)
			span.AddEvent(errMsg)
			log.Error(errMsg)
			s.sendTransferStatusUpdate(ctx, &drpc.TransferStatus{
				DealDBID: cmd.DealDBID,
				Failed:   true,
				Message:  errMsg,
			})
			return
		}

		s.trackTransfer(chanid, cmd.DealDBID)

		if err := s.sendRpcMessage(ctx, &drpc.Message{
			TraceCarrier: drpc.NewTraceCarrier(span.SpanContext()),
			Op:           drpc.OP_TransferStarted,
			Params: drpc.MsgParams{
				TransferStarted: &drpc.TransferStarted{
					DealDBID: cmd.DealDBID,
					Chanid:   chanid.String(),
				},
			},
		}); err != nil {
			errMsg := fmt.Sprintf("failed to nofity estuary primary node about transfer start: %s", err)
			span.AddEvent(errMsg)
			log.Error(errMsg)
		}
	}()
	return nil
}

func (s *Shuttle) sendTransferStatusUpdate(ctx context.Context, st *drpc.TransferStatus) {
	ctx, span := s.Tracer.Start(ctx, "sendTransferStatusUpdate")
	defer span.End()

	var extra string
	if st.State != nil {
		extra = fmt.Sprintf("%d %s", st.State.Status, st.State.Message)
	}
	log.Infof("sending transfer status update: %d %s", st.DealDBID, extra)
	if err := s.sendRpcMessage(ctx, &drpc.Message{
		TraceCarrier: drpc.NewTraceCarrier(span.SpanContext()),
		Op:           drpc.OP_TransferStatus,
		Params: drpc.MsgParams{
			TransferStatus: st,
		},
	}); err != nil {
		errMsg := fmt.Sprintf("failed to send transfer status update: %s", err)
		span.AddEvent(errMsg)
		log.Error(errMsg)
	}
}

func (s *Shuttle) handleRpcReqTxStatus(ctx context.Context, req *drpc.ReqTxStatus) error {
	ctx, span := s.Tracer.Start(ctx, "handleReqTxStatus", trace.WithAttributes(
		attribute.Int64("dealDbID", int64(req.DealDBID)),
	))
	defer span.End()

	go func() {
		ctx := context.TODO()
		st, err := s.Filc.TransferStatus(ctx, &req.ChanID)
		if err != nil {
			errMsg := fmt.Sprintf("failed to get requested transfer status: %s", err)
			span.AddEvent(errMsg)
			log.Error(errMsg)
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
	ctx, span := s.Tracer.Start(ctx, "handleRetrieveContent")
	defer span.End()

	return fmt.Errorf("not yet implemented")
}
