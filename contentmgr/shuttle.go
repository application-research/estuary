package contentmgr

import (
	"context"
	"fmt"
	"net/url"
	"time"

	"go.opentelemetry.io/otel/trace"
	"golang.org/x/xerrors"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"github.com/application-research/estuary/constants"
	"github.com/application-research/estuary/drpc"
	"github.com/application-research/estuary/model"
	"github.com/application-research/estuary/util"
	"github.com/application-research/filclient"
	"github.com/filecoin-project/go-address"
	datatransfer "github.com/filecoin-project/go-data-transfer"
	"github.com/libp2p/go-libp2p/core/peer"
)

type ShuttleConnection struct {
	Handle                string
	cmds                  chan *drpc.Command
	ctx                   context.Context
	Hostname              string
	addrInfo              peer.AddrInfo
	address               address.Address
	private               bool
	ContentAddingDisabled bool
	spaceLow              bool
	blockstoreSize        uint64
	blockstoreFree        uint64
	pinCount              int64
	pinQueueLength        int64
}

func (sc *ShuttleConnection) sendMessage(ctx context.Context, cmd *drpc.Command) error {
	select {
	case sc.cmds <- cmd:
		return nil
	case <-sc.ctx.Done():
		return ErrNoShuttleConnection
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (cm *ContentManager) RegisterShuttleConnection(handle string, hello *drpc.Hello) (chan *drpc.Command, func(), error) {
	cm.ShuttlesLk.Lock()
	defer cm.ShuttlesLk.Unlock()
	_, ok := cm.Shuttles[handle]
	if ok {
		cm.log.Warn("registering shuttle but found existing connection")
		return nil, nil, fmt.Errorf("shuttle already connected")
	}

	_, err := url.Parse(hello.Host)
	if err != nil {
		cm.log.Errorf("shuttle had invalid hostname %q: %s", hello.Host, err)
		hello.Host = ""
	}

	if err := cm.db.Model(model.Shuttle{}).Where("handle = ?", handle).UpdateColumns(map[string]interface{}{
		"host":            hello.Host,
		"peer_id":         hello.AddrInfo.ID.String(),
		"last_connection": time.Now(),
		"private":         hello.Private,
	}).Error; err != nil {
		return nil, nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())

	sc := &ShuttleConnection{
		Handle:                handle,
		address:               hello.Address,
		addrInfo:              hello.AddrInfo,
		Hostname:              hello.Host,
		cmds:                  make(chan *drpc.Command, cm.cfg.RPCMessage.OutgoingQueueSize),
		ctx:                   ctx,
		private:               hello.Private,
		ContentAddingDisabled: hello.ContentAddingDisabled,
	}

	cm.Shuttles[handle] = sc

	return sc.cmds, func() {
		cancel()
		cm.ShuttlesLk.Lock()
		outd, ok := cm.Shuttles[handle]
		if ok {
			if outd == sc {
				delete(cm.Shuttles, handle)
			}
		}
		cm.ShuttlesLk.Unlock()
	}, nil
}

var ErrNilParams = fmt.Errorf("shuttle message had nil params")

func (cm *ContentManager) HandleShuttleMessages(ctx context.Context, numHandlers int) {
	for i := 1; i <= numHandlers; i++ {
		go func() {
			for {
				select {
				case <-ctx.Done():
					return
				case msg := <-cm.IncomingRPCMessages:
					if err := cm.processShuttleMessage(msg.Handle, msg); err != nil {
						cm.log.Errorf("failed to process message from shuttle: %s", err)
					}
				}
			}
		}()
	}
}

func (cm *ContentManager) processShuttleMessage(handle string, msg *drpc.Message) error {
	ctx := context.TODO()

	// if the message contains a trace continue it here.
	if msg.HasTraceCarrier() {
		if sc := msg.TraceCarrier.AsSpanContext(); sc.IsValid() {
			ctx = trace.ContextWithRemoteSpanContext(ctx, sc)
		}
	}
	ctx, span := cm.tracer.Start(ctx, "processShuttleMessage")
	defer span.End()

	cm.log.Debugf("handling rpc message:%s, from shuttle:%s", msg.Op, handle)

	switch msg.Op {
	case drpc.OP_UpdatePinStatus:
		ups := msg.Params.UpdatePinStatus
		if ups == nil {
			return ErrNilParams
		}
		return cm.UpdatePinStatus(handle, ups.DBID, ups.Status)
	case drpc.OP_PinComplete:
		param := msg.Params.PinComplete
		if param == nil {
			return ErrNilParams
		}

		if err := cm.handlePinningComplete(ctx, handle, param); err != nil {
			cm.log.Errorw("handling pin complete message failed", "shuttle", handle, "err", err)
		}
		return nil
	case drpc.OP_CommPComplete:
		param := msg.Params.CommPComplete
		if param == nil {
			return ErrNilParams
		}

		if err := cm.handleRpcCommPComplete(ctx, handle, param); err != nil {
			cm.log.Errorf("handling commp complete message from shuttle %s: %s", handle, err)
		}
		return nil
	case drpc.OP_TransferStarted:
		param := msg.Params.TransferStarted
		if param == nil {
			return ErrNilParams
		}

		if err := cm.handleRpcTransferStarted(ctx, handle, param); err != nil {
			cm.log.Errorf("handling transfer started message from shuttle %s: %s", handle, err)
		}
		return nil
	case drpc.OP_TransferFinished:
		param := msg.Params.TransferFinished
		if param == nil {
			return ErrNilParams
		}

		if err := cm.handleRpcTransferFinished(ctx, handle, param); err != nil {
			cm.log.Errorf("handling transfer finished message from shuttle %s: %s", handle, err)
		}
		return nil
	case drpc.OP_TransferStatus:
		param := msg.Params.TransferStatus
		if param == nil {
			return ErrNilParams
		}

		if err := cm.HandleRpcTransferStatus(ctx, handle, param); err != nil {
			cm.log.Errorf("handling transfer status message from shuttle %s: %s", handle, err)
		}
		return nil
	case drpc.OP_ShuttleUpdate:
		param := msg.Params.ShuttleUpdate
		if param == nil {
			return ErrNilParams
		}

		if err := cm.handleRpcShuttleUpdate(ctx, handle, param); err != nil {
			cm.log.Errorf("handling shuttle update message from shuttle %s: %s", handle, err)
		}
		return nil
	case drpc.OP_GarbageCheck:
		param := msg.Params.GarbageCheck
		if param == nil {
			return ErrNilParams
		}

		if err := cm.handleRpcGarbageCheck(ctx, handle, param); err != nil {
			cm.log.Errorf("handling garbage check message from shuttle %s: %s", handle, err)
		}
		return nil
	case drpc.OP_SplitComplete:
		param := msg.Params.SplitComplete
		if param == nil {
			return ErrNilParams
		}

		if err := cm.handleRpcSplitComplete(ctx, handle, param); err != nil {
			cm.log.Errorf("handling split complete message from shuttle %s: %s", handle, err)
		}
		return nil
	case drpc.OP_SanityCheck:
		sc := msg.Params.SanityCheck
		if sc == nil {
			return ErrNilParams
		}
		cm.HandleSanityCheck(sc.CID, sc.ErrMsg)
		return nil
	default:
		return fmt.Errorf("unrecognized message op: %q", msg.Op)
	}
}

var ErrNoShuttleConnection = fmt.Errorf("no connection to requested shuttle")

func (cm *ContentManager) SendShuttleCommand(ctx context.Context, handle string, cmd *drpc.Command) error {
	if handle == "" {
		return fmt.Errorf("attempted to send command to empty shuttle handle")
	}

	cm.log.Debugf("sending rpc message:%s, to shuttle:%s", cmd.Op, handle)

	cm.ShuttlesLk.Lock()
	d, ok := cm.Shuttles[handle]
	cm.ShuttlesLk.Unlock()
	if ok {
		return d.sendMessage(ctx, cmd)
	}
	return ErrNoShuttleConnection
}

func (cm *ContentManager) ShuttleIsOnline(handle string) bool {
	cm.ShuttlesLk.Lock()
	sc, ok := cm.Shuttles[handle]
	cm.ShuttlesLk.Unlock()
	if !ok {
		return false
	}

	select {
	case <-sc.ctx.Done():
		return false
	default:
		return true
	}
}

func (cm *ContentManager) ShuttleCanAddContent(handle string) bool {
	cm.ShuttlesLk.Lock()
	defer cm.ShuttlesLk.Unlock()
	d, ok := cm.Shuttles[handle]
	if ok {
		return d.ContentAddingDisabled
	}
	return true
}

func (cm *ContentManager) ShuttleAddrInfo(handle string) *peer.AddrInfo {
	cm.ShuttlesLk.Lock()
	defer cm.ShuttlesLk.Unlock()
	d, ok := cm.Shuttles[handle]
	if ok {
		return &d.addrInfo
	}
	return nil
}

func (cm *ContentManager) ShuttleHostName(handle string) string {
	cm.ShuttlesLk.Lock()
	defer cm.ShuttlesLk.Unlock()
	d, ok := cm.Shuttles[handle]
	if ok {
		return d.Hostname
	}
	return ""
}

func (cm *ContentManager) ShuttleStorageStats(handle string) *util.ShuttleStorageStats {
	cm.ShuttlesLk.Lock()
	defer cm.ShuttlesLk.Unlock()
	d, ok := cm.Shuttles[handle]
	if !ok {
		return nil
	}

	return &util.ShuttleStorageStats{
		BlockstoreSize: d.blockstoreSize,
		BlockstoreFree: d.blockstoreFree,
		PinCount:       d.pinCount,
		PinQueueLength: d.pinQueueLength,
	}
}

func (cm *ContentManager) handleRpcCommPComplete(ctx context.Context, handle string, resp *drpc.CommPComplete) error {
	_, span := cm.tracer.Start(ctx, "handleRpcCommPComplete")
	defer span.End()

	opcr := model.PieceCommRecord{
		Data:    util.DbCID{CID: resp.Data},
		Piece:   util.DbCID{CID: resp.CommP},
		Size:    resp.Size,
		CarSize: resp.CarSize,
	}

	return cm.db.Clauses(clause.OnConflict{DoNothing: true}).Create(&opcr).Error
}

func (cm *ContentManager) handleRpcTransferStarted(ctx context.Context, handle string, param *drpc.TransferStartedOrFinished) error {
	if err := cm.SetDataTransferStartedOrFinished(ctx, param.DealDBID, param.Chanid, param.State, true); err != nil {
		return err
	}
	cm.log.Debugw("Started data transfer on shuttle", "chanid", param.Chanid, "shuttle", handle)
	return nil
}

func (cm *ContentManager) handleRpcTransferFinished(ctx context.Context, handle string, param *drpc.TransferStartedOrFinished) error {
	if err := cm.SetDataTransferStartedOrFinished(ctx, param.DealDBID, param.Chanid, param.State, false); err != nil {
		return err
	}
	cm.log.Debugw("Finished data transfer on shuttle", "chanid", param.Chanid, "shuttle", handle)
	return nil
}

func (cm *ContentManager) HandleRpcTransferStatus(ctx context.Context, handle string, param *drpc.TransferStatus) error {
	if param.DealDBID == 0 {
		return fmt.Errorf("received transfer status update with no identifier")
	}

	var cd model.ContentDeal
	if err := cm.db.First(&cd, "id = ?", param.DealDBID).Error; err != nil {
		return err
	}

	if cd.DTChan == "" {
		if err := cm.db.Model(model.ContentDeal{}).Where("id = ?", param.DealDBID).UpdateColumns(map[string]interface{}{
			"dt_chan": param.Chanid,
		}).Error; err != nil {
			return err
		}
	}

	if param.Failed {
		miner, err := cd.MinerAddr()
		if err != nil {
			return err
		}

		if oerr := cm.recordDealFailure(&DealFailureError{
			Miner:               miner,
			Phase:               "data-transfer-remote",
			Message:             fmt.Sprintf("failure from shuttle %s: %s", handle, param.Message),
			Content:             cd.Content,
			UserID:              cd.UserID,
			MinerVersion:        cd.MinerVersion,
			DealProtocolVersion: cd.DealProtocolVersion,
			DealUUID:            cd.DealUUID,
		}); oerr != nil {
			return oerr
		}

		if err := cm.db.Model(model.ContentDeal{}).Where("id = ?", cd.ID).UpdateColumns(map[string]interface{}{
			"failed":    true,
			"failed_at": time.Now(),
		}).Error; err != nil {
			return err
		}

		sts := datatransfer.Failed
		if param.State != nil {
			sts = param.State.Status
		}

		param.State = &filclient.ChannelState{
			Status:  sts,
			Message: fmt.Sprintf("failure from shuttle %s: %s", handle, param.Message),
		}
	}

	// update transfer state for only shuttles
	if handle != constants.ContentLocationLocal {
		cm.updateTransferStatus(ctx, handle, cd.ID, param.State)
	}
	return nil
}

func (cm *ContentManager) handleRpcShuttleUpdate(ctx context.Context, handle string, param *drpc.ShuttleUpdate) error {
	cm.ShuttlesLk.Lock()
	defer cm.ShuttlesLk.Unlock()
	d, ok := cm.Shuttles[handle]
	if !ok {
		return fmt.Errorf("shuttle connection not found while handling update for %q", handle)
	}

	d.spaceLow = param.BlockstoreFree < (param.BlockstoreSize / 10)
	d.blockstoreFree = param.BlockstoreFree
	d.blockstoreSize = param.BlockstoreSize
	d.pinCount = param.NumPins
	d.pinQueueLength = int64(param.PinQueueSize)

	return nil
}

func (cm *ContentManager) handleRpcGarbageCheck(ctx context.Context, handle string, param *drpc.GarbageCheck) error {
	var tounpin []uint
	for _, c := range param.Contents {
		var cont util.Content
		if err := cm.db.First(&cont, "id = ?", c).Error; err != nil {
			if xerrors.Is(err, gorm.ErrRecordNotFound) {
				tounpin = append(tounpin, c)
			} else {
				return err
			}
		}

		if cont.Location != handle || cont.Offloaded {
			tounpin = append(tounpin, c)
		}
	}
	return cm.sendUnpinCmd(ctx, handle, tounpin)
}

func (cm *ContentManager) handleRpcSplitComplete(ctx context.Context, handle string, param *drpc.SplitComplete) error {
	if param.ID == 0 {
		return fmt.Errorf("split complete send with ID = 0")
	}

	// TODO: do some sanity checks that the sub pieces were all made successfully...
	if err := cm.db.Model(util.Content{}).Where("id = ?", param.ID).UpdateColumns(map[string]interface{}{
		"dag_split": true,
		"active":    false,
		"size":      0,
	}).Error; err != nil {
		return fmt.Errorf("failed to update content for split complete: %w", err)
	}

	if err := cm.db.Delete(&util.ObjRef{}, "content = ?", param.ID).Error; err != nil {
		return fmt.Errorf("failed to delete object references for newly split object: %w", err)
	}
	return nil
}
