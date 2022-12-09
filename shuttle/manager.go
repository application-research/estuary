package shuttle

import (
	"context"
	"fmt"
	"sync"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
	"golang.org/x/xerrors"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"github.com/application-research/estuary/config"
	"github.com/application-research/estuary/constants"
	dealstatus "github.com/application-research/estuary/deal/status"
	"github.com/application-research/estuary/sanitycheck"
	"github.com/ipfs/go-cid"

	"github.com/application-research/estuary/drpc"
	"github.com/application-research/estuary/model"
	transferstatus "github.com/application-research/estuary/transfer/status"

	"github.com/application-research/estuary/util"
	"github.com/application-research/filclient"
	datatransfer "github.com/filecoin-project/go-data-transfer"
	lru "github.com/hashicorp/golang-lru"
	"github.com/libp2p/go-libp2p/core/peer"
)

type Manager struct {
	ShuttlesLk            sync.Mutex
	Shuttles              map[string]*ShuttleConnection
	log                   *zap.SugaredLogger
	db                    *gorm.DB
	cfg                   *config.Estuary
	IncomingRPCMessages   chan *drpc.Message
	tracer                trace.Tracer
	transferStatuses      *lru.ARCCache
	transferStatusUpdater *transferstatus.Updater
	dealStatusUpdater     *dealstatus.Updater
	sanityCheckMgr        *sanitycheck.Manager
	pinStatuses           *lru.ARCCache
}

func NewManager(db *gorm.DB, cfg *config.Estuary, log *zap.SugaredLogger) (*Manager, error) {
	cache, err := lru.NewARC(50000)
	if err != nil {
		return nil, err
	}

	return &Manager{
		cfg:                   cfg,
		db:                    db,
		transferStatuses:      cache,
		Shuttles:              make(map[string]*ShuttleConnection),
		tracer:                otel.Tracer("replicator"),
		IncomingRPCMessages:   make(chan *drpc.Message, cfg.RPCMessage.IncomingQueueSize),
		log:                   log,
		transferStatusUpdater: transferstatus.NewUpdater(db),
		dealStatusUpdater:     dealstatus.NewUpdater(db, log),
		sanityCheckMgr:        sanitycheck.NewManager(db, log),
	}, nil
}

var ErrNilParams = fmt.Errorf("shuttle message had nil params")

func (cm *Manager) HandleShuttleMessages(ctx context.Context, numHandlers int) {
	for i := 1; i <= numHandlers; i++ {
		go func() {
			for {
				select {
				case <-ctx.Done():
					return
				case msg := <-cm.IncomingRPCMessages:
					if err := cm.processShuttleMessage(msg); err != nil {
						cm.log.Errorf("failed to process message from shuttle: %s", err)
					}
				}
			}
		}()
	}
}

func (cm *Manager) processShuttleMessage(msg *drpc.Message) error {
	ctx := context.TODO()

	// if the message contains a trace continue it here.
	if msg.HasTraceCarrier() {
		if sc := msg.TraceCarrier.AsSpanContext(); sc.IsValid() {
			ctx = trace.ContextWithRemoteSpanContext(ctx, sc)
		}
	}
	ctx, span := cm.tracer.Start(ctx, "processShuttleMessage")
	defer span.End()

	cm.log.Debugf("handling rpc message:%s, from shuttle:%s", msg.Op, msg.Handle)

	switch msg.Op {
	case drpc.OP_UpdatePinStatus:
		ups := msg.Params.UpdatePinStatus
		if ups == nil {
			return ErrNilParams
		}
		return cm.pinMgr.UpdatePinStatus(msg.Handle, ups.DBID, ups.Status)
	case drpc.OP_PinComplete:
		param := msg.Params.PinComplete
		if param == nil {
			return ErrNilParams
		}

		if err := cm.pinMgr.HandlePinningComplete(ctx, msg.Handle, param); err != nil {
			cm.log.Errorw("handling pin complete message failed", "shuttle", msg.Handle, "err", err)
		}
		return nil
	case drpc.OP_CommPComplete:
		param := msg.Params.CommPComplete
		if param == nil {
			return ErrNilParams
		}

		if err := cm.handleRpcCommPComplete(ctx, msg.Handle, param); err != nil {
			cm.log.Errorf("handling commp complete message from shuttle %s: %s", msg.Handle, err)
		}
		return nil
	case drpc.OP_TransferStarted:
		param := msg.Params.TransferStarted
		if param == nil {
			return ErrNilParams
		}

		if err := cm.handleRpcTransferStarted(ctx, msg.Handle, param); err != nil {
			cm.log.Errorf("handling transfer started message from shuttle %s: %s", msg.Handle, err)
		}
		return nil
	case drpc.OP_TransferFinished:
		param := msg.Params.TransferFinished
		if param == nil {
			return ErrNilParams
		}

		if err := cm.handleRpcTransferFinished(ctx, msg.Handle, param); err != nil {
			cm.log.Errorf("handling transfer finished message from shuttle %s: %s", msg.Handle, err)
		}
		return nil
	case drpc.OP_TransferStatus:
		param := msg.Params.TransferStatus
		if param == nil {
			return ErrNilParams
		}

		if err := cm.HandleRpcTransferStatus(ctx, msg.Handle, param); err != nil {
			cm.log.Errorf("handling transfer status message from shuttle %s: %s", msg.Handle, err)
		}
		return nil
	case drpc.OP_ShuttleUpdate:
		param := msg.Params.ShuttleUpdate
		if param == nil {
			return ErrNilParams
		}

		if err := cm.handleRpcShuttleUpdate(ctx, msg.Handle, param); err != nil {
			cm.log.Errorf("handling shuttle update message from shuttle %s: %s", msg.Handle, err)
		}
		return nil
	case drpc.OP_GarbageCheck:
		param := msg.Params.GarbageCheck
		if param == nil {
			return ErrNilParams
		}

		if err := cm.handleRpcGarbageCheck(ctx, msg.Handle, param); err != nil {
			cm.log.Errorf("handling garbage check message from shuttle %s: %s", msg.Handle, err)
		}
		return nil
	case drpc.OP_SplitComplete:
		param := msg.Params.SplitComplete
		if param == nil {
			return ErrNilParams
		}

		if err := cm.handleRpcSplitComplete(ctx, msg.Handle, param); err != nil {
			cm.log.Errorf("handling split complete message from shuttle %s: %s", msg.Handle, err)
		}
		return nil
	case drpc.OP_SanityCheck:
		sc := msg.Params.SanityCheck
		if sc == nil {
			return ErrNilParams
		}
		cm.sanityCheckMgr.HandleMissingBlock(sc.CID, sc.ErrMsg)
		return nil
	default:
		return fmt.Errorf("unrecognized message op: %q", msg.Op)
	}
}

var ErrNoShuttleConnection = fmt.Errorf("no connection to requested shuttle")

func (cm *Manager) sendRPCMessage(ctx context.Context, handle string, cmd *drpc.Command) error {
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

func (cm *Manager) ShuttleIsOnline(handle string) bool {
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

func (cm *Manager) ShuttleCanAddContent(handle string) bool {
	cm.ShuttlesLk.Lock()
	defer cm.ShuttlesLk.Unlock()
	d, ok := cm.Shuttles[handle]
	if ok {
		return !d.ContentAddingDisabled
	}
	return true
}

func (cm *Manager) ShuttleAddrInfo(handle string) *peer.AddrInfo {
	cm.ShuttlesLk.Lock()
	defer cm.ShuttlesLk.Unlock()
	d, ok := cm.Shuttles[handle]
	if ok {
		return &d.addrInfo
	}
	return nil
}

func (cm *Manager) ShuttleHostName(handle string) string {
	cm.ShuttlesLk.Lock()
	defer cm.ShuttlesLk.Unlock()
	d, ok := cm.Shuttles[handle]
	if ok {
		return d.Hostname
	}
	return ""
}

func (cm *Manager) ShuttleStorageStats(handle string) *util.ShuttleStorageStats {
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

func (cm *Manager) handleRpcCommPComplete(ctx context.Context, handle string, resp *drpc.CommPComplete) error {
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

func (cm *Manager) handleRpcTransferStarted(ctx context.Context, handle string, param *drpc.TransferStartedOrFinished) error {
	if err := cm.transferStatusUpdater.SetDataTransferStartedOrFinished(ctx, param.DealDBID, param.Chanid, param.State, true); err != nil {
		return err
	}
	cm.log.Debugw("Started data transfer on shuttle", "chanid", param.Chanid, "shuttle", handle)
	return nil
}

func (cm *Manager) SendRestartTransferCmd(ctx context.Context, loc string, chanid datatransfer.ChannelID, d model.ContentDeal) error {
	return cm.sendRPCMessage(ctx, loc, &drpc.Command{
		Op: drpc.CMD_RestartTransfer,
		Params: drpc.CmdParams{
			RestartTransfer: &drpc.RestartTransfer{
				ChanID:    chanid,
				DealDBID:  d.ID,
				ContentID: d.Content,
			},
		},
	})
}

func (cm *Manager) handleRpcTransferFinished(ctx context.Context, handle string, param *drpc.TransferStartedOrFinished) error {
	if err := cm.transferStatusUpdater.SetDataTransferStartedOrFinished(ctx, param.DealDBID, param.Chanid, param.State, false); err != nil {
		return err
	}
	cm.log.Debugw("Finished data transfer on shuttle", "chanid", param.Chanid, "shuttle", handle)
	return nil
}

func (cm *Manager) HandleRpcTransferStatus(ctx context.Context, handle string, param *drpc.TransferStatus) error {
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

		if oerr := cm.dealStatusUpdater.RecordDealFailure(&dealstatus.DealFailureError{
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

type transferStatusRecord struct {
	State    *filclient.ChannelState
	Shuttle  string
	Received time.Time
}

func (cm *Manager) updateTransferStatus(ctx context.Context, loc string, dealdbid uint, st *filclient.ChannelState) {
	cm.transferStatuses.Add(dealdbid, &transferStatusRecord{
		State:    st,
		Shuttle:  loc,
		Received: time.Now(),
	})
}

func (cm *Manager) handleRpcShuttleUpdate(ctx context.Context, handle string, param *drpc.ShuttleUpdate) error {
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

func (cm *Manager) handleRpcGarbageCheck(ctx context.Context, handle string, param *drpc.GarbageCheck) error {
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
	return cm.SendUnpinCmd(ctx, handle, tounpin)
}

func (cm *Manager) SendCommPCmd(ctx context.Context, loc string, data cid.Cid) error {
	return cm.sendRPCMessage(ctx, loc, &drpc.Command{
		Op: drpc.CMD_ComputeCommP,
		Params: drpc.CmdParams{
			ComputeCommP: &drpc.ComputeCommP{
				Data: data,
			},
		},
	})
}

func (cm *Manager) SendUnpinCmd(ctx context.Context, loc string, conts []uint) error {
	return cm.sendRPCMessage(ctx, loc, &drpc.Command{
		Op: drpc.CMD_UnpinContent,
		Params: drpc.CmdParams{
			UnpinContent: &drpc.UnpinContent{
				Contents: conts,
			},
		},
	})
}

func (cm *Manager) GetTransferStatus(ctx context.Context, contLoc string, d *model.ContentDeal) (*filclient.ChannelState, error) {
	val, ok := cm.transferStatuses.Get(d.ID)
	if !ok {
		if err := cm.SendRequestTransferStatusCmd(ctx, contLoc, d.ID, d.DTChan); err != nil {
			return nil, err
		}
		return nil, nil
	}

	tsr, ok := val.(*transferStatusRecord)
	if !ok {
		return nil, fmt.Errorf("invalid type placed in remote transfer status cache: %T", val)
	}
	return tsr.State, nil
}

func (cm *Manager) SendRequestTransferStatusCmd(ctx context.Context, loc string, dealid uint, chid string) error {
	return cm.sendRPCMessage(ctx, loc, &drpc.Command{
		Op: drpc.CMD_ReqTxStatus,
		Params: drpc.CmdParams{
			ReqTxStatus: &drpc.ReqTxStatus{
				DealDBID: dealid,
				ChanID:   chid,
			},
		},
	})
}

func (cm *Manager) handleRpcSplitComplete(ctx context.Context, handle string, param *drpc.SplitComplete) error {
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
