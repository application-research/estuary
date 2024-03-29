package rpc

import (
	"context"
	"fmt"
	"time"

	commpstatus "github.com/application-research/estuary/content/commp/status"
	splitqueuemgr "github.com/application-research/estuary/content/split/queue"
	stgzonequeuemgr "github.com/application-research/estuary/content/stagingzone/queue"
	dealqueuemgr "github.com/application-research/estuary/deal/queue"

	dealstatus "github.com/application-research/estuary/deal/status"
	transferstatus "github.com/application-research/estuary/deal/transfer/status"
	lru "github.com/hashicorp/golang-lru"
	"github.com/labstack/echo/v4"

	"github.com/application-research/estuary/config"
	"github.com/application-research/estuary/constants"
	"github.com/application-research/estuary/model"
	"github.com/application-research/estuary/pinner/status"
	"github.com/application-research/estuary/sanitycheck"
	"github.com/application-research/estuary/shuttle/rpc/engines/queue"
	websocketeng "github.com/application-research/estuary/shuttle/rpc/engines/websocket"

	"github.com/application-research/estuary/util"
	"github.com/application-research/filclient"

	rpcevent "github.com/application-research/estuary/shuttle/rpc/event"
	datatransfer "github.com/filecoin-project/go-data-transfer"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
	"golang.org/x/xerrors"
	"gorm.io/gorm"
)

var ErrNilParams = fmt.Errorf("shuttle message had nil params")

type transferStatusRecord struct {
	State    *filclient.ChannelState
	Shuttle  string
	Received time.Time
}

type IManager interface {
	Connect(c echo.Context, handle string, done chan struct{}) error
	SendRPCMessage(ctx context.Context, handle string, cmd *rpcevent.Command) error
	GetTransferStatus(dealID uint) (*filclient.ChannelState, error)
}

type manager struct {
	db                    *gorm.DB
	cfg                   *config.Estuary
	log                   *zap.SugaredLogger
	tracer                trace.Tracer
	sanityCheckMgr        sanitycheck.IManager
	transferStatusUpdater transferstatus.IUpdater
	dealStatusUpdater     dealstatus.IUpdater
	transferStatuses      *lru.ARCCache
	websocketEng          websocketeng.IEstuaryRpcEngine
	queueEng              queue.IEstuaryRpcEngine
	stgZoneQueueMgr       stgzonequeuemgr.IManager
	dealQueueMgr          dealqueuemgr.IManager
	splitQueueMgr         splitqueuemgr.IManager
	commpStatusUpdater    commpstatus.IUpdater
	pinStatusUpdater      status.IUpdater
}

func NewEstuaryRpcManager(ctx context.Context, db *gorm.DB, cfg *config.Estuary, log *zap.SugaredLogger, sanitycheckMgr sanitycheck.IManager) (IManager, error) {
	cache, err := lru.NewARC(50000)
	if err != nil {
		return nil, err
	}

	rpcMgr := &manager{
		db:                    db,
		cfg:                   cfg,
		log:                   log,
		tracer:                otel.Tracer("shuttle"),
		sanityCheckMgr:        sanitycheckMgr,
		transferStatusUpdater: transferstatus.NewUpdater(db),
		dealStatusUpdater:     dealstatus.NewUpdater(db, log),
		transferStatuses:      cache,
		stgZoneQueueMgr:       stgzonequeuemgr.NewManager(log),
		dealQueueMgr:          dealqueuemgr.NewManager(cfg, log),
		splitQueueMgr:         splitqueuemgr.NewManager(log),
		commpStatusUpdater:    commpstatus.NewUpdater(db, log),
		pinStatusUpdater:      status.NewUpdater(db, log),
	}

	rpcMgr.websocketEng = websocketeng.NewEstuaryRpcEngine(ctx, db, cfg, log, rpcMgr.processMessage)

	if cfg.RpcEngine.Queue.Enabled {
		rpcEng, err := queue.NewEstuaryRpcEngine(cfg, log, rpcMgr.processMessage)
		if err != nil {
			return nil, err
		}
		rpcMgr.queueEng = rpcEng
	}
	return rpcMgr, nil
}

func (m *manager) Connect(c echo.Context, handle string, done chan struct{}) error {
	return m.websocketEng.Connect(c, handle, done)
}

func (m *manager) SendRPCMessage(ctx context.Context, handle string, cmd *rpcevent.Command) error {
	if handle == "" || handle == constants.ContentLocationLocal {
		return fmt.Errorf("attempted to send command to empty shuttle handle or local")
	}

	// if estuary has queue enabled, use it
	if m.cfg.RpcEngine.Queue.Enabled && m.queueEng != nil {
		if !rpcevent.CommandTopics[cmd.Op] {
			return fmt.Errorf("%s topic has not been registered properly", cmd.Op)
		}
		m.log.Debugf("sending rpc message: %s, to shuttle: %s using queue engine", cmd.Op, handle)
		return m.queueEng.SendMessage(cmd.Op, handle, cmd)
	}

	d, ok := m.websocketEng.GetShuttleConnection(handle)
	if ok {
		m.log.Debugf("sending rpc message: %s, to shuttle: %s using websocket engine", cmd.Op, handle)
		return d.SendMessage(ctx, cmd)
	}
	return websocketeng.ErrNoShuttleConnection
}

func (m *manager) processMessage(msg *rpcevent.Message, source string) error {
	ctx := context.TODO()

	// if the message contains a trace continue it here.
	if msg.HasTraceCarrier() {
		if sc := msg.TraceCarrier.AsSpanContext(); sc.IsValid() {
			ctx = trace.ContextWithRemoteSpanContext(ctx, sc)
		}
	}
	ctx, span := m.tracer.Start(ctx, "processShuttleMessage")
	defer span.End()

	m.log.Debugf("handling rpc message: %s, from shuttle: %s using %s engine", msg.Op, msg.Handle, source)

	switch msg.Op {
	case rpcevent.OP_UpdatePinStatus:
		ups := msg.Params.UpdatePinStatus
		if ups == nil {
			return ErrNilParams
		}
		return m.handlePinUpdate(msg.Handle, ups.DBID, ups.Status)
	case rpcevent.OP_PinComplete:
		param := msg.Params.PinComplete
		if param == nil {
			return ErrNilParams
		}

		if err := m.handlePinningComplete(ctx, msg.Handle, param); err != nil {
			m.log.Errorw("handling pin complete message failed", "shuttle", msg.Handle, "err", err)
		}
		return nil
	case rpcevent.OP_CommPComplete:
		param := msg.Params.CommPComplete
		if param == nil {
			return ErrNilParams
		}

		if err := m.handleRpcCommPComplete(ctx, msg.Handle, param); err != nil {
			m.log.Errorf("handling commp complete message from shuttle %s: %s", msg.Handle, err)
		}
		return nil
	case rpcevent.OP_CommPFailed:
		param := msg.Params.CommPFailed
		if param == nil {
			return ErrNilParams
		}

		if err := m.handleRpcCommPFailed(ctx, msg.Handle, param); err != nil {
			m.log.Errorf("handling commp failed message from shuttle %s: %s", msg.Handle, err)
		}
		return nil
	case rpcevent.OP_TransferStarted:
		param := msg.Params.TransferStarted
		if param == nil {
			return ErrNilParams
		}

		if err := m.handleRpcTransferStarted(ctx, msg.Handle, param); err != nil {
			m.log.Errorf("handling transfer started message from shuttle %s: %s", msg.Handle, err)
		}
		return nil
	case rpcevent.OP_TransferFinished:
		param := msg.Params.TransferFinished
		if param == nil {
			return ErrNilParams
		}

		if err := m.handleRpcTransferFinished(ctx, msg.Handle, param); err != nil {
			m.log.Errorf("handling transfer finished message from shuttle %s: %s", msg.Handle, err)
		}
		return nil
	case rpcevent.OP_TransferStatus:
		param := msg.Params.TransferStatus
		if param == nil {
			return ErrNilParams
		}

		if err := m.HandleRpcTransferStatus(ctx, msg.Handle, param); err != nil {
			m.log.Errorf("handling transfer status message from shuttle %s: %s", msg.Handle, err)
		}
		return nil
	case rpcevent.OP_ShuttleUpdate:
		param := msg.Params.ShuttleUpdate
		if param == nil {
			return ErrNilParams
		}

		if err := m.handleRpcShuttleUpdate(ctx, msg.Handle, param); err != nil {
			m.log.Errorf("handling shuttle update message from shuttle %s: %s", msg.Handle, err)
		}
		return nil
	case rpcevent.OP_GarbageCheck:
		param := msg.Params.GarbageCheck
		if param == nil {
			return ErrNilParams
		}

		if err := m.handleRpcGarbageCheck(ctx, msg.Handle, param); err != nil {
			m.log.Errorf("handling garbage check message from shuttle %s: %s", msg.Handle, err)
		}
		return nil
	case rpcevent.OP_SplitComplete:
		param := msg.Params.SplitComplete
		if param == nil {
			return ErrNilParams
		}

		if err := m.handleRpcSplitComplete(ctx, msg.Handle, param); err != nil {
			m.log.Errorf("handling split complete message from shuttle %s: %s", msg.Handle, err)
		}
		return nil
	case rpcevent.OP_SplitFailed:
		param := msg.Params.SplitFailed
		if param == nil {
			return ErrNilParams
		}

		if err := m.handleRpcSplitFailed(ctx, msg.Handle, param); err != nil {
			m.log.Errorf("handling split failed message from shuttle %s: %s", msg.Handle, err)
		}
		return nil
	case rpcevent.OP_SanityCheck:
		sc := msg.Params.SanityCheck
		if sc == nil {
			return ErrNilParams
		}
		go func() {
			m.sanityCheckMgr.HandleMissingBlocks(sc.CID, sc.ErrMsg)
		}()
		return nil
	default:
		return fmt.Errorf("unrecognized message op: %q", msg.Op)
	}
}

func (m *manager) handleRpcShuttleUpdate(ctx context.Context, handle string, param *rpcevent.ShuttleUpdate) error {
	if err := m.db.Model(model.ShuttleConnection{}).Where("handle = ?", handle).UpdateColumns(map[string]interface{}{
		"space_low":        param.BlockstoreFree < (param.BlockstoreSize / 10),
		"blockstore_free":  param.BlockstoreFree,
		"blockstore_size":  param.BlockstoreSize,
		"pin_count":        param.NumPins,
		"pin_queue_length": int64(param.PinQueueSize),
		"updated_at":       time.Now().UTC(),
	}).Error; err != nil {
		return xerrors.Errorf("failed to update content in database: %w", err)
	}
	return nil
}

func (m *manager) handleRpcGarbageCheck(ctx context.Context, handle string, param *rpcevent.GarbageCheck) error {
	var tounpin []uint64
	for _, c := range param.Contents {
		var cont util.Content
		if err := m.db.First(&cont, "id = ?", c).Error; err != nil {
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

	return m.SendRPCMessage(ctx, handle, &rpcevent.Command{
		Op: rpcevent.CMD_UnpinContent,
		Params: rpcevent.CmdParams{
			UnpinContent: &rpcevent.UnpinContent{
				Contents: tounpin,
			},
		},
	})
}

// handlePinUpdate exactly copied from UpdateContentPinStatus
func (m *manager) handlePinUpdate(location string, contID uint64, status status.PinningStatus) error {
	return m.pinStatusUpdater.UpdateContentPinStatus(contID, location, status)
}

// TODO merge with handlePinUpdate
func (m *manager) handlePinningComplete(ctx context.Context, handle string, pincomp *rpcevent.PinComplete) error {
	ctx, span := m.tracer.Start(ctx, "handlePinningComplete")
	defer span.End()

	var cont *util.Content
	if err := m.db.First(&cont, "id = ?", pincomp.DBID).Error; err != nil {
		if !xerrors.Is(err, gorm.ErrRecordNotFound) {
			return xerrors.Errorf("failed to look up content: %d (shuttle = %s): %w", pincomp.DBID, handle, err)
		}
		m.log.Warnf("content: %d not found for pin complete from shuttle: %s", pincomp.DBID, handle)
		return nil
	}

	// if content already active, no need to add objects, just update location
	// this is used by consolidated contents
	if cont.Active {
		return m.db.Model(util.Content{}).Where("id = ?", cont.ID).UpdateColumns(map[string]interface{}{
			"location": handle,
		}).Error
	}

	// if content is an aggregate zone
	if cont.Aggregate {
		if len(pincomp.Objects) != 1 {
			return fmt.Errorf("aggregate has more than 1 objects")
		}

		return m.db.Transaction(func(tx *gorm.DB) error {
			// create objet
			obj := &util.Object{
				Cid:  util.DbCID{CID: pincomp.Objects[0].Cid},
				Size: pincomp.Objects[0].Size,
			}
			if err := tx.Create(obj).Error; err != nil {
				return xerrors.Errorf("failed to create Object: %w", err)
			}

			// create obj ref
			if err := tx.Create(&util.ObjRef{
				Content: cont.ID,
				Object:  obj.ID,
			}).Error; err != nil {
				return xerrors.Errorf("failed to create Object reference: %w", err)
			}

			// update aggregate content
			if err := tx.Model(util.Content{}).Where("id = ?", cont.ID).UpdateColumns(map[string]interface{}{
				"active":   true,
				"pinning":  false,
				"cid":      util.DbCID{CID: pincomp.CID},
				"location": handle,
			}).Error; err != nil {
				return xerrors.Errorf("failed to update content in database: %w", err)
			}

			// update staging zone
			if err := tx.Model(model.StagingZone{}).Where("cont_id = ?", cont.ID).UpdateColumns(map[string]interface{}{
				"status":   model.ZoneStatusDone,
				"message":  model.ZoneMessageDone,
				"location": handle,
			}).Error; err != nil {
				return xerrors.Errorf("failed to update zone in database: %w", err)
			}

			// queue aggregate content for deal making
			return m.dealQueueMgr.QueueContent(cont.ID, tx)
		})
	}

	// for individual contents
	objects := make([]*util.Object, 0, len(pincomp.Objects))
	for _, o := range pincomp.Objects {
		objects = append(objects, &util.Object{
			Cid:  util.DbCID{CID: o.Cid},
			Size: o.Size,
		})
	}

	if err := m.addObjectsToDatabase(ctx, cont, objects, handle); err != nil {
		return xerrors.Errorf("failed to add objects to database: %w", err)
	}
	return nil
}

// addObjectsToDatabase creates entries on the estuary database for CIDs related to an already pinned CID (`root`)
// These entries are saved on the `objects` table, while metadata about the `root` CID is mostly kept on the `contents` table
// The link between the `objects` and `contents` tables is the `obj_refs` table
func (m *manager) addObjectsToDatabase(ctx context.Context, cont *util.Content, objects []*util.Object, loc string) error {
	_, span := m.tracer.Start(ctx, "addObjectsToDatabase")
	defer span.End()

	return m.db.Transaction(func(tx *gorm.DB) error {
		// create objects
		if err := tx.CreateInBatches(objects, 300).Error; err != nil {
			return xerrors.Errorf("failed to create objects in db: %w", err)
		}

		refs := make([]util.ObjRef, 0, len(objects))
		var contSize int64
		for _, o := range objects {
			refs = append(refs, util.ObjRef{
				Content: cont.ID,
				Object:  o.ID,
			})
			contSize += int64(o.Size)
		}

		span.SetAttributes(
			attribute.Int64("totalSize", contSize),
			attribute.Int("numObjects", len(objects)),
		)

		// create object refs
		if err := tx.CreateInBatches(refs, 500).Error; err != nil {
			return xerrors.Errorf("failed to create refs: %w", err)
		}

		// update content
		if err := tx.Model(util.Content{}).Where("id = ?", cont.ID).UpdateColumns(map[string]interface{}{
			"active":   true,
			"size":     contSize,
			"pinning":  false,
			"location": loc,
		}).Error; err != nil {
			return xerrors.Errorf("failed to update content in database: %w", err)
		}

		// if content can be staged, stage it
		if contSize < m.cfg.Content.MinSize {
			return m.stgZoneQueueMgr.QueueContent(cont, tx, false)
		}

		// if it is too large, queue it for splitting.
		// split worker will pick it up and split it,
		// its children will be pinned and dealed
		if contSize > m.cfg.Content.MaxSize {
			return m.splitQueueMgr.QueueContent(cont.ID, cont.UserID, tx)
		}
		// or queue it for deal making
		return m.dealQueueMgr.QueueContent(cont.ID, tx)
	})
}

// shuttle split complete will only update the parent content
// the childrent contents will come in as pincomplete and they will be queued for deal making
func (m *manager) handleRpcSplitComplete(ctx context.Context, handle string, param *rpcevent.SplitComplete) error {
	if param.ID == 0 {
		return fmt.Errorf("split complete send with ID = 0")
	}
	m.splitQueueMgr.SplitComplete(param.ID, m.db)
	return nil
}

func (m *manager) handleRpcSplitFailed(ctx context.Context, handle string, param *rpcevent.SplitFailed) error {
	if param.ID == 0 {
		return fmt.Errorf("split complete send with ID = 0")
	}
	m.splitQueueMgr.SplitFailed(param.ID, m.db)
	return nil
}

func (m *manager) handleRpcCommPComplete(ctx context.Context, handle string, resp *rpcevent.CommPComplete) error {
	_, span := m.tracer.Start(ctx, "handleRpcCommPComplete")
	defer span.End()

	m.commpStatusUpdater.ComputeCompleted(resp.Data, resp.CommP, resp.Size, resp.CarSize)
	return nil
}

func (m *manager) handleRpcCommPFailed(ctx context.Context, handle string, resp *rpcevent.CommPFailed) error {
	_, span := m.tracer.Start(ctx, "handleRpcCommPFailed")
	defer span.End()

	m.commpStatusUpdater.ComputeFailed(resp.Data)
	return nil
}

func (m *manager) handleRpcTransferStarted(ctx context.Context, handle string, param *rpcevent.TransferStartedOrFinished) error {
	if err := m.transferStatusUpdater.SetDataTransferStartedOrFinished(ctx, param.DealDBID, param.Chanid, param.State, true); err != nil {
		return err
	}
	m.log.Debugw("Started data transfer on shuttle", "chanid", param.Chanid, "shuttle", handle)
	return nil
}

func (m *manager) handleRpcTransferFinished(ctx context.Context, handle string, param *rpcevent.TransferStartedOrFinished) error {
	if err := m.transferStatusUpdater.SetDataTransferStartedOrFinished(ctx, param.DealDBID, param.Chanid, param.State, false); err != nil {
		return err
	}
	m.log.Debugw("Finished data transfer on shuttle", "chanid", param.Chanid, "shuttle", handle)
	return nil
}

func (m *manager) HandleRpcTransferStatus(ctx context.Context, handle string, param *rpcevent.TransferStatus) error {
	if param.DealDBID == 0 {
		return fmt.Errorf("received transfer status update with no identifier")
	}

	var cd model.ContentDeal
	if err := m.db.First(&cd, "id = ?", param.DealDBID).Error; err != nil {
		return err
	}

	if cd.DTChan == "" {
		if err := m.db.Model(model.ContentDeal{}).Where("id = ?", param.DealDBID).UpdateColumns(map[string]interface{}{
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

		if oerr := m.dealStatusUpdater.RecordDealFailure(&dealstatus.DealFailureError{
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

		if err := m.db.Model(model.ContentDeal{}).Where("id = ?", cd.ID).UpdateColumns(map[string]interface{}{
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

	m.updateTransferStatus(ctx, handle, cd.ID, param.State)

	return nil
}

func (m *manager) GetTransferStatus(dealID uint) (*filclient.ChannelState, error) {
	val, ok := m.transferStatuses.Get(dealID)
	if !ok {
		return nil, nil
	}

	tsr, ok := val.(*transferStatusRecord)
	if !ok {
		return nil, fmt.Errorf("invalid type placed in remote transfer status cache: %T", val)
	}
	return tsr.State, nil
}

func (m *manager) updateTransferStatus(ctx context.Context, loc string, dealdbid uint, st *filclient.ChannelState) {
	m.transferStatuses.Add(dealdbid, &transferStatusRecord{
		State:    st,
		Shuttle:  loc,
		Received: time.Now(),
	})
}
