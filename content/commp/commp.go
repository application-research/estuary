package commp

import (
	"context"
	"fmt"

	"github.com/application-research/estuary/config"
	"github.com/application-research/estuary/constants"
	commpstatus "github.com/application-research/estuary/content/commp/status"
	"github.com/application-research/estuary/model"

	"github.com/application-research/estuary/node"
	"github.com/application-research/estuary/shuttle"
	"github.com/application-research/estuary/util"
	"github.com/application-research/filclient"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/ipfs/go-cid"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
	"golang.org/x/xerrors"
	"gorm.io/gorm"
)

var ErrWaitForRemoteCompute = fmt.Errorf("waiting for remote commP computation")

type IManager interface {
	GetPieceCommitment(ctx context.Context, data cid.Cid, bs blockstore.Blockstore) (cid.Cid, uint64, abi.UnpaddedPieceSize, error)
	RunPieceCommCompute(ctx context.Context, data cid.Cid, bs blockstore.Blockstore) (cid.Cid, uint64, abi.UnpaddedPieceSize, error)
}

type manager struct {
	db                 *gorm.DB
	cfg                *config.Estuary
	log                *zap.SugaredLogger
	shuttleMgr         shuttle.IManager
	tracer             trace.Tracer
	blockstore         node.EstuaryBlockstore
	commpStatusUpdater commpstatus.IUpdater
}

func NewManager(ctx context.Context, db *gorm.DB, cfg *config.Estuary, log *zap.SugaredLogger, shuttleMgr shuttle.IManager, tbs *util.TrackingBlockstore) IManager {
	m := &manager{
		db:                 db,
		cfg:                cfg,
		log:                log,
		shuttleMgr:         shuttleMgr,
		tracer:             otel.Tracer("commp"),
		blockstore:         tbs.Under().(node.EstuaryBlockstore),
		commpStatusUpdater: commpstatus.NewUpdater(db, log),
	}

	m.runWorker(ctx)
	return m
}

func (m *manager) getPieceCommRecord(data cid.Cid) (*model.PieceCommRecord, error) {
	var pcrs []model.PieceCommRecord
	if err := m.db.Find(&pcrs, "data = ?", data.Bytes()).Error; err != nil {
		return nil, err
	}

	if len(pcrs) == 0 {
		return nil, gorm.ErrRecordNotFound
	}

	pcr := pcrs[0]
	if !pcr.Piece.CID.Defined() {
		return nil, fmt.Errorf("got an undefined thing back from database")
	}
	return &pcr, nil
}

// calculateCarSize works out the CAR size using the cids and block sizes
// for the content stored in the DB
func (m *manager) calculateCarSize(ctx context.Context, data cid.Cid) (uint64, error) {
	_, span := m.tracer.Start(ctx, "calculateCarSize")
	defer span.End()

	var objects []util.Object
	where := "id in (select object from obj_refs where content = (select id from contents where cid = ?))"
	if err := m.db.Find(&objects, where, data.Bytes()).Error; err != nil {
		return 0, err
	}

	if len(objects) == 0 {
		return 0, fmt.Errorf("not found")
	}

	os := make([]util.CarObject, len(objects))
	for i, o := range objects {
		os[i] = util.CarObject{Size: uint64(o.Size), Cid: o.Cid.CID}
	}
	return util.CalculateCarSize(data, os)
}

func (m *manager) RunPieceCommCompute(ctx context.Context, data cid.Cid, bs blockstore.Blockstore) (cid.Cid, uint64, abi.UnpaddedPieceSize, error) {
	var cont util.Content
	if err := m.db.First(&cont, "cid = ?", data.Bytes()).Error; err != nil {
		return cid.Undef, 0, 0, err
	}

	if cont.Location != constants.ContentLocationLocal {
		m.log.Debugf("requesting commmp for cont: %d from shuttle: %s", cont.ID, cont.Location)
		if err := m.shuttleMgr.CommPContent(ctx, cont.Location, data); err != nil {
			return cid.Undef, 0, 0, err
		}
		return cid.Undef, 0, 0, ErrWaitForRemoteCompute
	}

	m.log.Debugw("computing piece commitment", "data", cont.Cid.CID)

	pc, carSize, size, err := filclient.GeneratePieceCommitmentFFI(ctx, data, bs)
	if err != nil {
		return cid.Undef, 0, 0, err
	}
	return pc, carSize, size, nil
}

func (m *manager) GetPieceCommitment(ctx context.Context, data cid.Cid, bs blockstore.Blockstore) (cid.Cid, uint64, abi.UnpaddedPieceSize, error) {
	_, span := m.tracer.Start(ctx, "getPieceComm")
	defer span.End()

	// Get the piece comm record from the DB
	pcr, err := m.getPieceCommRecord(data)
	if err != nil {
		return cid.Undef, 0, 0, err
	}

	if pcr.CarSize > 0 {
		return pcr.Piece.CID, pcr.CarSize, pcr.Size, nil
	}

	// The CAR size field was added later, so if it's not on the piece comm
	// record, calculate it
	carSize, err := m.calculateCarSize(ctx, data)
	if err != nil {
		return cid.Undef, 0, 0, xerrors.Errorf("failed to calculate CAR size: %w", err)
	}
	pcr.CarSize = carSize

	// Save updated car size to DB
	if err = m.db.Model(model.PieceCommRecord{}).Where("piece = ?", pcr.Piece).UpdateColumns(map[string]interface{}{
		"car_size": carSize,
	}).Error; err != nil {
		return cid.Undef, 0, 0, err
	}
	return pcr.Piece.CID, pcr.CarSize, pcr.Size, nil

}
