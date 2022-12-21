package commp

import (
	"context"
	"fmt"
	"time"

	"github.com/application-research/estuary/config"
	"github.com/application-research/estuary/constants"
	"github.com/application-research/estuary/model"
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
	"gorm.io/gorm/clause"
)

var ErrWaitForRemoteCompute = fmt.Errorf("waiting for remote commP computation")

type IManager interface {
	GetOrRunPieceCommitment(ctx context.Context, data cid.Cid, bs blockstore.Blockstore) (cid.Cid, uint64, abi.UnpaddedPieceSize, error)
	GetPieceCommRecord(data cid.Cid) (*model.PieceCommRecord, error)
	RunPieceCommCompute(ctx context.Context, data cid.Cid, bs blockstore.Blockstore) (cid.Cid, uint64, abi.UnpaddedPieceSize, error)
	RunWorker(ctx context.Context)
}

type manager struct {
	db         *gorm.DB
	cfg        *config.Estuary
	log        *zap.SugaredLogger
	shuttleMgr shuttle.IManager
	tracer     trace.Tracer
}

func NewManager(db *gorm.DB, cfg *config.Estuary, log *zap.SugaredLogger, shuttleMgr shuttle.IManager) IManager {
	return &manager{
		db:         db,
		cfg:        cfg,
		log:        log,
		shuttleMgr: shuttleMgr,
		tracer:     otel.Tracer("commp"),
	}
}

func (m *manager) getSplitTrackerLastContentID() (*model.StagingZoneTracker, error) {
	var trks []*model.StagingZoneTracker
	if err := m.db.Find(&trks).Error; err != nil {
		return nil, err
	}

	if len(trks) == 0 {
		// for the first time it will be empty
		trk := &model.StagingZoneTracker{LastContID: 0}
		if err := m.db.Create(&trk).Error; err != nil {
			return nil, err
		}
		return trk, nil
	}
	return trks[0], nil
}

func (m *manager) RunWorker(ctx context.Context) {
	m.log.Infof("starting up commp worker")

	go m.run(ctx)

	m.log.Infof("spun up commp worker")
}

func (m *manager) run(ctx context.Context) {
	timer := time.NewTicker(m.cfg.StagingBucket.CreationInterval)
	for {
		select {
		case <-timer.C:
			m.log.Debug("running staging zone creation worker")

			// lastContID, err := m.getSplitTrackerLastContentID()
			// if err != nil {
			// 	m.log.Warnf("failed to get staging zone tracker last content id - %s", err)
			// 	continue
			// }

			// if err := m.splitContents(ctx, lastContID); err != nil {
			// 	m.log.Warnf("failed to add contents to staging zones - %s", err)
			// }
		}
	}
}

func (m *manager) GetPieceCommRecord(data cid.Cid) (*model.PieceCommRecord, error) {
	var pcrs []model.PieceCommRecord
	if err := m.db.Find(&pcrs, "data = ?", data.Bytes()).Error; err != nil {
		return nil, err
	}

	if len(pcrs) == 0 {
		return nil, nil
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
		m.log.Infof("calling commmp for cont: %d", cont.ID)
		if err := m.shuttleMgr.CommPContent(ctx, cont.Location, data); err != nil {
			return cid.Undef, 0, 0, err
		}
		return cid.Undef, 0, 0, ErrWaitForRemoteCompute
	}

	m.log.Debugw("computing piece commitment", "data", cont.Cid.CID)
	return filclient.GeneratePieceCommitmentFFI(ctx, data, bs)
}

func (m *manager) GetOrRunPieceCommitment(ctx context.Context, data cid.Cid, bs blockstore.Blockstore) (cid.Cid, uint64, abi.UnpaddedPieceSize, error) {
	_, span := m.tracer.Start(ctx, "getPieceComm")
	defer span.End()

	// Get the piece comm record from the DB
	pcr, err := m.GetPieceCommRecord(data)
	if err != nil {
		return cid.Undef, 0, 0, err
	}

	if pcr != nil {
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

	// The piece comm record isn't in the DB so calculate it
	pc, carSize, size, err := m.RunPieceCommCompute(ctx, data, bs)
	if err != nil {
		return cid.Undef, 0, 0, err
	}

	opcr := model.PieceCommRecord{
		Data:    util.DbCID{CID: data},
		Piece:   util.DbCID{CID: pc},
		CarSize: carSize,
		Size:    size,
	}
	if err := m.db.Clauses(clause.OnConflict{DoNothing: true}).Create(&opcr).Error; err != nil {
		return cid.Undef, 0, 0, err
	}
	return pc, carSize, size, nil
}
