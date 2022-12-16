package contentmgr

import (
	"context"
	"fmt"

	"github.com/application-research/estuary/constants"
	"github.com/application-research/estuary/model"
	"github.com/application-research/estuary/util"
	"github.com/application-research/filclient"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/ipfs/go-cid"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	"golang.org/x/xerrors"
	"gorm.io/gorm/clause"
)

func (cm *ContentManager) lookupPieceCommRecord(data cid.Cid) (*model.PieceCommRecord, error) {
	var pcrs []model.PieceCommRecord
	if err := cm.db.Find(&pcrs, "data = ?", data.Bytes()).Error; err != nil {
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
func (cm *ContentManager) calculateCarSize(ctx context.Context, data cid.Cid) (uint64, error) {
	_, span := cm.tracer.Start(ctx, "calculateCarSize")
	defer span.End()

	var objects []util.Object
	where := "id in (select object from obj_refs where content = (select id from contents where cid = ?))"
	if err := cm.db.Find(&objects, where, data.Bytes()).Error; err != nil {
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

var ErrWaitForRemoteCompute = fmt.Errorf("waiting for remote commP computation")

func (cm *ContentManager) runPieceCommCompute(ctx context.Context, data cid.Cid, bs blockstore.Blockstore) (cid.Cid, uint64, abi.UnpaddedPieceSize, error) {
	var cont util.Content
	if err := cm.db.First(&cont, "cid = ?", data.Bytes()).Error; err != nil {
		return cid.Undef, 0, 0, err
	}

	if cont.Location != constants.ContentLocationLocal {
		cm.log.Infof("calling commmp for cont: %d", cont.ID)
		if err := cm.shuttleMgr.CommPContent(ctx, cont.Location, data); err != nil {
			return cid.Undef, 0, 0, err
		}
		return cid.Undef, 0, 0, ErrWaitForRemoteCompute
	}

	cm.log.Debugw("computing piece commitment", "data", cont.Cid.CID)
	return filclient.GeneratePieceCommitmentFFI(ctx, data, bs)
}

func (cm *ContentManager) GetPieceCommitment(ctx context.Context, data cid.Cid, bs blockstore.Blockstore) (cid.Cid, uint64, abi.UnpaddedPieceSize, error) {
	_, span := cm.tracer.Start(ctx, "getPieceComm")
	defer span.End()

	// Get the piece comm record from the DB
	pcr, err := cm.lookupPieceCommRecord(data)
	if err != nil {
		return cid.Undef, 0, 0, err
	}
	if pcr != nil {
		if pcr.CarSize > 0 {
			return pcr.Piece.CID, pcr.CarSize, pcr.Size, nil
		}

		// The CAR size field was added later, so if it's not on the piece comm
		// record, calculate it
		carSize, err := cm.calculateCarSize(ctx, data)
		if err != nil {
			return cid.Undef, 0, 0, xerrors.Errorf("failed to calculate CAR size: %w", err)
		}

		pcr.CarSize = carSize

		// Save updated car size to DB
		cm.db.Model(model.PieceCommRecord{}).Where("piece = ?", pcr.Piece).UpdateColumns(map[string]interface{}{
			"car_size": carSize,
		}) //nolint:errcheck

		return pcr.Piece.CID, pcr.CarSize, pcr.Size, nil
	}

	// The piece comm record isn't in the DB so calculate it
	pc, carSize, size, err := cm.runPieceCommCompute(ctx, data, bs)
	if err != nil {
		return cid.Undef, 0, 0, err
	}

	opcr := model.PieceCommRecord{
		Data:    util.DbCID{CID: data},
		Piece:   util.DbCID{CID: pc},
		CarSize: carSize,
		Size:    size,
	}

	if err := cm.db.Clauses(clause.OnConflict{DoNothing: true}).Create(&opcr).Error; err != nil {
		return cid.Undef, 0, 0, err
	}
	return pc, carSize, size, nil
}
