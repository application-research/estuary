package status

import (
	"time"

	"github.com/application-research/estuary/model"
	"github.com/application-research/estuary/util"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/ipfs/go-cid"
	"go.uber.org/zap"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type IUpdater interface {
	ComputeCompleted(data cid.Cid, piece cid.Cid, size abi.UnpaddedPieceSize, carSize uint64)
	ComputeFailed(cid cid.Cid)
	ComputeRequested(cid cid.Cid)
	CommpExist(data cid.Cid)
}

type updater struct {
	db  *gorm.DB
	log *zap.SugaredLogger
}

func NewUpdater(db *gorm.DB, log *zap.SugaredLogger) IUpdater {
	return &updater{
		db:  db,
		log: log,
	}
}

func (up *updater) ComputeCompleted(data cid.Cid, piece cid.Cid, size abi.UnpaddedPieceSize, carSize uint64) {
	if err := up.db.Transaction(func(tx *gorm.DB) error {
		opcr := model.PieceCommRecord{
			Data:    util.DbCID{CID: data},
			Piece:   util.DbCID{CID: piece},
			Size:    size,
			CarSize: carSize,
		}
		if err := tx.Clauses(clause.OnConflict{DoNothing: true}).Create(&opcr).Error; err != nil {
			return err
		}
		return tx.Exec("UPDATE deal_queues SET commp_attempted = commp_attempted + 1, commp_failing = ?, commp_done = ? WHERE cont_cid = ?", false, true, data).Error
	}); err != nil {
		up.log.Errorf("failed to update deal queue (ComputeCompleted) for cid %d - %s", data, err)
	}
}

func (up *updater) ComputeFailed(data cid.Cid) {
	if err := up.db.Exec("UPDATE deal_queues SET commp_attempted = commp_attempted + 1, commp_failing = ?, commp_next_attempt_at = ? WHERE cont_cid = ?", true, time.Now().Add(1*time.Hour), data).Error; err != nil {
		up.log.Errorf("failed to update deal queue (ComputeFailed) for cid %d - %s", data, err)
	}
}

func (up *updater) ComputeRequested(data cid.Cid) {
	if err := up.db.Exec("UPDATE deal_queues SET commp_next_attempt_at = ? WHERE cont_cid = ?", time.Now().Add(1*time.Hour), data).Error; err != nil {
		up.log.Errorf("failed to update deal queue (ComputeRequested) for cid %d - %s", data, err)
	}
}

func (up *updater) CommpExist(data cid.Cid) {
	if err := up.db.Exec("UPDATE deal_queues SET commp_attempted = commp_attempted + 1, commp_failing = ?, commp_done = ? WHERE cont_cid = ?", false, true, data).Error; err != nil {
		up.log.Errorf("failed to update deal queue (CommpExist) for cid %d - %s", data, err)
	}
}
