package commpstatus

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
	ComputeCompleted(data cid.Cid, piece cid.Cid, size abi.UnpaddedPieceSize, carSize uint64) error
	ComputeFailed(cid cid.Cid) error
	ComputeRequested(cid cid.Cid) error
	CommpExist(data cid.Cid) error
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

func (up *updater) ComputeCompleted(data cid.Cid, piece cid.Cid, size abi.UnpaddedPieceSize, carSize uint64) error {
	return up.db.Transaction(func(tx *gorm.DB) error {
		opcr := model.PieceCommRecord{
			Data:    util.DbCID{CID: data},
			Piece:   util.DbCID{CID: piece},
			Size:    size,
			CarSize: carSize,
		}
		if err := tx.Clauses(clause.OnConflict{DoNothing: true}).Create(&opcr).Error; err != nil {
			return err
		}
		return tx.Exec("UPDATE deal_queue SET commp_attempted = commp_attempted + 1, commp_failing = ?, commp_done = ? WHERE cont_cid = ?", false, true, data).Error
	})
}

func (up *updater) ComputeFailed(data cid.Cid) error {
	return up.db.Exec("UPDATE deal_queue SET commp_attempted = commp_attempted + 1, commp_failing = ?, commp_next_attempt_at = ? WHERE cont_cid = ?", true, time.Now().Add(1*time.Hour), data).Error
}

func (up *updater) ComputeRequested(data cid.Cid) error {
	return up.db.Exec("UPDATE deal_queue SET commp_next_attempt_at = ? WHERE cont_cid = ?", time.Now().Add(1*time.Hour), data).Error
}

func (up *updater) CommpExist(data cid.Cid) error {
	return up.db.Exec("UPDATE deal_queue SET commp_attempted = commp_attempted + 1, commp_failing = ?, commp_done = ? WHERE cont_cid = ?", false, true, data).Error
}
