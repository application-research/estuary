package dealstatus

import (
	"go.uber.org/zap"
	"gorm.io/gorm"
)

type IUpdater interface {
	RecordDealFailure(dfe *DealFailureError) error
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

func (up *updater) RecordDealFailure(dfe *DealFailureError) error {
	up.log.Debugw("deal failure error", "miner", dfe.Miner, "uuid", dfe.DealUUID, "phase", dfe.Phase, "msg", dfe.Message, "content", dfe.Content)
	rec := dfe.record()
	return up.db.Create(rec).Error
}
