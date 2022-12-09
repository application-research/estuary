package status

import (
	"go.uber.org/zap"
	"gorm.io/gorm"
)

type Updater struct {
	db  *gorm.DB
	log *zap.SugaredLogger
}

func NewUpdater(db *gorm.DB, log *zap.SugaredLogger) *Updater {
	return &Updater{
		db:  db,
		log: log,
	}
}

func (cm *Updater) RecordDealFailure(dfe *DealFailureError) error {
	cm.log.Debugw("deal failure error", "miner", dfe.Miner, "uuid", dfe.DealUUID, "phase", dfe.Phase, "msg", dfe.Message, "content", dfe.Content)
	rec := dfe.record()
	return cm.db.Create(rec).Error
}
