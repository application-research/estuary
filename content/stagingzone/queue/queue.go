package queue

import (
	"time"

	"github.com/application-research/estuary/model"
	"github.com/application-research/estuary/util"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type IManager interface {
	QueueContent(cont *util.Content, tx *gorm.DB, isBackfilled bool) error
	StageComplete(contID uint64, tx *gorm.DB) error
	StageFailed(contID uint64, tx *gorm.DB) error
}

type manager struct {
	log    *zap.SugaredLogger
	tracer trace.Tracer
}

func NewManager(log *zap.SugaredLogger) IManager {
	return &manager{
		log:    log,
		tracer: otel.Tracer("deal"),
	}
}

func (m *manager) QueueContent(cont *util.Content, tx *gorm.DB, isBackfilled bool) error {
	m.log.Debugf("adding cont: %d to staging zone queue", cont.ID)

	task := &model.StagingZoneQueue{
		UserID:        uint64(cont.UserID),
		ContID:        cont.ID,
		NextAttemptAt: time.Now().UTC(),
		IsBackFilled:  isBackfilled,
	}
	return tx.Clauses(clause.OnConflict{DoNothing: true}).Create(&task).Error
}

func (m *manager) StageComplete(contID uint64, tx *gorm.DB) error {
	m.log.Debugf("cont: %d staged successfully", contID)
	return tx.Unscoped().Delete(&model.StagingZoneQueue{}, "cont_id = ?", contID).Error // delete permanently
}

func (m *manager) StageFailed(contID uint64, tx *gorm.DB) error {
	return tx.Exec("UPDATE staging_zone_queues SET failing = ?, next_attempt_at = ? WHERE cont_id = ?", true, time.Now().Add(1*time.Hour), contID).Error
}
