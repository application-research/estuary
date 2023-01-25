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
	QueueContent(cont *util.Content, isBackfilled bool) error
	StageComplete(contID uint64) error
	StageFailed(contID uint64) error
}

type manager struct {
	db     *gorm.DB
	log    *zap.SugaredLogger
	tracer trace.Tracer
}

func NewManager(db *gorm.DB, log *zap.SugaredLogger) IManager {
	return &manager{
		db:     db,
		log:    log,
		tracer: otel.Tracer("deal"),
	}
}

func (m *manager) QueueContent(cont *util.Content, isBackfilled bool) error {
	task := &model.StagingZoneQueue{
		UserID:        uint64(cont.UserID),
		ContID:        cont.ID,
		NextAttemptAt: time.Now().UTC(),
		IsBackFilled:  isBackfilled,
	}
	return m.db.Clauses(clause.OnConflict{DoNothing: true}).Create(&task).Error
}

func (m *manager) StageComplete(contID uint64) error {
	return m.db.Delete(&model.StagingZoneQueue{}, "cont_id = ?", contID).Error
}

func (m *manager) StageFailed(contID uint64) error {
	return m.db.Exec("UPDATE staging_zone_queue SET failing = ?, next_attempt_at = ? WHERE cont_id = ?", true, time.Now().Add(1*time.Hour), contID).Error
}
