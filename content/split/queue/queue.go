package queue

import (
	"fmt"
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
	QueueContent(contID uint64, userID uint) error
	SplitComplete(contID uint64) error
	SplitFailed(contID uint64) error
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

func (m *manager) QueueContent(contID uint64, userID uint) error {
	task := &model.SplitQueue{
		UserID:        uint64(userID),
		ContID:        contID,
		Failing:       false,
		Attempted:     0,
		NextAttemptAt: time.Now().UTC(),
	}
	return m.db.Clauses(clause.OnConflict{DoNothing: true}).Create(&task).Error
}

func (m *manager) SplitComplete(contID uint64) error {
	return m.db.Transaction(func(tx *gorm.DB) error {
		if err := tx.Model(util.Content{}).Where("id = ?", contID).UpdateColumns(map[string]interface{}{
			"dag_split": true,
			"active":    false,
			"size":      0,
			"pinning":   false,
		}).Error; err != nil {
			return fmt.Errorf("failed to update content for split complete - %w", err)
		}

		if err := tx.Delete(&util.ObjRef{}, "content = ?", contID).Error; err != nil {
			return fmt.Errorf("failed to delete object references for newly split object: %w", err)
		}
		return tx.Delete(&model.SplitQueue{}, "cont_id = ?", contID).Error
	})
}

func (m *manager) SplitFailed(contID uint64) error {
	return m.db.Exec("UPDATE split_queue SET attempted = attempted + 1, failing = ?, done = ?, next_attempt_at = ? WHERE cont_id = ?", true, false, time.Now().Add(1*time.Hour), contID).Error
}
