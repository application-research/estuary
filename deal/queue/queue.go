package queue

import (
	"time"

	"github.com/application-research/estuary/config"
	"github.com/application-research/estuary/model"
	"github.com/application-research/estuary/util"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type IManager interface {
	QueueContent(cont *util.Content) error
	DealComplete(contID uint64)
	DealFailed(contID uint64)
	DealCheckComplete(contID uint64, dealsToBeMade int)
	DealCheckFailed(contID uint64)
}

type manager struct {
	db     *gorm.DB
	cfg    *config.Estuary
	log    *zap.SugaredLogger
	tracer trace.Tracer
}

func NewManager(db *gorm.DB, cfg *config.Estuary, log *zap.SugaredLogger) IManager {
	return &manager{
		db:     db,
		cfg:    cfg,
		log:    log,
		tracer: otel.Tracer("deal"),
	}
}

func (m *manager) QueueContent(cont *util.Content) error {
	// if the content is not active or is in pinning state, do not proceed
	if !cont.Active || cont.Pinning {
		return nil
	}

	// If this content is aggregated inside another piece of content, nothing to do here, that content will be processed
	if cont.AggregatedIn > 0 {
		return nil
	}

	// If this is the 'root' of a dag split, we dont need to process it, as the splits will be processed instead
	if cont.DagSplit && cont.SplitFrom == 0 {
		return nil
	}

	// only queue content with dealable size
	if cont.Size < m.cfg.Content.MinSize {
		return nil
	}

	task := &model.DealQueue{
		UserID:                 cont.UserID,
		ContID:                 cont.ID,
		ContCID:                cont.Cid,
		CommpDone:              false, //it will be set by commp worker
		CommpAttempted:         0,     //it will be set by commp worker
		CommpNextAttemptAt:     time.Now().UTC(),
		CanDeal:                false, // it will be set by deal checker worker
		DealCount:              0,     // it will be set by deal checker worker
		DealCheckNextAttemptAt: time.Now().UTC(),
		DealNextAttemptAt:      time.Now().UTC(),
	}
	return m.db.Clauses(clause.OnConflict{DoNothing: true}).Create(&task).Error
}

func (m *manager) DealComplete(contID uint64) {
	if err := m.db.Model(model.DealQueue{}).Where("id = ?", contID).UpdateColumns(map[string]interface{}{
		"can_deal":   false,
		"deal_count": 0,
	}).Error; err != nil {
		m.log.Errorf("failed to update deal queue (DealComplete) for cont %d - %s", contID, err)
	}
}

func (m *manager) DealCheckComplete(contID uint64, dealsToBeMade int) {
	if err := m.db.Model(model.DealQueue{}).Where("id = ?", contID).UpdateColumns(map[string]interface{}{
		"can_deal":                    true,
		"deals_count":                 dealsToBeMade,
		"deals_check_next_attempt_at": time.Now().Add(72 * time.Hour).UTC(),
	}).Error; err != nil {
		m.log.Errorf("failed to update deal queue (DealCheckComplete) for cont %d - %s", contID, err)
	}
}

func (m *manager) DealFailed(contID uint64) {
	if err := m.db.Model(model.DealQueue{}).Where("id = ?", contID).UpdateColumns(map[string]interface{}{
		"deal_next_attempt_at": time.Now().Add(1 * time.Hour).UTC(),
	}).Error; err != nil {
		m.log.Errorf("failed to update deal queue (DealFailed) for cont %d - %s", contID, err)
	}
}

func (m *manager) DealCheckFailed(contID uint64) {
	if err := m.db.Model(model.DealQueue{}).Where("id = ?", contID).UpdateColumns(map[string]interface{}{
		"deals_check_next_attempt_at": time.Now().Add(1 * time.Hour).UTC(),
	}).Error; err != nil {
		m.log.Errorf("failed to update deal queue (DealCheckFailed) for cont %d - %s", contID, err)
	}
}
