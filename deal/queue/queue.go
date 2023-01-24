package dealqueue

import (
	"context"

	"github.com/application-research/estuary/config"
	"github.com/application-research/estuary/util"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

type IManager interface {
	QueueContent(ctx context.Context, contID uint64, contCID util.DbCID, userID uint) error
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

func (m *manager) QueueContent(ctx context.Context, contID uint64, contCID util.DbCID, userID uint) error {

	// // if the content is not active or is in pinning state, do not proceed
	// if !content.Active || content.Pinning {
	// 	return nil
	// }

	// // If this content is aggregated inside another piece of content, nothing to do here, that content will be processed
	// if content.AggregatedIn > 0 {
	// 	return nil
	// }

	// // If this is the 'root' of a dag split, we dont need to process it, as the splits will be processed instead
	// if content.DagSplit && content.SplitFrom == 0 {
	// 	return nil
	// }
	return nil
}
