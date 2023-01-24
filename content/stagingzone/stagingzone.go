package stagingzone

import (
	"context"

	"github.com/application-research/estuary/config"
	creation "github.com/application-research/estuary/content/stagingzone/creation"
	dealqueuemgr "github.com/application-research/estuary/deal/queue"

	"github.com/application-research/estuary/node"
	pinningtypes "github.com/application-research/estuary/pinner/types"
	shuttle "github.com/application-research/estuary/shuttle"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"

	"github.com/application-research/estuary/model"
	"github.com/application-research/estuary/util"
	ipld "github.com/ipfs/go-ipld-format"
	"github.com/pkg/errors"
	"golang.org/x/xerrors"
	"gorm.io/gorm"
)

type IManager interface {
	CreateAggregate(ctx context.Context, conts []util.Content) (ipld.Node, error)
	AggregateStagingZone(ctx context.Context, zone *model.StagingZone, zoneCont util.Content, loc string) error
	GetStagingZoneContents(ctx context.Context, user uint, zoneID uint, limit int, offset int) ([]util.Content, error)
	GetStagingZoneWithoutContents(ctx context.Context, userID uint, zoneID uint) (*model.StagingZone, error)
	GetStagingZonesForUser(ctx context.Context, userID uint, limit int, offset int) ([]*model.StagingZone, error)
}

type manager struct {
	db                 *gorm.DB
	blockstore         node.EstuaryBlockstore
	node               *node.Node
	cfg                *config.Estuary
	log                *zap.SugaredLogger
	shuttleMgr         shuttle.IManager
	tracer             trace.Tracer
	zoneCreationWorker creation.IManager
	dealQueueMgr       dealqueuemgr.IManager
}

func NewManager(
	ctx context.Context,
	db *gorm.DB,
	tbs *util.TrackingBlockstore,
	nd *node.Node,
	cfg *config.Estuary,
	log *zap.SugaredLogger,
	shuttleMgr shuttle.IManager,
) IManager {
	m := &manager{
		db:                 db,
		blockstore:         tbs.Under().(node.EstuaryBlockstore),
		node:               nd,
		cfg:                cfg,
		log:                log,
		shuttleMgr:         shuttleMgr,
		tracer:             otel.Tracer("stagingzone"),
		zoneCreationWorker: creation.NewManager(db, cfg, log),
		dealQueueMgr:       dealqueuemgr.NewManager(db, cfg, log),
	}

	m.runWorkers(ctx)
	return m
}

func (m *manager) runWorkers(ctx context.Context) {
	// if staging zone is enabled, run the workers
	if m.cfg.StagingBucket.Enabled {
		m.log.Infof("starting up staging zone workers")

		// run staging zone backfill worker
		go m.zoneCreationWorker.RunBackFillWorker(ctx)

		// run staging zone aggregation/consoliation worker
		go m.runAggregationWorker(ctx)

		m.log.Infof("spun up staging zone workers")
	}
}

func (m *manager) GetStagingZonesForUser(ctx context.Context, userID uint, limit int, offset int) ([]*model.StagingZone, error) {
	var zones []*model.StagingZone
	if err := m.db.Limit(limit).Offset(offset).Order("created_at desc").Find(&zones, "user_id = ?", userID).Error; err != nil {
		return nil, err
	}
	return zones, nil
}

func (m *manager) GetStagingZoneWithoutContents(ctx context.Context, userID uint, zoneID uint) (*model.StagingZone, error) {
	var zone *model.StagingZone
	if err := m.db.First(&zone, "id = ? and user_id = ?", zoneID, userID).Error; err != nil {
		return nil, errors.Wrapf(err, "zone not found or does not belong to user: %d", zoneID)
	}
	return zone, nil
}

func (m *manager) GetStagingZoneContents(ctx context.Context, user uint, zoneID uint, limit int, offset int) ([]util.Content, error) {
	var zc model.StagingZone
	if err := m.db.First(&zc, "id = ?", zoneID).Error; err != nil {
		if xerrors.Is(err, gorm.ErrRecordNotFound) {
			return make([]util.Content, 0), nil
		}
		return nil, err
	}

	var zoneConts []util.Content
	if err := m.db.Limit(limit).Offset(offset).Order("created_at desc").Find(&zoneConts, "active and user_id = ? and aggregated_in = ?", user, zc.ContID).Error; err != nil {
		return nil, errors.Wrapf(err, "could not get contents for staging zone: %d", zc.ID)
	}

	for i, c := range zoneConts {
		zoneConts[i].PinningStatus = string(pinningtypes.GetContentPinningStatus(c))
	}
	return zoneConts, nil
}
