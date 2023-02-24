package contentmgr

import (
	"context"
	"sync"

	"github.com/application-research/estuary/util"
	"github.com/ipfs/go-cid"
	"go.opentelemetry.io/otel/trace"
	"gorm.io/gorm"

	"github.com/application-research/estuary/config"

	"github.com/application-research/estuary/node"
	"github.com/application-research/estuary/shuttle"
	"github.com/application-research/filclient"
	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-fil-markets/retrievalmarket"
	blocks "github.com/ipfs/go-block-format"

	"go.opentelemetry.io/otel"
	"go.uber.org/zap"
)

type IManager interface {
	GarbageCollect(ctx context.Context) error
	RemoveContent(ctx context.Context, contID uint, now bool) error
	RefreshContent(ctx context.Context, cont uint64) error
	OffloadContents(ctx context.Context, conts []uint64) (int, error)
	ClearUnused(ctx context.Context, spaceRequest int64, loc string, users []uint, dryrun bool) (*collectionResult, error)
	GetRemovalCandidates(ctx context.Context, all bool, loc string, users []uint) ([]removalCandidateInfo, error)
	UnpinContent(ctx context.Context, contid uint) error
	GetContent(id uint64) (*util.Content, error)
	TryRetrieve(ctx context.Context, maddr address.Address, c cid.Cid, ask *retrievalmarket.QueryResponse) error
	RecordRetrievalFailure(rfr *util.RetrievalFailureRecord) error
	RefreshContentForCid(ctx context.Context, c cid.Cid) (blocks.Block, error)
}

type manager struct {
	db                   *gorm.DB
	fc                   *filclient.FilClient
	blockstore           node.EstuaryBlockstore
	node                 *node.Node
	cfg                  *config.Estuary
	log                  *zap.SugaredLogger
	shuttleMgr           shuttle.IManager
	tracer               trace.Tracer
	notifyBlockstore     *node.NotifyBlockstore
	retrLk               sync.Mutex
	retrievalsInProgress map[uint64]*util.RetrievalProgress
	contentLk            sync.RWMutex
	inflightCids         map[cid.Cid]uint
	inflightCidsLk       sync.Mutex
}

func NewManager(
	db *gorm.DB,
	fc *filclient.FilClient,
	tbs *util.TrackingBlockstore,
	nd *node.Node,
	cfg *config.Estuary,
	log *zap.SugaredLogger,
	shuttleMgr shuttle.IManager,
) IManager {
	return &manager{
		db:                   db,
		fc:                   fc,
		blockstore:           tbs.Under().(node.EstuaryBlockstore),
		node:                 nd,
		cfg:                  cfg,
		log:                  log,
		shuttleMgr:           shuttleMgr,
		tracer:               otel.Tracer("content"),
		notifyBlockstore:     nd.NotifBlockstore,
		retrievalsInProgress: make(map[uint64]*util.RetrievalProgress),
		inflightCids:         make(map[cid.Cid]uint),
	}
}

func (m *manager) GetContent(id uint64) (*util.Content, error) {
	var content util.Content
	if err := m.db.First(&content, "id = ?", id).Error; err != nil {
		return nil, err
	}
	return &content, nil
}
