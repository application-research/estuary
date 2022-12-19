package contentmgr

import (
	"sync"
	"time"

	"github.com/application-research/estuary/config"
	contentqueue "github.com/application-research/estuary/content/queue"
	"github.com/application-research/estuary/deal/transfer"
	"github.com/application-research/estuary/miner"
	"github.com/application-research/estuary/node"
	"github.com/application-research/estuary/shuttle"
	"github.com/application-research/estuary/util"
	"github.com/application-research/filclient"
	"github.com/filecoin-project/lotus/api"
	lru "github.com/hashicorp/golang-lru"
	"github.com/ipfs/go-cid"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
	"golang.org/x/xerrors"
	"gorm.io/gorm"
)

type ContentManager struct {
	db                   *gorm.DB
	api                  api.Gateway
	filClient            *filclient.FilClient
	node                 *node.Node
	cfg                  *config.Estuary
	tracer               trace.Tracer
	blockstore           node.EstuaryBlockstore
	notifyBlockstore     *node.NotifyBlockstore
	queueMgr             contentqueue.IQueueManager
	retrLk               sync.Mutex
	retrievalsInProgress map[uint]*util.RetrievalProgress
	contentLk            sync.RWMutex

	dealDisabledLk       sync.Mutex
	isDealMakingDisabled bool

	shuttleMgr           shuttle.IManager
	remoteTransferStatus *lru.ARCCache
	inflightCids         map[cid.Cid]uint
	inflightCidsLk       sync.Mutex
	minerManager         miner.IMinerManager
	log                  *zap.SugaredLogger
	transferMgr          transfer.IManager
}

func NewContentManager(
	db *gorm.DB,
	api api.Gateway,
	fc *filclient.FilClient,
	tbs *util.TrackingBlockstore,
	nd *node.Node,
	cfg *config.Estuary,
	minerManager miner.IMinerManager,
	log *zap.SugaredLogger,
	shuttleMgr shuttle.IManager,
	transferMgr transfer.IManager,
	queueMgr contentqueue.IQueueManager,
) (*ContentManager, error) {
	cache, err := lru.NewARC(50000)
	if err != nil {
		return nil, err
	}

	cm := &ContentManager{
		cfg:                  cfg,
		db:                   db,
		api:                  api,
		filClient:            fc,
		blockstore:           tbs.Under().(node.EstuaryBlockstore),
		node:                 nd,
		notifyBlockstore:     nd.NotifBlockstore,
		retrievalsInProgress: make(map[uint]*util.RetrievalProgress),
		remoteTransferStatus: cache,
		shuttleMgr:           shuttleMgr,
		inflightCids:         make(map[cid.Cid]uint),
		isDealMakingDisabled: cfg.Deal.IsDisabled,
		tracer:               otel.Tracer("replicator"),
		minerManager:         minerManager,
		log:                  log,
		transferMgr:          transferMgr,
		queueMgr:             queueMgr,
	}
	return cm, nil
}

func (cm *ContentManager) rebuildToCheckQueue() error {
	cm.log.Info("rebuilding contents queue .......")

	var allcontent []util.Content
	// select only active and not staged contents
	if err := cm.db.Where("active and aggregated_in = 0").FindInBatches(&allcontent, util.DefaultBatchSize, func(tx *gorm.DB, batch int) error {
		for i, c := range allcontent {
			// every 100 contents re-queued, wait 5 seconds to avoid over-saturating queues
			// time to requeue all: 10m / 100 * 5 seconds = 5.78 days
			if i%100 == 0 {
				time.Sleep(time.Second * 5)
			}
			cm.queueMgr.ToCheck(c.ID, c.Size)
		}
		return nil
	}).Error; err != nil {
		return xerrors.Errorf("finding all content in database: %w", err)
	}
	return nil
}

func (cm *ContentManager) ToCheck(contID uint, contSize int64) {
	cm.queueMgr.ToCheck(contID, contSize)
}
