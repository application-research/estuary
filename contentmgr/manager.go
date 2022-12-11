package contentmgr

import (
	"sync"
	"time"

	"github.com/application-research/estuary/config"
	contentqueue "github.com/application-research/estuary/contentmgr/queue"
	"github.com/application-research/estuary/drpc"
	"github.com/application-research/estuary/miner"
	"github.com/application-research/estuary/node"
	"golang.org/x/xerrors"

	"github.com/application-research/estuary/util"
	"github.com/application-research/filclient"
	datatransfer "github.com/filecoin-project/go-data-transfer"
	"github.com/filecoin-project/lotus/api"
	lru "github.com/hashicorp/golang-lru"
	"github.com/ipfs/go-cid"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
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
	ShuttlesLk           sync.Mutex
	Shuttles             map[string]*ShuttleConnection
	remoteTransferStatus *lru.ARCCache
	inflightCids         map[cid.Cid]uint
	inflightCidsLk       sync.Mutex
	IncomingRPCMessages  chan *drpc.Message
	minerManager         miner.IMinerManager
	log                  *zap.SugaredLogger

	// consolidatingZonesLk is used to serialize reads and writes to consolidatingZones
	consolidatingZonesLk sync.Mutex
	// aggregatingZonesLk is used to serialize reads and writes to aggregatingZones
	aggregatingZonesLk sync.Mutex
	// addStagingContentLk is used to serialize content adds to staging zones
	// otherwise, we'd risk creating multiple "initial" staging zones, or exceeding MaxDealContentSize
	addStagingContentLk sync.Mutex

	consolidatingZones map[uint]bool
	aggregatingZones   map[uint]bool

	// TODO move out to filc package
	TcLk             sync.Mutex
	TrackingChannels map[string]*util.ChanTrack
}

func NewContentManager(db *gorm.DB, api api.Gateway, fc *filclient.FilClient, tbs *util.TrackingBlockstore, nd *node.Node, cfg *config.Estuary, minerManager miner.IMinerManager, log *zap.SugaredLogger) (*ContentManager, contentqueue.IQueueManager, error) {
	cache, err := lru.NewARC(50000)
	if err != nil {
		return nil, nil, err
	}

	queueMgr := contentqueue.NewQueueManager(cfg.DisableFilecoinStorage)

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
		Shuttles:             make(map[string]*ShuttleConnection),
		inflightCids:         make(map[cid.Cid]uint),
		isDealMakingDisabled: cfg.Deal.IsDisabled,
		tracer:               otel.Tracer("replicator"),
		IncomingRPCMessages:  make(chan *drpc.Message, cfg.RPCMessage.IncomingQueueSize),
		minerManager:         minerManager,
		log:                  log,
		consolidatingZones:   make(map[uint]bool),
		aggregatingZones:     make(map[uint]bool),

		queueMgr: queueMgr,

		// TODO move out to filc package
		TrackingChannels: make(map[string]*util.ChanTrack),
	}
	return cm, queueMgr, nil
}

// TODO move out to filc package
func (cm *ContentManager) TrackTransfer(chanid *datatransfer.ChannelID, dealdbid uint, st *filclient.ChannelState) {
	cm.TcLk.Lock()
	defer cm.TcLk.Unlock()

	cm.TrackingChannels[chanid.String()] = &util.ChanTrack{
		Dbid: dealdbid,
		Last: st,
	}
}

func (cm *ContentManager) rebuildToCheckQueue() error {
	cm.log.Info("rebuilding contents queue .......")

	var allcontent []util.Content
	if err := cm.db.Where("active AND NOT aggregated_in > 0").FindInBatches(&allcontent, util.DefaultBatchSize, func(tx *gorm.DB, batch int) error {
		for i, c := range allcontent {
			// every 100 contents re-queued, wait 5 seconds to avoid over-saturating queues
			// time to requeue all: 10m / 100 * 5 seconds = 5.78 days
			if i%100 == 0 {
				time.Sleep(time.Second * 5)
			}
			cm.queueMgr.ToCheck(c.ID)
		}
		return nil
	}).Error; err != nil {
		return xerrors.Errorf("finding all content in database: %w", err)
	}
	return nil
}

func (cm *ContentManager) ToCheck(contID uint) {
	cm.queueMgr.ToCheck(contID)
}
