package contentmgr

import (
	"context"
	"sync"

	"github.com/application-research/estuary/collections"
	"github.com/application-research/estuary/config"
	stgzonecreation "github.com/application-research/estuary/content/stagingzone/creation"
	"github.com/application-research/estuary/node"
	"github.com/application-research/estuary/pinner/operation"
	"github.com/application-research/estuary/pinner/progress"
	"github.com/application-research/estuary/pinner/types"
	"github.com/application-research/estuary/shuttle"
	"github.com/application-research/estuary/util"
	"github.com/application-research/filclient"
	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-fil-markets/retrievalmarket"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	ipld "github.com/ipfs/go-ipld-format"
	"github.com/libp2p/go-libp2p/core/peer"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

type IManager interface {
	AddDatabaseTrackingToContent(ctx context.Context, cont *util.Content, dserv ipld.NodeGetter, root cid.Cid, cb func(int64)) error
	AddDatabaseTracking(ctx context.Context, u *util.User, dserv ipld.NodeGetter, root cid.Cid, filename string, replication int) (*util.Content, error)
	GarbageCollect(ctx context.Context) error

	PinContent(ctx context.Context, user uint, obj cid.Cid, filename string, cols []*collections.CollectionRef, origins []*peer.AddrInfo, replaceID uint, meta map[string]interface{}, makeDeal bool) (*types.IpfsPinStatusResponse, *operation.PinningOperation, error)
	PinDelegatesForContent(cont util.Content) []string
	GetPinOperation(cont util.Content, peers []*peer.AddrInfo, replaceID uint, makeDeal bool) *operation.PinningOperation
	DoPinning(ctx context.Context, op *operation.PinningOperation, cb progress.PinProgressCB) error
	UpdatePinStatus(contID uint, location string, status types.PinningStatus) error

	RefreshContent(ctx context.Context, cont uint) error
	OffloadContents(ctx context.Context, conts []uint) (int, error)
	ClearUnused(ctx context.Context, spaceRequest int64, loc string, users []uint, dryrun bool) (*collectionResult, error)
	GetRemovalCandidates(ctx context.Context, all bool, loc string, users []uint) ([]removalCandidateInfo, error)
	UnpinContent(ctx context.Context, contid uint) error
	PinStatus(cont util.Content, origins []*peer.AddrInfo) (*types.IpfsPinStatusResponse, error)
	GetContent(id uint) (*util.Content, error)
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
	stgZoneCreationMgr   stgzonecreation.IManager
	tracer               trace.Tracer
	notifyBlockstore     *node.NotifyBlockstore
	retrLk               sync.Mutex
	retrievalsInProgress map[uint]*util.RetrievalProgress
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
		stgZoneCreationMgr:   stgzonecreation.NewManager(db, cfg, log),
		tracer:               otel.Tracer("content"),
		notifyBlockstore:     nd.NotifBlockstore,
		retrievalsInProgress: make(map[uint]*util.RetrievalProgress),
		inflightCids:         make(map[cid.Cid]uint),
	}
}
