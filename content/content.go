package contentmgr

import (
	"context"
	"sync"
	"time"

	"github.com/application-research/estuary/constants"
	"github.com/application-research/estuary/util"
	"github.com/ipfs/go-cid"
	ipld "github.com/ipfs/go-ipld-format"
	"github.com/ipfs/go-merkledag"
	"github.com/libp2p/go-libp2p/core/peer"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/xerrors"
	"gorm.io/gorm"

	"github.com/application-research/estuary/collections"
	"github.com/application-research/estuary/config"
	splitqueuemgr "github.com/application-research/estuary/content/split/queue"
	stgzonecreation "github.com/application-research/estuary/content/stagingzone/creation"
	dealqueuemgr "github.com/application-research/estuary/deal/queue"

	"github.com/application-research/estuary/node"
	"github.com/application-research/estuary/pinner/operation"
	"github.com/application-research/estuary/pinner/progress"
	"github.com/application-research/estuary/pinner/types"
	"github.com/application-research/estuary/shuttle"
	"github.com/application-research/filclient"
	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-fil-markets/retrievalmarket"
	blocks "github.com/ipfs/go-block-format"

	"go.opentelemetry.io/otel"
	"go.uber.org/zap"
)

type IManager interface {
	AddDatabaseTrackingToContent(ctx context.Context, cont *util.Content, dserv ipld.NodeGetter, root cid.Cid, cb func(int64)) error
	AddDatabaseTracking(ctx context.Context, u *util.User, dserv ipld.NodeGetter, root cid.Cid, filename string, replication int) (*util.Content, error)
	GarbageCollect(ctx context.Context) error

	PinContent(ctx context.Context, user uint, obj cid.Cid, filename string, cols []*collections.CollectionRef, origins []*peer.AddrInfo, replaceID uint, meta map[string]interface{}, makeDeal bool) (*types.IpfsPinStatusResponse, *operation.PinningOperation, error)
	PinDelegatesForContent(cont util.Content) []string
	GetPinOperation(cont util.Content, peers []*peer.AddrInfo, replaceID uint, makeDeal bool) *operation.PinningOperation
	DoPinning(ctx context.Context, op *operation.PinningOperation, cb progress.PinProgressCB) error
	UpdatePinStatus(contID uint64, location string, status types.PinningStatus) error

	RefreshContent(ctx context.Context, cont uint64) error
	OffloadContents(ctx context.Context, conts []uint64) (int, error)
	ClearUnused(ctx context.Context, spaceRequest int64, loc string, users []uint, dryrun bool) (*collectionResult, error)
	GetRemovalCandidates(ctx context.Context, all bool, loc string, users []uint) ([]removalCandidateInfo, error)
	UnpinContent(ctx context.Context, contid uint) error
	PinStatus(cont util.Content, origins []*peer.AddrInfo) (*types.IpfsPinStatusResponse, error)
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
	stgZoneCreationMgr   stgzonecreation.IManager
	tracer               trace.Tracer
	notifyBlockstore     *node.NotifyBlockstore
	retrLk               sync.Mutex
	retrievalsInProgress map[uint64]*util.RetrievalProgress
	contentLk            sync.RWMutex
	inflightCids         map[cid.Cid]uint
	inflightCidsLk       sync.Mutex
	dealQueueMgr         dealqueuemgr.IManager
	splitQueueMgr        splitqueuemgr.IManager
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
		retrievalsInProgress: make(map[uint64]*util.RetrievalProgress),
		inflightCids:         make(map[cid.Cid]uint),
		dealQueueMgr:         dealqueuemgr.NewManager(db, cfg, log),
		splitQueueMgr:        splitqueuemgr.NewManager(db, log),
	}
}

func (m *manager) GetContent(id uint64) (*util.Content, error) {
	var content util.Content
	if err := m.db.First(&content, "id = ?", id).Error; err != nil {
		return nil, err
	}
	return &content, nil
}

// addObjectsToDatabase creates entries on the estuary database for CIDs related to an already pinned CID (`root`)
// These entries are saved on the `objects` table, while metadata about the `root` CID is mostly kept on the `contents` table
// The link between the `objects` and `contents` tables is the `obj_refs` table
func (m *manager) addObjectsToDatabase(ctx context.Context, cont *util.Content, objects []*util.Object, loc string) error {
	_, span := m.tracer.Start(ctx, "addObjectsToDatabase")
	defer span.End()

	return m.db.Transaction(func(tx *gorm.DB) error {
		// create objects
		if err := tx.CreateInBatches(objects, 300).Error; err != nil {
			return xerrors.Errorf("failed to create objects in db: %w", err)
		}

		refs := make([]util.ObjRef, 0, len(objects))
		var contSize int64
		for _, o := range objects {
			refs = append(refs, util.ObjRef{
				Content: cont.ID,
				Object:  o.ID,
			})
			contSize += int64(o.Size)
		}

		span.SetAttributes(
			attribute.Int64("totalSize", contSize),
			attribute.Int("numObjects", len(objects)),
		)

		// create object refs
		if err := tx.CreateInBatches(refs, 500).Error; err != nil {
			return xerrors.Errorf("failed to create refs: %w", err)
		}

		// update content
		if err := tx.Model(util.Content{}).Where("id = ?", cont.ID).UpdateColumns(map[string]interface{}{
			"active":   true,
			"size":     contSize,
			"pinning":  false,
			"location": loc,
		}).Error; err != nil {
			return xerrors.Errorf("failed to update content in database: %w", err)
		}

		// if content can be staged, stage it
		if contSize < m.cfg.Content.MinSize {
			return m.stgZoneCreationMgr.StageNewContent(ctx, cont, contSize)
		}

		// if it is too large, queue it for splitting.
		// split worker will pick it up and split it,
		// its children will be pinned and dealed
		if contSize > m.cfg.Content.MaxSize {
			return m.splitQueueMgr.QueueContent(ctx, cont.ID, cont.UserID)
		}
		// or queue it for deal making
		return m.dealQueueMgr.QueueContent(ctx, cont.ID, cont.Cid, cont.UserID)
	})
}

func (m *manager) addrInfoForContentLocation(handle string) (*peer.AddrInfo, error) {
	if handle == constants.ContentLocationLocal {
		return &peer.AddrInfo{
			ID:    m.node.Host.ID(),
			Addrs: m.node.Host.Addrs(),
		}, nil
	}
	return m.shuttleMgr.AddrInfo(handle)
}

var noDataTimeout = time.Minute * 10

func (m *manager) AddDatabaseTrackingToContent(ctx context.Context, cont *util.Content, dserv ipld.NodeGetter, root cid.Cid, cb func(int64)) error {
	ctx, span := m.tracer.Start(ctx, "computeObjRefsUpdate")
	defer span.End()

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	gotData := make(chan struct{}, 1)
	go func() {
		nodata := time.NewTimer(noDataTimeout)
		defer nodata.Stop()

		for {
			select {
			case <-nodata.C:
				cancel()
			case <-gotData:
				nodata.Reset(noDataTimeout)
			case <-ctx.Done():
				return
			}
		}
	}()

	var objlk sync.Mutex
	var objects []*util.Object
	cset := cid.NewSet()

	defer func() {
		m.inflightCidsLk.Lock()
		_ = cset.ForEach(func(c cid.Cid) error {
			v, ok := m.inflightCids[c]
			if !ok || v <= 0 {
				m.log.Errorf("cid should be inflight but isn't: %s", c)
			}

			m.inflightCids[c]--
			if m.inflightCids[c] == 0 {
				delete(m.inflightCids, c)
			}
			return nil
		})
		m.inflightCidsLk.Unlock()
	}()

	err := merkledag.Walk(ctx, func(ctx context.Context, c cid.Cid) ([]*ipld.Link, error) {
		// cset.Visit gets called first, so if we reach here we should immediately track the CID
		m.inflightCidsLk.Lock()
		m.inflightCids[c]++
		m.inflightCidsLk.Unlock()

		node, err := dserv.Get(ctx, c)
		if err != nil {
			return nil, err
		}

		cb(int64(len(node.RawData())))

		select {
		case gotData <- struct{}{}:
		case <-ctx.Done():
		}

		objlk.Lock()
		objects = append(objects, &util.Object{
			Cid:  util.DbCID{CID: c},
			Size: uint64(len(node.RawData())),
		})
		objlk.Unlock()

		if c.Type() == cid.Raw {
			return nil, nil
		}

		return util.FilterUnwalkableLinks(node.Links()), nil
	}, root, cset.Visit, merkledag.Concurrent())

	if err != nil {
		return err
	}
	return m.addObjectsToDatabase(ctx, cont, objects, constants.ContentLocationLocal)
}

func (m *manager) AddDatabaseTracking(ctx context.Context, u *util.User, dserv ipld.NodeGetter, root cid.Cid, filename string, replication int) (*util.Content, error) {
	ctx, span := m.tracer.Start(ctx, "computeObjRefs")
	defer span.End()

	content := &util.Content{
		Cid:         util.DbCID{CID: root},
		Name:        filename,
		Active:      false,
		Pinning:     true,
		UserID:      u.ID,
		Replication: replication,
		Location:    constants.ContentLocationLocal,
	}

	if err := m.db.Create(content).Error; err != nil {
		return nil, xerrors.Errorf("failed to track new content in database: %w", err)
	}

	if err := m.AddDatabaseTrackingToContent(ctx, content, dserv, root, func(int64) {}); err != nil {
		return nil, err
	}
	return content, nil
}
