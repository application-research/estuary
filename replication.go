package main

import (
	"bytes"
	"container/heap"
	"context"
	"fmt"
	marketv8 "github.com/filecoin-project/go-state-types/builtin/v8/market"
	"math/rand"
	"sort"
	"sync"
	"time"

	"github.com/application-research/estuary/config"
	"github.com/application-research/estuary/constants"
	drpc "github.com/application-research/estuary/drpc"
	"github.com/application-research/estuary/node"
	"github.com/application-research/estuary/pinner"
	util "github.com/application-research/estuary/util"
	dagsplit "github.com/application-research/estuary/util/dagsplit"
	"github.com/application-research/filclient"
	"github.com/filecoin-project/boost/transport/httptransport"
	"github.com/filecoin-project/go-address"
	cborutil "github.com/filecoin-project/go-cbor-util"
	datatransfer "github.com/filecoin-project/go-data-transfer"
	"github.com/filecoin-project/go-fil-markets/storagemarket"
	"github.com/filecoin-project/go-fil-markets/storagemarket/network"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/big"
	"github.com/filecoin-project/lotus/api"
	"github.com/filecoin-project/lotus/chain/types"
	market "github.com/filecoin-project/specs-actors/v6/actors/builtin/market"
	"github.com/google/uuid"
	lru "github.com/hashicorp/golang-lru"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-blockservice"
	"github.com/ipfs/go-cid"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	batched "github.com/ipfs/go-ipfs-provider/batched"
	cbor "github.com/ipfs/go-ipld-cbor"
	ipld "github.com/ipfs/go-ipld-format"
	"github.com/ipfs/go-merkledag"
	"github.com/ipfs/go-metrics-interface"
	"github.com/ipfs/go-unixfs"
	"github.com/labstack/echo/v4"
	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/protocol"
	"github.com/multiformats/go-multiaddr"
	"github.com/pkg/errors"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/xerrors"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type miner struct {
	address             address.Address
	dealProtocolVersion protocol.ID
	ask                 *minerStorageAsk
}

type deal struct {
	minerAddr      address.Address
	isPushTransfer bool
	contentDeal    *contentDeal
}

type ContentManager struct {
	DB        *gorm.DB
	Api       api.Gateway
	FilClient *filclient.FilClient
	Provider  *batched.BatchProvidingSystem
	Node      *node.Node
	cfg       *config.Estuary

	Host host.Host

	tracer trace.Tracer

	Blockstore       node.EstuaryBlockstore
	Tracker          *TrackingBlockstore
	NotifyBlockstore *node.NotifyBlockstore

	ToCheck  chan uint
	queueMgr *queueManager

	retrLk               sync.Mutex
	retrievalsInProgress map[uint]*util.RetrievalProgress

	contentLk sync.RWMutex

	contentSizeLimit int64

	// Some fields for miner reputation management
	minerLk      sync.Mutex
	sortedMiners []address.Address
	rawData      []*minerDealStats
	lastComputed time.Time

	// deal bucketing stuff
	bucketLk sync.Mutex
	buckets  map[uint][]*contentStagingZone

	// some behavior flags
	FailDealOnTransferFailure bool

	dealDisabledLk       sync.Mutex
	isDealMakingDisabled bool

	globalContentAddingDisabled bool
	localContentAddingDisabled  bool

	Replication int

	hostname string

	pinJobs map[uint]*pinner.PinningOperation
	pinLk   sync.Mutex

	pinMgr *pinner.PinManager

	shuttlesLk sync.Mutex
	shuttles   map[string]*ShuttleConnection

	remoteTransferStatus *lru.ARCCache

	inflightCids   map[cid.Cid]uint
	inflightCidsLk sync.Mutex

	DisableFilecoinStorage bool

	IncomingRPCMessages chan *drpc.Message

	EnabledDealProtocolsVersions map[protocol.ID]bool
}

func (cm *ContentManager) isInflight(c cid.Cid) bool {
	cm.inflightCidsLk.Lock()
	defer cm.inflightCidsLk.Unlock()

	v, ok := cm.inflightCids[c]
	return ok && v > 0
}

type contentStagingZone struct {
	ZoneOpened time.Time `json:"zoneOpened"`

	EarliestContent time.Time `json:"earliestContent"`
	CloseTime       time.Time `json:"closeTime"`

	Contents []util.Content `json:"contents"`

	MinSize int64 `json:"minSize"`
	MaxSize int64 `json:"maxSize"`

	MaxItems int `json:"maxItems"`

	CurSize int64 `json:"curSize"`

	User uint `json:"user"`

	ContID   uint   `json:"contentID"`
	Location string `json:"location"`

	MaxContentAge time.Duration `json:"max_content_age"`
	MinDealSize   int64         `json:"min_deal_size"`

	lk sync.Mutex
}

func (cb *contentStagingZone) DeepCopy() *contentStagingZone {
	cb.lk.Lock()
	defer cb.lk.Unlock()
	cb2 := &contentStagingZone{
		ZoneOpened:      cb.ZoneOpened,
		EarliestContent: cb.EarliestContent,
		CloseTime:       cb.CloseTime,
		Contents:        make([]util.Content, len(cb.Contents)),
		MinSize:         cb.MinSize,
		MaxSize:         cb.MaxSize,
		MaxItems:        cb.MaxItems,
		CurSize:         cb.CurSize,
		User:            cb.User,
		ContID:          cb.ContID,
		Location:        cb.Location,
		MaxContentAge:   cb.MaxContentAge,
		MinDealSize:     cb.MinSize,
	}
	copy(cb2.Contents, cb.Contents)
	return cb2
}

func (cm *ContentManager) newContentStagingZone(user uint, loc string) (*contentStagingZone, error) {
	content := &util.Content{
		Size:        0,
		Name:        "aggregate",
		Active:      false,
		Pinning:     true,
		UserID:      user,
		Replication: cm.Replication,
		Aggregate:   true,
		Location:    loc,
	}

	if err := cm.DB.Create(content).Error; err != nil {
		return nil, err
	}

	return &contentStagingZone{
		MinDealSize:   cm.cfg.StagingBucket.MinDealSize,
		MaxContentAge: cm.cfg.StagingBucket.MaxContentAge,
		ZoneOpened:    time.Now(),
		CloseTime:     time.Now().Add(cm.cfg.StagingBucket.MaxLifeTime),
		MinSize:       cm.cfg.StagingBucket.MinSize,
		MaxSize:       cm.cfg.StagingBucket.MaxSize,
		MaxItems:      cm.cfg.StagingBucket.MaxItems,
		User:          user,
		ContID:        content.ID,
		Location:      content.Location,
	}, nil
}

func (cb *contentStagingZone) isReady() bool {
	if cb.CurSize < cb.MinDealSize {
		return false
	}

	// if its above the size requirement, go right ahead
	if cb.CurSize > cb.MinSize {
		return true
	}

	if time.Now().After(cb.CloseTime) {
		return true
	}

	if time.Since(cb.EarliestContent) > cb.MaxContentAge {
		return true
	}

	// if its above the items count requirement, go right ahead
	if len(cb.Contents) >= cb.MaxItems {
		return true
	}
	return false
}

func (cm *ContentManager) tryAddContent(cb *contentStagingZone, c util.Content) (bool, error) {
	cb.lk.Lock()
	defer cb.lk.Unlock()
	if cb.CurSize+c.Size > cb.MaxSize {
		return false, nil
	}

	if len(cb.Contents) >= cb.MaxItems {
		return false, nil
	}

	if err := cm.DB.Model(util.Content{}).
		Where("id = ?", c.ID).
		UpdateColumn("aggregated_in", cb.ContID).Error; err != nil {
		return false, err
	}

	if len(cb.Contents) == 0 || c.CreatedAt.Before(cb.EarliestContent) {
		cb.EarliestContent = c.CreatedAt
	}

	cb.Contents = append(cb.Contents, c)
	cb.CurSize += c.Size

	nowPlus := time.Now().Add(cm.cfg.StagingBucket.KeepAlive)
	if cb.CloseTime.Before(nowPlus) {
		cb.CloseTime = nowPlus
	}
	return true, nil
}

func (cb *contentStagingZone) hasContent(c util.Content) bool {
	cb.lk.Lock()
	defer cb.lk.Unlock()

	for _, cont := range cb.Contents {
		if cont.ID == c.ID {
			return true
		}
	}
	return false
}

func NewContentManager(db *gorm.DB, api api.Gateway, fc *filclient.FilClient, tbs *TrackingBlockstore, nbs *node.NotifyBlockstore, prov *batched.BatchProvidingSystem, pinmgr *pinner.PinManager, nd *node.Node, cfg *config.Estuary) (*ContentManager, error) {
	cache, err := lru.NewARC(50000)
	if err != nil {
		return nil, err
	}

	cm := &ContentManager{
		cfg:                          cfg,
		Provider:                     prov,
		DB:                           db,
		Api:                          api,
		FilClient:                    fc,
		Blockstore:                   tbs.Under().(node.EstuaryBlockstore),
		Host:                         nd.Host,
		Node:                         nd,
		NotifyBlockstore:             nbs,
		Tracker:                      tbs,
		ToCheck:                      make(chan uint, 100000),
		retrievalsInProgress:         make(map[uint]*util.RetrievalProgress),
		buckets:                      make(map[uint][]*contentStagingZone),
		pinJobs:                      make(map[uint]*pinner.PinningOperation),
		pinMgr:                       pinmgr,
		remoteTransferStatus:         cache,
		shuttles:                     make(map[string]*ShuttleConnection),
		contentSizeLimit:             constants.DefaultContentSizeLimit,
		hostname:                     cfg.Hostname,
		inflightCids:                 make(map[cid.Cid]uint),
		FailDealOnTransferFailure:    cfg.Deal.FailOnTransferFailure,
		isDealMakingDisabled:         cfg.Deal.IsDisabled,
		globalContentAddingDisabled:  cfg.Content.DisableGlobalAdding,
		localContentAddingDisabled:   cfg.Content.DisableLocalAdding,
		Replication:                  cfg.Replication,
		tracer:                       otel.Tracer("replicator"),
		DisableFilecoinStorage:       cfg.DisableFilecoinStorage,
		IncomingRPCMessages:          make(chan *drpc.Message),
		EnabledDealProtocolsVersions: cfg.Deal.EnabledDealProtocolsVersions,
	}
	qm := newQueueManager(func(c uint) {
		cm.toCheck(c)
	})

	cm.queueMgr = qm
	return cm, nil
}

func (cm *ContentManager) toCheck(contID uint) {
	// if DisableFilecoinStorage is not enabled, queue content for deal making
	if !cm.DisableFilecoinStorage {
		cm.ToCheck <- contID
	}
}

func (cm *ContentManager) runStagingBucketWorker(ctx context.Context) {
	timer := time.NewTicker(cm.cfg.StagingBucket.AggregateInterval)
	for {
		select {
		case <-timer.C:
			log.Debugw("content check queue", "length", len(cm.queueMgr.queue.elems), "nextEvent", cm.queueMgr.nextEvent)

			buckets := cm.popReadyStagingZone()
			for _, b := range buckets {
				if err := cm.aggregateContent(ctx, b); err != nil {
					log.Errorf("content aggregation failed (bucket %d): %s", b.ContID, err)
					continue
				}
			}
		}
	}
}

func (cm *ContentManager) runDealWorker(ctx context.Context) {
	// run the deal reconciliation and deal making worker
	for {
		select {
		case c := <-cm.ToCheck:
			log.Debugf("checking content: %d", c)

			var content util.Content
			if err := cm.DB.First(&content, "id = ?", c).Error; err != nil {
				log.Errorf("finding content %d in database: %s", c, err)
				continue
			}

			err := cm.ensureStorage(context.TODO(), content, func(dur time.Duration) {
				cm.queueMgr.add(content.ID, dur)
			})
			if err != nil {
				log.Errorf("failed to ensure replication of content %d: %s", content.ID, err)
				cm.queueMgr.add(content.ID, time.Minute*5)
			}
		}
	}
}

func (cm *ContentManager) Run(ctx context.Context) {
	// if content adding is enabled, refresh pin queue for local contents
	if !cm.localContentAddingDisabled {
		go func() {
			if err := cm.refreshPinQueue(ctx, constants.ContentLocationLocal); err != nil {
				log.Errorf("failed to refresh pin queue: %s", err)
			}
		}()
	}

	// if staging buckets are enabled, rebuild the buckets, and run the bucket aggregate worker
	if cm.cfg.StagingBucket.Enabled {
		// rebuild the staging buckets
		if err := cm.rebuildStagingBuckets(); err != nil {
			log.Fatalf("failed to rebuild staging buckets: %s", err)
		}

		// run the staging bucket aggregator worker
		go cm.runStagingBucketWorker(ctx)
	}

	// if FilecoinStorage is enabled, check content deals or make content deals
	if !cm.DisableFilecoinStorage {
		go func() {
			// rebuild toCheck queue
			if err := cm.rebuildToCheckQueue(); err != nil {
				log.Errorf("failed to recheck existing content: %s", err)
			}

			// run the deal reconciliation and deal making worker
			cm.runDealWorker(ctx)
		}()
	}
}

type queueEntry struct {
	content   uint
	checkTime time.Time
}

type entryQueue struct {
	elems []*queueEntry
}

func (eq *entryQueue) Len() int {
	return len(eq.elems)
}

func (eq *entryQueue) Less(i, j int) bool {
	return eq.elems[i].checkTime.Before(eq.elems[j].checkTime)
}

func (eq *entryQueue) Swap(i, j int) {
	eq.elems[i], eq.elems[j] = eq.elems[j], eq.elems[i]
}

func (eq *entryQueue) Push(e interface{}) {
	eq.elems = append(eq.elems, e.(*queueEntry))
}

func (eq *entryQueue) Pop() interface{} {
	out := eq.elems[len(eq.elems)-1]
	eq.elems = eq.elems[:len(eq.elems)-1]
	return out
}

func (eq *entryQueue) PopEntry() *queueEntry {
	return heap.Pop(eq).(*queueEntry)
}

type queueManager struct {
	queue *entryQueue
	cb    func(uint)
	qlk   sync.Mutex

	nextEvent time.Time
	evtTimer  *time.Timer

	qsizeMetr metrics.Gauge
	qnextMetr metrics.Gauge
}

func newQueueManager(cb func(c uint)) *queueManager {
	metCtx := metrics.CtxScope(context.Background(), "content_manager")
	qsizeMetr := metrics.NewCtx(metCtx, "queue_size", "number of items in the replicator queue").Gauge()
	qnextMetr := metrics.NewCtx(metCtx, "queue_next", "next event time for queue").Gauge()

	qm := &queueManager{
		queue: new(entryQueue),
		cb:    cb,

		qsizeMetr: qsizeMetr,
		qnextMetr: qnextMetr,
	}

	heap.Init(qm.queue)
	return qm
}

func (qm *queueManager) add(content uint, wait time.Duration) {
	qm.qlk.Lock()
	defer qm.qlk.Unlock()

	at := time.Now().Add(wait)

	heap.Push(qm.queue, &queueEntry{
		content:   content,
		checkTime: at,
	})

	qm.qsizeMetr.Add(1)

	if qm.nextEvent.IsZero() || at.Before(qm.nextEvent) {
		qm.nextEvent = at
		qm.qnextMetr.Set(float64(at.Unix()))

		if qm.evtTimer != nil {
			qm.evtTimer.Reset(wait)
		} else {
			qm.evtTimer = time.AfterFunc(wait, func() {
				qm.processQueue()
			})
		}
	}
}

func (qm *queueManager) processQueue() {
	qm.qlk.Lock()
	defer qm.qlk.Unlock()

	for qm.queue.Len() > 0 {
		qe := qm.queue.PopEntry()
		if time.Now().After(qe.checkTime) {
			qm.qsizeMetr.Add(-1)
			go qm.cb(qe.content)
		} else {
			heap.Push(qm.queue, qe)
			qm.nextEvent = qe.checkTime
			qm.qnextMetr.Set(float64(qe.checkTime.Unix()))
			qm.evtTimer.Reset(time.Until(qe.checkTime))
			return
		}
	}
	qm.nextEvent = time.Time{}
}

func (cm *ContentManager) currentLocationForContent(c uint) (string, error) {
	var cont util.Content
	if err := cm.DB.First(&cont, "id = ?", c).Error; err != nil {
		return "", err
	}
	return cont.Location, nil
}

func (cm *ContentManager) stagedContentByLocation(ctx context.Context, b *contentStagingZone) (map[string][]util.Content, error) {
	out := make(map[string][]util.Content)
	for _, c := range b.Contents {
		loc, err := cm.currentLocationForContent(c.ID)
		if err != nil {
			return nil, err
		}

		out[loc] = append(out[loc], c)
	}
	return out, nil
}

func (cm *ContentManager) consolidateStagedContent(ctx context.Context, b *contentStagingZone) error {
	var primary string
	var curMax int64
	dataByLoc := make(map[string]int64)
	contentByLoc := make(map[string][]util.Content)

	for _, c := range b.Contents {
		loc, err := cm.currentLocationForContent(c.ID)
		if err != nil {
			return err
		}

		contentByLoc[loc] = append(contentByLoc[loc], c)

		ntot := dataByLoc[loc] + c.Size
		dataByLoc[loc] = ntot

		// temp: dont ever migrate content back to primary instance for aggregation, always prefer elsewhere
		if ntot > curMax && loc != constants.ContentLocationLocal {
			curMax = ntot
			primary = loc
		}
	}

	// okay, move everything to 'primary'
	var toMove []util.Content
	for loc, conts := range contentByLoc {
		if loc != primary {
			toMove = append(toMove, conts...)
		}
	}

	log.Debugw("consolidating content to single location for aggregation", "user", b.User, "primary", primary, "numItems", len(toMove), "primaryWeight", curMax)
	if primary == constants.ContentLocationLocal {
		return cm.migrateContentsToLocalNode(ctx, toMove)
	} else {
		return cm.sendConsolidateContentCmd(ctx, primary, toMove)
	}
}

func (cm *ContentManager) aggregateContent(ctx context.Context, b *contentStagingZone) error {
	ctx, span := cm.tracer.Start(ctx, "aggregateContent")
	defer span.End()

	cbl, err := cm.stagedContentByLocation(ctx, b)
	if err != nil {
		return err
	}

	if len(cbl) > 1 {
		// Need to migrate content all to the same shuttle
		cm.bucketLk.Lock()
		// put the staging zone back in the list
		cm.buckets[b.User] = append(cm.buckets[b.User], b)
		cm.bucketLk.Unlock()

		go func() {
			if err := cm.consolidateStagedContent(ctx, b); err != nil {
				log.Errorf("failed to consolidate staged content: %s", err)
			}
		}()

		return nil
	}

	var loc string
	for k := range cbl {
		loc = k
	}

	dir, err := cm.createAggregate(ctx, b.Contents)
	if err != nil {
		return xerrors.Errorf("failed to create aggregate: %w", err)
	}

	ncid := dir.Cid()
	size, err := dir.Size()
	if err != nil {
		return err
	}

	if size == 0 {
		log.Warnf("content %d aggregate dir apparent size is zero", b.ContID)
	}

	if err := cm.DB.Model(util.Content{}).Where("id = ?", b.ContID).UpdateColumns(map[string]interface{}{
		"cid":  util.DbCID{CID: ncid},
		"size": size,
	}).Error; err != nil {
		return err
	}

	var content util.Content
	if err := cm.DB.First(&content, "id = ?", b.ContID).Error; err != nil {
		return err
	}

	if loc == constants.ContentLocationLocal {
		obj := &util.Object{
			Cid:  util.DbCID{CID: ncid},
			Size: int(size),
		}
		if err := cm.DB.Create(obj).Error; err != nil {
			return err
		}

		if err := cm.DB.Create(&util.ObjRef{
			Content: b.ContID,
			Object:  obj.ID,
		}).Error; err != nil {
			return err
		}

		if err := cm.Blockstore.Put(ctx, dir); err != nil {
			return err
		}

		if err := cm.DB.Model(util.Content{}).Where("id = ?", b.ContID).UpdateColumns(map[string]interface{}{
			"active":  true,
			"pinning": false,
		}).Error; err != nil {
			return err
		}

		go func() {
			cm.toCheck(b.ContID)
		}()

		return nil
	} else {
		var ids []uint
		for _, c := range b.Contents {
			ids = append(ids, c.ID)
		}
		return cm.sendAggregateCmd(ctx, loc, content, ids, dir.RawData())
	}
}

func (cm *ContentManager) createAggregate(ctx context.Context, conts []util.Content) (*merkledag.ProtoNode, error) {
	log.Debug("aggregating contents in staging zone into new content")

	sort.Slice(conts, func(i, j int) bool {
		return conts[i].ID < conts[j].ID
	})

	dir := unixfs.EmptyDirNode()
	for _, c := range conts {
		err := dir.AddRawLink(fmt.Sprintf("%d-%s", c.ID, c.Name), &ipld.Link{
			Size: uint64(c.Size),
			Cid:  c.Cid.CID,
		})
		if err != nil {
			return nil, err
		}
	}
	return dir, nil
}

func (cm *ContentManager) rebuildStagingBuckets() error {
	log.Info("rebuilding staging buckets.......")

	var stages []util.Content
	if err := cm.DB.Find(&stages, "not active and pinning and aggregate").Error; err != nil {
		return err
	}

	zones := make(map[uint][]*contentStagingZone)
	for _, c := range stages {
		z := &contentStagingZone{
			MinDealSize:   cm.cfg.StagingBucket.MinDealSize,
			MaxContentAge: cm.cfg.StagingBucket.MaxContentAge,
			ZoneOpened:    c.CreatedAt,
			CloseTime:     c.CreatedAt.Add(cm.cfg.StagingBucket.MaxLifeTime),
			MinSize:       cm.cfg.StagingBucket.MinSize,
			MaxSize:       cm.cfg.StagingBucket.MaxSize,
			MaxItems:      cm.cfg.StagingBucket.MaxItems,
			User:          c.UserID,
			ContID:        c.ID,
			Location:      c.Location,
		}

		minClose := time.Now().Add(cm.cfg.StagingBucket.KeepAlive)
		if z.CloseTime.Before(minClose) {
			z.CloseTime = minClose
		}

		var inZones []util.Content
		if err := cm.DB.Find(&inZones, "aggregated_in = ?", c.ID).Error; err != nil {
			return err
		}
		z.Contents = inZones

		for _, zc := range inZones {
			// TODO: do some sanity checking that we havent messed up and added
			// too many items to this staging zone
			z.CurSize += zc.Size
		}
		zones[c.UserID] = append(zones[c.UserID], z)
	}
	cm.buckets = zones
	return nil
}

func (cm *ContentManager) rebuildToCheckQueue() error {
	log.Info("rebuilding contents queue .......")

	var allcontent []util.Content
	if err := cm.DB.Find(&allcontent, "active AND NOT aggregated_in > 0").Error; err != nil {
		return xerrors.Errorf("finding all content in database: %w", err)
	}

	go func() {
		for _, c := range allcontent {
			cm.ToCheck <- c.ID
		}
	}()
	return nil
}

type estimateResponse struct {
	Total *abi.TokenAmount
	Asks  []*minerStorageAsk
}

func (cm *ContentManager) estimatePrice(ctx context.Context, repl int, pieceSize abi.PaddedPieceSize, duration abi.ChainEpoch, verified bool) (*estimateResponse, error) {
	ctx, span := cm.tracer.Start(ctx, "estimatePrice", trace.WithAttributes(
		attribute.Int("replication", repl),
	))
	defer span.End()

	miners, err := cm.pickMiners(ctx, repl, pieceSize, nil, false)
	if err != nil {
		return nil, err
	}

	if len(miners) == 0 {
		return nil, fmt.Errorf("failed to find any miners for estimating deal price")
	}

	asks := make([]*minerStorageAsk, 0)
	total := abi.NewTokenAmount(0)
	for _, m := range miners {
		dealSize := pieceSize
		if dealSize < m.ask.MinPieceSize {
			dealSize = m.ask.MinPieceSize
		}

		price := m.ask.GetPrice(verified)
		cost, err := filclient.ComputePrice(price, dealSize, duration)
		if err != nil {
			return nil, err
		}

		asks = append(asks, m.ask)
		total = types.BigAdd(total, *cost)
	}

	return &estimateResponse{
		Total: &total,
		Asks:  asks,
	}, nil
}

type minerStorageAsk struct {
	gorm.Model          `json:"-"`
	Miner               string              `gorm:"unique" json:"miner"`
	Price               string              `json:"price"`
	VerifiedPrice       string              `json:"verifiedPrice"`
	PriceBigInt         big.Int             `gorm:"-" json:"-"`
	VerifiedPriceBigInt big.Int             `gorm:"-" json:"-"`
	MinPieceSize        abi.PaddedPieceSize `json:"minPieceSize"`
	MaxPieceSize        abi.PaddedPieceSize `json:"maxPieceSize"`
	MinerVersion        string              `json:"miner_version"`
}

func (msa *minerStorageAsk) GetPrice(verified bool) types.BigInt {
	if verified {
		return msa.VerifiedPriceBigInt
	}
	return msa.PriceBigInt
}

func (cm *ContentManager) pickMinerDist(n int) (int, int) {
	if n < 3 {
		return n, 0
	}

	if n < 7 {
		return 2, n - 2
	}
	return n - (n / 2), n / 2
}

func (cm *ContentManager) pickMiners(ctx context.Context, n int, pieceSize abi.PaddedPieceSize, exclude map[address.Address]bool, filterByPrice bool) ([]miner, error) {
	ctx, span := cm.tracer.Start(ctx, "pickMiners", trace.WithAttributes(
		attribute.Int("count", n),
	))
	defer span.End()
	if exclude == nil {
		exclude = make(map[address.Address]bool)
	}

	// some portion of the miners will be 'first N of our best miners' and the rest will be randomly chosen from our list
	// over time, our miner list will be all fairly high quality so this should just serve to shake things up a bit and
	// give miners more of a chance to prove themselves
	_, nrand := cm.pickMinerDist(n)

	out, err := cm.randomMinerListForDeal(ctx, nrand, pieceSize, exclude, filterByPrice)
	if err != nil {
		return nil, err
	}
	return cm.sortedMinersForDeal(ctx, out, n, pieceSize, exclude, filterByPrice)
}

// TODO - this is currently not used, if we choose to use it,
// add a check to make sure miners selected is still active in db
func (cm *ContentManager) sortedMinersForDeal(ctx context.Context, out []miner, n int, pieceSize abi.PaddedPieceSize, exclude map[address.Address]bool, filterByPrice bool) ([]miner, error) {
	sortedMiners, _, err := cm.sortedMinerList()
	if err != nil {
		return nil, err
	}

	if len(sortedMiners) == 0 {
		return out, nil
	}

	if len(sortedMiners) > constants.TopMinerSel {
		sortedMiners = sortedMiners[:constants.TopMinerSel]
	}

	rand.Shuffle(len(sortedMiners), func(i, j int) {
		sortedMiners[i], sortedMiners[j] = sortedMiners[j], sortedMiners[i]
	})

	for _, m := range sortedMiners {
		if len(out) >= n {
			break
		}

		if exclude[m] {
			continue
		}

		proto, err := cm.FilClient.DealProtocolForMiner(ctx, m)
		if err != nil {
			log.Errorf("getting deal protocol for %s failed: %s", m, err)
			continue
		}

		_, ok := cm.EnabledDealProtocolsVersions[proto]
		if !ok {
			continue
		}

		ask, err := cm.getAsk(ctx, m, time.Minute*30)
		if err != nil {
			log.Errorf("getting ask from %s failed: %s", m, err)
			continue
		}

		if filterByPrice {
			price := ask.GetPrice(cm.cfg.Deal.IsVerified)
			if cm.priceIsTooHigh(price) {
				continue
			}
		}

		if cm.sizeIsCloseEnough(pieceSize, ask.MinPieceSize, ask.MaxPieceSize) {
			out = append(out, miner{address: m, dealProtocolVersion: proto, ask: ask})
			exclude[m] = true
		}
	}
	return out, nil
}

func (cm *ContentManager) randomMinerListForDeal(ctx context.Context, n int, pieceSize abi.PaddedPieceSize, exclude map[address.Address]bool, filterByPrice bool) ([]miner, error) {
	var dbminers []storageMiner
	if err := cm.DB.Find(&dbminers, "not suspended").Error; err != nil {
		return nil, err
	}

	out := make([]miner, 0)
	if len(dbminers) == 0 {
		return out, nil
	}

	rand.Shuffle(len(dbminers), func(i, j int) {
		dbminers[i], dbminers[j] = dbminers[j], dbminers[i]
	})

	for _, dbm := range dbminers {
		if len(out) >= n {
			break
		}

		if exclude[dbm.Address.Addr] {
			continue
		}

		proto, err := cm.FilClient.DealProtocolForMiner(ctx, dbm.Address.Addr)
		if err != nil {
			log.Errorf("getting deal protocol for %s failed: %s", dbm.Address.Addr, err)
			continue
		}

		_, ok := cm.EnabledDealProtocolsVersions[proto]
		if !ok {
			continue
		}

		ask, err := cm.getAsk(ctx, dbm.Address.Addr, time.Minute*30)
		if err != nil {
			log.Errorf("getting ask from %s failed: %s", dbm.Address.Addr, err)
			continue
		}

		if filterByPrice {
			price := ask.GetPrice(cm.cfg.Deal.IsVerified)
			if cm.priceIsTooHigh(price) {
				continue
			}
		}

		if cm.sizeIsCloseEnough(pieceSize, ask.MinPieceSize, ask.MaxPieceSize) {
			out = append(out, miner{address: dbm.Address.Addr, dealProtocolVersion: proto, ask: ask})
			exclude[dbm.Address.Addr] = true
		}
	}
	return out, nil
}

func (cm *ContentManager) getAsk(ctx context.Context, m address.Address, maxCacheAge time.Duration) (*minerStorageAsk, error) {
	ctx, span := cm.tracer.Start(ctx, "getAsk", trace.WithAttributes(
		attribute.Stringer("miner", m),
	))
	defer span.End()

	var asks []minerStorageAsk
	if err := cm.DB.Find(&asks, "miner = ?", m.String()).Error; err != nil {
		return nil, err
	}

	minerVersion, err := cm.updateMinerVersion(ctx, m)
	if err != nil {
		log.Warnf("failed to update miner version: %s", err)
	}

	if len(asks) > 0 && time.Since(asks[0].UpdatedAt) < maxCacheAge {
		ask := asks[0]
		priceBigInt, err := types.BigFromString(ask.Price)
		if err != nil {
			return nil, err
		}
		ask.PriceBigInt = priceBigInt

		verifiedPriceBigInt, err := types.BigFromString(ask.VerifiedPrice)
		if err != nil {
			return nil, err
		}
		ask.VerifiedPriceBigInt = verifiedPriceBigInt

		if ask.MinerVersion == "" {
			ask.MinerVersion = minerVersion
		}
		return &ask, nil
	}

	netask, err := cm.FilClient.GetAsk(ctx, m)
	if err != nil {
		span.RecordError(err)
		return nil, err
	}

	nmsa := toDBAsk(netask)
	nmsa.UpdatedAt = time.Now()
	nmsa.MinerVersion = minerVersion

	if err := cm.DB.Clauses(clause.OnConflict{
		Columns: []clause.Column{
			{Name: "miner"},
		},
		DoUpdates: clause.AssignmentColumns([]string{"price", "verified_price", "min_piece_size", "updated_at", "miner_version"}),
	}).Create(nmsa).Error; err != nil {
		span.RecordError(err)
		return nil, err
	}
	return nmsa, nil
}

func (cm *ContentManager) updateMinerVersion(ctx context.Context, m address.Address) (string, error) {
	vers, err := cm.FilClient.GetMinerVersion(ctx, m)
	if err != nil {
		return "", err
	}

	if vers != "" {
		if err := cm.DB.Model(storageMiner{}).Where("address = ?", m.String()).Update("version", vers).Error; err != nil {
			return "", err
		}
	}
	return vers, nil
}

func (cm *ContentManager) sizeIsCloseEnough(pieceSize, askMinPieceSize, askMaxPieceSize abi.PaddedPieceSize) bool {
	if pieceSize > askMinPieceSize && pieceSize < askMaxPieceSize {
		return true
	}
	return false
}

func toDBAsk(netask *network.AskResponse) *minerStorageAsk {
	return &minerStorageAsk{
		Miner:               netask.Ask.Ask.Miner.String(),
		Price:               netask.Ask.Ask.Price.String(),
		VerifiedPrice:       netask.Ask.Ask.VerifiedPrice.String(),
		MinPieceSize:        netask.Ask.Ask.MinPieceSize,
		MaxPieceSize:        netask.Ask.Ask.MaxPieceSize,
		PriceBigInt:         netask.Ask.Ask.Price,
		VerifiedPriceBigInt: netask.Ask.Ask.VerifiedPrice,
	}
}

type contentDeal struct {
	gorm.Model
	Content          uint       `json:"content" gorm:"index:,option:CONCURRENTLY"`
	UserID           uint       `json:"user_id" gorm:"index:,option:CONCURRENTLY"`
	PropCid          util.DbCID `json:"propCid"`
	DealUUID         string     `json:"dealUuid"`
	Miner            string     `json:"miner"`
	DealID           int64      `json:"dealId"`
	Failed           bool       `json:"failed"`
	Verified         bool       `json:"verified"`
	Slashed          bool       `json:"slashed"`
	FailedAt         time.Time  `json:"failedAt,omitempty"`
	DTChan           string     `json:"dtChan" gorm:"index"`
	TransferStarted  time.Time  `json:"transferStarted"`
	TransferFinished time.Time  `json:"transferFinished"`

	OnChainAt           time.Time   `json:"onChainAt"`
	SealedAt            time.Time   `json:"sealedAt"`
	DealProtocolVersion protocol.ID `json:"deal_protocol_version"`
	MinerVersion        string      `json:"miner_version"`
}

func (cd contentDeal) MinerAddr() (address.Address, error) {
	return address.NewFromString(cd.Miner)
}

var ErrNoChannelID = fmt.Errorf("no data transfer channel id in deal")

func (cd contentDeal) ChannelID() (datatransfer.ChannelID, error) {
	if cd.DTChan == "" {
		return datatransfer.ChannelID{}, ErrNoChannelID
	}

	chid, err := filclient.ChannelIDFromString(cd.DTChan)
	if err != nil {
		err = fmt.Errorf("incorrectly formatted data transfer channel ID in contentDeal record: %w", err)
		return datatransfer.ChannelID{}, err
	}

	return *chid, nil
}

func (cm *ContentManager) contentInStagingZone(ctx context.Context, content util.Content) bool {
	cm.bucketLk.Lock()
	defer cm.bucketLk.Unlock()

	bucks, ok := cm.buckets[content.UserID]
	if !ok {
		return false
	}

	for _, b := range bucks {
		if b.hasContent(content) {
			return true
		}
	}
	return false
}

func (cm *ContentManager) getStagingZonesForUser(ctx context.Context, user uint) []*contentStagingZone {
	cm.bucketLk.Lock()
	defer cm.bucketLk.Unlock()

	blist, ok := cm.buckets[user]
	if !ok {
		return []*contentStagingZone{}
	}

	var out []*contentStagingZone
	for _, b := range blist {
		out = append(out, b.DeepCopy())
	}
	return out
}

func (cm *ContentManager) getStagingZoneSnapshot(ctx context.Context) map[uint][]*contentStagingZone {
	cm.bucketLk.Lock()
	defer cm.bucketLk.Unlock()

	out := make(map[uint][]*contentStagingZone)
	for u, blist := range cm.buckets {
		var copylist []*contentStagingZone

		for _, b := range blist {
			copylist = append(copylist, b.DeepCopy())
		}
		out[u] = copylist
	}
	return out
}

func (cm *ContentManager) addContentToStagingZone(ctx context.Context, content util.Content) error {
	_, span := cm.tracer.Start(ctx, "stageContent")
	defer span.End()
	if content.AggregatedIn > 0 {
		log.Warnf("attempted to add content to staging zone that was already staged: %d (is in %d)", content.ID, content.AggregatedIn)
		return nil
	}

	log.Debugf("adding content to staging zone: %d", content.ID)
	cm.bucketLk.Lock()
	defer cm.bucketLk.Unlock()

	blist, ok := cm.buckets[content.UserID]
	if !ok {
		b, err := cm.newContentStagingZone(content.UserID, content.Location)
		if err != nil {
			return fmt.Errorf("failed to create new staging zone content: %w", err)
		}

		_, err = cm.tryAddContent(b, content)
		if err != nil {
			return fmt.Errorf("failed to add content to staging zone: %w", err)
		}

		cm.buckets[content.UserID] = []*contentStagingZone{b}
		return nil
	}

	for _, b := range blist {
		ok, err := cm.tryAddContent(b, content)
		if err != nil {
			return err
		}

		if ok {
			return nil
		}
	}

	b, err := cm.newContentStagingZone(content.UserID, content.Location)
	if err != nil {
		return err
	}
	cm.buckets[content.UserID] = append(blist, b)

	_, err = cm.tryAddContent(b, content)
	if err != nil {
		return err
	}

	return nil
}

func (cm *ContentManager) popReadyStagingZone() []*contentStagingZone {
	cm.bucketLk.Lock()
	defer cm.bucketLk.Unlock()

	var out []*contentStagingZone
	for uid, blist := range cm.buckets {
		var keep []*contentStagingZone
		for _, b := range blist {
			if b.isReady() {
				out = append(out, b)
			} else {
				keep = append(keep, b)
			}
		}
		cm.buckets[uid] = keep
	}
	return out
}

func (cm *ContentManager) ensureStorage(ctx context.Context, content util.Content, done func(time.Duration)) error {
	ctx, span := cm.tracer.Start(ctx, "ensureStorage", trace.WithAttributes(
		attribute.Int("content", int(content.ID)),
	))
	defer span.End()

	// If this content is aggregated inside another piece of content, nothing to do here, that content will be processed
	if content.AggregatedIn > 0 {
		return nil
	}

	// If this is the 'root' of a split dag, we dont need to process it
	if content.DagSplit && content.SplitFrom == 0 {
		return nil
	}

	// If this is a shuttle content and the shuttle is not online, do not proceed, retry it 15 mins
	if content.Location != constants.ContentLocationLocal && !cm.shuttleIsOnline(content.Location) {
		log.Debugf("content shuttle: %s, is not online", content.Location)
		done(time.Minute * 15)
		return nil
	}

	// If this content is already scheduled to be aggregated and is waiting in a bucket
	if cm.contentInStagingZone(ctx, content) {
		return nil
	}

	// it's too big, need to split it up into chunks, no need to requeue dagsplit root content
	if content.Size > cm.contentSizeLimit {
		return cm.splitContent(ctx, content, cm.contentSizeLimit)
	}

	// check if content has enough deals made for it
	// if not enough deals, go make more
	// check all existing deals, ensure they are still active
	// if not active, repair!
	var deals []contentDeal
	if err := cm.DB.Find(&deals, "content = ? AND NOT failed", content.ID).Error; err != nil {
		if !xerrors.Is(err, gorm.ErrRecordNotFound) {
			return err
		}
	}

	// if staging bucket is enabled, try to bucket the content
	if cm.canStageContent(content) {
		// Only contents that have no deals and that are not themselves buckets(aggregates) that can be placed in a bucket
		if len(deals) == 0 && !content.Aggregate {
			return cm.addContentToStagingZone(ctx, content)
		}
	}

	// check on each of the existing deals, see if they need fixing
	var countLk sync.Mutex
	var numSealed, numPublished, numProgress int
	errs := make([]error, len(deals))
	var wg sync.WaitGroup
	for i := range deals {
		wg.Add(1)
		go func(i int) {
			d := deals[i]
			defer wg.Done()
			status, err := cm.checkDeal(ctx, &d)
			if err != nil {
				var dfe *DealFailureError
				if xerrors.As(err, &dfe) {
					return
				} else {
					errs[i] = err
					return
				}
			}

			countLk.Lock()
			defer countLk.Unlock()
			switch status {
			case DEAL_CHECK_UNKNOWN, DEAL_NEARLY_EXPIRED:
				if err := cm.repairDeal(&d); err != nil {
					errs[i] = xerrors.Errorf("repairing deal failed: %w", err)
					return
				}
			case DEAL_CHECK_SECTOR_ON_CHAIN:
				numSealed++
			case DEAL_CHECK_DEALID_ON_CHAIN:
				numPublished++
			case DEAL_CHECK_PROGRESS:
				numProgress++
			default:
				log.Errorf("unrecognized deal check status: %d", status)
			}
		}(i)
	}
	wg.Wait()

	// return the last error found, log the rest
	var retErr error
	for _, err := range errs {
		if err != nil {
			if retErr != nil {
				log.Errorf("check deal failure: %s", err)
			}
			retErr = err
		}
	}
	if retErr != nil {
		return fmt.Errorf("deal check errored: %w", retErr)
	}
	goodDeals := numSealed + numPublished + numProgress

	replicationFactor := cm.Replication
	if content.Replication > 0 {
		replicationFactor = content.Replication
	}

	dealsToBeMade := replicationFactor - goodDeals
	if dealsToBeMade <= 0 {
		if numSealed >= replicationFactor {
			done(time.Hour * 24)
		} else if numSealed+numPublished >= replicationFactor {
			done(time.Hour)
		} else {
			done(time.Minute * 10)
		}
		return nil
	}

	pc, err := cm.lookupPieceCommRecord(content.Cid.CID)
	if err != nil {
		return err
	}

	if pc == nil {
		// pre-compute piece commitment in a goroutine and dont block the checker loop while doing so
		go func() {
			_, _, _, err := cm.getPieceCommitment(context.Background(), content.Cid.CID, cm.Blockstore)
			if err != nil {
				log.Errorf("failed to compute piece commitment for content %d: %s", content.ID, err)
				done(time.Minute * 5)
			} else {
				done(time.Second * 10)
			}
		}()
		return nil
	}

	if content.Offloaded {
		go func() {
			if err := cm.RefreshContent(context.Background(), content.ID); err != nil {
				log.Errorf("failed to retrieve content in need of repair %d: %s", content.ID, err)
			}
			done(time.Second * 30)
		}()
		return nil
	}

	if cm.dealMakingDisabled() {
		log.Warnf("deal making is disabled for now")
		done(time.Minute * 60)
		return nil
	}

	// only verified deals need datacap checks
	if cm.cfg.Deal.IsVerified {
		bl, err := cm.FilClient.Balance(ctx)
		if err != nil {
			return errors.Wrap(err, "could not retrieve dataCap from client balance")
		}

		if bl.VerifiedClientBalance.LessThan(big.NewIntUnsigned(uint64(abi.UnpaddedPieceSize(content.Size).Padded()))) {
			// how do we notify admin to top up datacap?
			return errors.Wrapf(err, "will not make deal, client address dataCap:%d GiB is lower than content size:%d GiB", big.Div(*bl.VerifiedClientBalance, big.NewIntUnsigned(uint64(1073741824))), abi.UnpaddedPieceSize(content.Size).Padded()/1073741824)
		}
	}

	// to make sure replicas are distributed, make new deals with miners that currently don't store this content
	excludedMiners := make(map[address.Address]bool)
	for _, d := range deals {
		if d.Failed {
			// TODO: this is an interesting choice, because it gives miners more chances to try again if they fail.
			// I think that as we get a more diverse set of stable miners, we can *not* do this.
			continue
		}
		maddr, err := d.MinerAddr()
		if err != nil {
			return err
		}
		excludedMiners[maddr] = true
	}

	go func() {
		// make some more deals!
		log.Infow("making more deals for content", "content", content.ID, "curDealCount", len(deals), "newDeals", dealsToBeMade)
		if err := cm.makeDealsForContent(ctx, content, dealsToBeMade, excludedMiners); err != nil {
			log.Errorf("failed to make more deals: %s", err)
		}
		done(time.Minute * 10)
	}()
	return nil

}

func (cm *ContentManager) canStageContent(cont util.Content) bool {
	return cont.Size < cm.cfg.StagingBucket.IndividualDealThreshold && cm.cfg.StagingBucket.Enabled
}

func (cm *ContentManager) splitContent(ctx context.Context, cont util.Content, size int64) error {
	ctx, span := cm.tracer.Start(ctx, "splitContent")
	defer span.End()

	var u User
	if err := cm.DB.First(&u, "id = ?", cont.UserID).Error; err != nil {
		return fmt.Errorf("failed to load contents user from db: %w", err)
	}

	if !u.FlagSplitContent() {
		return fmt.Errorf("user does not have content splitting enabled")
	}

	log.Infof("splitting content %d (size: %d)", cont.ID, size)

	if cont.Location == constants.ContentLocationLocal {
		go func() {
			if err := cm.splitContentLocal(ctx, cont, size); err != nil {
				log.Errorw("failed to split local content", "cont", cont.ID, "size", size, "err", err)
			}
		}()
		return nil
	} else {
		return cm.sendSplitContentCmd(ctx, cont.Location, cont.ID, size)
	}
}

func (cm *ContentManager) getContent(id uint) (*util.Content, error) {
	var content util.Content
	if err := cm.DB.First(&content, "id = ?", id).Error; err != nil {
		return nil, err
	}
	return &content, nil
}

const (
	DEAL_CHECK_UNKNOWN = iota
	DEAL_CHECK_PROGRESS
	DEAL_CHECK_DEALID_ON_CHAIN
	DEAL_CHECK_SECTOR_ON_CHAIN
	DEAL_NEARLY_EXPIRED
)

func (cm *ContentManager) checkDeal(ctx context.Context, d *contentDeal) (int, error) {
	ctx, cancel := context.WithTimeout(ctx, time.Minute*5) // NB: if we ever hit this, its bad. but we at least need *some* timeout there
	defer cancel()

	ctx, span := cm.tracer.Start(ctx, "checkDeal", trace.WithAttributes(
		attribute.Int("deal", int(d.ID)),
	))
	defer span.End()
	log.Debugw("checking deal", "miner", d.Miner, "content", d.Content, "dbid", d.ID)

	maddr, err := d.MinerAddr()
	if err != nil {
		return DEAL_CHECK_UNKNOWN, err
	}

	if d.DealID != 0 {
		ok, deal, err := cm.FilClient.CheckChainDeal(ctx, abi.DealID(d.DealID))
		if err != nil {
			return DEAL_CHECK_UNKNOWN, fmt.Errorf("failed to check chain deal: %w", err)
		}
		if !ok {
			return DEAL_CHECK_UNKNOWN, nil
		}

		if deal.State.SlashEpoch > 0 {
			// Deal slashed!
			if err := cm.DB.Model(contentDeal{}).Where("id = ?", d.ID).UpdateColumn("slashed", true).Error; err != nil {
				return DEAL_CHECK_UNKNOWN, err
			}

			if err := cm.recordDealFailure(&DealFailureError{
				Miner:               maddr,
				Phase:               "check-chain-deal",
				Message:             fmt.Sprintf("deal %d was slashed at epoch %d", d.DealID, deal.State.SlashEpoch),
				Content:             d.Content,
				UserID:              d.UserID,
				DealProtocolVersion: d.DealProtocolVersion,
				MinerVersion:        d.MinerVersion,
			}); err != nil {
				return DEAL_CHECK_UNKNOWN, err
			}
			return DEAL_CHECK_UNKNOWN, nil
		}

		head, err := cm.Api.ChainHead(ctx)
		if err != nil {
			return DEAL_CHECK_UNKNOWN, fmt.Errorf("failed to check chain head: %w", err)
		}

		if deal.Proposal.EndEpoch-head.Height() < constants.MinSafeDealLifetime {
			return DEAL_NEARLY_EXPIRED, nil
		}

		if d.SealedAt.IsZero() && deal.State.SectorStartEpoch > 0 {
			if err := cm.DB.Model(contentDeal{}).Where("id = ?", d.ID).UpdateColumn("sealed_at", time.Now()).Error; err != nil {
				return DEAL_CHECK_UNKNOWN, err
			}
			return DEAL_CHECK_SECTOR_ON_CHAIN, nil
		}
		return DEAL_CHECK_DEALID_ON_CHAIN, nil
	}

	// case where deal isnt yet on chain...

	log.Debugw("checking deal status", "miner", maddr, "propcid", d.PropCid.CID, "dealUUID", d.DealUUID)
	subctx, cancel := context.WithTimeout(ctx, time.Second*10)
	defer cancel()

	// Get deal UUID, if there is one for the deal.
	// (There should be a UUID for deals made with deal protocol v1.2.0)
	statusCheckID := d.PropCid.CID.String()
	var dealUUID *uuid.UUID
	if d.DealUUID != "" {
		statusCheckID = d.DealUUID
		parsed, parseErr := uuid.Parse(d.DealUUID)
		if parseErr == nil {
			dealUUID = &parsed
		} else {
			err = fmt.Errorf("parsing deal uuid %s: %w", d.DealUUID, parseErr)
		}
	}

	var provds *storagemarket.ProviderDealState
	if err == nil {
		provds, err = cm.FilClient.DealStatus(subctx, maddr, d.PropCid.CID, dealUUID)
	}
	if err != nil {
		log.Warnf("failed to check deal status for deal %s with miner %s: %s", statusCheckID, maddr, err)
		// if we cant get deal status from a miner and the data hasnt landed on
		// chain what do we do?
		expired, err := cm.dealHasExpired(ctx, d)
		if err != nil {
			return DEAL_CHECK_UNKNOWN, xerrors.Errorf("failed to check if deal was expired: %w", err)
		}
		if expired {
			// deal expired, miner didnt start it in time
			if err := cm.recordDealFailure(&DealFailureError{
				Miner:               maddr,
				Phase:               "check-status",
				Message:             "was unable to check deal status with miner and now deal has expired",
				Content:             d.Content,
				UserID:              d.UserID,
				DealProtocolVersion: d.DealProtocolVersion,
				MinerVersion:        d.MinerVersion,
			}); err != nil {
				return DEAL_CHECK_UNKNOWN, err
			}
			return DEAL_CHECK_UNKNOWN, nil
		}

		// dont fail it out until they run out of time, they might just be offline momentarily
		return DEAL_CHECK_PROGRESS, nil
	}

	if provds == nil {
		return DEAL_CHECK_UNKNOWN, fmt.Errorf("failed to lookup provider deal state")
	}

	if provds.State == storagemarket.StorageDealError {
		log.Errorf("deal state for deal %s from miner %s is error: %s",
			statusCheckID, maddr.String(), provds.Message)
	}

	content, err := cm.getContent(d.Content)
	if err != nil {
		return DEAL_CHECK_UNKNOWN, err
	}

	head, err := cm.Api.ChainHead(ctx)
	if err != nil {
		return DEAL_CHECK_UNKNOWN, fmt.Errorf("failed to check chain head: %w", err)
	}

	if provds.DealID != 0 {
		deal, err := cm.Api.StateMarketStorageDeal(ctx, provds.DealID, types.EmptyTSK)
		if err != nil || deal == nil {
			return DEAL_CHECK_UNKNOWN, fmt.Errorf("failed to lookup deal on chain: %w", err)
		}

		pcr, err := cm.lookupPieceCommRecord(content.Cid.CID)
		if err != nil || pcr == nil {
			return DEAL_CHECK_UNKNOWN, xerrors.Errorf("failed to look up piece commitment for content: %w", err)
		}

		if deal.Proposal.Provider != maddr || deal.Proposal.PieceCID != pcr.Piece.CID {
			log.Errorf("proposal in deal ID miner sent back did not match our expectations")
			return DEAL_CHECK_UNKNOWN, nil
		}

		log.Infof("Confirmed deal ID, updating in database: %d %d %d", d.Content, d.ID, provds.DealID)
		if err := cm.updateDealID(d, int64(provds.DealID)); err != nil {
			return DEAL_CHECK_UNKNOWN, err
		}

		return DEAL_CHECK_DEALID_ON_CHAIN, nil
	}

	if provds.PublishCid != nil {
		log.Debugw("checking publish CID", "content", d.Content, "miner", d.Miner, "propcid", d.PropCid.CID, "publishCid", *provds.PublishCid)
		id, err := cm.getDealID(ctx, *provds.PublishCid, d)
		if err != nil {
			log.Infof("failed to find message on chain: %s", *provds.PublishCid)
			if provds.Proposal.StartEpoch < head.Height() {
				// deal expired, miner didn`t start it in time
				if err := cm.recordDealFailure(&DealFailureError{
					Miner:               maddr,
					Phase:               "check-status",
					Message:             "deal did not make it on chain in time (but has publish deal cid set)",
					Content:             d.Content,
					UserID:              d.UserID,
					DealProtocolVersion: d.DealProtocolVersion,
					MinerVersion:        d.MinerVersion,
				}); err != nil {
					return DEAL_CHECK_UNKNOWN, err
				}
				return DEAL_CHECK_UNKNOWN, nil
			}
			return DEAL_CHECK_PROGRESS, nil
		}

		log.Infof("Found deal ID, updating in database: %d %d %d", d.Content, d.ID, id)
		if err := cm.updateDealID(d, int64(id)); err != nil {
			return DEAL_CHECK_UNKNOWN, err
		}
		return DEAL_CHECK_DEALID_ON_CHAIN, nil
	}

	if provds.Proposal == nil {
		log.Errorw("response from miner has nil Proposal", "miner", maddr, "propcid", d.PropCid.CID, "dealUUID", d.DealUUID)
		if time.Since(d.CreatedAt) > time.Hour*24*14 {
			if err := cm.recordDealFailure(&DealFailureError{
				Miner:               maddr,
				Phase:               "check-status",
				Message:             "miner returned nil response proposal and deal expired",
				Content:             d.Content,
				UserID:              d.UserID,
				DealProtocolVersion: d.DealProtocolVersion,
				MinerVersion:        d.MinerVersion,
			}); err != nil {
				return DEAL_CHECK_UNKNOWN, err
			}
			return DEAL_CHECK_UNKNOWN, nil
		}
		return DEAL_CHECK_UNKNOWN, fmt.Errorf("bad response from miner %s for deal %s deal status check: %s",
			statusCheckID, maddr.String(), provds.Message)
	}

	if provds.Proposal.StartEpoch < head.Height() {
		// deal expired, miner didnt start it in time
		if err := cm.recordDealFailure(&DealFailureError{
			Miner:               maddr,
			Phase:               "check-status",
			Message:             "deal did not make it on chain in time",
			Content:             d.Content,
			UserID:              d.UserID,
			DealProtocolVersion: d.DealProtocolVersion,
			MinerVersion:        d.MinerVersion,
		}); err != nil {
			return DEAL_CHECK_UNKNOWN, err
		}
		return DEAL_CHECK_UNKNOWN, nil
	}
	// miner still has time...

	if d.DTChan == "" {
		if content.Location != constants.ContentLocationLocal {
			log.Warnw("have not yet received confirmation of transfer start from remote", "loc", content.Location, "content", content.ID, "deal", d.ID)
			if time.Since(d.CreatedAt) > time.Hour {
				return DEAL_CHECK_UNKNOWN, nil
			}

			return DEAL_CHECK_PROGRESS, nil
		} else {
			// Weird case where we somehow dont have the data transfer started for this deal
			log.Warnf("creating new data transfer for local deal that is missing it: %d", d.ID)
			if err := cm.StartDataTransfer(ctx, d); err != nil {
				log.Errorw("failed to start new data transfer for weird state deal", "deal", d.ID, "miner", d.Miner, "err", err)
				// If this fails out, just fail the deal and start from
				// scratch. This is already a weird state.
				return DEAL_CHECK_UNKNOWN, nil
			}
		}
	}

	status, err := cm.GetTransferStatus(ctx, d, content)
	if err != nil {
		return DEAL_CHECK_UNKNOWN, err
	}

	if status == nil {
		// no status for transfer, could be because the remote hasnt reported it to us yet?
		log.Warnf("no status for deal: %d", d.ID)
		if d.DTChan == "" {
			// No channel ID yet, shouldnt be able to get here actually
			return DEAL_CHECK_PROGRESS, fmt.Errorf("unexpected state, no transfer status despite earlier check")
		} else {
			if d.DTChan == "" {
				return DEAL_CHECK_UNKNOWN, fmt.Errorf("failed to parse dtchan in deal %d: empty dtchan", d.ID)
			}
			if err := cm.sendRequestTransferStatusCmd(ctx, content.Location, d.ID, d.DTChan); err != nil {
				return DEAL_CHECK_UNKNOWN, err
			}
			return DEAL_CHECK_PROGRESS, nil
		}
	}

	switch status.Status {
	case datatransfer.Failed:
		if err := cm.recordDealFailure(&DealFailureError{
			Miner:               maddr,
			Phase:               "data-transfer",
			Message:             fmt.Sprintf("transfer failed: %s", status.Message),
			Content:             content.ID,
			UserID:              d.UserID,
			DealProtocolVersion: d.DealProtocolVersion,
			MinerVersion:        d.MinerVersion,
		}); err != nil {
			return DEAL_CHECK_UNKNOWN, err
		}

		// TODO: returning unknown==error here feels excessive
		// but since 'Failed' is a terminal state, we kinda just have to make a new deal altogether
		if cm.FailDealOnTransferFailure {
			return DEAL_CHECK_UNKNOWN, nil
		}
	case datatransfer.Cancelled:
		if err := cm.recordDealFailure(&DealFailureError{
			Miner:               maddr,
			Phase:               "data-transfer",
			Message:             fmt.Sprintf("transfer cancelled: %s", status.Message),
			Content:             content.ID,
			UserID:              d.UserID,
			DealProtocolVersion: d.DealProtocolVersion,
			MinerVersion:        d.MinerVersion,
		}); err != nil {
			return DEAL_CHECK_UNKNOWN, err
		}
		return DEAL_CHECK_UNKNOWN, nil
	case datatransfer.Failing:
		// I guess we just wait until its failed all the way?
	case datatransfer.Requested:
		// fmt.Println("transfer is requested, hasnt started yet")
		// probably okay
	case datatransfer.TransferFinished, datatransfer.Finalizing, datatransfer.Completing, datatransfer.Completed:
		if d.TransferFinished.IsZero() {
			if err := cm.DB.Model(contentDeal{}).Where("id = ?", d.ID).Updates(map[string]interface{}{
				"transfer_finished": time.Now(),
			}).Error; err != nil {
				return DEAL_CHECK_UNKNOWN, err
			}
		}

		// these are all okay
		// fmt.Println("transfer is finished-ish!", status.Status)
	case datatransfer.Ongoing:
		//fmt.Println("transfer status is ongoing!")
		/* For now, dont call restart?
		if err := cm.FilClient.CheckOngoingTransfer(ctx, maddr, status); err != nil {
			cm.recordDealFailure(&DealFailureError{
				Miner:   maddr,
				Phase:   "data-transfer",
				Message: fmt.Sprintf("error while checking transfer: %s", err),
				Content: content.ID,
				UserID:  d.UserID,
			})
			return DEAL_CHECK_UNKNOWN, nil // TODO: returning unknown==error here feels excessive
		}
		*/
		// expected, this is fine
	default:
		fmt.Printf("Unexpected data transfer state: %d (msg = %s)\n", status.Status, status.Message)
	}
	return DEAL_CHECK_PROGRESS, nil
}

func (cm *ContentManager) updateDealID(d *contentDeal, id int64) error {
	if err := cm.DB.Model(contentDeal{}).Where("id = ?", d.ID).Updates(map[string]interface{}{
		"deal_id":     id,
		"on_chain_at": time.Now(),
	}).Error; err != nil {
		return err
	}
	return nil
}

func (cm *ContentManager) dealHasExpired(ctx context.Context, d *contentDeal) (bool, error) {
	prop, err := cm.getProposalRecord(d.PropCid.CID)
	if err != nil {
		log.Warnf("failed to get proposal record for deal %d: %s", d.ID, err)

		if time.Since(d.CreatedAt) > time.Hour*24*14 {
			return true, nil
		}
		return false, err
	}

	head, err := cm.Api.ChainHead(ctx)
	if err != nil {
		return false, err
	}

	if prop.Proposal.StartEpoch < head.Height() {
		return true, nil
	}
	return false, nil
}

type transferStatusRecord struct {
	State    *filclient.ChannelState
	Shuttle  string
	Received time.Time
}

func (cm *ContentManager) GetTransferStatus(ctx context.Context, d *contentDeal, content *util.Content) (*filclient.ChannelState, error) {
	ctx, span := cm.tracer.Start(ctx, "getTransferStatus")
	defer span.End()

	if content.Location == constants.ContentLocationLocal {
		return cm.getLocalTransferStatus(ctx, d, content)
	}

	val, ok := cm.remoteTransferStatus.Get(d.ID)
	if !ok {
		// return nil, fmt.Errorf("no transfer status found for deal %d (loc: %s)", d.ID, content.Location)
		return nil, nil
	}

	tsr, ok := val.(*transferStatusRecord)
	if !ok {
		return nil, fmt.Errorf("invalid type placed in remote transfer status cache: %T", val)
	}
	return tsr.State, nil
}

func (cm *ContentManager) updateTransferStatus(ctx context.Context, loc string, dealdbid uint, st *filclient.ChannelState) {
	cm.remoteTransferStatus.Add(dealdbid, &transferStatusRecord{
		State:    st,
		Shuttle:  loc,
		Received: time.Now(),
	})
}

func (cm *ContentManager) getLocalTransferStatus(ctx context.Context, d *contentDeal, content *util.Content) (*filclient.ChannelState, error) {
	ccid := content.Cid.CID

	miner, err := d.MinerAddr()
	if err != nil {
		return nil, err
	}

	if d.DTChan != "" {
		return cm.FilClient.TransferStatusByID(ctx, d.DTChan)
	}

	chanst, err := cm.FilClient.TransferStatusForContent(ctx, ccid, miner)
	if err != nil && err != filclient.ErrNoTransferFound {
		return nil, err
	}

	if chanst != nil {
		if err := cm.DB.Model(contentDeal{}).Where("id = ?", d.ID).UpdateColumns(map[string]interface{}{
			"dt_chan": chanst.TransferID,
		}).Error; err != nil {
			return nil, err
		}
	}
	return chanst, nil
}

var ErrNotOnChainYet = fmt.Errorf("message not found on chain")

func (cm *ContentManager) getDealID(ctx context.Context, pubcid cid.Cid, d *contentDeal) (abi.DealID, error) {
	mlookup, err := cm.Api.StateSearchMsg(ctx, types.EmptyTSK, pubcid, 1000, false)
	if err != nil {
		return 0, xerrors.Errorf("could not find published deal on chain: %w", err)
	}

	if mlookup == nil {
		return 0, ErrNotOnChainYet
	}

	if mlookup.Message != pubcid {
		// TODO: can probably deal with this by checking the message contents?
		return 0, xerrors.Errorf("publish deal message was replaced on chain")
	}

	msg, err := cm.Api.ChainGetMessage(ctx, mlookup.Message)
	if err != nil {
		return 0, err
	}

	var params market.PublishStorageDealsParams
	if err := params.UnmarshalCBOR(bytes.NewReader(msg.Params)); err != nil {
		return 0, err
	}

	dealix := -1
	for i, pd := range params.Deals {
		pd := pd
		nd, err := cborutil.AsIpld(&pd)
		if err != nil {
			return 0, xerrors.Errorf("failed to compute deal proposal ipld node: %w", err)
		}

		if nd.Cid() == d.PropCid.CID {
			dealix = i
			break
		}
	}

	if dealix == -1 {
		return 0, fmt.Errorf("our deal was not in this publish message")
	}

	if mlookup.Receipt.ExitCode != 0 {
		return 0, xerrors.Errorf("miners deal publish failed (exit: %d)", mlookup.Receipt.ExitCode)
	}

	var retval market.PublishStorageDealsReturn
	if err := retval.UnmarshalCBOR(bytes.NewReader(mlookup.Receipt.Return)); err != nil {
		return 0, xerrors.Errorf("publish deal return was improperly formatted: %w", err)
	}

	if len(retval.IDs) != len(params.Deals) {
		return 0, fmt.Errorf("return value from publish deals did not match length of params")
	}
	return retval.IDs[dealix], nil
}

func (cm *ContentManager) repairDeal(d *contentDeal) error {
	if d.DealID != 0 {
		log.Debugw("miner faulted on deal", "deal", d.DealID, "content", d.Content, "miner", d.Miner)
		maddr, err := d.MinerAddr()
		if err != nil {
			log.Errorf("failed to get miner address from deal (%s): %w", d.Miner, err)
		}

		if err := cm.recordDealFailure(&DealFailureError{
			Miner:               maddr,
			Phase:               "fault",
			Message:             fmt.Sprintf("miner faulted on deal: %d", d.DealID),
			Content:             d.Content,
			UserID:              d.UserID,
			DealProtocolVersion: d.DealProtocolVersion,
			MinerVersion:        d.MinerVersion,
		}); err != nil {
			return err
		}
	}

	log.Debugw("repair deal", "propcid", d.PropCid.CID, "miner", d.Miner, "content", d.Content)
	if err := cm.DB.Model(contentDeal{}).Where("id = ?", d.ID).UpdateColumns(map[string]interface{}{
		"failed":    true,
		"failed_at": time.Now(),
	}).Error; err != nil {
		return err
	}
	return nil
}

var priceMax abi.TokenAmount

func init() {
	max, err := types.ParseFIL("0.00000003")
	if err != nil {
		panic(err)
	}
	priceMax = abi.TokenAmount(max)
}

func (cm *ContentManager) priceIsTooHigh(price abi.TokenAmount) bool {
	if cm.cfg.Deal.IsVerified {
		return types.BigCmp(price, abi.NewTokenAmount(0)) > 0
	}
	return types.BigCmp(price, priceMax) > 0
}

type proposalRecord struct {
	PropCid util.DbCID `gorm:"index"`
	Data    []byte
}

func (cm *ContentManager) makeDealsForContent(ctx context.Context, content util.Content, count int, exclude map[address.Address]bool) error {
	ctx, span := cm.tracer.Start(ctx, "makeDealsForContent", trace.WithAttributes(
		attribute.Int64("content", int64(content.ID)),
		attribute.Int("count", count),
	))
	defer span.End()

	if content.Size < (256 << 10) {
		return fmt.Errorf("content %d too small to make deals for. (size: %d)", content.ID, content.Size)
	}

	if content.Offloaded {
		return fmt.Errorf("cannot make more deals for offloaded content, must retrieve first")
	}

	_, _, pieceSize, err := cm.getPieceCommitment(ctx, content.Cid.CID, cm.Blockstore)
	if err != nil {
		return xerrors.Errorf("failed to compute piece commitment while making deals %d: %w", content.ID, err)
	}

	miners, err := cm.pickMiners(ctx, count*2, pieceSize.Padded(), exclude, true)
	if err != nil {
		return err
	}

	var readyDeals []deal
	for _, m := range miners {
		price := m.ask.GetPrice(cm.cfg.Deal.IsVerified)
		prop, err := cm.FilClient.MakeDeal(ctx, m.address, content.Cid.CID, price, m.ask.MinPieceSize, cm.cfg.Deal.Duration, cm.cfg.Deal.IsVerified)
		if err != nil {
			return xerrors.Errorf("failed to construct a deal proposal: %w", err)
		}

		dp, err := cm.putProposalRecord(prop.DealProposal)
		if err != nil {
			return err
		}

		propnd, err := cborutil.AsIpld(prop.DealProposal)
		if err != nil {
			return xerrors.Errorf("failed to compute deal proposal ipld node: %w", err)
		}

		dealUUID := uuid.New()
		cd := &contentDeal{
			Content:             content.ID,
			PropCid:             util.DbCID{CID: propnd.Cid()},
			DealUUID:            dealUUID.String(),
			Miner:               m.address.String(),
			Verified:            cm.cfg.Deal.IsVerified,
			UserID:              content.UserID,
			DealProtocolVersion: m.dealProtocolVersion,
			MinerVersion:        m.ask.MinerVersion,
		}

		if err := cm.DB.Create(cd).Error; err != nil {
			return xerrors.Errorf("failed to create database entry for deal: %w", err)
		}

		// Send the deal proposal to the storage provider
		var cleanupDealPrep func() error
		var propPhase bool
		isPushTransfer := m.dealProtocolVersion == filclient.DealProtocolv110

		switch m.dealProtocolVersion {
		case filclient.DealProtocolv110:
			propPhase, err = cm.FilClient.SendProposalV110(ctx, *prop, propnd.Cid())
		case filclient.DealProtocolv120:
			cleanupDealPrep, propPhase, err = cm.sendProposalV120(ctx, content.Location, *prop, propnd.Cid(), dealUUID, cd.ID)
		default:
			err = fmt.Errorf("unrecognized deal protocol %s", m.dealProtocolVersion)
		}

		if err != nil {
			// Clean up the contentDeal database entry
			if err := cm.DB.Delete(&contentDeal{}, cd).Error; err != nil {
				return fmt.Errorf("failed to delete content deal from db: %w", err)
			}

			// Clean up the proposal database entry
			if err := cm.DB.Delete(&proposalRecord{}, dp).Error; err != nil {
				return fmt.Errorf("failed to delete deal proposal from db: %w", err)
			}

			// Clean up the preparation for deal request - deal protocol v120
			if cleanupDealPrep != nil {
				if err := cleanupDealPrep(); err != nil {
					log.Errorw("cleaning up deal prepared request", "error", err)
				}
			}

			// Record a deal failure
			phase := "send-proposal"
			if propPhase {
				phase = "propose"
			}

			if err = cm.recordDealFailure(&DealFailureError{
				Miner:               m.address,
				Phase:               phase,
				Message:             err.Error(),
				Content:             content.ID,
				UserID:              content.UserID,
				DealProtocolVersion: m.dealProtocolVersion,
				MinerVersion:        m.ask.MinerVersion,
			}); err != nil {
				log.Errorw("failed to record deail failure", "error", err)
			}
			continue
		}

		readyDeals = append(readyDeals, deal{minerAddr: m.address, isPushTransfer: isPushTransfer, contentDeal: cd})
		if len(readyDeals) >= count {
			break
		}
	}

	// Now start up some data transfers!
	// note: its okay if we dont start all the data transfers, we can just do it next time around
	for _, deal := range readyDeals {
		// If the data transfer is a pull transfer, we don't need to explicitly
		// start the transfer (the Storage Provider will start pulling data as
		// soon as it accepts the proposal)
		if !deal.isPushTransfer {
			continue
		}

		if err := cm.StartDataTransfer(ctx, deal.contentDeal); err != nil {
			log.Errorw("failed to start data transfer", "err", err, "miner", deal.minerAddr)
			continue
		}
	}
	return nil
}

func (cm *ContentManager) sendProposalV120(ctx context.Context, contentLoc string, netprop network.Proposal, propCid cid.Cid, dealUUID uuid.UUID, dbid uint) (func() error, bool, error) {
	// In deal protocol v120 the transfer will be initiated by the
	// storage provider (a pull transfer) so we need to prepare for
	// the data request

	// Create an auth token to be used in the request
	authToken, err := httptransport.GenerateAuthToken()
	if err != nil {
		return nil, false, xerrors.Errorf("generating auth token for deal: %w", err)
	}

	rootCid := netprop.Piece.Root
	size := netprop.Piece.RawBlockSize
	var announceAddr multiaddr.Multiaddr
	if contentLoc == constants.ContentLocationLocal {
		if len(cm.Node.Config.AnnounceAddrs) == 0 {
			return nil, false, xerrors.Errorf("cannot serve deal data: no announce address configured for estuary node")
		}

		addrstr := cm.Node.Config.AnnounceAddrs[0] + "/p2p/" + cm.Node.Host.ID().String()
		announceAddr, err = multiaddr.NewMultiaddr(addrstr)
		if err != nil {
			return nil, false, xerrors.Errorf("cannot parse announce address '%s': %w", addrstr, err)
		}

		// Add an auth token for the data to the auth DB
		err := cm.FilClient.Libp2pTransferMgr.PrepareForDataRequest(ctx, dbid, authToken, propCid, rootCid, size)
		if err != nil {
			return nil, false, xerrors.Errorf("preparing for data request: %w", err)
		}
	} else {
		// first check if shuttle is online
		if !cm.shuttleIsOnline(contentLoc) {
			return nil, false, xerrors.Errorf("shuttle is not online: %s", contentLoc)
		}

		addrInfo := cm.shuttleAddrInfo(contentLoc)
		// TODO: This is the address that the shuttle reports to the Estuary
		// primary node, but is it ok if it's also the address reported
		// as where to download files publically? If it's a public IP does
		// that mean that messages from Estuary primary node would go through
		// public internet to get to shuttle?
		if addrInfo == nil || len(addrInfo.Addrs) == 0 {
			return nil, false, xerrors.Errorf("no address found for shuttle: %s", contentLoc)
		}
		addrstr := addrInfo.Addrs[0].String() + "/p2p/" + addrInfo.ID.String()
		announceAddr, err = multiaddr.NewMultiaddr(addrstr)
		if err != nil {
			return nil, false, xerrors.Errorf("cannot parse announce address '%s': %w", addrstr, err)
		}

		// If the content is not on the primary estuary node (it's on a shuttle)
		// The Storage Provider will pull the data from the shuttle,
		// so add an auth token for the data to the shuttle's auth DB
		err := cm.sendPrepareForDataRequestCommand(ctx, contentLoc, dbid, authToken, propCid, rootCid, size)
		if err != nil {
			return nil, false, xerrors.Errorf("sending prepare for data request command to shuttle: %w", err)
		}
	}

	cleanup := func() error {
		if contentLoc == constants.ContentLocationLocal {
			return cm.FilClient.Libp2pTransferMgr.CleanupPreparedRequest(ctx, dbid, authToken)
		}
		return cm.sendCleanupPreparedRequestCommand(ctx, contentLoc, dbid, authToken)
	}

	// Send the deal proposal to the storage provider
	propPhase, err := cm.FilClient.SendProposalV120(ctx, dbid, netprop, dealUUID, announceAddr, authToken)
	return cleanup, propPhase, err
}

func (cm *ContentManager) makeDealWithMiner(ctx context.Context, content util.Content, miner address.Address) (uint, error) {
	ctx, span := cm.tracer.Start(ctx, "makeDealWithMiner", trace.WithAttributes(
		attribute.Int64("content", int64(content.ID)),
		attribute.Stringer("miner", miner),
	))
	defer span.End()

	if content.Offloaded {
		return 0, fmt.Errorf("cannot make more deals for offloaded content, must retrieve first")
	}

	// if it's a shuttle content and the shuttle is not online, do not proceed
	if content.Location != constants.ContentLocationLocal && !cm.shuttleIsOnline(content.Location) {
		return 0, fmt.Errorf("content shuttle: %s, is not online", content.Location)
	}

	proto, err := cm.FilClient.DealProtocolForMiner(ctx, miner)
	if err != nil {
		return 0, cm.recordDealFailure(&DealFailureError{
			Miner:   miner,
			Phase:   "deal-protocol-version",
			Message: err.Error(),
			Content: content.ID,
			UserID:  content.UserID,
		})
	}

	_, ok := cm.EnabledDealProtocolsVersions[proto]
	if !ok {
		err = fmt.Errorf("miner deal protocol:%s is not currently enabeld", proto)
		return 0, cm.recordDealFailure(&DealFailureError{
			Miner:   miner,
			Phase:   "deal-protocol-version",
			Message: err.Error(),
			Content: content.ID,
			UserID:  content.UserID,
		})
	}

	ask, err := cm.getAsk(ctx, miner, 0)
	if err != nil {
		var clientErr *filclient.Error
		if !(xerrors.As(err, &clientErr) && clientErr.Code == filclient.ErrLotusError) {
			if err := cm.recordDealFailure(&DealFailureError{
				Miner:               miner,
				Phase:               "query-ask",
				Message:             err.Error(),
				Content:             content.ID,
				UserID:              content.UserID,
				DealProtocolVersion: proto,
			}); err != nil {
				return 0, xerrors.Errorf("failed to record deal failure: %w", err)
			}
		}
		return 0, xerrors.Errorf("failed to get ask for miner %s: %w", miner, err)
	}

	price := ask.PriceBigInt
	if cm.cfg.Deal.IsVerified {
		price = ask.VerifiedPriceBigInt
	}

	if cm.priceIsTooHigh(price) {
		return 0, fmt.Errorf("miners price is too high: %s %s", miner, price)
	}

	prop, err := cm.FilClient.MakeDeal(ctx, miner, content.Cid.CID, price, ask.MinPieceSize, cm.cfg.Deal.Duration, cm.cfg.Deal.IsVerified)
	if err != nil {
		return 0, xerrors.Errorf("failed to construct a deal proposal: %w", err)
	}

	dp, err := cm.putProposalRecord(prop.DealProposal)
	if err != nil {
		return 0, err
	}

	propnd, err := cborutil.AsIpld(prop.DealProposal)
	if err != nil {
		return 0, xerrors.Errorf("failed to compute deal proposal ipld node: %w", err)
	}

	dealUUID := uuid.New()
	deal := &contentDeal{
		Content:             content.ID,
		PropCid:             util.DbCID{CID: propnd.Cid()},
		DealUUID:            dealUUID.String(),
		Miner:               miner.String(),
		Verified:            cm.cfg.Deal.IsVerified,
		UserID:              content.UserID,
		DealProtocolVersion: proto,
		MinerVersion:        ask.MinerVersion,
	}

	if err := cm.DB.Create(deal).Error; err != nil {
		return 0, xerrors.Errorf("failed to create database entry for deal: %w", err)
	}

	// Send the deal proposal to the storage provider
	var cleanupDealPrep func() error
	var propPhase bool
	isPushTransfer := proto == filclient.DealProtocolv110

	switch proto {
	case filclient.DealProtocolv110:
		propPhase, err = cm.FilClient.SendProposalV110(ctx, *prop, propnd.Cid())
	case filclient.DealProtocolv120:
		cleanupDealPrep, propPhase, err = cm.sendProposalV120(ctx, content.Location, *prop, propnd.Cid(), dealUUID, deal.ID)
	default:
		err = fmt.Errorf("unrecognized deal protocol %s", proto)
	}

	if err != nil {
		// Clean up the database entry
		if err := cm.DB.Delete(&contentDeal{}, deal).Error; err != nil {
			return 0, fmt.Errorf("failed to delete content deal from db: %w", err)
		}

		// Clean up the proposal database entry
		if err := cm.DB.Delete(&proposalRecord{}, dp).Error; err != nil {
			return 0, fmt.Errorf("failed to delete deal proposal from db: %w", err)
		}

		// Clean up the preparation for deal request
		if cleanupDealPrep != nil {
			if err := cleanupDealPrep(); err != nil {
				log.Errorw("cleaning up deal prepared request", "error", err)
			}
		}

		// Record a deal failure
		phase := "send-proposal"
		if propPhase {
			phase = "propose"
		}
		if err := cm.recordDealFailure(&DealFailureError{
			Miner:               miner,
			Phase:               phase,
			Message:             err.Error(),
			Content:             content.ID,
			UserID:              content.UserID,
			DealProtocolVersion: proto,
			MinerVersion:        ask.MinerVersion,
		}); err != nil {
			return 0, fmt.Errorf("failed to record deal failure: %w", err)
		}
		return 0, err
	}

	// If the data transfer is a pull transfer, we don't need to explicitly
	// start the transfer (the Storage Provider will start pulling data as
	// soon as it accepts the proposal)
	if !isPushTransfer {
		return deal.ID, nil
	}

	// It's a push transfer, so start the data transfer
	if err := cm.StartDataTransfer(ctx, deal); err != nil {
		return 0, fmt.Errorf("failed to start data transfer: %w", err)
	}
	return deal.ID, nil
}

func (cm *ContentManager) StartDataTransfer(ctx context.Context, cd *contentDeal) error {
	var cont util.Content
	if err := cm.DB.First(&cont, "id = ?", cd.Content).Error; err != nil {
		return err
	}

	if cont.Location != constants.ContentLocationLocal {
		return cm.sendStartTransferCommand(ctx, cont.Location, cd, cont.Cid.CID)
	}

	miner, err := cd.MinerAddr()
	if err != nil {
		return err
	}

	chanid, err := cm.FilClient.StartDataTransfer(ctx, miner, cd.PropCid.CID, cont.Cid.CID)
	if err != nil {
		if oerr := cm.recordDealFailure(&DealFailureError{
			Miner:               miner,
			Phase:               "start-data-transfer",
			Message:             err.Error(),
			Content:             cont.ID,
			UserID:              cont.UserID,
			DealProtocolVersion: cd.DealProtocolVersion,
			MinerVersion:        cd.MinerVersion,
		}); oerr != nil {
			return oerr
		}
		return nil
	}

	cd.DTChan = chanid.String()

	if err := cm.DB.Model(contentDeal{}).Where("id = ?", cd.ID).UpdateColumns(map[string]interface{}{
		"dt_chan":           chanid.String(),
		"transfer_started":  time.Now(),
		"transfer_finished": time.Time{},
	}).Error; err != nil {
		return xerrors.Errorf("failed to update deal with channel ID: %w", err)
	}

	log.Debugw("Started data transfer", "chanid", chanid)
	return nil
}

func (cm *ContentManager) putProposalRecord(dealprop *marketv8.ClientDealProposal) (*proposalRecord, error) {
	nd, err := cborutil.AsIpld(dealprop)
	if err != nil {
		return nil, err
	}

	dp := &proposalRecord{
		PropCid: util.DbCID{CID: nd.Cid()},
		Data:    nd.RawData(),
	}

	if err := cm.DB.Create(dp).Error; err != nil {
		return nil, err
	}
	return dp, nil
}

func (cm *ContentManager) getProposalRecord(propCid cid.Cid) (*market.ClientDealProposal, error) {
	var proprec proposalRecord
	if err := cm.DB.First(&proprec, "prop_cid = ?", propCid.Bytes()).Error; err != nil {
		return nil, err
	}

	var prop market.ClientDealProposal
	if err := prop.UnmarshalCBOR(bytes.NewReader(proprec.Data)); err != nil {
		return nil, err
	}

	return &prop, nil
}

func (cm *ContentManager) recordDealFailure(dfe *DealFailureError) error {
	log.Debugw("deal failure error", "miner", dfe.Miner, "phase", dfe.Phase, "msg", dfe.Message, "content", dfe.Content)
	rec := dfe.Record()
	return cm.DB.Create(rec).Error
}

type DealFailureError struct {
	Miner               address.Address
	Phase               string
	Message             string
	Content             uint
	UserID              uint
	MinerAddress        string
	DealProtocolVersion protocol.ID
	MinerVersion        string
}

type dfeRecord struct {
	gorm.Model
	Miner               string      `json:"miner"`
	Phase               string      `json:"phase"`
	Message             string      `json:"message"`
	Content             uint        `json:"content" gorm:"index"`
	MinerVersion        string      `json:"minerVersion"`
	UserID              uint        `json:"user_id" gorm:"index"`
	DealProtocolVersion protocol.ID `json:"deal_protocol_version"`
}

func (dfe *DealFailureError) Record() *dfeRecord {
	return &dfeRecord{
		Miner:               dfe.Miner.String(),
		Phase:               dfe.Phase,
		Message:             dfe.Message,
		Content:             dfe.Content,
		UserID:              dfe.UserID,
		MinerVersion:        dfe.MinerVersion,
		DealProtocolVersion: dfe.DealProtocolVersion,
	}
}

func (dfe *DealFailureError) Error() string {
	return fmt.Sprintf("deal with miner %s failed in phase %s: %s", dfe.Message, dfe.Phase, dfe.Message)
}

type PieceCommRecord struct {
	Data    util.DbCID `gorm:"unique"`
	Piece   util.DbCID
	CarSize uint64
	Size    abi.UnpaddedPieceSize
}

func (cm *ContentManager) lookupPieceCommRecord(data cid.Cid) (*PieceCommRecord, error) {
	var pcrs []PieceCommRecord
	if err := cm.DB.Find(&pcrs, "data = ?", data.Bytes()).Error; err != nil {
		return nil, err
	}

	if len(pcrs) == 0 {
		return nil, nil
	}

	pcr := pcrs[0]
	if !pcr.Piece.CID.Defined() {
		return nil, fmt.Errorf("got an undefined thing back from database")
	}

	return &pcr, nil
}

// calculateCarSize works out the CAR size using the cids and block sizes
// for the content stored in the DB
func (cm *ContentManager) calculateCarSize(ctx context.Context, data cid.Cid) (uint64, error) {
	_, span := cm.tracer.Start(ctx, "calculateCarSize")
	defer span.End()

	var objects []util.Object
	where := "id in (select object from obj_refs where content = (select id from contents where cid = ?))"
	if err := cm.DB.Find(&objects, where, data.Bytes()).Error; err != nil {
		return 0, err
	}

	if len(objects) == 0 {
		return 0, fmt.Errorf("not found")
	}

	os := make([]util.CarObject, len(objects))
	for i, o := range objects {
		os[i] = util.CarObject{Size: uint64(o.Size), Cid: o.Cid.CID}
	}

	return util.CalculateCarSize(data, os)
}

var ErrWaitForRemoteCompute = fmt.Errorf("waiting for remote commP computation")

func (cm *ContentManager) runPieceCommCompute(ctx context.Context, data cid.Cid, bs blockstore.Blockstore) (cid.Cid, uint64, abi.UnpaddedPieceSize, error) {
	var cont util.Content
	if err := cm.DB.First(&cont, "cid = ?", data.Bytes()).Error; err != nil {
		return cid.Undef, 0, 0, err
	}

	if cont.Location != constants.ContentLocationLocal {
		if err := cm.sendShuttleCommand(ctx, cont.Location, &drpc.Command{
			Op: drpc.CMD_ComputeCommP,
			Params: drpc.CmdParams{
				ComputeCommP: &drpc.ComputeCommP{
					Data: data,
				},
			},
		}); err != nil {
			return cid.Undef, 0, 0, err
		}

		return cid.Undef, 0, 0, ErrWaitForRemoteCompute
	}

	log.Debugw("computing piece commitment", "data", cont.Cid.CID)
	return filclient.GeneratePieceCommitmentFFI(ctx, data, bs)
}

func (cm *ContentManager) getPieceCommitment(ctx context.Context, data cid.Cid, bs blockstore.Blockstore) (cid.Cid, uint64, abi.UnpaddedPieceSize, error) {
	_, span := cm.tracer.Start(ctx, "getPieceComm")
	defer span.End()

	// Get the piece comm record from the DB
	pcr, err := cm.lookupPieceCommRecord(data)
	if err != nil {
		return cid.Undef, 0, 0, err
	}
	if pcr != nil {
		if pcr.CarSize > 0 {
			return pcr.Piece.CID, pcr.CarSize, pcr.Size, nil
		}

		// The CAR size field was added later, so if it's not on the piece comm
		// record, calculate it
		carSize, err := cm.calculateCarSize(ctx, data)
		if err != nil {
			return cid.Undef, 0, 0, xerrors.Errorf("failed to calculate CAR size: %w", err)
		}

		pcr.CarSize = carSize

		// Save updated car size to DB
		cm.DB.Model(PieceCommRecord{}).Where("piece = ?", pcr.Piece).UpdateColumns(map[string]interface{}{
			"car_size": carSize,
		}) //nolint:errcheck

		return pcr.Piece.CID, pcr.CarSize, pcr.Size, nil
	}

	// The piece comm record isn't in the DB so calculate it
	pc, carSize, size, err := cm.runPieceCommCompute(ctx, data, bs)
	if err != nil {
		return cid.Undef, 0, 0, xerrors.Errorf("failed to generate piece commitment: %w", err)
	}

	opcr := PieceCommRecord{
		Data:    util.DbCID{CID: data},
		Piece:   util.DbCID{CID: pc},
		CarSize: carSize,
		Size:    size,
	}

	if err := cm.DB.Clauses(clause.OnConflict{DoNothing: true}).Create(&opcr).Error; err != nil {
		return cid.Undef, 0, 0, err
	}

	return pc, carSize, size, nil
}

func (cm *ContentManager) RefreshContentForCid(ctx context.Context, c cid.Cid) (blocks.Block, error) {
	ctx, span := cm.tracer.Start(ctx, "refreshForCid", trace.WithAttributes(
		attribute.Stringer("cid", c),
	))
	defer span.End()

	var obj util.Object
	if err := cm.DB.First(&obj, "cid = ?", c.Bytes()).Error; err != nil {
		return nil, xerrors.Errorf("failed to get object from db: %s", err)
	}

	var refs []util.ObjRef
	if err := cm.DB.Find(&refs, "object = ?", obj.ID).Error; err != nil {
		return nil, err
	}

	var contentToFetch uint
	switch len(refs) {
	case 0:
		return nil, xerrors.Errorf("have no object references for object %d in database", obj.ID)
	case 1:
		// easy case, fetch this thing.
		contentToFetch = refs[0].Content
	default:
		// have more than one reference for the same object. Need to pick one to retrieve

		// if one of the referenced contents has the requested cid as its root, then we should probably fetch that one

		var contents []util.Content
		if err := cm.DB.Find(&contents, "cid = ?", c.Bytes()).Error; err != nil {
			return nil, err
		}

		if len(contents) == 0 {
			// okay, this isnt anythings root cid. Just pick one I guess?
			contentToFetch = refs[0].Content
		} else {
			// good, this is a root cid, lets fetch that one.
			contentToFetch = contents[0].ID
		}
	}

	ch := cm.NotifyBlockstore.WaitFor(ctx, c)

	go func() {
		if err := cm.retrieveContent(ctx, contentToFetch); err != nil {
			log.Errorf("failed to retrieve content to serve %d: %w", contentToFetch, err)
		}
	}()

	select {
	case blk := <-ch:
		return blk, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (cm *ContentManager) RefreshContent(ctx context.Context, cont uint) error {
	ctx, span := cm.tracer.Start(ctx, "refreshContent")
	defer span.End()

	// TODO: this retrieval needs to mark all of its content as 'referenced'
	// until we can update its offloading status in the database
	var c util.Content
	if err := cm.DB.First(&c, "id = ?", cont).Error; err != nil {
		return err
	}

	loc, err := cm.selectLocationForRetrieval(ctx, c)
	if err != nil {
		return err
	}
	log.Infof("refreshing content %d onto shuttle %s", cont, loc)

	switch loc {
	case constants.ContentLocationLocal:
		if err := cm.retrieveContent(ctx, cont); err != nil {
			return err
		}

		if err := cm.DB.Model(&util.Content{}).Where("id = ?", cont).Update("offloaded", false).Error; err != nil {
			return err
		}

		if err := cm.DB.Model(&util.ObjRef{}).Where("content = ?", cont).Update("offloaded", 0).Error; err != nil {
			return err
		}
	default:
		return cm.sendRetrieveContentMessage(ctx, loc, c)
	}

	return nil
}

func (cm *ContentManager) sendRetrieveContentMessage(ctx context.Context, loc string, cont util.Content) error {
	return fmt.Errorf("not retrieving content yet until implementation is finished")
	/*
		var activeDeals []contentDeal
		if err := cm.DB.Find(&activeDeals, "content = ? and not failed and deal_id > 0", cont.ID).Error; err != nil {
			return err
		}

		if len(activeDeals) == 0 {
			log.Errorf("attempted to retrieve content %d but have no active deals", cont.ID)
			return fmt.Errorf("no active deals for content %d, cannot retrieve", cont.ID)
		}

		var deals []drpc.StorageDeal
		for _, d := range activeDeals {
			ma, err := d.MinerAddr()
			if err != nil {
				log.Errorf("failed to parse miner addres for deal %d: %s", d.ID, err)
				continue
			}

			deals = append(deals, drpc.StorageDeal{
				Miner:  ma,
				DealID: d.DealID,
			})
		}

		return cm.sendShuttleCommand(ctx, loc, &drpc.Command{
			Op: drpc.CMD_RetrieveContent,
			Params: drpc.CmdParams{
				RetrieveContent: &drpc.RetrieveContent{
					Content: cont.ID,
					Cid:     cont.Cid.CID,
					Deals:   deals,
				},
			},
		})
	*/
}

func (cm *ContentManager) retrieveContent(ctx context.Context, contentToFetch uint) error {
	ctx, span := cm.tracer.Start(ctx, "retrieveContent", trace.WithAttributes(
		attribute.Int("content", int(contentToFetch)),
	))
	defer span.End()

	cm.retrLk.Lock()
	prog, ok := cm.retrievalsInProgress[contentToFetch]
	if !ok {
		prog = &util.RetrievalProgress{
			Wait: make(chan struct{}),
		}
		cm.retrievalsInProgress[contentToFetch] = prog
	}
	cm.retrLk.Unlock()

	if ok {
		select {
		case <-prog.Wait:
			return prog.EndErr
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	defer func() {
		cm.retrLk.Lock()
		delete(cm.retrievalsInProgress, contentToFetch)
		cm.retrLk.Unlock()

		close(prog.Wait)
	}()

	if err := cm.runRetrieval(ctx, contentToFetch); err != nil {
		prog.EndErr = err
		return err
	}

	return nil
}

func (cm *ContentManager) indexForAggregate(ctx context.Context, aggregateID, contID uint) (int, error) {
	return 0, fmt.Errorf("selector based retrieval not yet implemented")
}

func (cm *ContentManager) runRetrieval(ctx context.Context, contentToFetch uint) error {
	ctx, span := cm.tracer.Start(ctx, "runRetrieval")
	defer span.End()

	var content util.Content
	if err := cm.DB.First(&content, contentToFetch).Error; err != nil {
		return err
	}

	index := -1
	if content.AggregatedIn > 0 {
		rootContent := content.AggregatedIn
		ix, err := cm.indexForAggregate(ctx, rootContent, contentToFetch)
		if err != nil {
			return err
		}
		index = ix
	}
	_ = index

	var deals []contentDeal
	if err := cm.DB.Find(&deals, "content = ? and not failed", contentToFetch).Error; err != nil {
		return err
	}

	if len(deals) == 0 {
		return xerrors.Errorf("no active deals for content %d we are trying to retrieve", contentToFetch)
	}

	// TODO: probably need some better way to pick miners to retrieve from...
	perm := rand.Perm(len(deals))
	for _, i := range perm {
		deal := deals[i]

		maddr, err := deal.MinerAddr()
		if err != nil {
			log.Errorf("deal %d had bad miner address: %s", deal.ID, err)
			continue
		}

		log.Infow("attempting retrieval deal", "content", contentToFetch, "miner", maddr)

		ask, err := cm.FilClient.RetrievalQuery(ctx, maddr, content.Cid.CID)
		if err != nil {
			span.RecordError(err)

			log.Errorw("failed to query retrieval", "miner", maddr, "content", content.Cid.CID, "err", err)
			if err := cm.recordRetrievalFailure(&util.RetrievalFailureRecord{
				Miner:   maddr.String(),
				Phase:   "query",
				Message: err.Error(),
				Content: content.ID,
				Cid:     content.Cid,
			}); err != nil {
				return xerrors.Errorf("failed to record deal failure: %w", err)
			}
			continue
		}
		log.Infow("got retrieval ask", "content", content, "miner", maddr, "ask", ask)

		if err := cm.tryRetrieve(ctx, maddr, content.Cid.CID, ask); err != nil {
			span.RecordError(err)
			log.Errorw("failed to retrieve content", "miner", maddr, "content", content.Cid.CID, "err", err)
			if err := cm.recordRetrievalFailure(&util.RetrievalFailureRecord{
				Miner:   maddr.String(),
				Phase:   "retrieval",
				Message: err.Error(),
				Content: content.ID,
				Cid:     content.Cid,
			}); err != nil {
				return xerrors.Errorf("failed to record deal failure: %w", err)
			}
			continue
		}

		// success
		return nil
	}

	return fmt.Errorf("failed to retrieve with any miner we have deals with")
}

func (s *Server) handleFixupDeals(c echo.Context) error {
	ctx := context.Background()
	var deals []contentDeal
	if err := s.DB.Order("deal_id desc").Find(&deals, "deal_id > 0 AND on_chain_at < ?", time.Now().Add(time.Hour*24*-100)).Error; err != nil {
		return err
	}

	gentime, err := time.Parse("2006-01-02 15:04:05", "2020-08-24 15:00:00")
	if err != nil {
		return err
	}

	head, err := s.Api.ChainHead(ctx)
	if err != nil {
		return err
	}

	sem := make(chan struct{}, 50)
	for _, dll := range deals {
		sem <- struct{}{}
		go func(d contentDeal) {
			defer func() {
				<-sem
			}()
			miner, err := d.MinerAddr()
			if err != nil {
				log.Error(err)
				return
			}

			subctx, cancel := context.WithTimeout(ctx, time.Second*5)
			defer cancel()

			// Get deal UUID, if there is one for the deal.
			// (There should be a UUID for deals made with deal protocol v1.2.0)
			var dealUUID *uuid.UUID
			if d.DealUUID != "" {
				parsed, parseErr := uuid.Parse(d.DealUUID)
				if parseErr != nil {
					log.Errorf("failed to get deal status: parsing deal uuid %s: %d %s: %s",
						d.DealUUID, d.ID, miner, parseErr)
					return
				}
				dealUUID = &parsed
			}
			provds, err := s.CM.FilClient.DealStatus(subctx, miner, d.PropCid.CID, dealUUID)
			if err != nil {
				log.Errorf("failed to get deal status: %d %s: %s", d.ID, miner, err)
				return
			}

			if provds.PublishCid == nil {
				log.Errorf("no publish cid for deal: %d", d.DealID)
				return
			}

			subctx2, cancel2 := context.WithTimeout(ctx, time.Second*20)
			defer cancel2()
			wait, err := s.Api.StateSearchMsg(subctx2, head.Key(), *provds.PublishCid, 100000, true)
			if err != nil {
				log.Errorf("failed to search message: %s", err)
				return
			}

			if wait == nil {
				log.Errorf("failed to find message: %d %s", d.ID, *provds.PublishCid)
				return
			}

			ontime := gentime.Add(time.Second * 30 * time.Duration(wait.Height))
			log.Infof("updating onchainat time for deal %d %d to %s", d.ID, d.DealID, ontime)
			if err := s.DB.Model(contentDeal{}).Where("id = ?", d.ID).Update("on_chain_at", ontime).Error; err != nil {
				log.Error(err)
				return
			}
		}(dll)
	}

	return nil
}

// addObjectsToDatabase creates entries on the estuary database for CIDs related to an already pinned CID (`root`)
// These entries are saved on the `objects` table, while metadata about the `root` CID is mostly kept on the `contents` table
// The link between the `objects` and `contents` tables is the `obj_refs` table
func (cm *ContentManager) addObjectsToDatabase(ctx context.Context, content uint, dserv ipld.NodeGetter, root cid.Cid, objects []*util.Object, loc string) error {
	_, span := cm.tracer.Start(ctx, "addObjectsToDatabase")
	defer span.End()

	if err := cm.DB.CreateInBatches(objects, 300).Error; err != nil {
		return xerrors.Errorf("failed to create objects in db: %w", err)
	}

	refs := make([]util.ObjRef, 0, len(objects))
	var totalSize int64
	for _, o := range objects {
		refs = append(refs, util.ObjRef{
			Content: content,
			Object:  o.ID,
		})
		totalSize += int64(o.Size)
	}

	span.SetAttributes(
		attribute.Int64("totalSize", totalSize),
		attribute.Int("numObjects", len(objects)),
	)

	if err := cm.DB.Model(util.Content{}).Where("id = ?", content).UpdateColumns(map[string]interface{}{
		"active":   true,
		"size":     totalSize,
		"pinning":  false,
		"location": loc,
	}).Error; err != nil {
		return xerrors.Errorf("failed to update content in database: %w", err)
	}

	if err := cm.DB.CreateInBatches(refs, 500).Error; err != nil {
		return xerrors.Errorf("failed to create refs: %w", err)
	}

	return nil
}

func (cm *ContentManager) migrateContentsToLocalNode(ctx context.Context, toMove []util.Content) error {
	for _, c := range toMove {
		if err := cm.migrateContentToLocalNode(ctx, c); err != nil {
			return err
		}
	}

	return nil
}

func (cm *ContentManager) migrateContentToLocalNode(ctx context.Context, cont util.Content) error {
	done, err := cm.safeFetchData(ctx, cont.Cid.CID)
	if err != nil {
		return fmt.Errorf("failed to fetch data: %w", err)
	}

	defer done()

	if err := cm.DB.Model(util.ObjRef{}).Where("id = ?", cont.ID).UpdateColumns(map[string]interface{}{
		"offloaded": 0,
	}).Error; err != nil {
		return err
	}

	if err := cm.DB.Model(util.Content{}).Where("id = ?", cont.ID).UpdateColumns(map[string]interface{}{
		"offloaded": false,
		"location":  constants.ContentLocationLocal,
	}).Error; err != nil {
		return err
	}

	// TODO: send unpin command to where the content was migrated from

	return nil
}

func (cm *ContentManager) safeFetchData(ctx context.Context, c cid.Cid) (func(), error) {
	// TODO: this method should mark each object it fetches as 'needed' before pulling the data so that
	// any concurrent deletion tasks can avoid deleting our data as we fetch it

	bserv := blockservice.New(cm.Blockstore, cm.Node.Bitswap)
	dserv := merkledag.NewDAGService(bserv)

	deref := func() {
		log.Warnf("TODO: implement safe fetch data protections")
	}

	cset := cid.NewSet()
	err := merkledag.Walk(ctx, func(ctx context.Context, c cid.Cid) ([]*ipld.Link, error) {
		node, err := dserv.Get(ctx, c)
		if err != nil {
			return nil, err
		}

		if c.Type() == cid.Raw {
			return nil, nil
		}

		return util.FilterUnwalkableLinks(node.Links()), nil
	}, c, cset.Visit, merkledag.Concurrent())
	if err != nil {
		return deref, err
	}

	return deref, nil
}

func (cm *ContentManager) addrInfoForShuttle(handle string) (*peer.AddrInfo, error) {
	if handle == constants.ContentLocationLocal {
		return &peer.AddrInfo{
			ID:    cm.Host.ID(),
			Addrs: cm.Host.Addrs(),
		}, nil
	}

	cm.shuttlesLk.Lock()
	defer cm.shuttlesLk.Unlock()
	conn, ok := cm.shuttles[handle]
	if !ok {
		return nil, nil
	}

	return &conn.addrInfo, nil
}

func (cm *ContentManager) sendPrepareForDataRequestCommand(ctx context.Context, loc string, dbid uint, authToken string, propCid cid.Cid, payloadCid cid.Cid, size uint64) error {
	return cm.sendShuttleCommand(ctx, loc, &drpc.Command{
		Op: drpc.CMD_PrepareForDataRequest,
		Params: drpc.CmdParams{
			PrepareForDataRequest: &drpc.PrepareForDataRequest{
				DealDBID:    dbid,
				AuthToken:   authToken,
				ProposalCid: propCid,
				PayloadCid:  payloadCid,
				Size:        size,
			},
		},
	})
}

func (cm *ContentManager) sendCleanupPreparedRequestCommand(ctx context.Context, loc string, dbid uint, authToken string) error {
	return cm.sendShuttleCommand(ctx, loc, &drpc.Command{
		Op: drpc.CMD_CleanupPreparedRequest,
		Params: drpc.CmdParams{
			CleanupPreparedRequest: &drpc.CleanupPreparedRequest{
				DealDBID:  dbid,
				AuthToken: authToken,
			},
		},
	})
}

func (cm *ContentManager) sendStartTransferCommand(ctx context.Context, loc string, cd *contentDeal, datacid cid.Cid) error {
	miner, err := cd.MinerAddr()
	if err != nil {
		return err
	}
	return cm.sendShuttleCommand(ctx, loc, &drpc.Command{
		Op: drpc.CMD_StartTransfer,
		Params: drpc.CmdParams{
			StartTransfer: &drpc.StartTransfer{
				DealDBID:  cd.ID,
				ContentID: cd.Content,
				Miner:     miner,
				PropCid:   cd.PropCid.CID,
				DataCid:   datacid,
			},
		},
	})
}

func (cm *ContentManager) sendAggregateCmd(ctx context.Context, loc string, cont util.Content, aggr []uint, blob []byte) error {
	return cm.sendShuttleCommand(ctx, loc, &drpc.Command{
		Op: drpc.CMD_AggregateContent,
		Params: drpc.CmdParams{
			AggregateContent: &drpc.AggregateContent{
				DBID:     cont.ID,
				UserID:   cont.UserID,
				Contents: aggr,
				Root:     cont.Cid.CID,
				ObjData:  blob,
			},
		},
	})
}

func (cm *ContentManager) sendRequestTransferStatusCmd(ctx context.Context, loc string, dealid uint, chid string) error {
	return cm.sendShuttleCommand(ctx, loc, &drpc.Command{
		Op: drpc.CMD_ReqTxStatus,
		Params: drpc.CmdParams{
			ReqTxStatus: &drpc.ReqTxStatus{
				DealDBID: dealid,
				ChanID:   chid,
			},
		},
	})
}

func (cm *ContentManager) sendSplitContentCmd(ctx context.Context, loc string, cont uint, size int64) error {
	return cm.sendShuttleCommand(ctx, loc, &drpc.Command{
		Op: drpc.CMD_SplitContent,
		Params: drpc.CmdParams{
			SplitContent: &drpc.SplitContent{
				Content: cont,
				Size:    size,
			},
		},
	})
}

func (cm *ContentManager) sendConsolidateContentCmd(ctx context.Context, loc string, contents []util.Content) error {
	fromLocs := make(map[string]struct{})

	tc := &drpc.TakeContent{}
	for _, c := range contents {
		fromLocs[c.Location] = struct{}{}

		tc.Contents = append(tc.Contents, drpc.ContentFetch{
			ID:     c.ID,
			Cid:    c.Cid.CID,
			UserID: c.UserID,
		})
	}

	for handle := range fromLocs {
		ai, err := cm.addrInfoForShuttle(handle)
		if err != nil {
			return err
		}

		if ai == nil {
			log.Warnf("no addr info for node: %s", handle)
			continue
		}

		tc.Sources = append(tc.Sources, *ai)
	}

	return cm.sendShuttleCommand(ctx, loc, &drpc.Command{
		Op: drpc.CMD_TakeContent,
		Params: drpc.CmdParams{
			TakeContent: tc,
		},
	})
}

func (cm *ContentManager) sendUnpinCmd(ctx context.Context, loc string, conts []uint) error {
	return cm.sendShuttleCommand(ctx, loc, &drpc.Command{
		Op: drpc.CMD_UnpinContent,
		Params: drpc.CmdParams{
			UnpinContent: &drpc.UnpinContent{
				Contents: conts,
			},
		},
	})
}

func (cm *ContentManager) dealMakingDisabled() bool {
	cm.dealDisabledLk.Lock()
	defer cm.dealDisabledLk.Unlock()
	return cm.isDealMakingDisabled
}

func (cm *ContentManager) setDealMakingEnabled(enable bool) {
	cm.dealDisabledLk.Lock()
	defer cm.dealDisabledLk.Unlock()
	cm.isDealMakingDisabled = !enable
}

func (cm *ContentManager) splitContentLocal(ctx context.Context, cont util.Content, size int64) error {
	dserv := merkledag.NewDAGService(blockservice.New(cm.Node.Blockstore, nil))
	b := dagsplit.NewBuilder(dserv, uint64(size), 0)
	if err := b.Pack(ctx, cont.Cid.CID); err != nil {
		return err
	}

	cst := cbor.NewCborStore(cm.Node.Blockstore)

	var boxCids []cid.Cid
	for _, box := range b.Boxes() {
		cc, err := cst.Put(ctx, box)
		if err != nil {
			return err
		}

		boxCids = append(boxCids, cc)
	}

	for i, c := range boxCids {
		content := &util.Content{
			Cid:         util.DbCID{CID: c},
			Name:        fmt.Sprintf("%s-%d", cont.Name, i),
			Active:      true,
			Pinning:     true,
			UserID:      cont.UserID,
			Replication: cont.Replication,
			Location:    constants.ContentLocationLocal,
			DagSplit:    true,
			SplitFrom:   cont.ID,
		}

		if err := cm.DB.Create(content).Error; err != nil {
			return xerrors.Errorf("failed to track new content in database: %w", err)
		}

		if err := cm.addDatabaseTrackingToContent(ctx, content.ID, dserv, c, func(int64) {}); err != nil {
			return err
		}

		// queue splited contents
		go func() {
			log.Debugw("queuing splited content child", "parent_contID", cont.ID, "child_contID", content.ID)
			cm.toCheck(content.ID)
		}()
	}

	if err := cm.DB.Model(util.Content{}).Where("id = ?", cont.ID).UpdateColumns(map[string]interface{}{
		"dag_split": true,
		"active":    false,
		"size":      0,
	}).Error; err != nil {
		return err
	}

	return nil
}
