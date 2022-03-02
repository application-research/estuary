package main

import (
	"bytes"
	"container/heap"
	"context"
	"fmt"
	"math/rand"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	drpc "github.com/application-research/estuary/drpc"
	"github.com/application-research/estuary/node"
	"github.com/application-research/estuary/pinner"
	"github.com/application-research/estuary/util"
	dagsplit "github.com/application-research/estuary/util/dagsplit"
	"github.com/application-research/filclient"
	"github.com/filecoin-project/go-address"
	cborutil "github.com/filecoin-project/go-cbor-util"
	datatransfer "github.com/filecoin-project/go-data-transfer"
	"github.com/filecoin-project/go-fil-markets/storagemarket"
	"github.com/filecoin-project/go-fil-markets/storagemarket/network"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/big"
	"github.com/filecoin-project/lotus/api"
	"github.com/filecoin-project/lotus/chain/types"
	"github.com/filecoin-project/specs-actors/v6/actors/builtin/market"
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
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/xerrors"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

var defaultReplication = 6

const defaultContentSizeLimit = 34_000_000_000

// Making default deal duration be three weeks less than the maximum to ensure
// miners who start their deals early dont run into issues
const dealDuration = 1555200 - (2880 * 21)

type ContentManager struct {
	DB        *gorm.DB
	Api       api.Gateway
	FilClient *filclient.FilClient
	Provider  *batched.BatchProvidingSystem
	Node      *node.Node

	Host host.Host

	tracer trace.Tracer

	Blockstore       node.EstuaryBlockstore
	Tracker          *TrackingBlockstore
	NotifyBlockstore *node.NotifyBlockstore

	ToCheck  chan uint
	queueMgr *queueManager

	retrLk               sync.Mutex
	retrievalsInProgress map[uint]*retrievalProgress

	contentLk sync.RWMutex

	contentSizeLimit int64

	// Some fields for miner reputation management
	minerLk      sync.Mutex
	sortedMiners []address.Address
	lastComputed time.Time

	// deal bucketing stuff
	bucketLk sync.Mutex
	buckets  map[uint][]*contentStagingZone

	// some behavior flags
	FailDealOnTransferFailure bool

	dealDisabledLk       sync.Mutex
	isDealMakingDisabled bool

	contentAddingDisabled      bool
	localContentAddingDisabled bool

	hostname string

	pinJobs map[uint]*pinner.PinningOperation
	pinLk   sync.Mutex

	pinMgr *pinner.PinManager

	shuttlesLk sync.Mutex
	shuttles   map[string]*shuttleConnection

	remoteTransferStatus *lru.ARCCache

	inflightCids   map[cid.Cid]uint
	inflightCidsLk sync.Mutex
}

func (cm *ContentManager) isInflight(c cid.Cid) bool {
	cm.inflightCidsLk.Lock()
	defer cm.inflightCidsLk.Unlock()

	v, ok := cm.inflightCids[c]
	return ok && v > 0
}

// 90% of the unpadded data size for a 4GB piece
// the 10% gap is to accommodate car file packing overhead, can probably do this better
var individualDealThreshold = (abi.PaddedPieceSize(4<<30).Unpadded() * 9) / 10

var stagingZoneSizeLimit = (abi.PaddedPieceSize(16<<30).Unpadded() * 9) / 10

type contentStagingZone struct {
	ZoneOpened time.Time `json:"zoneOpened"`

	EarliestContent time.Time `json:"earliestContent"`
	CloseTime       time.Time `json:"closeTime"`

	Contents []Content `json:"contents"`

	MinSize int64 `json:"minSize"`
	MaxSize int64 `json:"maxSize"`

	MaxItems int `json:"maxItems"`

	CurSize int64 `json:"curSize"`

	User uint `json:"user"`

	ContID   uint   `json:"contentID"`
	Location string `json:"location"`

	lk sync.Mutex
}

func (cb *contentStagingZone) DeepCopy() *contentStagingZone {
	cb.lk.Lock()
	defer cb.lk.Unlock()
	cb2 := &contentStagingZone{
		ZoneOpened:      cb.ZoneOpened,
		EarliestContent: cb.EarliestContent,
		CloseTime:       cb.CloseTime,
		Contents:        make([]Content, len(cb.Contents)),
		MinSize:         cb.MinSize,
		MaxSize:         cb.MaxSize,
		MaxItems:        cb.MaxItems,
		CurSize:         cb.CurSize,
		User:            cb.User,
		ContID:          cb.ContID,
		Location:        cb.Location,
	}
	copy(cb2.Contents, cb.Contents)
	return cb2
}

func (cm *ContentManager) newContentStagingZone(user uint, loc string) (*contentStagingZone, error) {
	content := &Content{
		Size:        0,
		Name:        "aggregate",
		Active:      false,
		Pinning:     true,
		UserID:      user,
		Replication: defaultReplication,
		Aggregate:   true,
		Location:    loc,
	}

	if err := cm.DB.Create(content).Error; err != nil {
		return nil, err
	}

	return &contentStagingZone{
		ZoneOpened: time.Now(),
		CloseTime:  time.Now().Add(maxStagingZoneLifetime),
		MinSize:    int64(stagingZoneSizeLimit - (1 << 30)),
		MaxSize:    int64(stagingZoneSizeLimit),
		MaxItems:   maxBucketItems,
		User:       user,
		ContID:     content.ID,
		Location:   content.Location,
	}, nil
}

// amount of time a staging zone will remain open before we aggregate it into a piece of content
const maxStagingZoneLifetime = time.Hour * 8

// maximum amount of time a piece of content will go without either being aggregated or having a deal made for it
const maxContentAge = time.Hour * 24 * 7

// staging zones will remain open for at least this long after the last piece of content is added to them (unless they are full)
const stagingZoneKeepalive = time.Minute * 40

const minDealSize = 256 << 20

const maxBucketItems = 10000

func (cb *contentStagingZone) isReady() bool {
	if cb.CurSize < minDealSize {
		return false
	}

	// if its above the size requirement, go right ahead
	if cb.CurSize > cb.MinSize {
		return true
	}

	if time.Now().After(cb.CloseTime) {
		return true
	}

	if time.Since(cb.EarliestContent) > maxContentAge {
		return true
	}

	if len(cb.Contents) >= cb.MaxItems {
		return true
	}

	return false
}

func (cb *contentStagingZone) hasRoomForContent(c Content) bool {
	cb.lk.Lock()
	defer cb.lk.Unlock()

	if len(cb.Contents) >= cb.MaxItems {
		return false
	}

	return cb.CurSize+c.Size <= cb.MaxSize
}

func (cm *ContentManager) tryAddContent(cb *contentStagingZone, c Content) (bool, error) {
	cb.lk.Lock()
	defer cb.lk.Unlock()
	if cb.CurSize+c.Size > cb.MaxSize {
		return false, nil
	}

	if len(cb.Contents) >= cb.MaxItems {
		return false, nil
	}

	if err := cm.DB.Model(Content{}).
		Where("id = ?", c.ID).
		UpdateColumn("aggregated_in", cb.ContID).Error; err != nil {
		return false, err
	}

	if len(cb.Contents) == 0 || c.CreatedAt.Before(cb.EarliestContent) {
		cb.EarliestContent = c.CreatedAt
	}

	cb.Contents = append(cb.Contents, c)
	cb.CurSize += c.Size

	nowPlus := time.Now().Add(stagingZoneKeepalive)
	if cb.CloseTime.Before(nowPlus) {
		cb.CloseTime = nowPlus
	}

	return true, nil
}

func (cb *contentStagingZone) hasContent(c Content) bool {
	cb.lk.Lock()
	defer cb.lk.Unlock()

	for _, cont := range cb.Contents {
		if cont.ID == c.ID {
			return true
		}
	}
	return false
}

func NewContentManager(db *gorm.DB, api api.Gateway, fc *filclient.FilClient, tbs *TrackingBlockstore, nbs *node.NotifyBlockstore, prov *batched.BatchProvidingSystem, pinmgr *pinner.PinManager, nd *node.Node, hostname string) (*ContentManager, error) {
	cache, err := lru.NewARC(50000)
	if err != nil {
		return nil, err
	}

	var stages []Content
	if err := db.Find(&stages, "not active and pinning and aggregate").Error; err != nil {
		return nil, err
	}

	zones := make(map[uint][]*contentStagingZone)
	for _, c := range stages {
		z := &contentStagingZone{
			ZoneOpened: c.CreatedAt,
			CloseTime:  c.CreatedAt.Add(maxStagingZoneLifetime),
			MinSize:    int64(stagingZoneSizeLimit - (1 << 30)),
			MaxSize:    int64(stagingZoneSizeLimit),
			MaxItems:   maxBucketItems,
			User:       c.UserID,
			ContID:     c.ID,
			Location:   c.Location,
		}

		minClose := time.Now().Add(stagingZoneKeepalive)
		if z.CloseTime.Before(minClose) {
			z.CloseTime = minClose
		}

		var inzone []Content
		if err := db.Find(&inzone, "aggregated_in = ?", c.ID).Error; err != nil {
			return nil, err
		}

		z.Contents = inzone

		for _, zc := range inzone {
			// TODO: do some sanity checking that we havent messed up and added
			// too many items to this staging zone
			z.CurSize += zc.Size
		}

		zones[c.UserID] = append(zones[c.UserID], z)
	}

	cm := &ContentManager{
		Provider:             prov,
		DB:                   db,
		Api:                  api,
		FilClient:            fc,
		Blockstore:           tbs.Under().(node.EstuaryBlockstore),
		Host:                 nd.Host,
		Node:                 nd,
		NotifyBlockstore:     nbs,
		Tracker:              tbs,
		ToCheck:              make(chan uint, 100000),
		retrievalsInProgress: make(map[uint]*retrievalProgress),
		buckets:              zones,
		pinJobs:              make(map[uint]*pinner.PinningOperation),
		pinMgr:               pinmgr,
		remoteTransferStatus: cache,
		shuttles:             make(map[string]*shuttleConnection),
		contentSizeLimit:     defaultContentSizeLimit,
		hostname:             hostname,
		inflightCids:         make(map[cid.Cid]uint),
	}
	qm := newQueueManager(func(c uint) {
		cm.ToCheck <- c
	})

	cm.queueMgr = qm
	return cm, nil
}

func (cm *ContentManager) ContentWatcher() {
	if err := cm.startup(); err != nil {
		log.Errorf("failed to recheck existing content: %s", err)
	}

	timer := time.NewTimer(time.Minute * 5)

	for {
		select {
		case c := <-cm.ToCheck:
			var content Content
			if err := cm.DB.First(&content, "id = ?", c).Error; err != nil {
				log.Errorf("finding content %d in database: %s", c, err)
				continue
			}

			log.Infof("checking content: %d", content.ID)
			err := cm.ensureStorage(context.TODO(), content, func(dur time.Duration) {
				cm.queueMgr.add(content.ID, dur)
			})
			if err != nil {
				log.Errorf("failed to ensure replication of content %d: %s", content.ID, err)
				cm.queueMgr.add(content.ID, time.Minute*5)
			}

		case <-timer.C:
			log.Infow("content check queue", "length", len(cm.queueMgr.queue.elems), "nextEvent", cm.queueMgr.nextEvent)

			/*
				if err := cm.queueAllContent(); err != nil {
					log.Errorf("rechecking content: %s", err)
					continue
				}
			*/

			buckets := cm.popReadyStagingZone()
			for _, b := range buckets {
				if err := cm.aggregateContent(context.TODO(), b); err != nil {
					log.Errorf("content aggregation failed (bucket %d): %s", b.ContID, err)
					continue
				}
			}

			timer.Reset(time.Minute * 5)
		}
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
			qm.evtTimer.Reset(qe.checkTime.Sub(time.Now()))
			return
		}
	}
	qm.nextEvent = time.Time{}
}

func (cm *ContentManager) currentLocationForContent(c uint) (string, error) {
	var cont Content
	if err := cm.DB.First(&cont, "id = ?", c).Error; err != nil {
		return "", err
	}

	return cont.Location, nil
}

func (cm *ContentManager) stagedContentByLocation(ctx context.Context, b *contentStagingZone) (map[string][]Content, error) {
	out := make(map[string][]Content)
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
	contentByLoc := make(map[string][]Content)

	for _, c := range b.Contents {
		loc, err := cm.currentLocationForContent(c.ID)
		if err != nil {
			return err
		}

		contentByLoc[loc] = append(contentByLoc[loc], c)

		ntot := dataByLoc[loc] + c.Size
		dataByLoc[loc] = ntot

		// temp: dont ever migrate content back to primary instance for aggregation, always prefer elsewhere
		if ntot > curMax && loc != "local" {
			curMax = ntot
			primary = loc
		}
	}

	// okay, move everything to 'primary'
	var toMove []Content
	for loc, conts := range contentByLoc {
		if loc != primary {
			toMove = append(toMove, conts...)
		}
	}

	log.Infow("consolidating content to single location for aggregation", "user", b.User, "primary", primary, "numItems", len(toMove), "primaryWeight", curMax)
	if primary == "local" {
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

	if err := cm.DB.Model(Content{}).Where("id = ?", b.ContID).UpdateColumns(map[string]interface{}{
		"cid":  util.DbCID{ncid},
		"size": size,
	}).Error; err != nil {
		return err
	}

	var content Content
	if err := cm.DB.First(&content, "id = ?", b.ContID).Error; err != nil {
		return err
	}

	if loc == "local" {
		obj := &Object{
			Cid:  util.DbCID{ncid},
			Size: int(size),
		}
		if err := cm.DB.Create(obj).Error; err != nil {
			return err
		}

		if err := cm.DB.Create(&ObjRef{
			Content: b.ContID,
			Object:  obj.ID,
		}).Error; err != nil {
			return err
		}

		if err := cm.Blockstore.Put(ctx, dir); err != nil {
			return err
		}

		if err := cm.DB.Model(Content{}).Where("id = ?", b.ContID).UpdateColumns(map[string]interface{}{
			"active":  true,
			"pinning": false,
		}).Error; err != nil {
			return err
		}

		go func() {
			cm.ToCheck <- b.ContID
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

func (cm *ContentManager) createAggregate(ctx context.Context, conts []Content) (*merkledag.ProtoNode, error) {
	sort.Slice(conts, func(i, j int) bool {
		return conts[i].ID < conts[j].ID
	})

	log.Info("aggregating contents in staging zone into new content")
	dir := unixfs.EmptyDirNode()
	for _, c := range conts {
		dir.AddRawLink(fmt.Sprintf("%d-%s", c.ID, c.Name), &ipld.Link{
			Size: uint64(c.Size),
			Cid:  c.Cid.CID,
		})
	}

	return dir, nil
}

func (cm *ContentManager) startup() error {
	return cm.queueAllContent()
}

func (cm *ContentManager) queueAllContent() error {
	var allcontent []Content
	if err := cm.DB.Find(&allcontent, "active AND NOT aggregated_in > 0").Error; err != nil {
		return xerrors.Errorf("finding all content in database: %w", err)
	}

	log.Infof("queueing all content for checking: %d", len(allcontent))

	go func() {
		for _, c := range allcontent {
			log.Infof("queueing content: %d", c.ID)
			cm.ToCheck <- c.ID
		}
	}()
	/* TODO: this should be more correct, just testing the above out though to ensure the things from here are first in queue
	for _, c := range allcontent {
		cm.queueMgr.add(c.ID, 0)
	}
	*/

	return nil
}

type estimateResponse struct {
	Total *abi.TokenAmount
	Asks  []*minerStorageAsk
}

func (cm *ContentManager) estimatePrice(ctx context.Context, repl int, size abi.PaddedPieceSize, duration abi.ChainEpoch, verified bool) (*estimateResponse, error) {
	ctx, span := cm.tracer.Start(ctx, "estimatePrice", trace.WithAttributes(
		attribute.Int("replication", repl),
	))
	defer span.End()

	miners, err := cm.pickMiners(ctx, Content{}, repl, size, nil)
	if err != nil {
		return nil, err
	}
	if len(miners) == 0 {
		return nil, fmt.Errorf("failed to find any miners for estimating deal price")
	}

	var asks []*minerStorageAsk
	total := abi.NewTokenAmount(0)
	for _, m := range miners {
		ask, err := cm.getAsk(ctx, m, time.Minute*30)
		if err != nil {
			return nil, err
		}

		asks = append(asks, ask)

		var price *abi.TokenAmount
		if verified {
			p, err := ask.GetVerifiedPrice()
			if err != nil {
				return nil, err
			}
			price = p
		} else {
			p, err := ask.GetPrice()
			if err != nil {
				return nil, err
			}
			price = p
		}

		dealSize := size
		if dealSize < ask.MinPieceSize {
			dealSize = ask.MinPieceSize
		}

		cost, err := filclient.ComputePrice(*price, dealSize, duration)
		if err != nil {
			return nil, err
		}

		total = types.BigAdd(total, *cost)
	}

	return &estimateResponse{
		Total: &total,
		Asks:  asks,
	}, nil
}

type minerStorageAsk struct {
	gorm.Model    `json:"-"`
	Miner         string              `gorm:"unique" json:"miner"`
	Price         string              `json:"price"`
	VerifiedPrice string              `json:"verifiedPrice"`
	MinPieceSize  abi.PaddedPieceSize `json:"minPieceSize"`
	MaxPieceSize  abi.PaddedPieceSize `json:"maxPieceSize"`
}

func (msa *minerStorageAsk) GetPrice() (*types.BigInt, error) {
	v, err := types.BigFromString(msa.Price)
	if err != nil {
		return nil, err
	}

	return &v, nil
}

func (msa *minerStorageAsk) GetVerifiedPrice() (*types.BigInt, error) {
	v, err := types.BigFromString(msa.VerifiedPrice)
	if err != nil {
		return nil, err
	}

	return &v, nil
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

const topMinerSel = 15

func (cm *ContentManager) pickMiners(ctx context.Context, cont Content, n int, size abi.PaddedPieceSize, exclude map[address.Address]bool) ([]address.Address, error) {
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

	randminers, err := cm.randomMinerList()
	if err != nil {
		return nil, err
	}

	var out []address.Address
	for _, m := range randminers {
		if len(out) >= nrand {
			break
		}

		if exclude[m] {
			continue
		}

		exclude[m] = true

		ask, err := cm.getAsk(ctx, m, time.Minute*30)
		if err != nil {
			log.Errorf("getting ask from %s failed: %s", m, err)
			continue
		}

		if cm.sizeIsCloseEnough(size, ask.MinPieceSize) {
			out = append(out, m)
		}
	}

	sortedminers, err := cm.sortedMinerList()
	if err != nil {
		return nil, err
	}

	if len(sortedminers) > topMinerSel {
		sortedminers = sortedminers[:topMinerSel]
	}

	rand.Shuffle(len(sortedminers), func(i, j int) {
		sortedminers[i], sortedminers[j] = sortedminers[j], sortedminers[i]
	})

	for _, m := range sortedminers {
		if len(out) >= n {
			break
		}

		if exclude[m] {
			continue
		}

		ask, err := cm.getAsk(ctx, m, time.Minute*30)
		if err != nil {
			log.Errorf("getting ask from %s failed: %s", m, err)
			continue
		}

		if cm.sizeIsCloseEnough(size, ask.MinPieceSize) {
			out = append(out, m)
		}
	}

	return out, nil
}

func (cm *ContentManager) randomMinerList() ([]address.Address, error) {
	var dbminers []storageMiner
	if err := cm.DB.Find(&dbminers, "not suspended").Error; err != nil {
		return nil, err
	}

	out := make([]address.Address, 0, len(dbminers))
	for _, dbm := range dbminers {
		out = append(out, dbm.Address.Addr)
	}

	rand.Shuffle(len(dbminers), func(i, j int) {
		out[i], out[j] = out[j], out[i]
	})

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

	var msa minerStorageAsk
	if len(asks) > 0 {
		msa = asks[0]
	}

	if time.Since(msa.UpdatedAt) < maxCacheAge {
		return &msa, nil
	}

	netask, err := cm.FilClient.GetAsk(ctx, m)
	if err != nil {
		var clientErr *filclient.Error
		if !(xerrors.As(err, &clientErr) && clientErr.Code == filclient.ErrLotusError) {
			cm.recordDealFailure(&DealFailureError{
				Miner:   m,
				Phase:   "query-ask",
				Message: err.Error(),
			})
		}
		span.RecordError(err)
		return nil, err
	}

	if err := cm.updateMinerVersion(ctx, m); err != nil {
		log.Warnf("failed to update miner version: %s", err)
	}

	nmsa := toDBAsk(netask)

	nmsa.UpdatedAt = time.Now()

	if err := cm.DB.Clauses(clause.OnConflict{
		Columns: []clause.Column{
			{Name: "miner"},
		},
		DoUpdates: clause.AssignmentColumns([]string{"price", "verified_price", "min_piece_size", "updated_at"}),
	}).Create(nmsa).Error; err != nil {
		span.RecordError(err)
		return nil, err
	}

	return nmsa, nil
}

func (cm *ContentManager) updateMinerVersion(ctx context.Context, m address.Address) error {
	vers, err := cm.FilClient.GetMinerVersion(ctx, m)
	if err != nil {
		return err
	}

	var sm storageMiner
	if err := cm.DB.First(&sm, "address = ?", m.String()).Error; err != nil {
		if xerrors.Is(err, gorm.ErrRecordNotFound) {
			return nil
		}
		return err
	}

	if sm.Version == vers {
		return nil
	}

	if err := cm.DB.Model(storageMiner{}).Where("address = ?", m.String()).Update("version", vers).Error; err != nil {
		return err
	}

	return nil
}

func (cm *ContentManager) sizeIsCloseEnough(fsize, limit abi.PaddedPieceSize) bool {
	if fsize > limit {
		return true
	}

	/*
		if fsize*64 > limit {
			return true
		}
	*/

	return false
}

func toDBAsk(netask *network.AskResponse) *minerStorageAsk {
	return &minerStorageAsk{
		Miner:         netask.Ask.Ask.Miner.String(),
		Price:         netask.Ask.Ask.Price.String(),
		VerifiedPrice: netask.Ask.Ask.VerifiedPrice.String(),
		MinPieceSize:  netask.Ask.Ask.MinPieceSize,
		MaxPieceSize:  netask.Ask.Ask.MaxPieceSize,
	}
}

type contentDeal struct {
	gorm.Model
	Content          uint       `json:"content" gorm:"index:,option:CONCURRENTLY"`
	PropCid          util.DbCID `json:"propCid"`
	Miner            string     `json:"miner"`
	DealID           int64      `json:"dealId"`
	Failed           bool       `json:"failed"`
	Verified         bool       `json:"verified"`
	FailedAt         time.Time  `json:"failedAt,omitempty"`
	DTChan           string     `json:"dtChan" gorm:"index"`
	TransferStarted  time.Time  `json:"transferStarted"`
	TransferFinished time.Time  `json:"transferFinished"`

	OnChainAt time.Time `json:"onChainAt"`
	SealedAt  time.Time `json:"sealedAt"`
}

func (cd contentDeal) MinerAddr() (address.Address, error) {
	return address.NewFromString(cd.Miner)
}

var ErrNoChannelID = fmt.Errorf("no data transfer channel id in deal")

func (cd contentDeal) ChannelID() (datatransfer.ChannelID, error) {
	if cd.DTChan == "" {
		return datatransfer.ChannelID{}, ErrNoChannelID
	}

	parts := strings.Split(cd.DTChan, "-")
	if len(parts) != 3 {
		return datatransfer.ChannelID{}, fmt.Errorf("incorrectly formatted data transfer channel ID in contentDeal record")
	}

	initiator, err := peer.Decode(parts[0])
	if err != nil {
		return datatransfer.ChannelID{}, err
	}

	responder, err := peer.Decode(parts[1])
	if err != nil {
		return datatransfer.ChannelID{}, err
	}

	id, err := strconv.ParseUint(parts[2], 10, 64)
	if err != nil {
		return datatransfer.ChannelID{}, err
	}

	return datatransfer.ChannelID{
		Initiator: initiator,
		Responder: responder,
		ID:        datatransfer.TransferID(id),
	}, nil
}

func (cm *ContentManager) contentInStagingZone(ctx context.Context, content Content) bool {
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

func (cm *ContentManager) addContentToStagingZone(ctx context.Context, content Content) error {
	ctx, span := cm.tracer.Start(ctx, "stageContent")
	defer span.End()
	if content.AggregatedIn > 0 {
		log.Warnf("attempted to add content to staging zone that was already staged: %d (is in %d)", content.ID, content.AggregatedIn)
		return nil
	}

	log.Infof("adding content to staging zone: %d", content.ID)
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

const bucketingEnabled = true

const errDelay = time.Minute * 5

func (cm *ContentManager) ensureStorage(ctx context.Context, content Content, done func(time.Duration)) error {
	ctx, span := cm.tracer.Start(ctx, "ensureStorage", trace.WithAttributes(
		attribute.Int("content", int(content.ID)),
	))
	defer span.End()

	verified := true

	if content.AggregatedIn > 0 {
		// This content is aggregated inside another piece of content, nothing to do here
		return nil
	}

	if content.DagSplit && content.SplitFrom == 0 {
		// This is the 'root' of a split dag, we dont need to process it
		return nil
	}

	if cm.contentInStagingZone(ctx, content) {
		// This content is already scheduled to be aggregated and is waiting in a bucket
		return nil
	}

	// its too big, need to split it up into chunks
	if content.Size > cm.contentSizeLimit {
		if err := cm.splitContent(ctx, content, cm.contentSizeLimit); err != nil {
			return err
		}
		done(time.Minute * 15)
		return nil
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

	var user User
	if err := cm.DB.First(&user, "id = ?", content.UserID).Error; err != nil {
		return err
	}

	if len(deals) == 0 &&
		content.Size < int64(individualDealThreshold) &&
		!content.Aggregate &&
		bucketingEnabled {
		// Put it in a bucket!
		if err := cm.addContentToStagingZone(ctx, content); err != nil {
			return err
		}
		return nil
	}

	replicationFactor := defaultReplication
	if content.Replication > 0 {
		replicationFactor = content.Replication
	}

	minersAlready := make(map[address.Address]bool)
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
		minersAlready[maddr] = true
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
					cm.recordDealFailure(dfe)
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
	if goodDeals < replicationFactor {
		pc, err := cm.lookupPieceCommRecord(content.Cid.CID)
		if err != nil {
			return err
		}

		if pc == nil {
			// pre-compute piece commitment in a goroutine and dont block the checker loop while doing so
			go func() {
				_, _, err := cm.getPieceCommitment(context.Background(), content.Cid.CID, cm.Blockstore)
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
		go func() {
			// make some more deals!
			log.Infow("making more deals for content", "content", content.ID, "curDealCount", len(deals), "newDeals", replicationFactor-len(deals))
			if err := cm.makeDealsForContent(ctx, content, replicationFactor-len(deals), minersAlready, verified); err != nil {
				log.Errorf("failed to make more deals: %s", err)
			}
			done(time.Minute * 10)
		}()
		return nil
	}

	if numSealed >= replicationFactor {
		done(time.Hour * 24)
	} else if numSealed+numPublished >= replicationFactor {
		done(time.Hour)
	} else {
		done(time.Minute * 10)
	}

	return nil
}

func (cm *ContentManager) splitContent(ctx context.Context, cont Content, size int64) error {
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

	if cont.Location == "local" {
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

func (cm *ContentManager) getContent(id uint) (*Content, error) {
	var content Content
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

const minSafeDealLifetime = (2880 * 21) // three weeks

func (cm *ContentManager) checkDeal(ctx context.Context, d *contentDeal) (int, error) {
	ctx, cancel := context.WithTimeout(ctx, time.Minute*5) // NB: if we ever hit this, its bad. but we at least need *some* timeout there
	defer cancel()

	ctx, span := cm.tracer.Start(ctx, "checkDeal", trace.WithAttributes(
		attribute.Int("deal", int(d.ID)),
	))
	defer span.End()
	log.Infow("checking deal", "miner", d.Miner, "content", d.Content, "dbid", d.ID)

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
			cm.recordDealFailure(&DealFailureError{
				Miner:   maddr,
				Phase:   "check-chain-deal",
				Message: fmt.Sprintf("deal %d was slashed at epoch %d", d.DealID, deal.State.SlashEpoch),
			})
			return DEAL_CHECK_UNKNOWN, nil
		}

		head, err := cm.Api.ChainHead(ctx)
		if err != nil {
			return DEAL_CHECK_UNKNOWN, fmt.Errorf("failed to check chain head: %w", err)
		}

		if deal.Proposal.EndEpoch-head.Height() < minSafeDealLifetime {
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

	log.Infow("checking deal status", "miner", maddr, "propcid", d.PropCid.CID)
	subctx, cancel := context.WithTimeout(ctx, time.Second*10)
	defer cancel()
	provds, err := cm.FilClient.DealStatus(subctx, maddr, d.PropCid.CID)
	if err != nil {
		log.Warnf("failed to check deal status with miner %s: %s", maddr, err)
		// if we cant get deal status from a miner and the data hasnt landed on
		// chain what do we do?
		expired, err := cm.dealHasExpired(ctx, d)
		if err != nil {
			return DEAL_CHECK_UNKNOWN, xerrors.Errorf("failed to check if deal was expired: %w", err)
		}
		if expired {
			// deal expired, miner didnt start it in time
			cm.recordDealFailure(&DealFailureError{
				Miner:   maddr,
				Phase:   "check-status",
				Message: "was unable to check deal status with miner and now deal has expired",
				Content: d.Content,
			})
			return DEAL_CHECK_UNKNOWN, nil
		}

		// dont fail it out until they run out of time, they might just be offline momentarily
		return DEAL_CHECK_PROGRESS, nil
	}

	if provds.State == storagemarket.StorageDealError {
		log.Errorf("deal state from miner is error: %s", provds.Message)
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
		if err != nil {
			return DEAL_CHECK_UNKNOWN, fmt.Errorf("failed to lookup deal on chain: %w", err)
		}

		pcr, err := cm.lookupPieceCommRecord(content.Cid.CID)
		if err != nil {
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
		log.Infow("checking publish CID", "content", d.Content, "miner", d.Miner, "propcid", d.PropCid.CID, "publishCid", *provds.PublishCid)
		id, err := cm.getDealID(ctx, *provds.PublishCid, d)
		if err != nil {
			log.Infof("failed to find message on chain: %s", *provds.PublishCid)
			if provds.Proposal.StartEpoch < head.Height() {
				// deal expired, miner didn`t start it in time
				cm.recordDealFailure(&DealFailureError{
					Miner:   maddr,
					Phase:   "check-status",
					Message: "deal did not make it on chain in time (but has publish deal cid set)",
					Content: d.Content,
				})
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
		log.Errorw("response from miner has nil Proposal", "miner", maddr, "propcid", d.PropCid.CID)
		if time.Since(d.CreatedAt) > time.Hour*24*14 {
			cm.recordDealFailure(&DealFailureError{
				Miner:   maddr,
				Phase:   "check-status",
				Message: "miner returned nil response proposal and deal expired",
				Content: d.Content,
			})
			return DEAL_CHECK_UNKNOWN, nil

		}
		return DEAL_CHECK_UNKNOWN, fmt.Errorf("bad response from miner for deal status check")
	}

	if provds.Proposal.StartEpoch < head.Height() {
		// deal expired, miner didnt start it in time
		cm.recordDealFailure(&DealFailureError{
			Miner:   maddr,
			Phase:   "check-status",
			Message: "deal did not make it on chain in time",
			Content: d.Content,
		})
		return DEAL_CHECK_UNKNOWN, nil
	}
	// miner still has time...

	if d.DTChan == "" {
		if content.Location != "local" {
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
			chid, err := d.ChannelID()
			if err != nil {
				return DEAL_CHECK_UNKNOWN, fmt.Errorf("failed to parse dtchan in deal %d: %w", d.ID, err)
			}
			if err := cm.sendRequestTransferStatusCmd(ctx, content.Location, d.ID, chid); err != nil {
				return DEAL_CHECK_UNKNOWN, err
			}

			return DEAL_CHECK_PROGRESS, nil
		}
	}

	switch status.Status {
	case datatransfer.Failed:
		cm.recordDealFailure(&DealFailureError{
			Miner:   maddr,
			Phase:   "data-transfer",
			Message: fmt.Sprintf("transfer failed: %s", status.Message),
			Content: content.ID,
		})

		// TODO: returning unknown==error here feels excessive
		// but since 'Failed' is a terminal state, we kinda just have to make a new deal altogether
		if cm.FailDealOnTransferFailure {
			return DEAL_CHECK_UNKNOWN, nil
		}
	case datatransfer.Cancelled:
		cm.recordDealFailure(&DealFailureError{
			Miner:   maddr,
			Phase:   "data-transfer",
			Message: fmt.Sprintf("transfer cancelled: %s", status.Message),
			Content: content.ID,
		})
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

func (cm *ContentManager) GetTransferStatus(ctx context.Context, d *contentDeal, content *Content) (*filclient.ChannelState, error) {
	ctx, span := cm.tracer.Start(ctx, "getTransferStatus")
	defer span.End()

	if content.Location == "local" {
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

func (cm *ContentManager) getLocalTransferStatus(ctx context.Context, d *contentDeal, content *Content) (*filclient.ChannelState, error) {
	ccid := content.Cid.CID

	miner, err := d.MinerAddr()
	if err != nil {
		return nil, err
	}

	chanid, err := d.ChannelID()
	switch err {
	case nil:
		chanst, err := cm.FilClient.TransferStatus(ctx, &chanid)
		if err != nil {
			return nil, err
		}
		return chanst, nil
	case ErrNoChannelID:
		chanst, err := cm.FilClient.TransferStatusForContent(ctx, ccid, miner)
		if err != nil && err != filclient.ErrNoTransferFound {
			return nil, err
		}

		if chanst != nil {
			if err := cm.DB.Model(contentDeal{}).Where("id = ?", d.ID).UpdateColumns(map[string]interface{}{
				"dt_chan": chanst.ChannelID.String(),
			}).Error; err != nil {
				return nil, err
			}
		}

		return chanst, nil
	default:
		return nil, err
	}
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
		log.Infow("miner faulted on deal", "deal", d.DealID, "content", d.Content, "miner", d.Miner)
		maddr, err := d.MinerAddr()
		if err != nil {
			log.Errorf("failed to get miner address from deal (%s): %w", d.Miner, err)
		}

		cm.recordDealFailure(&DealFailureError{
			Miner:   maddr,
			Phase:   "fault",
			Message: fmt.Sprintf("miner faulted on deal: %d", d.DealID),
			Content: d.Content,
		})
	}
	log.Infow("repair deal", "propcid", d.PropCid.CID, "miner", d.Miner, "content", d.Content)
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

func (cm *ContentManager) priceIsTooHigh(price abi.TokenAmount, verified bool) bool {
	if verified {
		if types.BigCmp(price, abi.NewTokenAmount(0)) > 0 {
			return true
		}
		return false
	}

	if types.BigCmp(price, priceMax) > 0 {
		return true
	}

	return false
}

type proposalRecord struct {
	PropCid util.DbCID `gorm:"index"`
	Data    []byte
}

func (cm *ContentManager) makeDealsForContent(ctx context.Context, content Content, count int, exclude map[address.Address]bool, verified bool) error {
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

	_, size, err := cm.getPieceCommitment(ctx, content.Cid.CID, cm.Blockstore)
	if err != nil {
		return xerrors.Errorf("failed to compute piece commitment while making deals %d: %w", content.ID, err)
	}

	minerpool, err := cm.pickMiners(ctx, content, count*2, size.Padded(), exclude)
	if err != nil {
		return err
	}

	var asks []*network.AskResponse
	var ms []address.Address
	var successes int
	for _, m := range minerpool {
		ask, err := cm.FilClient.GetAsk(ctx, m)
		if err != nil {
			var clientErr *filclient.Error
			if !(xerrors.As(err, &clientErr) && clientErr.Code == filclient.ErrLotusError) {
				cm.recordDealFailure(&DealFailureError{
					Miner:   m,
					Phase:   "query-ask",
					Message: err.Error(),
					Content: content.ID,
				})
			}
			log.Warnf("failed to get ask for miner %s: %s\n", m, err)
			continue
		}

		price := ask.Ask.Ask.Price
		if verified {
			price = ask.Ask.Ask.VerifiedPrice
		}

		if cm.priceIsTooHigh(price, verified) {
			log.Infow("miners price is too high", "miner", m, "price", price)
			cm.recordDealFailure(&DealFailureError{
				Miner:   m,
				Phase:   "miner-search",
				Message: fmt.Sprintf("miners price is too high: %s (verified = %v)", types.FIL(price), verified),
				Content: content.ID,
			})
			continue
		}

		ms = append(ms, m)
		asks = append(asks, ask)
		successes++
		if len(ms) >= count {
			break
		}
	}

	proposals := make([]*network.Proposal, len(ms))
	for i, m := range ms {
		if asks[i] == nil {
			continue
		}

		price := asks[i].Ask.Ask.Price
		if verified {
			price = asks[i].Ask.Ask.VerifiedPrice
		}

		prop, err := cm.FilClient.MakeDeal(ctx, m, content.Cid.CID, price, asks[i].Ask.Ask.MinPieceSize, dealDuration, verified)
		if err != nil {
			return xerrors.Errorf("failed to construct a deal proposal: %w", err)
		}

		proposals[i] = prop

		if err := cm.putProposalRecord(prop.DealProposal); err != nil {
			return err
		}
	}

	deals := make([]*contentDeal, len(ms))
	responses := make([]*network.SignedResponse, len(ms))
	for i, p := range proposals {
		if p == nil {
			continue
		}

		dealresp, err := cm.FilClient.SendProposal(ctx, p)
		if err != nil {
			cm.recordDealFailure(&DealFailureError{
				Miner:   ms[i],
				Phase:   "send-proposal",
				Message: err.Error(),
				Content: content.ID,
			})
			continue
		}

		// TODO: verify signature!
		switch dealresp.Response.State {
		case storagemarket.StorageDealError:
			if err := cm.recordDealFailure(&DealFailureError{
				Miner:   ms[i],
				Phase:   "propose",
				Message: dealresp.Response.Message,
				Content: content.ID,
			}); err != nil {
				return err
			}
		case storagemarket.StorageDealProposalRejected:
			if err := cm.recordDealFailure(&DealFailureError{
				Miner:   ms[i],
				Phase:   "propose",
				Message: dealresp.Response.Message,
				Content: content.ID,
			}); err != nil {
				return err
			}
		default:
			if err := cm.recordDealFailure(&DealFailureError{
				Miner:   ms[i],
				Phase:   "propose",
				Message: fmt.Sprintf("unrecognized response state %d: %s", dealresp.Response.State, dealresp.Response.Message),
				Content: content.ID,
			}); err != nil {
				return err
			}
		case storagemarket.StorageDealWaitingForData, storagemarket.StorageDealProposalAccepted:
			responses[i] = dealresp

			cd := &contentDeal{
				Content:  content.ID,
				PropCid:  util.DbCID{dealresp.Response.Proposal},
				Miner:    ms[i].String(),
				Verified: verified,
			}

			if err := cm.DB.Create(cd).Error; err != nil {
				return xerrors.Errorf("failed to create database entry for deal: %w", err)
			}

			deals[i] = cd
		}
	}

	// Now start up some data transfers!
	// note: its okay if we dont start all the data transfers, we can just do it next time around
	for i, resp := range responses {
		if resp == nil {
			continue
		}

		cd := deals[i]
		if cd == nil {
			log.Warnf("have no contentDeal for response we are about to start transfer on")
			continue
		}

		// im so paranoid
		if cd.PropCid.CID != resp.Response.Proposal {
			log.Errorf("proposal in saved deal did not match response (%s != %s)", cd.PropCid.CID, resp.Response.Proposal)
		}

		err := cm.StartDataTransfer(ctx, cd)
		if err != nil {
			log.Errorw("failed to start data transfer", "err", err, "miner", ms[i])
			continue
		}

	}

	return nil
}

func (cm *ContentManager) makeDealWithMiner(ctx context.Context, content Content, miner address.Address, verified bool) (uint, error) {
	ctx, span := cm.tracer.Start(ctx, "makeDealWithMiner", trace.WithAttributes(
		attribute.Int64("content", int64(content.ID)),
		attribute.Stringer("miner", miner),
	))
	defer span.End()

	if content.Offloaded {
		return 0, fmt.Errorf("cannot make more deals for offloaded content, must retrieve first")
	}

	ask, err := cm.FilClient.GetAsk(ctx, miner)
	if err != nil {
		var clientErr *filclient.Error
		if !(xerrors.As(err, &clientErr) && clientErr.Code == filclient.ErrLotusError) {
			cm.recordDealFailure(&DealFailureError{
				Miner:   miner,
				Phase:   "query-ask",
				Message: err.Error(),
				Content: content.ID,
			})
		}

		return 0, xerrors.Errorf("failed to get ask for miner %s: %w", miner, err)
	}

	price := ask.Ask.Ask.Price
	if verified {
		price = ask.Ask.Ask.VerifiedPrice
	}

	if cm.priceIsTooHigh(price, verified) {
		return 0, fmt.Errorf("miners price is too high: %s %s", miner, price)
	}

	prop, err := cm.FilClient.MakeDeal(ctx, miner, content.Cid.CID, price, ask.Ask.Ask.MinPieceSize, dealDuration, verified)
	if err != nil {
		return 0, xerrors.Errorf("failed to construct a deal proposal: %w", err)
	}

	if err := cm.putProposalRecord(prop.DealProposal); err != nil {
		return 0, err
	}

	var deal *contentDeal
	dealresp, err := cm.FilClient.SendProposal(ctx, prop)
	if err != nil {
		cm.recordDealFailure(&DealFailureError{
			Miner:   miner,
			Phase:   "send-proposal",
			Message: err.Error(),
			Content: content.ID,
		})
		return 0, fmt.Errorf("failed to send proposal to miner: %w", err)
	}

	// TODO: verify signature!
	switch dealresp.Response.State {
	case storagemarket.StorageDealError:
		if err := cm.recordDealFailure(&DealFailureError{
			Miner:   miner,
			Phase:   "propose",
			Message: dealresp.Response.Message,
			Content: content.ID,
		}); err != nil {
			return 0, err
		}
		return 0, fmt.Errorf("deal errored: %s", dealresp.Response.Message)
	case storagemarket.StorageDealProposalRejected:
		if err := cm.recordDealFailure(&DealFailureError{
			Miner:   miner,
			Phase:   "propose",
			Message: dealresp.Response.Message,
			Content: content.ID,
		}); err != nil {
			return 0, err
		}
		return 0, fmt.Errorf("deal rejected: %s", dealresp.Response.Message)
	default:
		if err := cm.recordDealFailure(&DealFailureError{
			Miner:   miner,
			Phase:   "propose",
			Message: fmt.Sprintf("unrecognized response state %d: %s", dealresp.Response.State, dealresp.Response.Message),
			Content: content.ID,
		}); err != nil {
			return 0, err
		}
		return 0, fmt.Errorf("unrecognized proposal response state %d: %s", dealresp.Response.State, dealresp.Response.Message)
	case storagemarket.StorageDealWaitingForData, storagemarket.StorageDealProposalAccepted:

		deal = &contentDeal{
			Content:  content.ID,
			PropCid:  util.DbCID{dealresp.Response.Proposal},
			Miner:    miner.String(),
			Verified: verified,
		}

		if err := cm.DB.Create(deal).Error; err != nil {
			return 0, xerrors.Errorf("failed to create database entry for deal: %w", err)
		}
	}

	if err := cm.StartDataTransfer(ctx, deal); err != nil {
		return 0, fmt.Errorf("failed to start data transfer: %w", err)
	}

	return deal.ID, nil
}

func (cm *ContentManager) StartDataTransfer(ctx context.Context, cd *contentDeal) error {
	var cont Content
	if err := cm.DB.First(&cont, "id = ?", cd.Content).Error; err != nil {
		return err
	}

	if cont.Location != "local" {
		return cm.sendStartTransferCommand(ctx, cont.Location, cd, cont.Cid.CID)
	}

	miner, err := cd.MinerAddr()
	if err != nil {
		return err
	}

	chanid, err := cm.FilClient.StartDataTransfer(ctx, miner, cd.PropCid.CID, cont.Cid.CID)
	if err != nil {
		if oerr := cm.recordDealFailure(&DealFailureError{
			Miner:   miner,
			Phase:   "start-data-transfer",
			Message: err.Error(),
			Content: cont.ID,
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

	log.Infow("Started data transfer", "chanid", chanid)
	return nil
}

func (cm *ContentManager) putProposalRecord(dealprop *market.ClientDealProposal) error {
	nd, err := cborutil.AsIpld(dealprop)
	if err != nil {
		return err
	}
	// fmt.Println("proposal cid: ", nd.Cid())

	if err := cm.DB.Create(&proposalRecord{
		PropCid: util.DbCID{nd.Cid()},
		Data:    nd.RawData(),
	}).Error; err != nil {
		return err
	}

	return nil
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
	log.Infow("deal failure error", "miner", dfe.Miner, "phase", dfe.Phase, "msg", dfe.Message, "content", dfe.Content)
	rec := dfe.Record()
	if dfe.Miner != address.Undef {
		var m storageMiner
		if err := cm.DB.First(&m, "address = ?", dfe.Miner.String()).Error; err != nil {
			log.Errorf("failed to look up miner while recording deal failure: %s", err)
		}
		rec.MinerVersion = m.Version
	}
	return cm.DB.Create(rec).Error
}

type DealFailureError struct {
	Miner   address.Address
	Phase   string
	Message string
	Content uint
}

type dfeRecord struct {
	gorm.Model
	Miner        string `json:"miner"`
	Phase        string `json:"phase"`
	Message      string `json:"message"`
	Content      uint   `json:"content" gorm:"index"`
	MinerVersion string `json:"minerVersion"`
}

func (dfe *DealFailureError) Record() *dfeRecord {
	return &dfeRecord{
		Miner:   dfe.Miner.String(),
		Phase:   dfe.Phase,
		Message: dfe.Message,
		Content: dfe.Content,
	}
}

func (dfe *DealFailureError) Error() string {
	return fmt.Sprintf("deal with miner %s failed in phase %s: %s", dfe.Message, dfe.Phase, dfe.Message)
}

func averageAskPrice(asks []*network.AskResponse) types.FIL {
	total := abi.NewTokenAmount(0)
	for _, a := range asks {
		total = types.BigAdd(total, a.Ask.Ask.Price)
	}

	return types.FIL(big.Div(total, big.NewInt(int64(len(asks)))))
}

type PieceCommRecord struct {
	Data  util.DbCID `gorm:"unique"`
	Piece util.DbCID
	Size  abi.UnpaddedPieceSize
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

var ErrWaitForRemoteCompute = fmt.Errorf("waiting for remote commP computation")

func (cm *ContentManager) runPieceCommCompute(ctx context.Context, data cid.Cid, bs blockstore.Blockstore) (cid.Cid, abi.UnpaddedPieceSize, error) {
	var cont Content
	if err := cm.DB.First(&cont, "cid = ?", data.Bytes()).Error; err != nil {
		return cid.Undef, 0, err
	}

	if cont.Location != "local" {
		if err := cm.sendShuttleCommand(ctx, cont.Location, &drpc.Command{
			Op: drpc.CMD_ComputeCommP,
			Params: drpc.CmdParams{
				ComputeCommP: &drpc.ComputeCommP{
					Data: data,
				},
			},
		}); err != nil {
			return cid.Undef, 0, err
		}

		return cid.Undef, 0, ErrWaitForRemoteCompute
	}

	log.Infow("computing piece commitment", "data", cont.Cid.CID)
	return filclient.GeneratePieceCommitmentFFI(ctx, data, bs)
}

func (cm *ContentManager) getPieceCommitment(ctx context.Context, data cid.Cid, bs blockstore.Blockstore) (cid.Cid, abi.UnpaddedPieceSize, error) {
	_, span := cm.tracer.Start(ctx, "getPieceComm")
	defer span.End()

	pcr, err := cm.lookupPieceCommRecord(data)
	if err != nil {
		return cid.Undef, 0, err
	}
	if pcr != nil {
		return pcr.Piece.CID, pcr.Size, nil
	}

	pc, size, err := cm.runPieceCommCompute(ctx, data, bs)
	if err != nil {
		return cid.Undef, 0, xerrors.Errorf("failed to generate piece commitment: %w", err)
	}

	opcr := PieceCommRecord{
		Data:  util.DbCID{data},
		Piece: util.DbCID{pc},
		Size:  size,
	}

	if err := cm.DB.Clauses(clause.OnConflict{DoNothing: true}).Create(&opcr).Error; err != nil {
		return cid.Undef, 0, err
	}

	return pc, size, nil
}

func (cm *ContentManager) RefreshContentForCid(ctx context.Context, c cid.Cid) (blocks.Block, error) {
	ctx, span := cm.tracer.Start(ctx, "refreshForCid", trace.WithAttributes(
		attribute.Stringer("cid", c),
	))
	defer span.End()

	var obj Object
	if err := cm.DB.First(&obj, "cid = ?", c.Bytes()).Error; err != nil {
		return nil, xerrors.Errorf("failed to get object from db: ", err)
	}

	var refs []ObjRef
	if err := cm.DB.Find(&refs, "object = ?", obj.ID).Error; err != nil {
		return nil, err
	}

	var contentToFetch uint
	switch len(refs) {
	case 0:
		return nil, xerrors.Errorf("have no object references for object %d in database")
	case 1:
		// easy case, fetch this thing.
		contentToFetch = refs[0].Content
	default:
		// have more than one reference for the same object. Need to pick one to retrieve

		// if one of the referenced contents has the requested cid as its root, then we should probably fetch that one

		var contents []Content
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
	var c Content
	if err := cm.DB.First(&c, "id = ?", cont).Error; err != nil {
		return err
	}

	loc, err := cm.selectLocationForRetrieval(ctx, c)
	if err != nil {
		return err
	}
	log.Infof("refreshing content %d onto shuttle %s", cont, loc)

	switch loc {
	case "local":
		if err := cm.retrieveContent(ctx, cont); err != nil {
			return err
		}

		if err := cm.DB.Model(&Content{}).Where("id = ?", cont).Update("offloaded", false).Error; err != nil {
			return err
		}

		if err := cm.DB.Model(&ObjRef{}).Where("content = ?", cont).Update("offloaded", 0).Error; err != nil {
			return err
		}
	default:
		return cm.sendRetrieveContentMessage(ctx, loc, c)
	}

	return nil
}

func (cm *ContentManager) sendRetrieveContentMessage(ctx context.Context, loc string, cont Content) error {
	return fmt.Errorf("not retrieving content yet until implementation is finished")

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
}

type retrievalProgress struct {
	wait   chan struct{}
	endErr error
}

func (cm *ContentManager) retrieveContent(ctx context.Context, contentToFetch uint) error {
	ctx, span := cm.tracer.Start(ctx, "retrieveContent", trace.WithAttributes(
		attribute.Int("content", int(contentToFetch)),
	))
	defer span.End()

	cm.retrLk.Lock()
	prog, ok := cm.retrievalsInProgress[contentToFetch]
	if !ok {
		prog = &retrievalProgress{
			wait: make(chan struct{}),
		}
		cm.retrievalsInProgress[contentToFetch] = prog
	}
	cm.retrLk.Unlock()

	if ok {
		select {
		case <-prog.wait:
			return prog.endErr
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	defer func() {
		cm.retrLk.Lock()
		delete(cm.retrievalsInProgress, contentToFetch)
		cm.retrLk.Unlock()

		close(prog.wait)
	}()

	if err := cm.runRetrieval(ctx, contentToFetch); err != nil {
		prog.endErr = err
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

	var content Content
	if err := cm.DB.First(&content, contentToFetch).Error; err != nil {
		return err
	}

	rootContent := content.ID

	index := -1
	if content.AggregatedIn > 0 {
		rootContent = content.AggregatedIn
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
			cm.recordRetrievalFailure(&util.RetrievalFailureRecord{
				Miner:   maddr.String(),
				Phase:   "query",
				Message: err.Error(),
				Content: content.ID,
				Cid:     content.Cid,
			})
			continue
		}
		log.Infow("got retrieval ask", "content", content, "miner", maddr, "ask", ask)

		if err := cm.tryRetrieve(ctx, maddr, content.Cid.CID, ask); err != nil {
			span.RecordError(err)
			log.Errorw("failed to retrieve content", "miner", maddr, "content", content.Cid.CID, "err", err)
			cm.recordRetrievalFailure(&util.RetrievalFailureRecord{
				Miner:   maddr.String(),
				Phase:   "retrieval",
				Message: err.Error(),
				Content: content.ID,
				Cid:     content.Cid,
			})
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

			provds, err := s.CM.FilClient.DealStatus(subctx, miner, d.PropCid.CID)
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
func (cm *ContentManager) addObjectsToDatabase(ctx context.Context, content uint, dserv ipld.NodeGetter, root cid.Cid, objects []*Object, loc string) error {
	ctx, span := cm.tracer.Start(ctx, "addObjectsToDatabase")
	defer span.End()

	if err := cm.DB.CreateInBatches(objects, 300).Error; err != nil {
		return xerrors.Errorf("failed to create objects in db: %w", err)
	}

	refs := make([]ObjRef, 0, len(objects))
	var totalSize int64
	for _, o := range objects {
		refs = append(refs, ObjRef{
			Content: content,
			Object:  o.ID,
		})
		totalSize += int64(o.Size)
	}

	span.SetAttributes(
		attribute.Int64("totalSize", totalSize),
		attribute.Int("numObjects", len(objects)),
	)

	if err := cm.DB.Model(Content{}).Where("id = ?", content).UpdateColumns(map[string]interface{}{
		"active":   true,
		"type":     util.FindCIDType(ctx, root, dserv),
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

func (cm *ContentManager) migrateContentsToLocalNode(ctx context.Context, toMove []Content) error {
	for _, c := range toMove {
		if err := cm.migrateContentToLocalNode(ctx, c); err != nil {
			return err
		}
	}

	return nil
}

func (cm *ContentManager) migrateContentToLocalNode(ctx context.Context, cont Content) error {
	done, err := cm.safeFetchData(ctx, cont.Cid.CID)
	if err != nil {
		return fmt.Errorf("failed to fetch data: %w", err)
	}

	defer done()

	if err := cm.DB.Model(ObjRef{}).Where("id = ?", cont.ID).UpdateColumns(map[string]interface{}{
		"offloaded": 0,
	}).Error; err != nil {
		return err
	}

	if err := cm.DB.Model(Content{}).Where("id = ?", cont.ID).UpdateColumns(map[string]interface{}{
		"offloaded": false,
		"location":  "local",
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

		return node.Links(), nil
	}, c, cset.Visit, merkledag.Concurrent())
	if err != nil {
		return deref, err
	}

	return deref, nil
}

func (cm *ContentManager) addrInfoForShuttle(handle string) (*peer.AddrInfo, error) {
	if handle == "local" {
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

func (cm *ContentManager) sendAggregateCmd(ctx context.Context, loc string, cont Content, aggr []uint, blob []byte) error {
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

func (cm *ContentManager) sendRequestTransferStatusCmd(ctx context.Context, loc string, dealid uint, chid datatransfer.ChannelID) error {
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

func (cm *ContentManager) sendConsolidateContentCmd(ctx context.Context, loc string, contents []Content) error {
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

func (cm *ContentManager) splitContentLocal(ctx context.Context, cont Content, size int64) error {
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
		content := &Content{
			Cid:          util.DbCID{c},
			Name:         fmt.Sprintf("%s-%d", cont.Name, i),
			Active:       false,
			Pinning:      true,
			UserID:       cont.UserID,
			Replication:  cont.Replication,
			Location:     "local",
			DagSplit:     true,
			AggregatedIn: cont.ID,
		}

		if err := cm.DB.Create(content).Error; err != nil {
			return xerrors.Errorf("failed to track new content in database: %w", err)
		}

		if err := cm.addDatabaseTrackingToContent(ctx, content.ID, dserv, cm.Node.Blockstore, c, func(int64) {}); err != nil {
			return err
		}
	}

	if err := cm.DB.Model(Content{}).Where("id = ?", cont.ID).UpdateColumns(map[string]interface{}{
		"dag_split": true,
	}).Error; err != nil {
		return err
	}

	return nil
}
