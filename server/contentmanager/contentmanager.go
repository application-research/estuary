package server

import (
	"bytes"
	"container/heap"
	"context"
	"fmt"
	"math/rand"
	"net/url"
	"sort"
	"sync"
	"time"

	"github.com/application-research/estuary/constants"
	"github.com/application-research/estuary/drpc"
	"github.com/application-research/estuary/node"
	"github.com/application-research/estuary/pinner"
	"github.com/application-research/estuary/replication"
	"github.com/application-research/estuary/trackbs"
	"github.com/application-research/estuary/util"
	dagsplit "github.com/application-research/estuary/util/dagsplit"
	"github.com/application-research/filclient"
	"github.com/application-research/filclient/retrievehelper"
	"github.com/filecoin-project/boost/transport/httptransport"
	"github.com/filecoin-project/go-address"
	cborutil "github.com/filecoin-project/go-cbor-util"
	datatransfer "github.com/filecoin-project/go-data-transfer"
	"github.com/filecoin-project/go-fil-markets/retrievalmarket"
	"github.com/filecoin-project/go-fil-markets/storagemarket"
	"github.com/filecoin-project/go-fil-markets/storagemarket/network"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/big"
	"github.com/filecoin-project/lotus/api"
	"github.com/filecoin-project/lotus/chain/types"
	"github.com/filecoin-project/specs-actors/actors/builtin/market"
	"github.com/google/uuid"
	lru "github.com/hashicorp/golang-lru"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-blockservice"
	"github.com/ipfs/go-cid"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	"github.com/ipfs/go-ipfs-provider/batched"
	cbor "github.com/ipfs/go-ipld-cbor"
	ipld "github.com/ipfs/go-ipld-format"
	logging "github.com/ipfs/go-log/v2"
	"github.com/ipfs/go-merkledag"
	"github.com/ipfs/go-metrics-interface"
	"github.com/ipfs/go-unixfs"
	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/multiformats/go-multiaddr"
	"github.com/pkg/errors"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/xerrors"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

var log = logging.Logger("contentManager")

type ContentManager struct {
	DB        *gorm.DB
	Api       api.Gateway
	FilClient *filclient.FilClient
	Provider  *batched.BatchProvidingSystem
	Node      *node.Node

	Host host.Host

	Tracer trace.Tracer

	Blockstore       node.EstuaryBlockstore
	Tracker          *trackbs.TrackingBlockstore
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
	buckets  map[uint][]*replication.ContentStagingZone

	// some behavior flags
	FailDealOnTransferFailure bool

	dealDisabledLk       sync.Mutex
	isDealMakingDisabled bool

	ContentAddingDisabled      bool
	LocalContentAddingDisabled bool

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

	VerifiedDeal bool
}

type RetrievalSuccessRecord struct {
	ID        uint      `gorm:"primarykey" json:"-"`
	CreatedAt time.Time `json:"createdAt"`

	Cid   util.DbCID `json:"cid"`
	Miner string     `json:"miner"`

	Peer         string `json:"peer"`
	Size         uint64 `json:"size"`
	DurationMs   int64  `json:"durationMs"`
	AverageSpeed uint64 `json:"averageSpeed"`
	TotalPayment string `json:"totalPayment"`
	NumPayments  int    `json:"numPayments"`
	AskPrice     string `json:"askPrice"`
}

func (cm *ContentManager) RestartTransfer(ctx context.Context, loc string, chanid datatransfer.ChannelID) error {
	if loc == "local" {
		st, err := cm.FilClient.TransferStatus(ctx, &chanid)
		if err != nil {
			return err
		}

		if util.TransferTerminated(st) {
			return fmt.Errorf("deal in database as being in progress, but data transfer is terminated: %d", st.Status)
		}

		return cm.FilClient.RestartTransfer(ctx, &chanid)
	}

	return cm.sendRestartTransferCmd(ctx, loc, chanid)
}

func (cm *ContentManager) sendRestartTransferCmd(ctx context.Context, loc string, chanid datatransfer.ChannelID) error {
	return cm.sendShuttleCommand(ctx, loc, &drpc.Command{
		Op: drpc.CMD_RestartTransfer,
		Params: drpc.CmdParams{
			RestartTransfer: &drpc.RestartTransfer{
				ChanID: chanid,
			},
		},
	})
}

func (cm *ContentManager) isInflight(c cid.Cid) bool {
	cm.inflightCidsLk.Lock()
	defer cm.inflightCidsLk.Unlock()

	v, ok := cm.inflightCids[c]
	return ok && v > 0
}

func (cm *ContentManager) newContentStagingZone(user uint, loc string) (*replication.ContentStagingZone, error) {
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

	return &replication.ContentStagingZone{
		ZoneOpened: time.Now(),
		CloseTime:  time.Now().Add(constants.MaxStagingZoneLifetime),
		MinSize:    int64(constants.StagingZoneSizeLimit - (1 << 30)),
		MaxSize:    int64(constants.StagingZoneSizeLimit),
		MaxItems:   constants.MaxBucketItems,
		User:       user,
		ContID:     content.ID,
		Location:   content.Location,
	}, nil
}

func (cm *ContentManager) tryAddContent(cb *replication.ContentStagingZone, c util.Content) (bool, error) {
	cb.Lk.Lock()
	defer cb.Lk.Unlock()
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

	nowPlus := time.Now().Add(constants.StagingZoneKeepalive)
	if cb.CloseTime.Before(nowPlus) {
		cb.CloseTime = nowPlus
	}

	return true, nil
}

func NewContentManager(db *gorm.DB, api api.Gateway, fc *filclient.FilClient, tbs *trackbs.TrackingBlockstore, nbs *node.NotifyBlockstore, prov *batched.BatchProvidingSystem, pinmgr *pinner.PinManager, nd *node.Node, hostname string) (*ContentManager, error) {
	cache, err := lru.NewARC(50000)
	if err != nil {
		return nil, err
	}

	var stages []util.Content
	if err := db.Find(&stages, "not active and pinning and aggregate").Error; err != nil {
		return nil, err
	}

	zones := make(map[uint][]*replication.ContentStagingZone)
	for _, c := range stages {
		z := &replication.ContentStagingZone{
			ZoneOpened: c.CreatedAt,
			CloseTime:  c.CreatedAt.Add(constants.MaxStagingZoneLifetime),
			MinSize:    int64(constants.StagingZoneSizeLimit - (1 << 30)),
			MaxSize:    int64(constants.StagingZoneSizeLimit),
			MaxItems:   constants.MaxBucketItems,
			User:       c.UserID,
			ContID:     c.ID,
			Location:   c.Location,
		}

		minClose := time.Now().Add(constants.StagingZoneKeepalive)
		if z.CloseTime.Before(minClose) {
			z.CloseTime = minClose
		}

		var inzone []util.Content
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
		retrievalsInProgress: make(map[uint]*util.RetrievalProgress),
		buckets:              zones,
		pinJobs:              make(map[uint]*pinner.PinningOperation),
		pinMgr:               pinmgr,
		remoteTransferStatus: cache,
		shuttles:             make(map[string]*ShuttleConnection),
		contentSizeLimit:     constants.DefaultContentSizeLimit,
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
			var content util.Content
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
	var cont util.Content
	if err := cm.DB.First(&cont, "id = ?", c).Error; err != nil {
		return "", err
	}

	return cont.Location, nil
}

func (cm *ContentManager) stagedContentByLocation(ctx context.Context, b *replication.ContentStagingZone) (map[string][]util.Content, error) {
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

func (cm *ContentManager) consolidateStagedContent(ctx context.Context, b *replication.ContentStagingZone) error {
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
		if ntot > curMax && loc != "local" {
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

	log.Infow("consolidating content to single location for aggregation", "user", b.User, "primary", primary, "numItems", len(toMove), "primaryWeight", curMax)
	if primary == "local" {
		return cm.migrateContentsToLocalNode(ctx, toMove)
	} else {
		return cm.sendConsolidateContentCmd(ctx, primary, toMove)
	}
}

func (cm *ContentManager) aggregateContent(ctx context.Context, b *replication.ContentStagingZone) error {
	ctx, span := cm.Tracer.Start(ctx, "aggregateContent")
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
		"cid":  util.DbCID{ncid},
		"size": size,
	}).Error; err != nil {
		return err
	}

	var content util.Content
	if err := cm.DB.First(&content, "id = ?", b.ContID).Error; err != nil {
		return err
	}

	if loc == "local" {
		obj := &util.Object{
			Cid:  util.DbCID{ncid},
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

func (cm *ContentManager) createAggregate(ctx context.Context, conts []util.Content) (*merkledag.ProtoNode, error) {
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
	var allcontent []util.Content
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
	Asks  []*MinerStorageAsk
}

func (cm *ContentManager) estimatePrice(ctx context.Context, repl int, size abi.PaddedPieceSize, duration abi.ChainEpoch, verified bool) (*estimateResponse, error) {
	ctx, span := cm.Tracer.Start(ctx, "estimatePrice", trace.WithAttributes(
		attribute.Int("replication", repl),
	))
	defer span.End()

	miners, err := cm.pickMiners(ctx, util.Content{}, repl, size, nil)
	if err != nil {
		return nil, err
	}
	if len(miners) == 0 {
		return nil, fmt.Errorf("failed to find any miners for estimating deal price")
	}

	var asks []*MinerStorageAsk
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

type MinerStorageAsk struct {
	gorm.Model    `json:"-"`
	Miner         string              `gorm:"unique" json:"miner"`
	Price         string              `json:"price"`
	VerifiedPrice string              `json:"verifiedPrice"`
	MinPieceSize  abi.PaddedPieceSize `json:"minPieceSize"`
	MaxPieceSize  abi.PaddedPieceSize `json:"maxPieceSize"`
}

func (msa *MinerStorageAsk) GetPrice() (*types.BigInt, error) {
	v, err := types.BigFromString(msa.Price)
	if err != nil {
		return nil, err
	}

	return &v, nil
}

func (msa *MinerStorageAsk) GetVerifiedPrice() (*types.BigInt, error) {
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

func (cm *ContentManager) pickMiners(ctx context.Context, cont util.Content, n int, size abi.PaddedPieceSize, exclude map[address.Address]bool) ([]address.Address, error) {
	ctx, span := cm.Tracer.Start(ctx, "pickMiners", trace.WithAttributes(
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

	sortedminers, _, err := cm.sortedMinerList()
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

func (cm *ContentManager) getAsk(ctx context.Context, m address.Address, maxCacheAge time.Duration) (*MinerStorageAsk, error) {
	ctx, span := cm.Tracer.Start(ctx, "getAsk", trace.WithAttributes(
		attribute.Stringer("miner", m),
	))
	defer span.End()

	var asks []MinerStorageAsk
	if err := cm.DB.Find(&asks, "miner = ?", m.String()).Error; err != nil {
		return nil, err
	}

	var msa MinerStorageAsk
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

func toDBAsk(netask *network.AskResponse) *MinerStorageAsk {
	return &MinerStorageAsk{
		Miner:         netask.Ask.Ask.Miner.String(),
		Price:         netask.Ask.Ask.Price.String(),
		VerifiedPrice: netask.Ask.Ask.VerifiedPrice.String(),
		MinPieceSize:  netask.Ask.Ask.MinPieceSize,
		MaxPieceSize:  netask.Ask.Ask.MaxPieceSize,
	}
}

func (cm *ContentManager) ContentInStagingZone(ctx context.Context, content util.Content) bool {
	cm.bucketLk.Lock()
	defer cm.bucketLk.Unlock()

	bucks, ok := cm.buckets[content.UserID]
	if !ok {
		return false
	}

	for _, b := range bucks {
		if b.HasContent(content) {
			return true
		}
	}

	return false
}

func (cm *ContentManager) getStagingZonesForUser(ctx context.Context, user uint) []*replication.ContentStagingZone {
	cm.bucketLk.Lock()
	defer cm.bucketLk.Unlock()

	blist, ok := cm.buckets[user]
	if !ok {
		return []*replication.ContentStagingZone{}
	}

	var out []*replication.ContentStagingZone
	for _, b := range blist {
		out = append(out, b.DeepCopy())
	}

	return out
}

func (cm *ContentManager) getStagingZoneSnapshot(ctx context.Context) map[uint][]*replication.ContentStagingZone {
	cm.bucketLk.Lock()
	defer cm.bucketLk.Unlock()

	out := make(map[uint][]*replication.ContentStagingZone)
	for u, blist := range cm.buckets {
		var copylist []*replication.ContentStagingZone

		for _, b := range blist {
			copylist = append(copylist, b.DeepCopy())
		}

		out[u] = copylist
	}
	return out
}

func (cm *ContentManager) addContentToStagingZone(ctx context.Context, content util.Content) error {
	ctx, span := cm.Tracer.Start(ctx, "stageContent")
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

		cm.buckets[content.UserID] = []*replication.ContentStagingZone{b}
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

func (cm *ContentManager) popReadyStagingZone() []*replication.ContentStagingZone {
	cm.bucketLk.Lock()
	defer cm.bucketLk.Unlock()

	var out []*replication.ContentStagingZone
	for uid, blist := range cm.buckets {
		var keep []*replication.ContentStagingZone
		for _, b := range blist {
			if b.IsReady() {
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

func (cm *ContentManager) ensureStorage(ctx context.Context, content util.Content, done func(time.Duration)) error {
	ctx, span := cm.Tracer.Start(ctx, "ensureStorage", trace.WithAttributes(
		attribute.Int("content", int(content.ID)),
	))
	defer span.End()

	verified := cm.VerifiedDeal

	if content.AggregatedIn > 0 {
		// This content is aggregated inside another piece of content, nothing to do here
		return nil
	}

	if content.DagSplit && content.SplitFrom == 0 {
		// This is the 'root' of a split dag, we dont need to process it
		return nil
	}

	if cm.ContentInStagingZone(ctx, content) {
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

	var deals []ContentDeal
	if err := cm.DB.Find(&deals, "content = ? AND NOT failed", content.ID).Error; err != nil {
		if !xerrors.Is(err, gorm.ErrRecordNotFound) {
			return err
		}
	}

	var user util.User
	if err := cm.DB.First(&user, "id = ?", content.UserID).Error; err != nil {
		return err
	}

	if len(deals) == 0 &&
		content.Size < int64(constants.IndividualDealThreshold) &&
		!content.Aggregate &&
		bucketingEnabled {
		// Put it in a bucket!
		if err := cm.addContentToStagingZone(ctx, content); err != nil {
			return err
		}
		return nil
	}

	replicationFactor := cm.Replication
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
		if verified {
			bl, err := cm.FilClient.Balance(ctx)
			if err != nil {
				return errors.Wrap(err, "could not retrieve dataCap from client balance")
			}

			if bl.VerifiedClientBalance.LessThan(big.NewIntUnsigned(uint64(abi.UnpaddedPieceSize(content.Size).Padded()))) {
				// how do we notify admin to top up datacap?
				return errors.Wrapf(err, "will not make deal, client address dataCap:%d GiB is lower than content size:%d GiB", big.Div(*bl.VerifiedClientBalance, big.NewIntUnsigned(uint64(1073741824))), abi.UnpaddedPieceSize(content.Size).Padded()/1073741824)
			}
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

func (cm *ContentManager) splitContent(ctx context.Context, cont util.Content, size int64) error {
	ctx, span := cm.Tracer.Start(ctx, "splitContent")
	defer span.End()

	var u util.User
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

const minSafeDealLifetime = (2880 * 21) // three weeks

func (cm *ContentManager) checkDeal(ctx context.Context, d *ContentDeal) (int, error) {
	ctx, cancel := context.WithTimeout(ctx, time.Minute*5) // NB: if we ever hit this, its bad. but we at least need *some* timeout there
	defer cancel()

	ctx, span := cm.Tracer.Start(ctx, "checkDeal", trace.WithAttributes(
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
			if err := cm.DB.Model(ContentDeal{}).Where("id = ?", d.ID).UpdateColumn("sealed_at", time.Now()).Error; err != nil {
				return DEAL_CHECK_UNKNOWN, err
			}
			return DEAL_CHECK_SECTOR_ON_CHAIN, nil
		}

		return DEAL_CHECK_DEALID_ON_CHAIN, nil
	}

	// case where deal isnt yet on chain...

	log.Infow("checking deal status", "miner", maddr, "propcid", d.PropCid.CID, "dealUUID", d.DealUUID)
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
		log.Errorw("response from miner has nil Proposal", "miner", maddr, "propcid", d.PropCid.CID, "dealUUID", d.DealUUID)
		if time.Since(d.CreatedAt) > time.Hour*24*14 {
			cm.recordDealFailure(&DealFailureError{
				Miner:   maddr,
				Phase:   "check-status",
				Message: "miner returned nil response proposal and deal expired",
				Content: d.Content,
			})
			return DEAL_CHECK_UNKNOWN, nil

		}
		return DEAL_CHECK_UNKNOWN, fmt.Errorf("bad response from miner %s for deal %s deal status check: %s",
			statusCheckID, maddr.String(), provds.Message)
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
			if err := cm.DB.Model(ContentDeal{}).Where("id = ?", d.ID).Updates(map[string]interface{}{
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

func (cm *ContentManager) updateDealID(d *ContentDeal, id int64) error {
	if err := cm.DB.Model(ContentDeal{}).Where("id = ?", d.ID).Updates(map[string]interface{}{
		"deal_id":     id,
		"on_chain_at": time.Now(),
	}).Error; err != nil {
		return err
	}
	return nil
}

func (cm *ContentManager) dealHasExpired(ctx context.Context, d *ContentDeal) (bool, error) {
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

func (cm *ContentManager) GetTransferStatus(ctx context.Context, d *ContentDeal, content *util.Content) (*filclient.ChannelState, error) {
	ctx, span := cm.Tracer.Start(ctx, "getTransferStatus")
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

func (cm *ContentManager) getLocalTransferStatus(ctx context.Context, d *ContentDeal, content *util.Content) (*filclient.ChannelState, error) {
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
		if err := cm.DB.Model(ContentDeal{}).Where("id = ?", d.ID).UpdateColumns(map[string]interface{}{
			"dt_chan": chanst.TransferID,
		}).Error; err != nil {
			return nil, err
		}
	}

	return chanst, nil
}

var ErrNotOnChainYet = fmt.Errorf("message not found on chain")

func (cm *ContentManager) getDealID(ctx context.Context, pubcid cid.Cid, d *ContentDeal) (abi.DealID, error) {
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

func (cm *ContentManager) repairDeal(d *ContentDeal) error {
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
	if err := cm.DB.Model(ContentDeal{}).Where("id = ?", d.ID).UpdateColumns(map[string]interface{}{
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

func (cm *ContentManager) makeDealsForContent(ctx context.Context, content util.Content, count int, exclude map[address.Address]bool, verified bool) error {
	ctx, span := cm.Tracer.Start(ctx, "makeDealsForContent", trace.WithAttributes(
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

	_, _, size, err := cm.getPieceCommitment(ctx, content.Cid.CID, cm.Blockstore)
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

		prop, err := cm.FilClient.MakeDeal(ctx, m, content.Cid.CID, price, asks[i].Ask.Ask.MinPieceSize, constants.DealDuration, verified)
		if err != nil {
			return xerrors.Errorf("failed to construct a deal proposal: %w", err)
		}

		proposals[i] = prop

		if err := cm.putProposalRecord(prop.DealProposal); err != nil {
			return err
		}
	}

	deals := make([]*ContentDeal, len(ms))
	responses := make([]*bool, len(ms))
	for i, p := range proposals {
		if p == nil {
			continue
		}

		proto, err := cm.FilClient.DealProtocolForMiner(ctx, ms[i])
		if err != nil {
			cm.recordDealFailure(&DealFailureError{
				Miner:   ms[i],
				Phase:   "send-proposal",
				Message: err.Error(),
				Content: content.ID,
			})
			continue
		}

		propnd, err := cborutil.AsIpld(p.DealProposal)
		if err != nil {
			return xerrors.Errorf("failed to compute deal proposal ipld node: %w", err)
		}

		dealUUID := uuid.New()
		cd := &ContentDeal{
			Content:  content.ID,
			PropCid:  util.DbCID{propnd.Cid()},
			DealUUID: dealUUID.String(),
			Miner:    ms[i].String(),
			Verified: verified,
		}

		if err := cm.DB.Create(cd).Error; err != nil {
			return xerrors.Errorf("failed to create database entry for deal: %w", err)
		}

		// Send the deal proposal to the storage provider
		var cleanupDealPrep func() error
		var propPhase bool
		isPushTransfer := proto == filclient.DealProtocolv110
		switch proto {
		case filclient.DealProtocolv110:
			propPhase, err = cm.FilClient.SendProposalV110(ctx, *p, propnd.Cid())
		case filclient.DealProtocolv120:
			cleanupDealPrep, propPhase, err = cm.sendProposalV120(ctx, content.Location, *p, propnd.Cid(), dealUUID, cd.ID)
		default:
			err = fmt.Errorf("unrecognized deal protocol %s", proto)
		}

		if err != nil {
			// Clean up the database entry
			if err := cm.DB.Delete(&ContentDeal{}, cd).Error; err != nil {
				return fmt.Errorf("failed to delete content deal from db: %w", err)
			}

			if cleanupDealPrep != nil {
				// Clean up the preparation for deal request
				if err := cleanupDealPrep(); err != nil {
					log.Errorw("cleaning up deal prepared request", "error", err)
				}
			}

			// Record a deal failure
			phase := "send-proposal"
			if propPhase {
				phase = "propose"
			}
			cm.recordDealFailure(&DealFailureError{
				Miner:   ms[i],
				Phase:   phase,
				Message: err.Error(),
				Content: content.ID,
			})
			continue
		}

		responses[i] = &isPushTransfer
		deals[i] = cd
	}

	// Now start up some data transfers!
	// note: its okay if we dont start all the data transfers, we can just do it next time around
	for i, isPushTransfer := range responses {
		if isPushTransfer == nil {
			continue
		}

		// If the data transfer is a pull transfer, we don't need to explicitly
		// start the transfer (the Storage Provider will start pulling data as
		// soon as it accepts the proposal)
		if !(*isPushTransfer) {
			continue
		}

		cd := deals[i]
		if cd == nil {
			log.Warnf("have no ContentDeal for response we are about to start transfer on")
			continue
		}

		err := cm.StartDataTransfer(ctx, cd)
		if err != nil {
			log.Errorw("failed to start data transfer", "err", err, "miner", ms[i])
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
	if contentLoc == "local" {
		if len(cm.Node.Config.AnnounceAddrs) == 0 {
			return nil, false, xerrors.Errorf("cannot serve deal data: no announce address configured")
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
		addrInfo := cm.shuttleAddrInfo(contentLoc)
		// TODO: This is the address that the shuttle reports to the Estuary
		// primary node, but is it ok if it's also the address reported
		// as where to download files publically? If it's a public IP does
		// that mean that messages from Estuary primary node would go through
		// public internet to get to shuttle?
		if addrInfo == nil || len(addrInfo.Addrs) == 0 {
			return nil, false, xerrors.Errorf("no announce address found for shuttle: %s", contentLoc)
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
		if contentLoc == "local" {
			return cm.FilClient.Libp2pTransferMgr.CleanupPreparedRequest(ctx, dbid, authToken)
		}
		return cm.sendCleanupPreparedRequestCommand(ctx, contentLoc, dbid, authToken)
	}

	// Send the deal proposal to the storage provider
	propPhase, err := cm.FilClient.SendProposalV120(ctx, dbid, netprop, dealUUID, announceAddr, authToken)
	return cleanup, propPhase, err
}

func (cm *ContentManager) makeDealWithMiner(ctx context.Context, content util.Content, miner address.Address, verified bool) (uint, error) {
	ctx, span := cm.Tracer.Start(ctx, "makeDealWithMiner", trace.WithAttributes(
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

	prop, err := cm.FilClient.MakeDeal(ctx, miner, content.Cid.CID, price, ask.Ask.Ask.MinPieceSize, constants.DealDuration, verified)
	if err != nil {
		return 0, xerrors.Errorf("failed to construct a deal proposal: %w", err)
	}

	if err := cm.putProposalRecord(prop.DealProposal); err != nil {
		return 0, err
	}

	proto, err := cm.FilClient.DealProtocolForMiner(ctx, miner)
	if err != nil {
		cm.recordDealFailure(&DealFailureError{
			Miner:   miner,
			Phase:   "send-proposal",
			Message: err.Error(),
			Content: content.ID,
		})
		return 0, err
	}

	propnd, err := cborutil.AsIpld(prop.DealProposal)
	if err != nil {
		return 0, xerrors.Errorf("failed to compute deal proposal ipld node: %w", err)
	}

	dealUUID := uuid.New()
	deal := &ContentDeal{
		Content:  content.ID,
		PropCid:  util.DbCID{propnd.Cid()},
		DealUUID: dealUUID.String(),
		Miner:    miner.String(),
		Verified: verified,
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
		if err := cm.DB.Delete(&ContentDeal{}, deal).Error; err != nil {
			return 0, fmt.Errorf("failed to delete content deal from db: %w", err)
		}

		if cleanupDealPrep != nil {
			// Clean up the preparation for deal request
			if err := cleanupDealPrep(); err != nil {
				log.Errorw("cleaning up deal prepared request", "error", err)
			}
		}

		// Record a deal failure
		phase := "send-proposal"
		if propPhase {
			phase = "propose"
		}
		cm.recordDealFailure(&DealFailureError{
			Miner:   miner,
			Phase:   phase,
			Message: err.Error(),
			Content: content.ID,
		})
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

func (cm *ContentManager) StartDataTransfer(ctx context.Context, cd *ContentDeal) error {
	var cont util.Content
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

	if err := cm.DB.Model(ContentDeal{}).Where("id = ?", cd.ID).UpdateColumns(map[string]interface{}{
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
	_, span := cm.Tracer.Start(ctx, "calculateCarSize")
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

	if cont.Location != "local" {
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

	log.Infow("computing piece commitment", "data", cont.Cid.CID)
	return filclient.GeneratePieceCommitmentFFI(ctx, data, bs)
}

func (cm *ContentManager) getPieceCommitment(ctx context.Context, data cid.Cid, bs blockstore.Blockstore) (cid.Cid, uint64, abi.UnpaddedPieceSize, error) {
	_, span := cm.Tracer.Start(ctx, "getPieceComm")
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
		Data:    util.DbCID{data},
		Piece:   util.DbCID{pc},
		CarSize: carSize,
		Size:    size,
	}

	if err := cm.DB.Clauses(clause.OnConflict{DoNothing: true}).Create(&opcr).Error; err != nil {
		return cid.Undef, 0, 0, err
	}

	return pc, carSize, size, nil
}

func (cm *ContentManager) RefreshContentForCid(ctx context.Context, c cid.Cid) (blocks.Block, error) {
	ctx, span := cm.Tracer.Start(ctx, "refreshForCid", trace.WithAttributes(
		attribute.Stringer("cid", c),
	))
	defer span.End()

	var obj util.Object
	if err := cm.DB.First(&obj, "cid = ?", c.Bytes()).Error; err != nil {
		return nil, xerrors.Errorf("failed to get object from db: ", err)
	}

	var refs []util.ObjRef
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
	ctx, span := cm.Tracer.Start(ctx, "refreshContent")
	defer span.End()

	// TODO: this retrieval needs to mark all of its content as 'referenced'
	// until we can update its offloading status in the database
	var c util.Content
	if err := cm.DB.First(&c, "id = ?", cont).Error; err != nil {
		return err
	}

	loc, err := cm.SelectLocationForRetrieval(ctx, c)
	if err != nil {
		return err
	}
	log.Infof("refreshing content %d onto shuttle %s", cont, loc)

	switch loc {
	case "local":
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

	var activeDeals []ContentDeal
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

func (cm *ContentManager) retrieveContent(ctx context.Context, contentToFetch uint) error {
	ctx, span := cm.Tracer.Start(ctx, "retrieveContent", trace.WithAttributes(
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
	ctx, span := cm.Tracer.Start(ctx, "runRetrieval")
	defer span.End()

	var content util.Content
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

	var deals []ContentDeal
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
			cm.RecordRetrievalFailure(&util.RetrievalFailureRecord{
				Miner:   maddr.String(),
				Phase:   "query",
				Message: err.Error(),
				Content: content.ID,
				Cid:     content.Cid,
			})
			continue
		}
		log.Infow("got retrieval ask", "content", content, "miner", maddr, "ask", ask)

		if err := cm.TryRetrieve(ctx, maddr, content.Cid.CID, ask); err != nil {
			span.RecordError(err)
			log.Errorw("failed to retrieve content", "miner", maddr, "content", content.Cid.CID, "err", err)
			cm.RecordRetrievalFailure(&util.RetrievalFailureRecord{
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

// addObjectsToDatabase creates entries on the estuary database for CIDs related to an already pinned CID (`root`)
// These entries are saved on the `objects` table, while metadata about the `root` CID is mostly kept on the `contents` table
// The link between the `objects` and `contents` tables is the `obj_refs` table
func (cm *ContentManager) addObjectsToDatabase(ctx context.Context, content uint, dserv ipld.NodeGetter, root cid.Cid, objects []*util.Object, loc string) error {
	ctx, span := cm.Tracer.Start(ctx, "addObjectsToDatabase")
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

func (cm *ContentManager) sendStartTransferCommand(ctx context.Context, loc string, cd *ContentDeal, datacid cid.Cid) error {
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

		if err := cm.AddDatabaseTrackingToContent(ctx, content.ID, dserv, cm.Node.Blockstore, c, func(int64) {}); err != nil {
			return err
		}
	}

	if err := cm.DB.Model(util.Content{}).Where("id = ?", cont.ID).UpdateColumns(map[string]interface{}{
		"dag_split": true,
	}).Error; err != nil {
		return err
	}

	return nil
}

func (cm *ContentManager) registerShuttleConnection(handle string, hello *drpc.Hello) (chan *drpc.Command, func(), error) {
	cm.shuttlesLk.Lock()
	defer cm.shuttlesLk.Unlock()
	_, ok := cm.shuttles[handle]
	if ok {
		log.Warn("registering shuttle but found existing connection")
		return nil, nil, fmt.Errorf("shuttle already connected")
	}

	_, err := url.Parse(hello.Host)
	if err != nil {
		log.Errorf("shuttle had invalid hostname %q: %s", hello.Host, err)
		hello.Host = ""
	}

	if err := cm.DB.Model(Shuttle{}).Where("handle = ?", handle).UpdateColumns(map[string]interface{}{
		"host":            hello.Host,
		"peer_id":         hello.AddrInfo.ID.String(),
		"last_connection": time.Now(),
		"private":         hello.Private,
	}).Error; err != nil {
		return nil, nil, err
	}

	d := &ShuttleConnection{
		handle:   handle,
		address:  hello.Address,
		addrInfo: hello.AddrInfo,
		hostname: hello.Host,
		cmds:     make(chan *drpc.Command, 32),
		closing:  make(chan struct{}),
		private:  hello.Private,
	}

	cm.shuttles[handle] = d

	return d.cmds, func() {
		close(d.closing)
		cm.shuttlesLk.Lock()
		outd, ok := cm.shuttles[handle]
		if ok {
			if outd == d {
				delete(cm.shuttles, handle)
			}
		}
		cm.shuttlesLk.Unlock()

	}, nil
}

var ErrNilParams = fmt.Errorf("shuttle message had nil params")

func (cm *ContentManager) processShuttleMessage(handle string, msg *drpc.Message) error {
	ctx := context.TODO()

	// if the message contains a trace continue it here.
	if msg.HasTraceCarrier() {
		if sc := msg.TraceCarrier.AsSpanContext(); sc.IsValid() {
			ctx = trace.ContextWithRemoteSpanContext(ctx, sc)
		}
	}
	ctx, span := cm.Tracer.Start(ctx, "processShuttleMessage")
	defer span.End()

	log.Infof("handling shuttle message: %s", msg.Op)
	switch msg.Op {
	case drpc.OP_UpdatePinStatus:
		ups := msg.Params.UpdatePinStatus
		if ups == nil {
			return ErrNilParams
		}
		cm.updatePinStatus(handle, ups.DBID, ups.Status)
		return nil
	case drpc.OP_PinComplete:
		param := msg.Params.PinComplete
		if param == nil {
			return ErrNilParams
		}

		if err := cm.handlePinningComplete(ctx, handle, param); err != nil {
			log.Errorw("handling pin complete message failed", "shuttle", handle, "err", err)
		}
		return nil
	case drpc.OP_CommPComplete:
		param := msg.Params.CommPComplete
		if param == nil {
			return ErrNilParams
		}

		if err := cm.handleRpcCommPComplete(ctx, handle, param); err != nil {
			log.Errorf("handling commp complete message from shuttle %s: %s", handle, err)
		}
		return nil
	case drpc.OP_TransferStarted:
		param := msg.Params.TransferStarted
		if param == nil {
			return ErrNilParams
		}

		if err := cm.handleRpcTransferStarted(ctx, handle, param); err != nil {
			log.Errorf("handling transfer started message from shuttle %s: %s", handle, err)
		}
		return nil
	case drpc.OP_TransferStatus:
		param := msg.Params.TransferStatus
		if param == nil {
			return ErrNilParams
		}

		if err := cm.handleRpcTransferStatus(ctx, handle, param); err != nil {
			log.Errorf("handling transfer status message from shuttle %s: %s", handle, err)
		}
		return nil
	case drpc.OP_ShuttleUpdate:
		param := msg.Params.ShuttleUpdate
		if param == nil {
			return ErrNilParams
		}

		if err := cm.handleRpcShuttleUpdate(ctx, handle, param); err != nil {
			log.Errorf("handling shuttle update message from shuttle %s: %s", handle, err)
		}
		return nil
	case drpc.OP_GarbageCheck:
		param := msg.Params.GarbageCheck
		if param == nil {
			return ErrNilParams
		}

		if err := cm.handleRpcGarbageCheck(ctx, handle, param); err != nil {
			log.Errorf("handling garbage check message from shuttle %s: %s", handle, err)
		}
		return nil
	case drpc.OP_SplitComplete:
		param := msg.Params.SplitComplete
		if param == nil {
			return ErrNilParams
		}

		if err := cm.handleRpcSplitComplete(ctx, handle, param); err != nil {
			log.Errorf("handling split complete message from shuttle %s: %s", handle, err)
		}
		return nil
	default:
		return fmt.Errorf("unrecognized message op: %q", msg.Op)
	}
}

var ErrNoShuttleConnection = fmt.Errorf("no connection to requested shuttle")

func (cm *ContentManager) sendShuttleCommand(ctx context.Context, handle string, cmd *drpc.Command) error {
	if handle == "" {
		return fmt.Errorf("attempted to send command to empty shuttle handle")
	}

	cm.shuttlesLk.Lock()
	d, ok := cm.shuttles[handle]
	cm.shuttlesLk.Unlock()
	if ok {
		return d.sendMessage(ctx, cmd)
	}

	return ErrNoShuttleConnection
}

func (cm *ContentManager) shuttleIsOnline(handle string) bool {
	cm.shuttlesLk.Lock()
	d, ok := cm.shuttles[handle]
	cm.shuttlesLk.Unlock()
	if !ok {
		return false
	}

	select {
	case <-d.closing:
		return false
	default:
		return true
	}
}

func (cm *ContentManager) shuttleAddrInfo(handle string) *peer.AddrInfo {
	cm.shuttlesLk.Lock()
	defer cm.shuttlesLk.Unlock()
	d, ok := cm.shuttles[handle]
	if ok {
		return &d.addrInfo
	}
	return nil
}

func (cm *ContentManager) shuttleHostName(handle string) string {
	cm.shuttlesLk.Lock()
	defer cm.shuttlesLk.Unlock()
	d, ok := cm.shuttles[handle]
	if ok {
		return d.hostname
	}
	return ""
}

func (cm *ContentManager) shuttleStorageStats(handle string) *util.ShuttleStorageStats {
	cm.shuttlesLk.Lock()
	defer cm.shuttlesLk.Unlock()
	d, ok := cm.shuttles[handle]
	if !ok {
		return nil
	}

	return &util.ShuttleStorageStats{
		BlockstoreSize: d.blockstoreSize,
		BlockstoreFree: d.blockstoreFree,
		PinCount:       d.pinCount,
		PinQueueLength: d.pinQueueLength,
	}
}

func (cm *ContentManager) handleRpcCommPComplete(ctx context.Context, handle string, resp *drpc.CommPComplete) error {
	ctx, span := cm.Tracer.Start(ctx, "handleRpcCommPComplete")
	defer span.End()

	opcr := PieceCommRecord{
		Data:    util.DbCID{resp.Data},
		Piece:   util.DbCID{resp.CommP},
		Size:    resp.Size,
		CarSize: resp.CarSize,
	}

	if err := cm.DB.Clauses(clause.OnConflict{DoNothing: true}).Create(&opcr).Error; err != nil {
		return err
	}

	return nil
}

func (cm *ContentManager) handleRpcTransferStarted(ctx context.Context, handle string, param *drpc.TransferStarted) error {
	if err := cm.DB.Model(ContentDeal{}).Where("id = ?", param.DealDBID).UpdateColumns(map[string]interface{}{
		"dt_chan":           param.Chanid,
		"transfer_started":  time.Now(),
		"transfer_finished": time.Time{},
	}).Error; err != nil {
		return xerrors.Errorf("failed to update deal with channel ID: %w", err)
	}

	log.Infow("Started data transfer on shuttle", "chanid", param.Chanid, "shuttle", handle)
	return nil
}

func (cm *ContentManager) handleRpcTransferStatus(ctx context.Context, handle string, param *drpc.TransferStatus) error {
	log.Infof("handling transfer status rpc update: %d %v", param.DealDBID, param.State == nil)

	var cd ContentDeal
	if param.DealDBID != 0 {
		if err := cm.DB.First(&cd, "id = ?", param.DealDBID).Error; err != nil {
			return err
		}
	} else if param.State != nil {
		if err := cm.DB.First(&cd, "dt_chan = ?", param.State.TransferID).Error; err != nil {
			return err
		}
	} else {
		return fmt.Errorf("received transfer status update with no identifiers")
	}

	if param.Failed {
		miner, err := cd.MinerAddr()
		if err != nil {
			return err
		}

		if oerr := cm.recordDealFailure(&DealFailureError{
			Miner:   miner,
			Phase:   "start-data-transfer-remote",
			Message: fmt.Sprintf("failure from shuttle %s: %s", handle, param.Message),
			Content: cd.Content,
		}); oerr != nil {
			return oerr
		}

		cm.updateTransferStatus(ctx, handle, cd.ID, &filclient.ChannelState{
			Status:  datatransfer.Failed,
			Message: fmt.Sprintf("failure from shuttle %s: %s", handle, param.Message),
		})
		return nil
	}
	cm.updateTransferStatus(ctx, handle, cd.ID, param.State)
	return nil
}

func (cm *ContentManager) handleRpcShuttleUpdate(ctx context.Context, handle string, param *drpc.ShuttleUpdate) error {
	cm.shuttlesLk.Lock()
	defer cm.shuttlesLk.Unlock()
	d, ok := cm.shuttles[handle]
	if !ok {
		return fmt.Errorf("shuttle connection not found while handling update for %q", handle)
	}

	d.spaceLow = (param.BlockstoreFree < (param.BlockstoreSize / 10))
	d.blockstoreFree = param.BlockstoreFree
	d.blockstoreSize = param.BlockstoreSize
	d.pinCount = param.NumPins
	d.pinQueueLength = int64(param.PinQueueSize)

	return nil
}

func (cm *ContentManager) handleRpcGarbageCheck(ctx context.Context, handle string, param *drpc.GarbageCheck) error {
	var tounpin []uint
	for _, c := range param.Contents {
		var cont util.Content
		if err := cm.DB.First(&cont, "id = ?", c).Error; err != nil {
			if xerrors.Is(err, gorm.ErrRecordNotFound) {
				tounpin = append(tounpin, c)
			} else {
				return err
			}
		}

		if cont.Location != handle || cont.Offloaded {
			tounpin = append(tounpin, c)
		}
	}

	return cm.sendUnpinCmd(ctx, handle, tounpin)
}

func (cm *ContentManager) handleRpcSplitComplete(ctx context.Context, handle string, param *drpc.SplitComplete) error {
	if param.ID == 0 {
		return fmt.Errorf("split complete send with ID = 0")
	}

	// TODO: do some sanity checks that the sub pieces were all made successfully...

	if err := cm.DB.Model(util.Content{}).Where("id = ?", param.ID).UpdateColumns(map[string]interface{}{
		"dag_split": true,
	}).Error; err != nil {
		return fmt.Errorf("failed to update content for split complete: %w", err)
	}

	if err := cm.DB.Delete(&util.ObjRef{}, "content = ?", param.ID).Error; err != nil {
		return fmt.Errorf("failed to delete object references for newly split object: %w", err)
	}

	return nil
}

func (cm *ContentManager) AddDatabaseTrackingToContent(ctx context.Context, cont uint, dserv ipld.NodeGetter, bs blockstore.Blockstore, root cid.Cid, cb func(int64)) error {
	ctx, span := cm.Tracer.Start(ctx, "computeObjRefsUpdate")
	defer span.End()

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	gotData := make(chan struct{}, 1)
	go func() {
		nodata := time.NewTimer(constants.NoDataTimeout)
		defer nodata.Stop()

		for {
			select {
			case <-nodata.C:
				cancel()
			case <-gotData:
				nodata.Reset(constants.NoDataTimeout)
			case <-ctx.Done():
				return
			}
		}
	}()

	var objlk sync.Mutex
	var objects []*util.Object
	cset := cid.NewSet()

	defer func() {
		cm.inflightCidsLk.Lock()
		_ = cset.ForEach(func(c cid.Cid) error {
			v, ok := cm.inflightCids[c]
			if !ok || v <= 0 {
				log.Errorf("cid should be inflight but isn't: %s", c)
			}

			cm.inflightCids[c]--
			if cm.inflightCids[c] == 0 {
				delete(cm.inflightCids, c)
			}
			return nil
		})
		cm.inflightCidsLk.Unlock()
	}()

	err := merkledag.Walk(ctx, func(ctx context.Context, c cid.Cid) ([]*ipld.Link, error) {
		// cset.Visit gets called first, so if we reach here we should immediately track the CID
		cm.inflightCidsLk.Lock()
		cm.inflightCids[c]++
		cm.inflightCidsLk.Unlock()

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
			Cid:  util.DbCID{c},
			Size: len(node.RawData()),
		})
		objlk.Unlock()

		if c.Type() == cid.Raw {
			return nil, nil
		}

		return node.Links(), nil
	}, root, cset.Visit, merkledag.Concurrent())

	if err != nil {
		return err
	}

	if err = cm.addObjectsToDatabase(ctx, cont, dserv, root, objects, "local"); err != nil {
		return err
	}

	return nil
}

func (cm *ContentManager) addDatabaseTracking(ctx context.Context, u *util.User, dserv ipld.NodeGetter, bs blockstore.Blockstore, root cid.Cid, fname string, replication int) (*util.Content, error) {
	ctx, span := cm.Tracer.Start(ctx, "computeObjRefs")
	defer span.End()

	content := &util.Content{
		Cid:         util.DbCID{root},
		Name:        fname,
		Active:      false,
		Pinning:     true,
		UserID:      u.ID,
		Replication: replication,
		Location:    "local",
	}

	if err := cm.DB.Create(content).Error; err != nil {
		return nil, xerrors.Errorf("failed to track new content in database: %w", err)
	}

	if err := cm.AddDatabaseTrackingToContent(ctx, content.ID, dserv, bs, root, func(int64) {}); err != nil {
		return nil, err
	}

	return content, nil
}

func (cm *ContentManager) SelectLocationForContent(ctx context.Context, obj cid.Cid, uid uint) (string, error) {
	ctx, span := cm.Tracer.Start(ctx, "selectLocation")
	defer span.End()

	var user util.User
	if err := cm.DB.First(&user, "id = ?", uid).Error; err != nil {
		return "", err
	}

	allShuttlesLowSpace := true
	lowSpace := make(map[string]bool)
	var activeShuttles []string
	cm.shuttlesLk.Lock()
	for d, sh := range cm.shuttles {
		if !sh.private {
			lowSpace[d] = sh.spaceLow
			activeShuttles = append(activeShuttles, d)
		} else {
			allShuttlesLowSpace = false
		}
	}
	cm.shuttlesLk.Unlock()

	var shuttles []Shuttle
	if err := cm.DB.Order("priority desc").Find(&shuttles, "handle in ? and open", activeShuttles).Error; err != nil {
		return "", err
	}

	// prefer shuttles that are not low on blockstore space
	sort.SliceStable(shuttles, func(i, j int) bool {
		lsI := lowSpace[shuttles[i].Handle]
		lsJ := lowSpace[shuttles[j].Handle]

		if lsI == lsJ {
			return false
		}

		return lsJ
	})

	if len(shuttles) == 0 {
		//log.Info("no shuttles available for content to be delegated to")
		if cm.LocalContentAddingDisabled {
			return "", fmt.Errorf("no shuttles available and local content adding disabled")
		}

		return "local", nil
	}

	// TODO: take into account existing staging zones and their primary
	// locations while choosing
	ploc, err := cm.primaryStagingLocation(ctx, uid)
	if err != nil {
		return "", err
	}

	if ploc != "" {
		if allShuttlesLowSpace || !lowSpace[ploc] {
			for _, sh := range shuttles {
				if sh.Handle == ploc {
					return ploc, nil
				}
			}
		}

		// TODO: maybe we should just assign the pin to the preferred shuttle
		// anyways, this could be the case where theres a small amount of
		// downtime from rebooting or something
		log.Warnf("preferred shuttle %q not online", ploc)
	}

	// since they are ordered by priority, just take the first
	return shuttles[0].Handle, nil
}

func (cm *ContentManager) SelectLocationForRetrieval(ctx context.Context, cont util.Content) (string, error) {
	ctx, span := cm.Tracer.Start(ctx, "selectLocationForRetrieval")
	defer span.End()

	var activeShuttles []string
	cm.shuttlesLk.Lock()
	for d, sh := range cm.shuttles {
		if !sh.private {
			activeShuttles = append(activeShuttles, d)
		}
	}
	cm.shuttlesLk.Unlock()

	var shuttles []Shuttle
	if err := cm.DB.Order("priority desc").Find(&shuttles, "handle in ? and open", activeShuttles).Error; err != nil {
		return "", err
	}

	if len(shuttles) == 0 {
		if cm.LocalContentAddingDisabled {
			return "", fmt.Errorf("no shuttles available and local content adding disabled")
		}

		return "local", nil
	}

	// prefer the shuttle the content is already on
	for _, sh := range shuttles {
		if sh.Handle == cont.Location {
			return sh.Handle, nil
		}
	}

	// since they are ordered by priority, just take the first
	return shuttles[0].Handle, nil
}

func (cm *ContentManager) primaryStagingLocation(ctx context.Context, uid uint) (string, error) {
	cm.bucketLk.Lock()
	defer cm.bucketLk.Unlock()
	zones, ok := cm.buckets[uid]
	if !ok {
		return "", nil
	}

	// TODO: maybe we could make this more complex, but for now, if we have a
	// staging zone opened in a particular location, just keep using that one
	for _, z := range zones {
		return z.Location, nil
	}

	log.Warnf("empty staging zone set for user %d", uid)
	return "", nil
}

func (cm *ContentManager) RecordRetrievalFailure(rfr *util.RetrievalFailureRecord) error {
	return cm.DB.Create(rfr).Error
}

func (cm *ContentManager) RecordRetrievalSuccess(cc cid.Cid, m address.Address, rstats *filclient.RetrievalStats) {
	if err := cm.DB.Create(&RetrievalSuccessRecord{
		Cid:          util.DbCID{cc},
		Miner:        m.String(),
		Peer:         rstats.Peer.String(),
		Size:         rstats.Size,
		DurationMs:   rstats.Duration.Milliseconds(),
		AverageSpeed: rstats.AverageSpeed,
		TotalPayment: rstats.TotalPayment.String(),
		NumPayments:  rstats.NumPayments,
		AskPrice:     rstats.AskPrice.String(),
	}).Error; err != nil {
		log.Errorf("failed to write retrieval success record: %s", err)
	}
}

func (cm *ContentManager) TryRetrieve(ctx context.Context, maddr address.Address, c cid.Cid, ask *retrievalmarket.QueryResponse) error {

	proposal, err := retrievehelper.RetrievalProposalForAsk(ask, c, nil)
	if err != nil {
		return err
	}

	stats, err := cm.FilClient.RetrieveContent(ctx, maddr, proposal)
	if err != nil {
		return err
	}

	cm.RecordRetrievalSuccess(c, maddr, stats)
	return nil
}
