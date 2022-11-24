package contentmgr

import (
	"bytes"
	"container/heap"
	"context"
	"fmt"
	"math/rand"
	"sort"
	"strings"
	"sync"
	"time"

	marketv9 "github.com/filecoin-project/go-state-types/builtin/v9/market"
	"go.uber.org/zap"

	"github.com/application-research/estuary/config"
	"github.com/application-research/estuary/constants"
	"github.com/application-research/estuary/drpc"
	"github.com/application-research/estuary/miner"
	"github.com/application-research/estuary/model"
	"github.com/application-research/estuary/node"
	"github.com/application-research/estuary/pinner"
	"github.com/application-research/estuary/util"
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
	"github.com/filecoin-project/specs-actors/v6/actors/builtin/market"
	"github.com/google/uuid"
	lru "github.com/hashicorp/golang-lru"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-blockservice"
	"github.com/ipfs/go-cid"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	cbor "github.com/ipfs/go-ipld-cbor"
	ipld "github.com/ipfs/go-ipld-format"
	"github.com/ipfs/go-merkledag"
	"github.com/ipfs/go-metrics-interface"
	uio "github.com/ipfs/go-unixfs/io"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	"github.com/multiformats/go-multiaddr"
	"github.com/pkg/errors"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/xerrors"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type deal struct {
	minerAddr      address.Address
	isPushTransfer bool
	contentDeal    *model.ContentDeal
}

type ContentManager struct {
	DB                   *gorm.DB
	Api                  api.Gateway
	FilClient            *filclient.FilClient
	Node                 *node.Node
	cfg                  *config.Estuary
	tracer               trace.Tracer
	Blockstore           node.EstuaryBlockstore
	Tracker              *util.TrackingBlockstore
	NotifyBlockstore     *node.NotifyBlockstore
	toCheck              chan uint
	queueMgr             *queueManager
	retrLk               sync.Mutex
	retrievalsInProgress map[uint]*util.RetrievalProgress
	contentLk            sync.RWMutex
	// deal bucketing stuff
	BucketLk sync.Mutex
	Buckets  map[uint][]*ContentStagingZone
	// some behavior flags
	FailDealOnTransferFailure bool
	dealDisabledLk            sync.Mutex
	isDealMakingDisabled      bool
	PinMgr                    *pinner.PinManager
	ShuttlesLk                sync.Mutex
	Shuttles                  map[string]*ShuttleConnection
	remoteTransferStatus      *lru.ARCCache
	inflightCids              map[cid.Cid]uint
	inflightCidsLk            sync.Mutex
	DisableFilecoinStorage    bool
	IncomingRPCMessages       chan *drpc.Message
	minerManager              miner.IMinerManager
	log                       *zap.SugaredLogger
}

func (cm *ContentManager) isInflight(c cid.Cid) bool {
	cm.inflightCidsLk.Lock()
	defer cm.inflightCidsLk.Unlock()

	v, ok := cm.inflightCids[c]
	return ok && v > 0
}

type stagingZoneReadiness struct {
	IsReady         bool   `json:"isReady"`
	ReadinessReason string `json:"readinessReason"`
}

type ContentStagingZone struct {
	ZoneOpened      time.Time            `json:"zoneOpened"`
	Contents        []util.Content       `json:"contents"`
	MinSize         int64                `json:"minSize"`
	MaxSize         int64                `json:"maxSize"`
	CurSize         int64                `json:"curSize"`
	User            uint                 `json:"user"`
	ContID          uint                 `json:"contentID"`
	Location        string               `json:"location"`
	IsConsolidating bool                 `json:"isConsolidating"`
	Readiness       stagingZoneReadiness `json:"readiness"`
	lk              sync.Mutex
}

func (cb *ContentStagingZone) DeepCopy() *ContentStagingZone {
	cb.lk.Lock()
	defer cb.lk.Unlock()

	cb2 := &ContentStagingZone{
		ZoneOpened: cb.ZoneOpened,
		Contents:   make([]util.Content, len(cb.Contents)),
		MinSize:    cb.MinSize,
		MaxSize:    cb.MaxSize,
		CurSize:    cb.CurSize,
		User:       cb.User,
		ContID:     cb.ContID,
		Location:   cb.Location,
		Readiness:  cb.Readiness,
	}
	copy(cb2.Contents, cb.Contents)
	return cb2
}

func (cm *ContentManager) newContentStagingZone(user uint, loc string) (*ContentStagingZone, error) {
	content := &util.Content{
		Size:        0,
		Name:        "aggregate",
		Active:      false,
		Pinning:     true,
		UserID:      user,
		Replication: cm.cfg.Replication,
		Aggregate:   true,
		Location:    loc,
	}

	if err := cm.DB.Create(content).Error; err != nil {
		return nil, err
	}

	return &ContentStagingZone{
		ZoneOpened: content.CreatedAt,
		MinSize:    cm.cfg.Content.MinSize,
		MaxSize:    cm.cfg.Content.MaxSize,
		User:       user,
		ContID:     content.ID,
		Location:   content.Location,
		Readiness:  stagingZoneReadiness{false, "Readiness not yet evaluated"},
	}, nil
}

func (cb *ContentStagingZone) updateReadiness() {
	// if it's above the size requirement, go right ahead
	if cb.CurSize > cb.MinSize {
		cb.Readiness.IsReady = true
		cb.Readiness.ReadinessReason = fmt.Sprintf(
			"Current deal size of %d bytes is above minimum size requirement of %d bytes",
			cb.CurSize,
			cb.MinSize)
		return
	}

	cb.Readiness.IsReady = false
	cb.Readiness.ReadinessReason = fmt.Sprintf(
		"Minimum size requirement not met (current: %d bytes, minimum: %d bytes)\n",
		cb.CurSize,
		cb.MinSize)
}

func (cm *ContentManager) tryAddContent(cb *ContentStagingZone, c util.Content) (bool, error) {
	cb.lk.Lock()
	defer cb.lk.Unlock()
	defer cb.updateReadiness()

	// TODO: propagate reason for failing to add content to bucket to end user

	// if this bucket is being consolidated, do not add anymore content
	if cb.IsConsolidating {
		return false, nil
	}

	if cb.CurSize+c.Size > cb.MaxSize {
		return false, nil
	}

	if err := cm.DB.Model(util.Content{}).
		Where("id = ?", c.ID).
		UpdateColumn("aggregated_in", cb.ContID).Error; err != nil {
		return false, err
	}

	cb.Contents = append(cb.Contents, c)
	cb.CurSize += c.Size
	return true, nil
}

// tryRemoveContent Removes content from in-memory buckets
// Assumes content is already removed from DB
func (cm *ContentManager) tryRemoveContent(cb *ContentStagingZone, c util.Content) (bool, error) {
	cb.lk.Lock()
	defer cb.lk.Unlock()

	// if this bucket is being consolidated, do not remove content
	if cb.IsConsolidating {
		return false, nil
	}

	newContents := make([]util.Content, 0)
	newSize := int64(0)
	for _, cont := range cb.Contents {
		if cont.ID != c.ID {
			newContents = append(newContents, cont)
			newSize += cont.Size
		}
	}
	cb.Contents = newContents
	cb.CurSize = newSize

	return true, nil
}

func (cb *ContentStagingZone) hasContent(c util.Content) bool {
	cb.lk.Lock()
	defer cb.lk.Unlock()

	for _, cont := range cb.Contents {
		if cont.ID == c.ID {
			return true
		}
	}
	return false
}

func NewContentManager(db *gorm.DB, api api.Gateway, fc *filclient.FilClient, tbs *util.TrackingBlockstore, pinmgr *pinner.PinManager, nd *node.Node, cfg *config.Estuary, minerManager miner.IMinerManager, log *zap.SugaredLogger) (*ContentManager, error) {
	cache, err := lru.NewARC(50000)
	if err != nil {
		return nil, err
	}

	cm := &ContentManager{
		cfg:                       cfg,
		DB:                        db,
		Api:                       api,
		FilClient:                 fc,
		Blockstore:                tbs.Under().(node.EstuaryBlockstore),
		Node:                      nd,
		NotifyBlockstore:          nd.NotifBlockstore,
		Tracker:                   tbs,
		toCheck:                   make(chan uint, 100000),
		retrievalsInProgress:      make(map[uint]*util.RetrievalProgress),
		Buckets:                   make(map[uint][]*ContentStagingZone),
		PinMgr:                    pinmgr,
		remoteTransferStatus:      cache,
		Shuttles:                  make(map[string]*ShuttleConnection),
		inflightCids:              make(map[cid.Cid]uint),
		FailDealOnTransferFailure: cfg.Deal.FailOnTransferFailure,
		isDealMakingDisabled:      cfg.Deal.IsDisabled,
		tracer:                    otel.Tracer("replicator"),
		DisableFilecoinStorage:    cfg.DisableFilecoinStorage,
		IncomingRPCMessages:       make(chan *drpc.Message, cfg.RPCMessage.IncomingQueueSize),
		minerManager:              minerManager,
		log:                       log,
	}

	cm.queueMgr = newQueueManager(func(c uint) {
		cm.ToCheck(c)
	})
	return cm, nil
}

func (cm *ContentManager) ToCheck(contID uint) {
	// if DisableFilecoinStorage is not enabled, queue content for deal making
	if !cm.DisableFilecoinStorage {
		cm.toCheck <- contID
	}
}

func (cm *ContentManager) runStagingBucketWorker(ctx context.Context) {
	timer := time.NewTicker(cm.cfg.StagingBucket.AggregateInterval)
	for {
		select {
		case <-timer.C:
			cm.log.Debugw("content check queue", "length", len(cm.queueMgr.queue.elems), "nextEvent", cm.queueMgr.nextEvent)

			zones := cm.popReadyStagingZone()
			for _, z := range zones {
				if err := cm.processStagingZone(ctx, z); err != nil {
					cm.log.Errorf("content aggregation failed (zone %d): %s", z.ContID, err)
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
		case c := <-cm.toCheck:
			cm.log.Debugf("checking content: %d", c)

			var content util.Content
			if err := cm.DB.First(&content, "id = ?", c).Error; err != nil {
				cm.log.Errorf("finding content %d in database: %s", c, err)
				continue
			}

			err := cm.ensureStorage(context.TODO(), content, func(dur time.Duration) {
				cm.queueMgr.add(content.ID, dur)
			})
			if err != nil {
				cm.log.Errorf("failed to ensure replication of content %d: %s", content.ID, err)
				cm.queueMgr.add(content.ID, time.Minute*5)
			}
		}
	}
}

func (cm *ContentManager) Run(ctx context.Context) {
	// if staging buckets are enabled, rebuild the buckets, and run the bucket aggregate worker
	if cm.cfg.StagingBucket.Enabled {
		// rebuild the staging buckets
		if err := cm.rebuildStagingBuckets(); err != nil {
			cm.log.Fatalf("failed to rebuild staging buckets: %s", err)
		}

		// run the staging bucket aggregator worker
		go cm.runStagingBucketWorker(ctx)
		cm.log.Infof("rebuilt staging buckets and spun up staging bucket worker")
	}

	// if FilecoinStorage is enabled, check content deals or make content deals
	if !cm.DisableFilecoinStorage {
		go func() {
			// rebuild toCheck queue
			if err := cm.rebuildToCheckQueue(); err != nil {
				cm.log.Errorf("failed to recheck existing content: %s", err)
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

func (cm *ContentManager) currentLocationForContent(cntId uint) (string, error) {
	var cont util.Content
	if err := cm.DB.First(&cont, "id = ?", cntId).Error; err != nil {
		return "", err
	}
	return cont.Location, nil
}

func (cm *ContentManager) getGroupedStagedContentLocations(ctx context.Context, b *ContentStagingZone) (map[string]string, error) {
	out := make(map[string]string)
	for _, c := range b.Contents {
		// need to get current location from db, incase this stage content was a part of a consolidated content - its location would have changed.
		// so we can group it into its current location
		loc, err := cm.currentLocationForContent(c.ID)
		if err != nil {
			return nil, err
		}
		out[c.Location] = loc
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("no location for staged contents")
	}
	return out, nil
}

func (cm *ContentManager) consolidateStagedContent(ctx context.Context, b *ContentStagingZone) error {
	var dstLocation string
	var curMax int64
	dataByLoc := make(map[string]int64)
	contentByLoc := make(map[string][]util.Content)

	// TODO: make this one batch DB query instead of querying per content
	for _, c := range b.Contents {
		loc, err := cm.currentLocationForContent(c.ID)
		if err != nil {
			return err
		}
		contentByLoc[loc] = append(contentByLoc[loc], c)

		ntot := dataByLoc[loc] + c.Size
		dataByLoc[loc] = ntot

		// temp: dont ever migrate content back to primary instance for aggregation, always prefer elsewhere
		if loc == constants.ContentLocationLocal {
			continue
		}

		if ntot > curMax && cm.ShuttleCanAddContent(loc) {
			curMax = ntot
			dstLocation = loc
		}
	}

	// okay, move everything to 'primary'
	var toMove []util.Content
	for loc, conts := range contentByLoc {
		if loc != dstLocation {
			toMove = append(toMove, conts...)
		}
	}

	cm.log.Debugw("consolidating content to single location for aggregation", "user", b.User, "dstLocation", dstLocation, "numItems", len(toMove), "primaryWeight", curMax)
	if dstLocation == constants.ContentLocationLocal {
		return cm.migrateContentsToLocalNode(ctx, toMove)
	} else {
		return cm.SendConsolidateContentCmd(ctx, dstLocation, toMove)
	}
}

func (cm *ContentManager) processStagingZone(ctx context.Context, b *ContentStagingZone) error {
	ctx, span := cm.tracer.Start(ctx, "aggregateContent")
	defer span.End()

	grpLocs, err := cm.getGroupedStagedContentLocations(ctx, b)
	if err != nil {
		return err
	}

	// if the staged contents of this bucket are in different locations (more than 1 group)
	// Need to migrate/consolidate the contents to the same location
	// TODO - we should avoid doing this, best we have staging by location - this process is just to expensive
	if len(grpLocs) > 1 {
		cm.BucketLk.Lock()
		// Need to migrate content all to the same shuttle
		// Only attempt consolidation on a zone if one is not ongoing, prevents re-consolidation request
		if !b.IsConsolidating {
			b.IsConsolidating = true
			go func() {
				if err := cm.consolidateStagedContent(ctx, b); err != nil {
					cm.log.Errorf("failed to consolidate staged content: %s", err)
				}
			}()
		}

		// put the staging zone back in the list
		cm.Buckets[b.User] = append(cm.Buckets[b.User], b)
		cm.BucketLk.Unlock()
		return nil
	}
	// if all contents are already in one location, proceed to aggregate them
	return cm.AggregateStagingZone(ctx, b, grpLocs)
}

func (cm *ContentManager) AggregateStagingZone(ctx context.Context, z *ContentStagingZone, grpLocs map[string]string) error {
	ctx, span := cm.tracer.Start(ctx, "aggregateStagingZone")
	defer span.End()

	cm.log.Debugf("aggregating zone: %d", z.ContID)

	// get the aggregate content location
	var loc string
	for _, cl := range grpLocs {
		loc = cl
		break
	}

	if loc == constants.ContentLocationLocal {
		dir, err := cm.CreateAggregate(ctx, z.Contents)
		if err != nil {
			return xerrors.Errorf("failed to create aggregate: %w", err)
		}

		ncid := dir.Cid()
		size, err := dir.Size()
		if err != nil {
			return err
		}

		if size == 0 {
			cm.log.Warnf("content %d aggregate dir apparent size is zero", z.ContID)
		}

		obj := &util.Object{
			Cid:  util.DbCID{CID: ncid},
			Size: int(size),
		}
		if err := cm.DB.Create(obj).Error; err != nil {
			return err
		}

		if err := cm.DB.Create(&util.ObjRef{
			Content: z.ContID,
			Object:  obj.ID,
		}).Error; err != nil {
			return err
		}

		if err := cm.Blockstore.Put(ctx, dir); err != nil {
			return err
		}

		if err := cm.DB.Model(util.Content{}).Where("id = ?", z.ContID).UpdateColumns(map[string]interface{}{
			"active":   true,
			"pinning":  false,
			"cid":      util.DbCID{CID: ncid},
			"size":     size,
			"location": loc,
		}).Error; err != nil {
			return err
		}

		go func() {
			cm.ToCheck(z.ContID)
		}()
		return nil
	}

	// handle aggregate on shuttle
	var aggrConts []drpc.AggregateContent
	for _, c := range z.Contents {
		aggrConts = append(aggrConts, drpc.AggregateContent{ID: c.ID, Name: c.Name, CID: c.Cid.CID})
	}

	var bContent util.Content
	if err := cm.DB.First(&bContent, "id = ?", z.ContID).Error; err != nil {
		return err
	}
	return cm.SendAggregateCmd(ctx, loc, bContent, aggrConts)
}

func (cm *ContentManager) CreateAggregate(ctx context.Context, conts []util.Content) (ipld.Node, error) {
	cm.log.Debug("aggregating contents in staging zone into new content")

	bserv := blockservice.New(cm.Blockstore, cm.Node.Bitswap)
	dserv := merkledag.NewDAGService(bserv)

	sort.Slice(conts, func(i, j int) bool {
		return conts[i].ID < conts[j].ID
	})

	dir := uio.NewDirectory(dserv)
	for _, c := range conts {
		nd, err := dserv.Get(ctx, c.Cid.CID)
		if err != nil {
			return nil, err
		}

		// TODO: consider removing the "<cid>-" prefix on the content name
		err = dir.AddChild(ctx, fmt.Sprintf("%d-%s", c.ID, c.Name), nd)
		if err != nil {
			return nil, err
		}
	}
	dirNd, err := dir.GetNode()
	if err != nil {
		return nil, err
	}
	return dirNd, nil
}

func (cm *ContentManager) rebuildStagingBuckets() error {
	cm.log.Info("rebuilding staging buckets.......")

	var stages []util.Content
	if err := cm.DB.Find(&stages, "not active and pinning and aggregate").Error; err != nil {
		return err
	}

	zones := make(map[uint][]*ContentStagingZone)
	for _, c := range stages {
		var inZones []util.Content
		if err := cm.DB.Find(&inZones, "aggregated_in = ?", c.ID).Error; err != nil {
			return err
		}

		var zSize int64
		for _, zc := range inZones {
			// TODO: do some sanity checking that we havent messed up and added
			// too many items to this staging zone
			zSize += zc.Size
		}

		z := &ContentStagingZone{
			ZoneOpened: c.CreatedAt,
			Contents:   inZones,
			MinSize:    cm.cfg.Content.MinSize,
			MaxSize:    cm.cfg.Content.MaxSize,
			CurSize:    zSize,
			User:       c.UserID,
			ContID:     c.ID,
			Location:   c.Location,
		}
		z.updateReadiness()
		zones[c.UserID] = append(zones[c.UserID], z)
	}
	cm.Buckets = zones
	return nil
}

func (cm *ContentManager) rebuildToCheckQueue() error {
	cm.log.Info("rebuilding contents queue .......")

	var allcontent []util.Content
	if err := cm.DB.Find(&allcontent, "active AND NOT aggregated_in > 0").Error; err != nil {
		return xerrors.Errorf("finding all content in database: %w", err)
	}

	go func() {
		for _, c := range allcontent {
			cm.ToCheck(c.ID)
		}
	}()
	return nil
}

func (cm *ContentManager) SetDataTransferStartedOrFinished(ctx context.Context, dealDBID uint, chanIDOrTransferID string, st *filclient.ChannelState, isStarted bool) error {
	if st == nil {
		return nil
	}

	var deal model.ContentDeal
	if err := cm.DB.First(&deal, "id = ?", dealDBID).Error; err != nil {
		return err
	}

	var cont util.Content
	if err := cm.DB.First(&cont, "id = ?", deal.Content).Error; err != nil {
		return err
	}

	updates := map[string]interface{}{
		"dt_chan": chanIDOrTransferID,
	}

	switch isStarted {
	case true:
		updates["transfer_started"] = time.Now() // boost transfers does not support stages, so we can't get actual timestamps
		if s := st.Stages.GetStage("Requested"); s != nil {
			updates["transfer_started"] = s.CreatedTime.Time()
		}
	default:
		updates["transfer_finished"] = time.Now() // boost transfers does not support stages, so we can't get actual timestamps
		if s := st.Stages.GetStage("TransferFinished"); s != nil {
			updates["transfer_finished"] = s.CreatedTime.Time()
		}
	}

	if err := cm.DB.Model(model.ContentDeal{}).Where("id = ?", dealDBID).UpdateColumns(updates).Error; err != nil {
		return xerrors.Errorf("failed to update deal with channel ID: %w", err)
	}
	return nil
}

func (cm *ContentManager) contentInStagingZone(ctx context.Context, content util.Content) bool {
	cm.BucketLk.Lock()
	defer cm.BucketLk.Unlock()

	bucks, ok := cm.Buckets[content.UserID]
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

func (cm *ContentManager) GetStagingZonesForUser(ctx context.Context, user uint) []*ContentStagingZone {
	cm.BucketLk.Lock()
	defer cm.BucketLk.Unlock()

	blist, ok := cm.Buckets[user]
	if !ok {
		return []*ContentStagingZone{}
	}

	var out []*ContentStagingZone
	for _, b := range blist {
		out = append(out, b.DeepCopy())
	}
	return out
}

func (cm *ContentManager) GetStagingZoneSnapshot(ctx context.Context) map[uint][]*ContentStagingZone {
	cm.BucketLk.Lock()
	defer cm.BucketLk.Unlock()

	out := make(map[uint][]*ContentStagingZone)
	for u, blist := range cm.Buckets {
		var copylist []*ContentStagingZone

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
		cm.log.Warnf("attempted to add content to staging zone that was already staged: %d (is in %d)", content.ID, content.AggregatedIn)
		return nil
	}

	cm.log.Debugf("adding content to staging zone: %d", content.ID)
	cm.BucketLk.Lock()
	defer cm.BucketLk.Unlock()

	blist, ok := cm.Buckets[content.UserID]
	if !ok {
		b, err := cm.newContentStagingZone(content.UserID, content.Location)
		if err != nil {
			return fmt.Errorf("failed to create new staging zone content: %w", err)
		}

		_, err = cm.tryAddContent(b, content)
		if err != nil {
			return fmt.Errorf("failed to add content to staging zone: %w", err)
		}

		cm.Buckets[content.UserID] = []*ContentStagingZone{b}
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
	cm.Buckets[content.UserID] = append(blist, b)

	_, err = cm.tryAddContent(b, content)
	if err != nil {
		return err
	}

	return nil
}

func (cm *ContentManager) popReadyStagingZone() []*ContentStagingZone {
	cm.BucketLk.Lock()
	defer cm.BucketLk.Unlock()

	var out []*ContentStagingZone
	for uid, blist := range cm.Buckets {
		var keep []*ContentStagingZone
		for _, b := range blist {
			b.updateReadiness()
			if b.Readiness.IsReady {
				out = append(out, b)
			} else {
				keep = append(keep, b)
			}
		}
		cm.Buckets[uid] = keep
	}
	return out
}

func (cm *ContentManager) ensureStorage(ctx context.Context, content util.Content, done func(time.Duration)) error {
	ctx, span := cm.tracer.Start(ctx, "ensureStorage", trace.WithAttributes(
		attribute.Int("content", int(content.ID)),
	))
	defer span.End()

	// if the content is not active or is in pinning state, do not proceed
	if !content.Active || content.Pinning {
		return nil
	}

	// If this content is aggregated inside another piece of content, nothing to do here, that content will be processed
	if content.AggregatedIn > 0 {
		return nil
	}

	// If this is the 'root' of a dag split, we dont need to process it, as the splits will be processed instead
	if content.DagSplit && content.SplitFrom == 0 {
		return nil
	}

	// If this content is already scheduled to be aggregated and is waiting in a bucket
	if cm.contentInStagingZone(ctx, content) {
		return nil
	}

	// it's too big, need to split it up into chunks, no need to requeue dagsplit root content
	if content.Size > cm.cfg.Content.MaxSize {
		return cm.splitContent(ctx, content, cm.cfg.Content.MaxSize)
	}

	// get content deals, if any
	var deals []model.ContentDeal
	if err := cm.DB.Find(&deals, "content = ? AND NOT failed", content.ID).Error; err != nil {
		if !xerrors.Is(err, gorm.ErrRecordNotFound) {
			return err
		}
	}

	// if staging bucket is enabled, try to bucket the content
	if cm.canStageContent(content, deals) {
		return cm.addContentToStagingZone(ctx, content)
	}

	// check on each of the existing deals, see if they any needs fixing
	var countLk sync.Mutex
	var numSealed, numPublished, numProgress int
	var wg sync.WaitGroup

	errs := make([]error, len(deals))
	for i, d := range deals {
		dl := d
		wg.Add(1)
		go func(i int) {
			d := deals[i]
			defer wg.Done()

			status, err := cm.checkDeal(ctx, &dl, content)
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
			case DEAL_CHECK_UNKNOWN, DEAL_NEARLY_EXPIRED, DEAL_CHECK_SLASHED:
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
				cm.log.Errorf("unrecognized deal check status: %d", status)
			}
		}(i)
	}
	wg.Wait()

	// return the last error found, log the rest
	var retErr error
	for _, err := range errs {
		if err != nil {
			if retErr != nil {
				cm.log.Errorf("check deal failure: %s", err)
			}
			retErr = err
		}
	}
	if retErr != nil {
		return fmt.Errorf("deal check errored: %w", retErr)
	}

	// after reconciling content deals,
	// check If this is a shuttle content and that the shuttle is online and can start data transfer
	if content.Location != constants.ContentLocationLocal && !cm.ShuttleIsOnline(content.Location) {
		cm.log.Debugf("content shuttle: %s, is not online", content.Location)
		done(time.Minute * 15)
		return nil
	}

	replicationFactor := cm.cfg.Replication
	if content.Replication > 0 {
		replicationFactor = content.Replication
	}

	// check if content has enough good deals after reconcialiation,
	// if not enough good deals, go make more
	goodDeals := numSealed + numPublished + numProgress
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
			_, _, _, err := cm.GetPieceCommitment(context.Background(), content.Cid.CID, cm.Blockstore)
			if err != nil {
				if err == ErrWaitForRemoteCompute {
					cm.log.Debugf("waiting for shuttle:%s to finish commp for cont:%d", content.Location, content.ID)
				} else {
					cm.log.Errorf("failed to compute piece commitment for content %d: %s", content.ID, err)
				}
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
				cm.log.Errorf("failed to retrieve content in need of repair %d: %s", content.ID, err)
			}
			done(time.Second * 30)
		}()
		return nil
	}

	if cm.DealMakingDisabled() {
		cm.log.Warnf("deal making is disabled for now")
		done(time.Minute * 60)
		return nil
	}

	// only verified deals need datacap checks
	if cm.cfg.Deal.IsVerified {
		bl, err := cm.FilClient.Balance(ctx)
		if err != nil {
			return errors.Wrap(err, "could not retrieve dataCap from client balance")
		}

		if bl == nil || bl.VerifiedClientBalance == nil {
			return errors.New("verifed deals requires datacap, please see https://verify.glif.io or use the --verified-deal=false for non-verified deals")
		}

		if bl.VerifiedClientBalance.LessThan(big.NewIntUnsigned(uint64(abi.UnpaddedPieceSize(content.Size).Padded()))) {
			// how do we notify admin to top up datacap?
			return errors.Wrapf(err, "will not make deal, client address dataCap:%d GiB is lower than content size:%d GiB", big.Div(*bl.VerifiedClientBalance, big.NewIntUnsigned(uint64(1073741824))), abi.UnpaddedPieceSize(content.Size).Padded()/1073741824)
		}
	}

	bserv := blockservice.New(cm.Blockstore, cm.Node.Bitswap)
	dserv := merkledag.NewDAGService(bserv)
	_, err = dserv.Get(ctx, content.Cid.CID)
	if err != nil {
		return errors.New("will not make deal as the content is unretrievable")
	}

	go func() {
		// make some more deals!
		cm.log.Debugw("making more deals for content", "content", content.ID, "curDealCount", len(deals), "newDeals", dealsToBeMade)
		if err := cm.makeDealsForContent(ctx, content, dealsToBeMade, deals); err != nil {
			cm.log.Errorf("failed to make more deals: %s", err)
		}
		done(time.Minute * 10)
	}()
	return nil
}

// if content has no deals, is not already staged, is below min content size,
// and staging zone is enabled
func (cm *ContentManager) canStageContent(cont util.Content, deals []model.ContentDeal) bool {
	return len(deals) == 0 && !cont.Aggregate && cont.Size < cm.cfg.Content.MinSize && cm.cfg.StagingBucket.Enabled
}

func (cm *ContentManager) splitContent(ctx context.Context, cont util.Content, size int64) error {
	ctx, span := cm.tracer.Start(ctx, "splitContent")
	defer span.End()

	var u util.User
	if err := cm.DB.First(&u, "id = ?", cont.UserID).Error; err != nil {
		return fmt.Errorf("failed to load contents user from db: %w", err)
	}

	if !u.FlagSplitContent() {
		return fmt.Errorf("user does not have content splitting enabled")
	}

	cm.log.Debugf("splitting content %d (size: %d)", cont.ID, size)

	if cont.Location == constants.ContentLocationLocal {
		go func() {
			if err := cm.splitContentLocal(ctx, cont, size); err != nil {
				cm.log.Errorw("failed to split local content", "cont", cont.ID, "size", size, "err", err)
			}
		}()
		return nil
	} else {
		return cm.sendSplitContentCmd(ctx, cont.Location, cont.ID, size)
	}
}

func (cm *ContentManager) GetContent(id uint) (*util.Content, error) {
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
	DEAL_CHECK_SLASHED
)

// first check deal protocol version 2, then check version 1
func (cm *ContentManager) GetProviderDealStatus(ctx context.Context, d *model.ContentDeal, maddr address.Address, dealUUID *uuid.UUID) (*storagemarket.ProviderDealState, bool, error) {
	isPushTransfer := false
	providerDealState, err := cm.FilClient.DealStatus(ctx, maddr, d.PropCid.CID, dealUUID)
	if err != nil && providerDealState == nil {
		isPushTransfer = true
		providerDealState, err = cm.FilClient.DealStatus(ctx, maddr, d.PropCid.CID, nil)
	}
	return providerDealState, isPushTransfer, err
}

// get the data transfer state by transfer ID (compatible with both deal protocol v1 and v2)
func (cm *ContentManager) transferStatusByID(ctx context.Context, id string) (*filclient.ChannelState, error) {
	chanst, err := cm.FilClient.TransferStatusByID(ctx, id)
	if err != nil && err != filclient.ErrNoTransferFound && !strings.Contains(err.Error(), "No channel for channel ID") && !strings.Contains(err.Error(), "datastore: key not found") {
		return nil, err
	}
	return chanst, nil
}

func (cm *ContentManager) checkDeal(ctx context.Context, d *model.ContentDeal, content util.Content) (int, error) {
	ctx, cancel := context.WithTimeout(ctx, time.Minute*5) // NB: if we ever hit this, its bad. but we at least need *some* timeout there
	defer cancel()

	ctx, span := cm.tracer.Start(ctx, "checkDeal", trace.WithAttributes(
		attribute.Int("deal", int(d.ID)),
	))
	defer span.End()
	cm.log.Debugw("checking deal", "miner", d.Miner, "content", d.Content, "dbid", d.ID)

	maddr, err := d.MinerAddr()
	if err != nil {
		return DEAL_CHECK_UNKNOWN, err
	}

	// get the deal data transfer state
	chanst, err := cm.GetTransferStatus(ctx, d, content.Cid.CID, content.Location)
	if err != nil {
		return DEAL_CHECK_UNKNOWN, err
	}

	// if data transfer state is available,
	// try to set the actual timestamp of transfer states - start, finished, failed, cancelled etc
	// NB: boost does not support stages
	if chanst != nil {
		updates := make(map[string]interface{})
		if chanst.Status == datatransfer.TransferFinished || chanst.Status == datatransfer.Completed {
			if d.TransferStarted.IsZero() && chanst.Status == datatransfer.Completed {
				updates["transfer_started"] = time.Now() // boost transfers does not support stages, so we can't get actual timestamps
				if s := chanst.Stages.GetStage("Requested"); s != nil {
					updates["transfer_started"] = s.CreatedTime.Time()
				}
			}

			if d.TransferFinished.IsZero() {
				updates["transfer_finished"] = time.Now() // boost transfers does not support stages, so we can't get actual timestamps
				if s := chanst.Stages.GetStage("TransferFinished"); s != nil {
					updates["transfer_finished"] = s.CreatedTime.Time()
				}
			}
		}

		// if transfer is Failed or Cancelled
		trsFailed, msgStage := util.TransferFailed(chanst)
		if d.FailedAt.IsZero() && trsFailed {
			updates["failed"] = true
			updates["failed_at"] = time.Now() // boost transfers does not support stages, so we can't get actual timestamps
			if s := chanst.Stages.GetStage(msgStage); s != nil {
				updates["failed_at"] = s.CreatedTime.Time()
			}
		}

		if len(updates) > 0 {
			if err := cm.DB.Model(model.ContentDeal{}).Where("id = ?", d.ID).UpdateColumns(updates).Error; err != nil {
				return DEAL_CHECK_UNKNOWN, err
			}
		}
	}

	// get chain head - needed to check expired deals below
	head, err := cm.Api.ChainHead(ctx)
	if err != nil {
		return DEAL_CHECK_UNKNOWN, fmt.Errorf("failed to check chain head: %w", err)
	}

	// if the deal is on chain, then check is it still healthy (slashed, expired etc) and it's sealed(active) state
	if d.DealID != 0 {
		ok, deal, err := cm.FilClient.CheckChainDeal(ctx, abi.DealID(d.DealID))
		if err != nil {
			return DEAL_CHECK_UNKNOWN, fmt.Errorf("failed to check chain deal: %w", err)
		}
		if !ok {
			return DEAL_CHECK_UNKNOWN, nil
		}

		// check slashed health
		if deal.State.SlashEpoch > 0 {
			if err := cm.DB.Model(model.ContentDeal{}).Where("id = ?", d.ID).UpdateColumn("slashed", true).Error; err != nil {
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
			return DEAL_CHECK_SLASHED, nil
		}

		// check expiration health
		if deal.Proposal.EndEpoch-head.Height() < constants.MinSafeDealLifetime {
			return DEAL_NEARLY_EXPIRED, nil
		}

		// checked sealed/active health - TODO set actual sealed time
		if deal.State.SectorStartEpoch > 0 {
			if d.SealedAt.IsZero() {
				if err := cm.DB.Model(model.ContentDeal{}).Where("id = ?", d.ID).UpdateColumn("sealed_at", time.Now()).Error; err != nil {
					return DEAL_CHECK_UNKNOWN, err
				}
			}
			return DEAL_CHECK_SECTOR_ON_CHAIN, nil
		}
		return DEAL_CHECK_DEALID_ON_CHAIN, nil
	}

	// case where deal isnt yet on chain, then check deal state with miner/provider
	cm.log.Debugw("checking deal status with miner", "miner", maddr, "propcid", d.PropCid.CID, "dealUUID", d.DealUUID)
	subctx, cancel := context.WithTimeout(ctx, time.Second*10)
	defer cancel()

	var dealUUID *uuid.UUID
	if d.DealUUID != "" {
		parsed, err := uuid.Parse(d.DealUUID)
		if err != nil {
			return DEAL_CHECK_UNKNOWN, fmt.Errorf("parsing deal uuid %s: %w", d.DealUUID, err)
		}
		dealUUID = &parsed
	}

	// pull deal state from miner/provider
	provds, isPushTransfer, err := cm.GetProviderDealStatus(subctx, d, maddr, dealUUID)
	if err != nil {
		// if we cant get deal status from a miner and the data hasnt landed on chain what do we do?
		expired, err := cm.dealHasExpired(ctx, d, head)
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

	// since not on chain and not on miner, give up and create a new deal
	if provds == nil {
		return DEAL_CHECK_UNKNOWN, fmt.Errorf("failed to lookup provider deal state")
	}

	if provds.Proposal == nil {
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
		}
		return DEAL_CHECK_UNKNOWN, nil
	}

	if provds.State == storagemarket.StorageDealError {
		cm.log.Warnf("deal state for deal %d from miner %s is error: %s", d.ID, maddr.String(), provds.Message)
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
			cm.log.Warnf("proposal in deal ID miner sent back did not match our expectations")
			return DEAL_CHECK_UNKNOWN, nil
		}

		cm.log.Debugf("Confirmed deal ID, updating in database: %d %d %d", d.Content, d.ID, provds.DealID)
		if err := cm.updateDealID(d, int64(provds.DealID)); err != nil {
			return DEAL_CHECK_UNKNOWN, err
		}
		return DEAL_CHECK_DEALID_ON_CHAIN, nil
	}

	if provds.PublishCid != nil {
		cm.log.Debugw("checking publish CID", "content", d.Content, "miner", d.Miner, "propcid", d.PropCid.CID, "publishCid", *provds.PublishCid)
		id, err := cm.getDealID(ctx, *provds.PublishCid, d)
		if err != nil {
			cm.log.Debugf("failed to find message on chain: %s", *provds.PublishCid)
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

		cm.log.Debugf("Found deal ID, updating in database: %d %d %d", d.Content, d.ID, id)
		if err := cm.updateDealID(d, int64(id)); err != nil {
			return DEAL_CHECK_UNKNOWN, err
		}
		return DEAL_CHECK_DEALID_ON_CHAIN, nil
	}

	// proposal expired, miner didnt start it in time
	if provds.Proposal.StartEpoch < head.Height() {
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

	// Weird case where we somehow dont have the data transfer started for this deal.
	// if data transfer has not started and miner still has time, start the data transfer
	if d.DTChan == "" && time.Since(d.CreatedAt) < time.Hour {
		if isPushTransfer {
			cm.log.Warnf("creating new data transfer for local deal that is missing it: %d", d.ID)
			if err := cm.StartDataTransfer(ctx, d); err != nil {
				cm.log.Errorw("failed to start new data transfer for weird state deal", "deal", d.ID, "miner", d.Miner, "err", err)
				// If this fails out, just fail the deal and start from
				// scratch. This is already a weird state.
				return DEAL_CHECK_UNKNOWN, nil
			}
		}
		return DEAL_CHECK_PROGRESS, nil
	}

	if chanst != nil {
		if trsFailed, _ := util.TransferFailed(chanst); trsFailed {
			if err := cm.recordDealFailure(&DealFailureError{
				Miner:               maddr,
				Phase:               "data-transfer",
				Message:             fmt.Sprintf("data transfer issue: %s", chanst.Message),
				Content:             content.ID,
				UserID:              d.UserID,
				DealProtocolVersion: d.DealProtocolVersion,
				MinerVersion:        d.MinerVersion,
			}); err != nil {
				return DEAL_CHECK_UNKNOWN, err
			}

			if cm.FailDealOnTransferFailure {
				return DEAL_CHECK_UNKNOWN, nil
			}
		}
	}
	return DEAL_CHECK_PROGRESS, nil
}

func (cm *ContentManager) updateDealID(d *model.ContentDeal, id int64) error {
	if err := cm.DB.Model(model.ContentDeal{}).Where("id = ?", d.ID).Updates(map[string]interface{}{
		"deal_id":     id,
		"on_chain_at": time.Now(),
	}).Error; err != nil {
		return err
	}
	return nil
}

func (cm *ContentManager) dealHasExpired(ctx context.Context, d *model.ContentDeal, head *types.TipSet) (bool, error) {
	prop, err := cm.getProposalRecord(d.PropCid.CID)
	if err != nil {
		cm.log.Warnf("failed to get proposal record for deal %d: %s", d.ID, err)

		if time.Since(d.CreatedAt) > time.Hour*24*14 {
			return true, nil
		}
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

func (cm *ContentManager) GetTransferStatus(ctx context.Context, d *model.ContentDeal, contCID cid.Cid, contLoc string) (*filclient.ChannelState, error) {
	ctx, span := cm.tracer.Start(ctx, "getTransferStatus")
	defer span.End()

	if d.DTChan == "" {
		return nil, nil
	}

	if contLoc == constants.ContentLocationLocal {
		chanst, err := cm.transferStatusByID(ctx, d.DTChan)
		if err != nil {
			return nil, err
		}
		return chanst, nil
	}

	val, ok := cm.remoteTransferStatus.Get(d.ID)
	if !ok {
		if err := cm.sendRequestTransferStatusCmd(ctx, contLoc, d.ID, d.DTChan); err != nil {
			return nil, err
		}
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

var ErrNotOnChainYet = fmt.Errorf("message not found on chain")

func (cm *ContentManager) getDealID(ctx context.Context, pubcid cid.Cid, d *model.ContentDeal) (abi.DealID, error) {
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

func (cm *ContentManager) repairDeal(d *model.ContentDeal) error {
	if d.DealID != 0 {
		cm.log.Debugw("miner faulted on deal", "deal", d.DealID, "content", d.Content, "miner", d.Miner)
		maddr, err := d.MinerAddr()
		if err != nil {
			cm.log.Errorf("failed to get miner address from deal (%s): %w", d.Miner, err)
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

	cm.log.Debugw("repair deal", "propcid", d.PropCid.CID, "miner", d.Miner, "content", d.Content)
	if err := cm.DB.Model(model.ContentDeal{}).Where("id = ?", d.ID).UpdateColumns(map[string]interface{}{
		"failed":    true,
		"failed_at": time.Now(),
	}).Error; err != nil {
		return err
	}
	return nil
}

func (cm *ContentManager) makeDealsForContent(ctx context.Context, content util.Content, dealsToBeMade int, existingContDeals []model.ContentDeal) error {
	ctx, span := cm.tracer.Start(ctx, "makeDealsForContent", trace.WithAttributes(
		attribute.Int64("content", int64(content.ID)),
		attribute.Int("count", dealsToBeMade),
	))
	defer span.End()

	if content.Size < cm.cfg.Content.MinSize {
		return fmt.Errorf("content %d below individual deal size threshold. (size: %d, threshold: %d)", content.ID, content.Size, cm.cfg.Content.MinSize)
	}

	if content.Offloaded {
		return fmt.Errorf("cannot make more deals for offloaded content, must retrieve first")
	}

	_, _, pieceSize, err := cm.GetPieceCommitment(ctx, content.Cid.CID, cm.Blockstore)
	if err != nil {
		return xerrors.Errorf("failed to compute piece commitment while making deals %d: %w", content.ID, err)
	}

	// to make sure content replicas are distributed, make new deals with miners that currently don't store this content
	excludedMiners := make(map[address.Address]bool)
	for _, d := range existingContDeals {
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

	miners, err := cm.minerManager.PickMiners(ctx, dealsToBeMade*2, pieceSize.Padded(), excludedMiners, true)
	if err != nil {
		return err
	}

	var readyDeals []deal
	for _, m := range miners {
		price := m.Ask.GetPrice(cm.cfg.Deal.IsVerified)
		prop, err := cm.FilClient.MakeDeal(ctx, m.Address, content.Cid.CID, price, m.Ask.MinPieceSize, cm.cfg.Deal.Duration, cm.cfg.Deal.IsVerified)
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
		cd := &model.ContentDeal{
			Content:             content.ID,
			PropCid:             util.DbCID{CID: propnd.Cid()},
			DealUUID:            dealUUID.String(),
			Miner:               m.Address.String(),
			Verified:            cm.cfg.Deal.IsVerified,
			UserID:              content.UserID,
			DealProtocolVersion: m.DealProtocolVersion,
			MinerVersion:        m.Ask.MinerVersion,
		}

		if err := cm.DB.Create(cd).Error; err != nil {
			return xerrors.Errorf("failed to create database entry for deal: %w", err)
		}

		// Send the deal proposal to the storage provider
		var cleanupDealPrep func() error
		var propPhase bool
		isPushTransfer := m.DealProtocolVersion == filclient.DealProtocolv110

		switch m.DealProtocolVersion {
		case filclient.DealProtocolv110:
			propPhase, err = cm.FilClient.SendProposalV110(ctx, *prop, propnd.Cid())
		case filclient.DealProtocolv120:
			cleanupDealPrep, propPhase, err = cm.sendProposalV120(ctx, content.Location, *prop, propnd.Cid(), dealUUID, cd.ID)
		default:
			err = fmt.Errorf("unrecognized deal protocol %s", m.DealProtocolVersion)
		}

		if err != nil {
			// Clean up the contentDeal database entry
			if err := cm.DB.Delete(&model.ContentDeal{}, cd).Error; err != nil {
				return fmt.Errorf("failed to delete content deal from db: %w", err)
			}

			// Clean up the proposal database entry
			if err := cm.DB.Delete(&model.ProposalRecord{}, dp).Error; err != nil {
				return fmt.Errorf("failed to delete deal proposal from db: %w", err)
			}

			// Clean up the preparation for deal request - deal protocol v120
			if cleanupDealPrep != nil {
				if err := cleanupDealPrep(); err != nil {
					cm.log.Errorw("cleaning up deal prepared request", "error", err)
				}
			}

			// Record a deal failure
			phase := "send-proposal"
			if propPhase {
				phase = "propose"
			}

			if err = cm.recordDealFailure(&DealFailureError{
				Miner:               m.Address,
				Phase:               phase,
				Message:             err.Error(),
				Content:             content.ID,
				UserID:              content.UserID,
				DealProtocolVersion: m.DealProtocolVersion,
				MinerVersion:        m.Ask.MinerVersion,
			}); err != nil {
				cm.log.Errorw("failed to record deal failure", "error", err)
			}
			continue
		}

		readyDeals = append(readyDeals, deal{minerAddr: m.Address, isPushTransfer: isPushTransfer, contentDeal: cd})
		if len(readyDeals) >= dealsToBeMade {
			break
		}
	}

	// Now start up some data transfers!
	// note: its okay if we dont start all the data transfers, we can just do it next time around
	for _, rDeal := range readyDeals {
		// If the data transfer is a pull transfer, we don't need to explicitly
		// start the transfer (the Storage Provider will start pulling data as
		// soon as it accepts the proposal)
		if !rDeal.isPushTransfer {
			continue
		}

		// start data transfer async
		go func(d deal) {
			if err := cm.StartDataTransfer(ctx, d.contentDeal); err != nil {
				cm.log.Errorw("failed to start data transfer", "err", err, "miner", d.minerAddr)
			}
		}(rDeal)
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
		if !cm.ShuttleIsOnline(contentLoc) {
			return nil, false, xerrors.Errorf("shuttle is not online: %s", contentLoc)
		}

		addrInfo := cm.ShuttleAddrInfo(contentLoc)
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

func (cm *ContentManager) MakeDealWithMiner(ctx context.Context, content util.Content, miner address.Address) (uint, error) {
	ctx, span := cm.tracer.Start(ctx, "makeDealWithMiner", trace.WithAttributes(
		attribute.Int64("content", int64(content.ID)),
		attribute.Stringer("miner", miner),
	))
	defer span.End()

	if content.Offloaded {
		return 0, fmt.Errorf("cannot make more deals for offloaded content, must retrieve first")
	}

	// if it's a shuttle content and the shuttle is not online, do not proceed
	if content.Location != constants.ContentLocationLocal && !cm.ShuttleIsOnline(content.Location) {
		return 0, fmt.Errorf("content shuttle: %s, is not online", content.Location)
	}

	proto, err := cm.minerManager.GetDealProtocolForMiner(ctx, miner)
	if err != nil {
		return 0, cm.recordDealFailure(&DealFailureError{
			Miner:   miner,
			Phase:   "deal-protocol-version",
			Message: err.Error(),
			Content: content.ID,
			UserID:  content.UserID,
		})
	}

	ask, err := cm.minerManager.GetAsk(ctx, miner, 0)
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

	if ask.PriceIsTooHigh(cm.cfg.Deal.IsVerified) {
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
	deal := &model.ContentDeal{
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
		if err := cm.DB.Delete(&model.ContentDeal{}, deal).Error; err != nil {
			return 0, fmt.Errorf("failed to delete content deal from db: %w", err)
		}

		// Clean up the proposal database entry
		if err := cm.DB.Delete(&model.ProposalRecord{}, dp).Error; err != nil {
			return 0, fmt.Errorf("failed to delete deal proposal from db: %w", err)
		}

		// Clean up the preparation for deal request
		if cleanupDealPrep != nil {
			if err := cleanupDealPrep(); err != nil {
				cm.log.Errorw("cleaning up deal prepared request", "error", err)
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

func (cm *ContentManager) StartDataTransfer(ctx context.Context, cd *model.ContentDeal) error {
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

	if err := cm.DB.Model(model.ContentDeal{}).Where("id = ?", cd.ID).UpdateColumns(map[string]interface{}{
		"dt_chan": chanid.String(),
	}).Error; err != nil {
		return xerrors.Errorf("failed to update deal with channel ID: %w", err)
	}
	cm.log.Debugw("Started data transfer", "chanid", chanid)
	return nil
}

func (cm *ContentManager) putProposalRecord(dealprop *marketv9.ClientDealProposal) (*model.ProposalRecord, error) {
	nd, err := cborutil.AsIpld(dealprop)
	if err != nil {
		return nil, err
	}

	dp := &model.ProposalRecord{
		PropCid: util.DbCID{CID: nd.Cid()},
		Data:    nd.RawData(),
	}

	if err := cm.DB.Create(dp).Error; err != nil {
		return nil, err
	}
	return dp, nil
}

func (cm *ContentManager) getProposalRecord(propCid cid.Cid) (*market.ClientDealProposal, error) {
	var proprec model.ProposalRecord
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
	cm.log.Debugw("deal failure error", "miner", dfe.Miner, "uuid", dfe.DealUUID, "phase", dfe.Phase, "msg", dfe.Message, "content", dfe.Content)
	rec := dfe.Record()
	return cm.DB.Create(rec).Error
}

type DealFailureError struct {
	Miner               address.Address
	DealUUID            string
	Phase               string
	Message             string
	Content             uint
	UserID              uint
	MinerAddress        string
	DealProtocolVersion protocol.ID
	MinerVersion        string
}

func (dfe *DealFailureError) Record() *model.DfeRecord {
	return &model.DfeRecord{
		Miner:               dfe.Miner.String(),
		DealUUID:            dfe.DealUUID,
		Phase:               dfe.Phase,
		Message:             dfe.Message,
		Content:             dfe.Content,
		UserID:              dfe.UserID,
		MinerVersion:        dfe.MinerVersion,
		DealProtocolVersion: dfe.DealProtocolVersion,
	}
}

func (dfe *DealFailureError) Error() string {
	return fmt.Sprintf("deal %s with miner %s failed in phase %s: %s", dfe.DealUUID, dfe.Message, dfe.Phase, dfe.Message)
}

func (cm *ContentManager) lookupPieceCommRecord(data cid.Cid) (*model.PieceCommRecord, error) {
	var pcrs []model.PieceCommRecord
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
		if err := cm.SendShuttleCommand(ctx, cont.Location, &drpc.Command{
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

	cm.log.Debugw("computing piece commitment", "data", cont.Cid.CID)
	return filclient.GeneratePieceCommitmentFFI(ctx, data, bs)
}

func (cm *ContentManager) GetPieceCommitment(ctx context.Context, data cid.Cid, bs blockstore.Blockstore) (cid.Cid, uint64, abi.UnpaddedPieceSize, error) {
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
		cm.DB.Model(model.PieceCommRecord{}).Where("piece = ?", pcr.Piece).UpdateColumns(map[string]interface{}{
			"car_size": carSize,
		}) //nolint:errcheck

		return pcr.Piece.CID, pcr.CarSize, pcr.Size, nil
	}

	// The piece comm record isn't in the DB so calculate it
	pc, carSize, size, err := cm.runPieceCommCompute(ctx, data, bs)
	if err != nil {
		return cid.Undef, 0, 0, err
	}

	opcr := model.PieceCommRecord{
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
			cm.log.Errorf("failed to retrieve content to serve %d: %w", contentToFetch, err)
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
	cm.log.Infof("refreshing content %d onto shuttle %s", cont, loc)

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
			cm.log.Errorf("attempted to retrieve content %d but have no active deals", cont.ID)
			return fmt.Errorf("no active deals for content %d, cannot retrieve", cont.ID)
		}

		var deals []drpc.StorageDeal
		for _, d := range activeDeals {
			ma, err := d.MinerAddr()
			if err != nil {
				cm.log.Errorf("failed to parse miner addres for deal %d: %s", d.ID, err)
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

	var deals []model.ContentDeal
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
			cm.log.Errorf("deal %d had bad miner address: %s", deal.ID, err)
			continue
		}

		cm.log.Infow("attempting retrieval deal", "content", contentToFetch, "miner", maddr)

		ask, err := cm.FilClient.RetrievalQuery(ctx, maddr, content.Cid.CID)
		if err != nil {
			span.RecordError(err)

			cm.log.Errorw("failed to query retrieval", "miner", maddr, "content", content.Cid.CID, "err", err)
			if err := cm.RecordRetrievalFailure(&util.RetrievalFailureRecord{
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
		cm.log.Infow("got retrieval ask", "content", content, "miner", maddr, "ask", ask)

		if err := cm.TryRetrieve(ctx, maddr, content.Cid.CID, ask); err != nil {
			span.RecordError(err)
			cm.log.Errorw("failed to retrieve content", "miner", maddr, "content", content.Cid.CID, "err", err)
			if err := cm.RecordRetrievalFailure(&util.RetrievalFailureRecord{
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

// addObjectsToDatabase creates entries on the estuary database for CIDs related to an already pinned CID (`root`)
// These entries are saved on the `objects` table, while metadata about the `root` CID is mostly kept on the `contents` table
// The link between the `objects` and `contents` tables is the `obj_refs` table
func (cm *ContentManager) addObjectsToDatabase(ctx context.Context, contID uint, objects []*util.Object, loc string) error {
	_, span := cm.tracer.Start(ctx, "addObjectsToDatabase")
	defer span.End()

	if err := cm.DB.CreateInBatches(objects, 300).Error; err != nil {
		return xerrors.Errorf("failed to create objects in db: %w", err)
	}

	refs := make([]util.ObjRef, 0, len(objects))
	var totalSize int64
	for _, o := range objects {
		refs = append(refs, util.ObjRef{
			Content: contID,
			Object:  o.ID,
		})
		totalSize += int64(o.Size)
	}

	span.SetAttributes(
		attribute.Int64("totalSize", totalSize),
		attribute.Int("numObjects", len(objects)),
	)

	if err := cm.DB.CreateInBatches(refs, 500).Error; err != nil {
		return xerrors.Errorf("failed to create refs: %w", err)
	}

	if err := cm.DB.Model(util.Content{}).Where("id = ?", contID).UpdateColumns(map[string]interface{}{
		"active":   true,
		"size":     totalSize,
		"pinning":  false,
		"location": loc,
	}).Error; err != nil {
		return xerrors.Errorf("failed to update content in database: %w", err)
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
		cm.log.Warnf("TODO: implement safe fetch data protections")
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
			ID:    cm.Node.Host.ID(),
			Addrs: cm.Node.Host.Addrs(),
		}, nil
	}

	cm.ShuttlesLk.Lock()
	defer cm.ShuttlesLk.Unlock()
	conn, ok := cm.Shuttles[handle]
	if !ok {
		return nil, nil
	}
	return &conn.addrInfo, nil
}

func (cm *ContentManager) sendPrepareForDataRequestCommand(ctx context.Context, loc string, dbid uint, authToken string, propCid cid.Cid, payloadCid cid.Cid, size uint64) error {
	return cm.SendShuttleCommand(ctx, loc, &drpc.Command{
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
	return cm.SendShuttleCommand(ctx, loc, &drpc.Command{
		Op: drpc.CMD_CleanupPreparedRequest,
		Params: drpc.CmdParams{
			CleanupPreparedRequest: &drpc.CleanupPreparedRequest{
				DealDBID:  dbid,
				AuthToken: authToken,
			},
		},
	})
}

func (cm *ContentManager) sendStartTransferCommand(ctx context.Context, loc string, cd *model.ContentDeal, datacid cid.Cid) error {
	miner, err := cd.MinerAddr()
	if err != nil {
		return err
	}

	return cm.SendShuttleCommand(ctx, loc, &drpc.Command{
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

func (cm *ContentManager) SendAggregateCmd(ctx context.Context, loc string, cont util.Content, aggr []drpc.AggregateContent) error {
	return cm.SendShuttleCommand(ctx, loc, &drpc.Command{
		Op: drpc.CMD_AggregateContent,
		Params: drpc.CmdParams{
			AggregateContent: &drpc.AggregateContents{
				DBID:     cont.ID,
				UserID:   cont.UserID,
				Contents: aggr,
			},
		},
	})
}

func (cm *ContentManager) sendRequestTransferStatusCmd(ctx context.Context, loc string, dealid uint, chid string) error {
	return cm.SendShuttleCommand(ctx, loc, &drpc.Command{
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
	return cm.SendShuttleCommand(ctx, loc, &drpc.Command{
		Op: drpc.CMD_SplitContent,
		Params: drpc.CmdParams{
			SplitContent: &drpc.SplitContent{
				Content: cont,
				Size:    size,
			},
		},
	})
}

func (cm *ContentManager) SendConsolidateContentCmd(ctx context.Context, loc string, contents []util.Content) error {
	tc := &drpc.TakeContent{}
	for _, c := range contents {
		prs := make([]*peer.AddrInfo, 0)

		pr, err := cm.addrInfoForShuttle(c.Location)
		if err != nil {
			return err
		}

		if pr != nil {
			prs = append(prs, pr)
		}

		if pr == nil {
			cm.log.Warnf("no addr info for node: %s", loc)
		}

		tc.Contents = append(tc.Contents, drpc.ContentFetch{
			ID:     c.ID,
			Cid:    c.Cid.CID,
			UserID: c.UserID,
			Peers:  prs,
		})
	}

	return cm.SendShuttleCommand(ctx, loc, &drpc.Command{
		Op: drpc.CMD_TakeContent,
		Params: drpc.CmdParams{
			TakeContent: tc,
		},
	})
}

func (cm *ContentManager) sendUnpinCmd(ctx context.Context, loc string, conts []uint) error {
	return cm.SendShuttleCommand(ctx, loc, &drpc.Command{
		Op: drpc.CMD_UnpinContent,
		Params: drpc.CmdParams{
			UnpinContent: &drpc.UnpinContent{
				Contents: conts,
			},
		},
	})
}

func (cm *ContentManager) DealMakingDisabled() bool {
	cm.dealDisabledLk.Lock()
	defer cm.dealDisabledLk.Unlock()
	return cm.isDealMakingDisabled
}

func (cm *ContentManager) SetDealMakingEnabled(enable bool) {
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
			Active:      false, // will be active after it's blocks are saved
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

		if err := cm.AddDatabaseTrackingToContent(ctx, content.ID, dserv, c, func(int64) {}); err != nil {
			return err
		}

		// queue splited contents
		go func() {
			cm.log.Debugw("queuing splited content child", "parent_contID", cont.ID, "child_contID", content.ID)
			cm.ToCheck(content.ID)
		}()
	}

	if err := cm.DB.Model(util.Content{}).Where("id = ?", cont.ID).UpdateColumns(map[string]interface{}{
		"dag_split": true,
		"size":      0,
		"active":    false,
		"pinning":   false,
	}).Error; err != nil {
		return err
	}
	return cm.DB.Where("content = ?", cont.ID).Delete(&util.ObjRef{}).Error
}

var noDataTimeout = time.Minute * 10

func (cm *ContentManager) AddDatabaseTrackingToContent(ctx context.Context, cont uint, dserv ipld.NodeGetter, root cid.Cid, cb func(int64)) error {
	ctx, span := cm.tracer.Start(ctx, "computeObjRefsUpdate")
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
		cm.inflightCidsLk.Lock()
		_ = cset.ForEach(func(c cid.Cid) error {
			v, ok := cm.inflightCids[c]
			if !ok || v <= 0 {
				cm.log.Errorf("cid should be inflight but isn't: %s", c)
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
			Cid:  util.DbCID{CID: c},
			Size: len(node.RawData()),
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
	return cm.addObjectsToDatabase(ctx, cont, objects, constants.ContentLocationLocal)
}

func (cm *ContentManager) AddDatabaseTracking(ctx context.Context, u *util.User, dserv ipld.NodeGetter, root cid.Cid, filename string, replication int) (*util.Content, error) {
	ctx, span := cm.tracer.Start(ctx, "computeObjRefs")
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

	if err := cm.DB.Create(content).Error; err != nil {
		return nil, xerrors.Errorf("failed to track new content in database: %w", err)
	}

	if err := cm.AddDatabaseTrackingToContent(ctx, content.ID, dserv, root, func(int64) {}); err != nil {
		return nil, err
	}
	return content, nil
}
