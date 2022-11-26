package contentmgr

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/application-research/estuary/constants"
	"github.com/application-research/estuary/drpc"
	"github.com/application-research/estuary/util"
	"github.com/ipfs/go-blockservice"
	ipld "github.com/ipfs/go-ipld-format"
	"github.com/ipfs/go-merkledag"
	uio "github.com/ipfs/go-unixfs/io"
	"golang.org/x/xerrors"
)

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

func (cm *ContentManager) GetStagingZone() map[uint][]*ContentStagingZone {
	cm.bucketLk.Lock()
	defer cm.bucketLk.Unlock()
	return cm.buckets
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

	if err := cm.db.Create(content).Error; err != nil {
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

	if err := cm.db.Model(util.Content{}).
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
		cm.bucketLk.Lock()
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
		cm.buckets[b.User] = append(cm.buckets[b.User], b)
		cm.bucketLk.Unlock()
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
		if err := cm.db.Create(obj).Error; err != nil {
			return err
		}

		if err := cm.db.Create(&util.ObjRef{
			Content: z.ContID,
			Object:  obj.ID,
		}).Error; err != nil {
			return err
		}

		if err := cm.blockstore.Put(ctx, dir); err != nil {
			return err
		}

		if err := cm.db.Model(util.Content{}).Where("id = ?", z.ContID).UpdateColumns(map[string]interface{}{
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
	if err := cm.db.First(&bContent, "id = ?", z.ContID).Error; err != nil {
		return err
	}
	return cm.SendAggregateCmd(ctx, loc, bContent, aggrConts)
}

func (cm *ContentManager) CreateAggregate(ctx context.Context, conts []util.Content) (ipld.Node, error) {
	cm.log.Debug("aggregating contents in staging zone into new content")

	bserv := blockservice.New(cm.blockstore, cm.node.Bitswap)
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
	if err := cm.db.Find(&stages, "not active and pinning and aggregate").Error; err != nil {
		return err
	}

	zones := make(map[uint][]*ContentStagingZone)
	for _, c := range stages {
		var inZones []util.Content
		if err := cm.db.Find(&inZones, "aggregated_in = ?", c.ID).Error; err != nil {
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
	cm.buckets = zones
	return nil
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

func (cm *ContentManager) GetStagingZonesForUser(ctx context.Context, user uint) []*ContentStagingZone {
	cm.bucketLk.Lock()
	defer cm.bucketLk.Unlock()

	blist, ok := cm.buckets[user]
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
	cm.bucketLk.Lock()
	defer cm.bucketLk.Unlock()

	out := make(map[uint][]*ContentStagingZone)
	for u, blist := range cm.buckets {
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

		cm.buckets[content.UserID] = []*ContentStagingZone{b}
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

func (cm *ContentManager) popReadyStagingZone() []*ContentStagingZone {
	cm.bucketLk.Lock()
	defer cm.bucketLk.Unlock()

	var out []*ContentStagingZone
	for uid, blist := range cm.buckets {
		var keep []*ContentStagingZone
		for _, b := range blist {
			b.updateReadiness()
			if b.Readiness.IsReady {
				out = append(out, b)
			} else {
				keep = append(keep, b)
			}
		}
		cm.buckets[uid] = keep
	}
	return out
}

// if content is not already staged, if it is below min deal content size, and staging zone is enabled
func (cm *ContentManager) canStageContent(cont util.Content) bool {
	return !cont.Aggregate && cont.Size < cm.cfg.Content.MinSize && cm.cfg.StagingBucket.Enabled
}

func (cm *ContentManager) setUpStaging(ctx context.Context) {
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
}
