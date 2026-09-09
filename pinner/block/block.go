package block

import (
	"context"
	"sync"
	"time"

	"github.com/application-research/estuary/constants"
	"github.com/application-research/estuary/util"
	"github.com/ipfs/go-cid"
	ipld "github.com/ipfs/go-ipld-format"
	"github.com/ipfs/go-merkledag"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/xerrors"
	"gorm.io/gorm"

	"github.com/application-research/estuary/config"
	splitqueuemgr "github.com/application-research/estuary/content/split/queue"
	stgzonequeuemgr "github.com/application-research/estuary/content/stagingzone/queue"
	dealqueuemgr "github.com/application-research/estuary/deal/queue"

	"go.opentelemetry.io/otel"
	"go.uber.org/zap"
)

type IManager interface {
	WalkAndSaveBlocks(ctx context.Context, cont *util.Content, dserv ipld.NodeGetter, root cid.Cid) error
}

type manager struct {
	db              *gorm.DB
	cfg             *config.Estuary
	log             *zap.SugaredLogger
	tracer          trace.Tracer
	inflightCids    map[cid.Cid]uint
	inflightCidsLk  sync.Mutex
	stgZoneQueueMgr stgzonequeuemgr.IManager
	dealQueueMgr    dealqueuemgr.IManager
	splitQueueMgr   splitqueuemgr.IManager
}

func NewManager(db *gorm.DB, cfg *config.Estuary, log *zap.SugaredLogger) IManager {
	return &manager{
		db:              db,
		cfg:             cfg,
		log:             log,
		tracer:          otel.Tracer("content"),
		inflightCids:    make(map[cid.Cid]uint),
		stgZoneQueueMgr: stgzonequeuemgr.NewManager(log),
		dealQueueMgr:    dealqueuemgr.NewManager(cfg, log),
		splitQueueMgr:   splitqueuemgr.NewManager(log),
	}
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

		var usc *util.UsersStorageCapacity
		err := tx.First(&usc, "user_id = ?", cont.UserID).Error
		if err != nil {
			usc.UserId = cont.UserID
			usc.Size = 0
		}

		usc.Size += contSize
		tx.Save(&usc)

		// if content can be staged, stage it
		if contSize < m.cfg.Content.MinSize {
			return m.stgZoneQueueMgr.QueueContent(cont, tx, false)
		}

		// if it is too large, queue it for splitting.
		// split worker will pick it up and split it,
		// its children will be pinned and dealed
		if contSize > m.cfg.Content.MaxSize {
			return m.splitQueueMgr.QueueContent(cont.ID, cont.UserID, tx)
		}
		// or queue it for deal making
		return m.dealQueueMgr.QueueContent(cont.ID, tx)
	})
}

var noDataTimeout = time.Minute * 10

func (m *manager) WalkAndSaveBlocks(ctx context.Context, cont *util.Content, dserv ipld.NodeGetter, root cid.Cid) error {
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
