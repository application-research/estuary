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
)

func (m *manager) ensureStorage(ctx context.Context, content util.Content, done func(time.Duration)) error {
	ctx, span := m.tracer.Start(ctx, "ensureStorage", trace.WithAttributes(
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

	// if content is offloaded, do not proceed - since it needs the blocks for commp and data transfer
	if content.Offloaded {
		m.log.Warnf("cont: %d offloaded for deal making", content.ID)
		go func() {
			if err := m.RefreshContent(context.Background(), content.ID); err != nil {
				m.log.Errorf("failed to retrieve content in need of repair %d: %s", content.ID, err)
			}
			done(time.Second * 30)
		}()
		return nil
	}
	return nil
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
			return m.stgZoneCreationMgr.TryAddNewContentToStagingZone(ctx, cont, contSize, nil)
		}
		return nil
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
