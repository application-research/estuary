package split

import (
	"context"
	"fmt"
	"time"

	"github.com/application-research/estuary/config"
	"github.com/application-research/estuary/constants"
	content "github.com/application-research/estuary/content"
	"github.com/application-research/estuary/model"

	"github.com/application-research/estuary/node"
	"github.com/application-research/estuary/shuttle"
	"github.com/application-research/estuary/util"
	dagsplit "github.com/application-research/estuary/util/dagsplit"
	"github.com/ipfs/go-blockservice"
	"github.com/ipfs/go-cid"
	cbor "github.com/ipfs/go-ipld-cbor"
	"github.com/ipfs/go-merkledag"
	"go.opentelemetry.io/otel"

	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
	"golang.org/x/xerrors"
	"gorm.io/gorm"
)

type IManager interface {
	RunWorker(ctx context.Context)
	SplitContent(ctx context.Context, cont util.Content, size int64) error
}

type manager struct {
	db         *gorm.DB
	node       *node.Node
	cfg        *config.Estuary
	log        *zap.SugaredLogger
	shuttleMgr shuttle.IManager
	contMgr    content.IManager
	tracer     trace.Tracer
}

func NewManager(
	db *gorm.DB,
	nd *node.Node,
	cfg *config.Estuary,
	log *zap.SugaredLogger,
	shuttleMgr shuttle.IManager,
	cntMgr content.IManager,
) IManager {
	return &manager{
		db:         db,
		node:       nd,
		cfg:        cfg,
		log:        log,
		shuttleMgr: shuttleMgr,
		contMgr:    cntMgr,
		tracer:     otel.Tracer("replicator"),
	}
}

func (m *manager) getSplitTrackerLastContentID() (*model.StagingZoneTracker, error) {
	var trks []*model.StagingZoneTracker
	if err := m.db.Find(&trks).Error; err != nil {
		return nil, err
	}

	if len(trks) == 0 {
		// for the first time it will be empty
		trk := &model.StagingZoneTracker{LastContID: 0}
		if err := m.db.Create(&trk).Error; err != nil {
			return nil, err
		}
		return trk, nil
	}
	return trks[0], nil
}

func (m *manager) RunWorker(ctx context.Context) {
	m.log.Infof("starting up split worker")

	go m.run(ctx)

	m.log.Infof("spun up split worker")
}

func (m *manager) run(ctx context.Context) {
	timer := time.NewTicker(m.cfg.StagingBucket.CreationInterval)
	for {
		select {
		case <-timer.C:
			m.log.Debug("running split worker")

			lastContID, err := m.getSplitTrackerLastContentID()
			if err != nil {
				m.log.Warnf("failed to get split tracker last content id - %s", err)
				continue
			}

			if err := m.splitContents(ctx, lastContID); err != nil {
				m.log.Warnf("failed to split contents - %s", err)
			}
		}
	}
}

func (m *manager) splitContents(ctx context.Context, tracker *model.StagingZoneTracker) error {
	var largeContents []util.Content
	return m.db.Where("id > ? and size <= ?", tracker.LastContID, m.cfg.Content.MinSize).Order("id asc").FindInBatches(&largeContents, 2000, func(tx *gorm.DB, batch int) error {
		m.log.Infof("trying to split the next sets of contents: %d - %d", largeContents[0].ID, largeContents[len(largeContents)-1].ID)

		for _, c := range largeContents {
			if err := m.SplitContent(ctx, c, m.cfg.Content.MaxSize); err != nil {
				return err
			}
		}
		return nil
	}).Error
}

func (m *manager) SplitContent(ctx context.Context, cont util.Content, size int64) error {
	ctx, span := m.tracer.Start(ctx, "splitContent")
	defer span.End()

	var u util.User
	if err := m.db.First(&u, "id = ?", cont.UserID).Error; err != nil {
		return fmt.Errorf("failed to load contents user from db: %w", err)
	}

	if !u.FlagSplitContent() {
		return fmt.Errorf("user does not have content splitting enabled")
	}

	m.log.Debugf("splitting content %d (size: %d)", cont.ID, size)

	if cont.Location == constants.ContentLocationLocal {
		go func() {
			if err := m.splitContentLocal(ctx, cont, size); err != nil {
				m.log.Errorw("failed to split local content", "cont", cont.ID, "size", size, "err", err)
			}
		}()
		return nil
	} else {
		return m.shuttleMgr.SplitContent(ctx, cont.Location, cont.ID, size)
	}
}

func (m *manager) splitContentLocal(ctx context.Context, cont util.Content, size int64) error {
	dserv := merkledag.NewDAGService(blockservice.New(m.node.Blockstore, nil))
	b := dagsplit.NewBuilder(dserv, uint64(size), 0)
	if err := b.Pack(ctx, cont.Cid.CID); err != nil {
		return err
	}

	cst := cbor.NewCborStore(m.node.Blockstore)

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

		if err := m.db.Create(content).Error; err != nil {
			return xerrors.Errorf("failed to track new content in database: %w", err)
		}

		if err := m.contMgr.AddDatabaseTrackingToContent(ctx, content, dserv, c, func(int64) {}); err != nil {
			return err
		}
		m.log.Debugw("queuing splited content child", "parent_contID", cont.ID, "child_contID", content.ID)
	}

	if err := m.db.Model(util.Content{}).Where("id = ?", cont.ID).UpdateColumns(map[string]interface{}{
		"dag_split": true,
		"size":      0,
		"active":    false,
		"pinning":   false,
	}).Error; err != nil {
		return err
	}
	return m.db.Where("content = ?", cont.ID).Delete(&util.ObjRef{}).Error
}
