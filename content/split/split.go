package split

import (
	"context"
	"fmt"

	"github.com/application-research/estuary/config"
	"github.com/application-research/estuary/constants"
	content "github.com/application-research/estuary/content"
	splitqueuemgr "github.com/application-research/estuary/content/split/queue"
	"github.com/application-research/estuary/pinner/block"
	dagsplit "github.com/application-research/estuary/util/dagsplit"

	"github.com/ipfs/go-blockservice"
	"github.com/ipfs/go-cid"
	cbor "github.com/ipfs/go-ipld-cbor"
	"github.com/ipfs/go-merkledag"
	"golang.org/x/xerrors"

	"github.com/application-research/estuary/node"
	"github.com/application-research/estuary/shuttle"
	"github.com/application-research/estuary/util"
	"go.opentelemetry.io/otel"

	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

type IManager interface {
	SplitContent(ctx context.Context, cont util.Content, size int64) error
}

type manager struct {
	db             *gorm.DB
	node           *node.Node
	cfg            *config.Estuary
	log            *zap.SugaredLogger
	shuttleMgr     shuttle.IManager
	contMgr        content.IManager
	tracer         trace.Tracer
	splitQueueMgr  splitqueuemgr.IManager
	pinnerBlockMgr block.IManager
}

func NewManager(
	ctx context.Context,
	db *gorm.DB,
	nd *node.Node,
	cfg *config.Estuary,
	log *zap.SugaredLogger,
	shuttleMgr shuttle.IManager,
	cntMgr content.IManager,
) IManager {
	m := &manager{
		db:             db,
		node:           nd,
		cfg:            cfg,
		log:            log,
		shuttleMgr:     shuttleMgr,
		contMgr:        cntMgr,
		tracer:         otel.Tracer("replicator"),
		splitQueueMgr:  splitqueuemgr.NewManager(log),
		pinnerBlockMgr: block.NewManager(db, cfg, log),
	}

	m.runWorkers(ctx)
	return m
}

func (m *manager) SplitContent(ctx context.Context, cont util.Content, size int64) error {
	ctx, span := m.tracer.Start(ctx, "splitContent")
	defer span.End()

	m.log.Debugf("trying to split cont %d", cont.ID)

	var u util.User
	if err := m.db.First(&u, "id = ?", cont.UserID).Error; err != nil {
		return fmt.Errorf("failed to load contents user from db: %w", err)
	}

	if !u.FlagSplitContent() {
		m.log.Warnf("user: %d does not have content splitting enabled", u.ID)
		return nil
	}

	m.log.Debugf("splitting content %d (size: %d)", cont.ID, size)

	if cont.Location == constants.ContentLocationLocal {
		go func() {
			if err := m.splitContentLocal(ctx, cont, size); err != nil {
				m.log.Errorw("failed to split local content", "cont", cont.ID, "size", size, "err", err)
				m.splitQueueMgr.SplitFailed(cont.ID, m.db)
			} else {
				m.splitQueueMgr.SplitComplete(cont.ID, m.db)
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

		if err := m.pinnerBlockMgr.WalkAndSaveBlocks(ctx, content, dserv, c); err != nil {
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
