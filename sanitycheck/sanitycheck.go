package sanitycheck

import (
	"github.com/application-research/estuary/model"
	"github.com/application-research/estuary/util"
	"github.com/ipfs/go-cid"
	"go.uber.org/zap"
	"golang.org/x/xerrors"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type IManager interface {
	HandleMissingBlocks(cc cid.Cid, errMsg string)
}

type manager struct {
	db  *gorm.DB
	log *zap.SugaredLogger
}

func NewManager(db *gorm.DB, log *zap.SugaredLogger) IManager {
	return &manager{
		db:  db,
		log: log,
	}
}

func (m *manager) HandleMissingBlocks(cc cid.Cid, errMsg string) {
	m.log.Warnf("handling missing block for cid: %s", cc)

	// if already handled, ignore
	var exist *model.SanityCheck
	if err := m.db.First(&exist, "block_cid = ?", cc.Bytes()).Error; err != nil {
		if !xerrors.Is(err, gorm.ErrRecordNotFound) {
			m.log.Errorf("sanity check failed for cid:%s, err: %w", cc.String(), err)
			return
		}
		exist = nil
	}

	if exist != nil {
		m.log.Debugf("already handled this missing block for cid: %s", cc)
		return
	}

	// get all contents affected by this missing block on estuary or from shuttles
	var cnts []util.Content
	var cntsBatch []util.Content
	where := "id in (select content from obj_refs where object in (select id from objects where cid = ?))"
	if err := m.db.Where(where, cc.Bytes()).FindInBatches(&cntsBatch, 500, func(tx *gorm.DB, batch int) error {
		cnts = append(cnts, cntsBatch...)
		return nil
	}).Error; err != nil {
		m.log.Errorf("sanity check failed to get content(s) for cid: %s, err: %w", cc.String(), err)
		return
	}

	// mark all contents affected by this missing block
	marks := make(map[uint64]bool, 0)
	for _, cnt := range cnts {
		if _, ok := marks[cnt.ID]; !ok {
			m.log.Debugf("setting sanity check for cont: %d", cnt.ID)

			chk := &model.SanityCheck{
				BlockCid:  util.DbCID{CID: cc},
				ContentID: cnt.ID,
				ErrMsg:    errMsg,
			}
			if err := m.db.Clauses(clause.OnConflict{DoNothing: true}).Create(chk).Error; err != nil {
				m.log.Errorf("failed to create sanity check mark for content: %d, err: %w", cnt.ID, err)
				return
			}
			marks[cnt.ID] = true
		}
	}
}
