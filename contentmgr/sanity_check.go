package contentmgr

import (
	"github.com/application-research/estuary/model"
	"github.com/application-research/estuary/util"
	"github.com/ipfs/go-cid"
	"gorm.io/gorm/clause"
)

func (cm *ContentManager) HandleSanityCheck(cc cid.Cid, errMsg string) {
	cm.log.Warnf("running sanity check for cid: %s", cc)

	// get all contents affected by this missing block on estuary or from shuttles
	var cnts []util.Content
	where := "id in (select content from obj_refs where object = (select id from objects where cid = ?))"
	if err := cm.db.Find(&cnts, where, cc.Bytes()).Error; err != nil {
		cm.log.Errorf("sanity check failed to get content(s) for cid: %s, err: %w", cc.String(), err)
		return
	}

	// mark all contents affected by this missing block
	marks := make(map[uint]bool, 0)
	for _, cnt := range cnts {
		if _, ok := marks[cnt.ID]; !ok {
			cm.log.Debugf("setting sanity check for cont: %d", cnt.ID)

			chk := &model.SanityCheck{
				BlockCid:  util.DbCID{CID: cc},
				ContentID: cnt.ID,
				ErrMsg:    errMsg,
			}
			if err := cm.db.Clauses(clause.OnConflict{DoNothing: true}).Create(chk).Error; err != nil {
				cm.log.Errorf("failed to create sanity check mark for content: %d, err: %w", cnt.ID, err)
				return
			}
			marks[cnt.ID] = true
		}
	}
}
