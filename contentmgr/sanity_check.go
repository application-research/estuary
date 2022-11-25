package contentmgr

import (
	"github.com/application-research/estuary/util"
	"github.com/ipfs/go-cid"
)

func (cm *ContentManager) HandleSanityCheck(cc cid.Cid, err error) {
	// get a contents affected by this missing block
	var cnts []util.Content
	where := "id in (select content from obj_refs where object = (select id from objects where cid = ?))"
	if err := cm.DB.Find(&cnts, where, cc.Bytes()).Error; err != nil {
		cm.log.Errorf("sanity check failed to get content(s) for cid: %s, err: %w", cc.String(), err)
		return
	}

	// mark all contents affected by this missing block
	var marks map[uint]bool
	for _, cnt := range cnts {
		if _, ok := marks[cnt.ID]; !ok {
			if err := cm.DB.Model(&util.Content{}).Where("id = ?", cnt.ID).Update("failed_sanity_check", true).Error; err != nil {
				cm.log.Errorf("sanity check failed to mark content: %d, err: %w", cnt.ID, err)
				return
			}
			marks[cnt.ID] = true
		}
	}
}
