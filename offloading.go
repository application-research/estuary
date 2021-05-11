package main

import (
	"context"

	"golang.org/x/xerrors"
)

func (cm *ContentManager) ClearUnused() error {
	// first, gather candidates for removal
	// that is any content we have made the correct number of deals for, that
	// hasnt been fetched from us in X days

	return nil
}

type refResult struct {
	Cid dbCID
}

func (cm *ContentManager) OffloadContent(ctx context.Context, c uint) error {
	cm.contentLk.Lock()
	defer cm.contentLk.Unlock()
	var cont Content
	if err := cm.DB.First(&cont, "id = ?", c).Error; err != nil {
		return err
	}

	if err := cm.DB.Model(&Content{}).Where("id = ?", c).Update("offloaded", true).Error; err != nil {
		return err
	}

	if err := cm.DB.Model(&ObjRef{}).Where("content = ?", c).Update("offloaded", true).Error; err != nil {
		return err
	}

	// select * from obj_refs group by object having MIN(obj_refs.offloaded) = 1 and obj_refs.content = 1;
	q := cm.DB.Debug().Model(&ObjRef{}).
		Select("objects.cid").
		Joins("left join objects on obj_refs.object = objects.id").
		Group("object").
		Having("obj_refs.content = ? and MIN(obj_refs.offloaded) = 1", c)

	rows, err := q.Rows()
	if err != nil {
		return err
	}

	// TODO: I believe that we need to hold a lock for the entire period that
	// we are deleting objects from the blockstore, otherwise a new file could
	// come in that has overlapping blocks, and have its blocks deleted by this
	// process.
	for rows.Next() {
		var dbc dbCID
		if err := rows.Scan(&dbc); err != nil {
			return err
		}

		if err := cm.Blockstore.DeleteBlock(dbc.CID); err != nil {
			return err
		}
	}
	return nil
}

func (cm *ContentManager) getRemovalCandidates() ([]Content, error) {
	var conts []Content
	if err := cm.DB.Find(&conts, "active and not offloaded").Error; err != nil {
		return nil, err
	}

	var toOffload []Content
	for _, c := range conts {
		ok, err := cm.contentIsProperlyReplicated(c.ID, c.Replication)
		if err != nil {
			return nil, xerrors.Errorf("failed to check replication of %d: %w", c.ID, err)
		}

		if !ok {
			// maybe kick off repairs?
			log.Infof("content %d is in need of repairs", c.ID)
			continue
		}

		toOffload = append(toOffload, c)
	}

	return toOffload, nil
}

func (cm *ContentManager) contentIsProperlyReplicated(c uint, repl int) (bool, error) {
	var contentDeals []contentDeal
	if err := cm.DB.Find(&contentDeals, "content = ? and not failed and deal_id > 0", c).Error; err != nil {
		return false, err
	}

	if len(contentDeals) >= repl {
		return true, nil
	}

	return false, nil
}
