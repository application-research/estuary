package main

import (
	"context"
	"fmt"

	"golang.org/x/xerrors"
)

const cacheThreshold = 0.50

func (cm *ContentManager) ClearUnused() error {
	// first, gather candidates for removal
	// that is any content we have made the correct number of deals for, that
	// hasnt been fetched from us in X days

	candidates, err := cm.getRemovalCandidates(context.TODO())
	if err != nil {
		return err
	}
	_ = candidates

	return nil
}

type refResult struct {
	Cid dbCID
}

func (cm *ContentManager) OffloadContent(ctx context.Context, c uint) error {
	ctx, span := cm.tracer.Start(ctx, "OffloadContent")
	defer span.End()

	cm.contentLk.Lock()
	defer cm.contentLk.Unlock()
	var cont Content
	if err := cm.DB.First(&cont, "id = ?", c).Error; err != nil {
		return err
	}

	if cont.AggregatedIn > 0 {
		return fmt.Errorf("cannot offload aggregated content")
	}

	if err := cm.DB.Model(&Content{}).Where("id = ?", c).Update("offloaded", true).Error; err != nil {
		return err
	}

	if err := cm.DB.Model(&ObjRef{}).Where("content = ?", c).Update("offloaded", 1).Error; err != nil {
		return err
	}

	/*
		// FIXME: this query works on sqlite, but apparently not on postgres.
		// select * from obj_refs group by object having MIN(obj_refs.offloaded) = 1 and obj_refs.content = 1;
		q := cm.DB.Debug().Model(&ObjRef{}).
			Select("objects.cid").
			Joins("left join objects on obj_refs.object = objects.id").
			Group("object").
			Having("obj_refs.content = ? and MIN(obj_refs.offloaded) = 1", c)
	*/

	// FIXME: this query doesnt filter down to just the content we're looking at, but at least it works?
	q := cm.DB.Debug().Model(&ObjRef{}).
		Select("cid").
		Joins("left join objects on obj_refs.object = objects.id").
		Group("cid").
		Having("MIN(obj_refs.offloaded) = 1")

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

type removalCandidateInfo struct {
	Content
	TotalDeals      int `json:"totalDeals"`
	ActiveDeals     int `json:"activeDeals"`
	InProgressDeals int `json:"inProgressDeals"`
}

func (cm *ContentManager) getRemovalCandidates(ctx context.Context) ([]Content, error) {
	ctx, span := cm.tracer.Start(ctx, "getRemovalCandidates")
	defer span.End()

	var conts []Content
	if err := cm.DB.Find(&conts, "active and not offloaded and (aggregate or not aggregated_in > 0)").Error; err != nil {
		return nil, err
	}

	var toOffload []Content
	for _, c := range conts {
		ok, err := cm.contentIsProperlyReplicated(ctx, c.ID, c.Replication)
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

func (cm *ContentManager) contentIsProperlyReplicated(ctx context.Context, c uint, repl int) (bool, error) {
	var contentDeals []contentDeal
	if err := cm.DB.Find(&contentDeals, "content = ?", c).Error; err != nil {
		return false, err
	}

	var goodCount int
	for _, d := range contentDeals {
		if !d.Failed && d.DealID > 0 {
			goodCount++
		}
	}

	if goodCount >= repl {
		return true, nil
	}

	return false, nil
}
