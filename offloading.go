package main

import (
	"context"
	"fmt"
	"sort"
	"time"

	"golang.org/x/xerrors"
)

const cacheThreshold = 0.50

type offloadCandidate struct {
	Content
	LastAccess time.Time
}

type collectionResult struct {
	SpaceRequest int64
	SpaceFreed   int64

	ContentsFreed        []offloadCandidate
	CandidatesConsidered int
}

func (cm *ContentManager) ClearUnused(ctx context.Context, spaceRequest int64, dryrun bool) (*collectionResult, error) {
	ctx, span := cm.tracer.Start(ctx, "clearUnused")
	defer span.End()
	// first, gather candidates for removal
	// that is any content we have made the correct number of deals for, that
	// hasnt been fetched from us in X days

	candidates, err := cm.getRemovalCandidates(ctx)
	if err != nil {
		return nil, err
	}

	// sort candidates by 'last used'
	var offs []offloadCandidate
	for _, c := range candidates {
		la, err := cm.getLastAccessForContent(c)
		if err != nil {
			return nil, err
		}

		offs = append(offs, offloadCandidate{
			Content:    c,
			LastAccess: la,
		})
	}

	sort.Slice(offs, func(i, j int) bool {
		return offs[i].LastAccess.Before(offs[j].LastAccess)
	})

	// grab enough candidates to fulfil the requested space
	bytesRemaining := spaceRequest
	var toRemove []offloadCandidate
	for _, o := range offs {
		toRemove = append(toRemove, o)
		bytesRemaining -= o.Size

		if bytesRemaining <= 0 {
			break
		}
	}

	result := &collectionResult{
		SpaceRequest:         spaceRequest,
		SpaceFreed:           spaceRequest - bytesRemaining,
		ContentsFreed:        toRemove,
		CandidatesConsidered: len(candidates),
	}

	// go offload them all
	if dryrun {
		return result, nil
	}

	for _, c := range toRemove {
		// TODO: a bulk content offloading function seems like it would be pretty efficient
		if err := cm.OffloadContent(ctx, c.ID); err != nil {
			log.Warnf("failed to offload content %d: %s", c.ID, err)
		}
	}

	return result, nil
}

// TODO: this is only looking at the root, maybe we could find an efficient way to check more of the objects?
// additionally, for aggregates, we should check each aggregated item under the root
func (cm *ContentManager) getLastAccessForContent(cont Content) (time.Time, error) {
	var obj Object
	if err := cm.DB.First(&obj, "cid = ?", cont.Cid).Error; err != nil {
		return time.Time{}, err
	}

	return obj.LastAccess, nil
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
