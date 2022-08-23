package main

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/application-research/estuary/constants"
	"github.com/application-research/estuary/util"
	"golang.org/x/xerrors"
)

type offloadCandidate struct {
	util.Content
	LastAccess time.Time
}

type collectionResult struct {
	SpaceRequest int64 `json:"spaceRequest"`
	SpaceFreed   int64 `json:"spaceFreed"`

	ContentsFreed        []offloadCandidate `json:"contentsFreed"`
	CandidatesConsidered int                `json:"candidatesConsidered"`
	BlocksRemoved        int                `json:"blocksRemoved"`
	DryRun               bool               `json:"dryRun"`
	OffloadError         string             `json:"offloadError,omitempty"`
}

func (cm *ContentManager) ClearUnused(ctx context.Context, spaceRequest int64, loc string, users []uint, dryrun bool) (*collectionResult, error) {
	ctx, span := cm.tracer.Start(ctx, "clearUnused")
	defer span.End()
	// first, gather candidates for removal
	// that is any content we have made the correct number of deals for, that
	// hasnt been fetched from us in X days

	candidates, err := cm.getRemovalCandidates(ctx, false, loc, users)
	if err != nil {
		return nil, fmt.Errorf("failed to get removal candidates: %w", err)
	}

	offs, err := cm.getLastAccesses(ctx, candidates)
	if err != nil {
		return nil, fmt.Errorf("failed to get last accesses: %w", err)
	}

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
		DryRun:               dryrun,
	}

	if dryrun {
		return result, nil
	}

	// go offload them all
	var ids []uint
	for _, tr := range toRemove {
		ids = append(ids, tr.Content.ID)
	}

	rem, err := cm.OffloadContents(ctx, ids)
	if err != nil {
		result.OffloadError = err.Error()
		log.Warnf("failed to offload contents: %s", err)
	}

	result.BlocksRemoved = rem

	return result, nil
}
func (cm *ContentManager) getLastAccesses(ctx context.Context, candidates []removalCandidateInfo) ([]offloadCandidate, error) {
	_, span := cm.tracer.Start(ctx, "getLastAccesses")
	defer span.End()

	var offs []offloadCandidate
	for _, c := range candidates {
		la, err := cm.getLastAccessForContent(c.Content)
		if err != nil {
			log.Errorf("check last access for %d: %s", c.Content, err)
			continue
		}

		offs = append(offs, offloadCandidate{
			Content:    c.Content,
			LastAccess: la,
		})
	}

	// sort candidates by 'last used'
	sort.Slice(offs, func(i, j int) bool {
		return offs[i].LastAccess.Before(offs[j].LastAccess)
	})

	return offs, nil
}

// TODO: this is only looking at the root, maybe we could find an efficient way to check more of the objects?
// additionally, for aggregates, we should check each aggregated item under the root
func (cm *ContentManager) getLastAccessForContent(cont util.Content) (time.Time, error) {
	var obj util.Object
	if err := cm.DB.First(&obj, "cid = ?", cont.Cid).Error; err != nil {
		return time.Time{}, err
	}

	return obj.LastAccess, nil
}

func (cm *ContentManager) OffloadContents(ctx context.Context, conts []uint) (int, error) {
	ctx, span := cm.tracer.Start(ctx, "OffloadContents")
	defer span.End()

	var local []uint

	remote := make(map[string][]uint)

	cm.contentLk.Lock()
	defer cm.contentLk.Unlock()
	for _, c := range conts {
		var cont util.Content
		if err := cm.DB.First(&cont, "id = ?", c).Error; err != nil {
			return 0, err
		}

		if cont.Location == constants.ContentLocationLocal {
			local = append(local, cont.ID)
		} else {
			remote[cont.Location] = append(remote[cont.Location], cont.ID)
		}

		if cont.AggregatedIn > 0 {
			return 0, fmt.Errorf("cannot offload aggregated content")
		}

		if err := cm.DB.Model(&util.Content{}).Where("id = ?", c).Update("offloaded", true).Error; err != nil {
			return 0, err
		}

		if err := cm.DB.Model(&util.ObjRef{}).Where("content = ?", c).Update("offloaded", 1).Error; err != nil {
			return 0, err
		}

		if cont.Aggregate {
			if err := cm.DB.Model(&util.Content{}).Where("aggregated_in = ?", c).Update("offloaded", true).Error; err != nil {
				return 0, err
			}

			if err := cm.DB.Model(&util.ObjRef{}).
				Where("content in (?)",
					cm.DB.Model(util.Content{}).
						Where("aggregated_in = ?", c).
						Select("id")).
				Update("offloaded", 1).Error; err != nil {
				return 0, err
			}

			var children []util.Content
			if err := cm.DB.Find(&children, "aggregated_in = ?", c).Error; err != nil {
				return 0, err
			}

			for _, c := range children {
				if cont.Location == constants.ContentLocationLocal {
					local = append(local, c.ID)
				} else {
					remote[cont.Location] = append(remote[cont.Location], c.ID)
				}
			}
		}
	}

	for loc, conts := range remote {
		if err := cm.sendUnpinCmd(ctx, loc, conts); err != nil {
			log.Errorf("failed to send unpin command to shuttle: %s", err)
		}
	}

	var deleteCount int
	for _, c := range local {
		objs, err := cm.objectsForPin(ctx, c)
		if err != nil {
			return 0, err
		}

		for _, o := range objs {
			del, err := cm.deleteIfNotPinnedLock(ctx, o)
			if err != nil {
				return deleteCount, err
			}

			if del {
				deleteCount++
			}
		}
	}

	return deleteCount, nil
}

type removalCandidateInfo struct {
	util.Content
	TotalDeals      int `json:"totalDeals"`
	ActiveDeals     int `json:"activeDeals"`
	InProgressDeals int `json:"inProgressDeals"`
}

func (cm *ContentManager) getRemovalCandidates(ctx context.Context, all bool, loc string, users []uint) ([]removalCandidateInfo, error) {
	ctx, span := cm.tracer.Start(ctx, "getRemovalCandidates")
	defer span.End()

	q := cm.DB.Model(util.Content{}).Where("active and not offloaded and (aggregate or not aggregated_in > 0)")
	if loc != "" {
		q = q.Where("location = ?", loc)
	}

	if len(users) > 0 {
		q = q.Where("user_id in ?", users)
	}

	var conts []util.Content
	if err := q.Scan(&conts).Error; err != nil {
		return nil, fmt.Errorf("scanning removal candidates failed: %w", err)
	}

	var toOffload []removalCandidateInfo
	for _, c := range conts {
		good, progress, failed, err := cm.contentIsProperlyReplicated(ctx, c.ID)
		if err != nil {
			return nil, xerrors.Errorf("failed to check replication of %d: %w", c.ID, err)
		}

		if all || good >= c.Replication {
			toOffload = append(toOffload, removalCandidateInfo{
				Content:         c,
				TotalDeals:      good + progress + failed,
				ActiveDeals:     good,
				InProgressDeals: progress,
			})
		}
	}

	return toOffload, nil
}

func (cm *ContentManager) contentIsProperlyReplicated(ctx context.Context, c uint) (int, int, int, error) {
	var contentDeals []contentDeal
	if err := cm.DB.Find(&contentDeals, "content = ?", c).Error; err != nil {
		return 0, 0, 0, err
	}

	var goodCount, inprog, failed int
	for _, d := range contentDeals {
		if d.Failed {
			failed++
		} else if !d.Failed && d.DealID > 0 {
			goodCount++
		} else {
			inprog++
		}
	}

	return goodCount, inprog, failed, nil
}
