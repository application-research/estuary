package shuttle

import (
	"context"
	"fmt"
	"sort"

	"github.com/application-research/estuary/constants"
	"github.com/application-research/estuary/model"
	"github.com/application-research/estuary/util"
	"github.com/ipfs/go-cid"
)

func (cm *Manager) SelectLocationForStorage(ctx context.Context, obj cid.Cid, uid uint) (string, error) {
	ctx, span := cm.tracer.Start(ctx, "selectLocation")
	defer span.End()

	allShuttlesLowSpace := true
	lowSpace := make(map[string]bool)
	var activeShuttles []string
	cm.ShuttlesLk.Lock()
	for d, sh := range cm.Shuttles {
		if !sh.private && !sh.ContentAddingDisabled {
			lowSpace[d] = sh.spaceLow
			activeShuttles = append(activeShuttles, d)
		} else {
			allShuttlesLowSpace = false
		}
	}
	cm.ShuttlesLk.Unlock()

	var shuttles []model.Shuttle
	if err := cm.db.Order("priority desc").Find(&shuttles, "handle in ? and open", activeShuttles).Error; err != nil {
		return "", err
	}

	// prefer shuttles that are not low on blockstore space
	sort.SliceStable(shuttles, func(i, j int) bool {
		lsI := lowSpace[shuttles[i].Handle]
		lsJ := lowSpace[shuttles[j].Handle]

		if lsI == lsJ {
			return false
		}

		return lsJ
	})

	if len(shuttles) == 0 {
		if cm.cfg.Content.DisableLocalAdding {
			return "", fmt.Errorf("no shuttles available and local content adding disabled")
		}
		return constants.ContentLocationLocal, nil
	}

	// TODO: take into account existing staging zones and their primary
	// locations while choosing
	ploc := cm.primaryStagingLocation(ctx, uid)
	if ploc == "" {
		cm.log.Warnf("empty staging zone set for user %d", uid)
	}

	if ploc != "" {
		if allShuttlesLowSpace || !lowSpace[ploc] {
			for _, sh := range shuttles {
				if sh.Handle == ploc {
					return ploc, nil
				}
			}
		}

		// TODO: maybe we should just assign the pin to the preferred shuttle
		// anyways, this could be the case where theres a small amount of
		// downtime from rebooting or something
		cm.log.Warnf("preferred shuttle %q not online", ploc)
	}
	// since they are ordered by priority, just take the first
	return shuttles[0].Handle, nil
}

func (cm *Manager) primaryStagingLocation(ctx context.Context, uid uint) string {
	var zones []util.Content
	if err := cm.db.Find(&zones, "user_id = ? and aggregate", uid).Error; err != nil {
		return ""
	}

	// TODO: maybe we could make this more complex, but for now, if we have a
	// staging zone opened in a particular location, just keep using that one
	for _, z := range zones {
		return z.Location
	}
	return ""
}

func (cm *Manager) SelectLocationForRetrieval(ctx context.Context, cont util.Content) (string, error) {
	_, span := cm.tracer.Start(ctx, "selectLocationForRetrieval")
	defer span.End()

	var activeShuttles []string
	cm.ShuttlesLk.Lock()
	for d, sh := range cm.Shuttles {
		if !sh.private {
			activeShuttles = append(activeShuttles, d)
		}
	}
	cm.ShuttlesLk.Unlock()

	var shuttles []model.Shuttle
	if err := cm.db.Order("priority desc").Find(&shuttles, "handle in ? and open", activeShuttles).Error; err != nil {
		return "", err
	}

	if len(shuttles) == 0 {
		if cm.cfg.Content.DisableLocalAdding {
			return "", fmt.Errorf("no shuttles available and local content adding disabled")
		}
		return constants.ContentLocationLocal, nil
	}

	// prefer the shuttle the content is already on
	for _, sh := range shuttles {
		if sh.Handle == cont.Location {
			return sh.Handle, nil
		}
	}
	// since they are ordered by priority, just take the first
	return shuttles[0].Handle, nil
}
