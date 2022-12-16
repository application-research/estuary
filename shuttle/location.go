package shuttle

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/application-research/estuary/constants"
	"github.com/application-research/estuary/model"
	"github.com/application-research/estuary/util"
	"github.com/ipfs/go-cid"
)

func (m *manager) GetLocationForStorage(ctx context.Context, obj cid.Cid, uid uint) (string, error) {
	ctx, span := m.tracer.Start(ctx, "selectLocation")
	defer span.End()

	allShuttlesLowSpace := true
	lowSpace := make(map[string]bool)
	var activeShuttles []string

	connectedShuttles, err := m.getConnections()
	if err != nil {
		return "", err
	}

	for _, sh := range connectedShuttles {
		if !sh.Private && !sh.ContentAddingDisabled {
			lowSpace[sh.Handle] = sh.SpaceLow
			activeShuttles = append(activeShuttles, sh.Handle)
		} else {
			allShuttlesLowSpace = false
		}
	}

	var shuttles []model.Shuttle
	if err := m.db.Order("priority desc").Find(&shuttles, "handle in ? and open", activeShuttles).Error; err != nil {
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
		if m.cfg.Content.DisableLocalAdding {
			return "", fmt.Errorf("no shuttles available and local content adding disabled")
		}
		return constants.ContentLocationLocal, nil
	}

	// TODO: take into account existing staging zones and their primary
	// locations while choosing
	ploc := m.primaryStagingLocation(ctx, uid)
	if ploc == "" {
		m.log.Warnf("empty staging zone set for user %d", uid)
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
		m.log.Warnf("preferred shuttle %q not online", ploc)
	}
	// since they are ordered by priority, just take the first
	return shuttles[0].Handle, nil
}

func (cm *manager) primaryStagingLocation(ctx context.Context, uid uint) string {
	var zones []util.Content
	if err := cm.db.First(&zones, "user_id = ? and aggregate and not active", uid).Error; err != nil {
		return ""
	}

	// TODO: maybe we could make this more complex, but for now, if we have a
	// staging zone opened in a particular location, just keep using that one
	for _, z := range zones {
		return z.Location
	}
	return ""
}

func (m *manager) GetLocationForRetrieval(ctx context.Context, cont util.Content) (string, error) {
	_, span := m.tracer.Start(ctx, "selectLocationForRetrieval")
	defer span.End()

	var activeShuttles []string
	connectedShuttles, err := m.getConnections()
	if err != nil {
		return "", err
	}

	for _, sh := range connectedShuttles {
		if !sh.Private {
			activeShuttles = append(activeShuttles, sh.Handle)
		}
	}

	var shuttles []model.Shuttle
	if err := m.db.Order("priority desc").Find(&shuttles, "handle in ? and open", activeShuttles).Error; err != nil {
		return "", err
	}

	if len(shuttles) == 0 {
		if m.cfg.Content.DisableLocalAdding {
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

// TODO: this should be a lotttttt smarter
func (m *manager) GetPreferredUploadEndpoints(u *util.User) ([]string, error) {
	var shuttles []model.Shuttle
	connectedShuttles, err := m.getConnections()
	if err != nil {
		return nil, err
	}

	for _, sh := range connectedShuttles {
		if sh.ContentAddingDisabled {
			m.log.Debugf("shuttle %+v content adding is disabled", sh)
			continue
		}

		if sh.Hostname == "" {
			m.log.Debugf("shuttle %+v has empty hostname", sh)
			continue
		}

		var shuttle model.Shuttle
		if err := m.db.First(&shuttle, "handle = ?", sh.Handle).Error; err != nil {
			m.log.Errorf("failed to look up shuttle by handle: %s", err)
			continue
		}

		if !shuttle.Open {
			m.log.Debugf("shuttle %+v is not open, skipping", shuttle)
			continue
		}
		shuttles = append(shuttles, shuttle)
	}

	sort.Slice(shuttles, func(i, j int) bool {
		return shuttles[i].Priority > shuttles[j].Priority
	})

	var out []string
	for _, sh := range shuttles {
		host := "https://" + sh.Host
		if strings.HasPrefix(sh.Host, "http://") || strings.HasPrefix(sh.Host, "https://") {
			host = sh.Host
		}
		out = append(out, host+"/content/add")
	}

	if !m.cfg.Content.DisableLocalAdding {
		out = append(out, m.cfg.Hostname+"/content/add")
	}
	return out, nil
}
