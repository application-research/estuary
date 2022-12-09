package shuttle

import (
	"sort"
	"strings"

	"github.com/application-research/estuary/model"
	"github.com/application-research/estuary/util"
)

func (mgr *Manager) GetPreferredUploadEndpoints(u *util.User) ([]string, error) {
	// TODO: this should be a lotttttt smarter
	mgr.ShuttlesLk.Lock()
	defer mgr.ShuttlesLk.Unlock()
	var shuttles []model.Shuttle
	for hnd, sh := range mgr.Shuttles {
		if sh.ContentAddingDisabled {
			mgr.log.Debugf("shuttle %+v content adding is disabled", sh)
			continue
		}

		if sh.Hostname == "" {
			mgr.log.Debugf("shuttle %+v has empty hostname", sh)
			continue
		}

		var shuttle model.Shuttle
		if err := mgr.db.First(&shuttle, "handle = ?", hnd).Error; err != nil {
			mgr.log.Errorf("failed to look up shuttle by handle: %s", err)
			continue
		}

		if !shuttle.Open {
			mgr.log.Debugf("shuttle %+v is not open, skipping", shuttle)
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

	if !mgr.cfg.Content.DisableLocalAdding {
		out = append(out, mgr.cfg.Hostname+"/content/add")
	}
	return out, nil
}
