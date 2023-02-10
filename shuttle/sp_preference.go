package shuttle

import (
	"encoding/json"
	"io/ioutil"

	"github.com/filecoin-project/go-address"
	explru "github.com/paskal/golang-lru/simplelru"
	"github.com/pkg/errors"
	"golang.org/x/xerrors"
)

// BuildCache function
// -> query for ALL shuttles,
// make 2 maps, for forward and reverse lookup

// Cache.Lookup()
// Cache.ReverseLookup()

type SPShuttlePing struct {
	SP      address.Address `json:"address"`
	Latency int64           `json:"latency"`
}

type ShuttleSPPing struct {
	Shuttle string `json:"address"`
	Latency int64  `json:"latency"`
}

type PrefCache struct {
	// *manager
	ShuttlePingCache explru.ExpirableLRU
}

func (m *manager) PopulatePrefCache() error {
	var p PrefCache

	connectedShuttles, err := m.getConnections()
	if err != nil {
		return err
	}

	for _, sh := range connectedShuttles {
		p.ByShuttle(sh.Hostname)
	}

	return nil
}

func (p *PrefCache) ByShuttle(hostname string) ([]SPShuttlePing, error) {
	cached, inCache := p.ShuttlePingCache.Get(hostname)
	if inCache {
		out, isParsed := cached.([]SPShuttlePing)
		if isParsed {
			return out, nil
		} else {
			return nil, xerrors.Errorf("could not parse sp preference from cache")
		}
	} else {
		pref, err := p.QuerySPPreference(hostname)
		if err != nil {
			return nil, err
		}
		p.ShuttlePingCache.Add(hostname, pref)
		return pref, nil
	}
}

// Get a list of shuttles and their pings, from the perspective of an SP
func (p *PrefCache) BySP(spAddr address.Address) ([]ShuttleSPPing, error) {
	// Ensure the cache is completely populated first
	p.PopulatePrefCache()

	var result []ShuttleSPPing

	keys := p.ShuttlePingCache.Keys()

	for _, s := range keys {
		shuttle := s.(string)
		cached, _ := p.ShuttlePingCache.Get(shuttle)
		pings, _ := cached.([]SPShuttlePing)

		for _, sp := range pings {
			if sp.SP == spAddr {
				result = append(result, ShuttleSPPing{
					Shuttle: shuttle,
					Latency: sp.Latency,
				})
			}
		}
	}

	return result, nil
}

// Queries the shuttle for storage providers, sorted by latency
func (p *PrefCache) QuerySPPreference(hostname string) ([]SPShuttlePing, error) {
	var out []SPShuttlePing

	// error 503 indicates cache is not ready yet
	resp, err := callAPI("GET", hostname, "/storage-provider/list", "") // TODO: Auth token?
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		_, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return nil, err
		}
		return nil, err
	}

	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, errors.Errorf("failed to decode shuttle-SP list response: %s", err)
	}

	return out, nil
}
