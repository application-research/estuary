package shuttle

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"

	"github.com/application-research/estuary/util"
	"github.com/pkg/errors"
)

func (mgr *Manager) GetShuttlesConfig(u *util.User) (interface{}, error) {
	var shts []interface{}
	for _, sh := range mgr.Shuttles {
		if sh.Hostname == "" {
			mgr.log.Warnf("failed to get shuttle(%s) config, shuttle hostname is not set", sh.Handle)
			continue
		}

		out, err := getShuttleConfig(sh.Hostname, u.AuthToken.Token)
		if err != nil {
			return nil, err
		}
		shts = append(shts, out)
	}
	return shts, nil
}

func getShuttleConfig(hostname string, authToken string) (interface{}, error) {
	u, err := url.Parse(hostname)
	if err != nil {
		return nil, errors.Errorf("failed to parse url for shuttle(%s) config: %s", hostname, err)
	}
	u.Path = ""

	req, err := http.NewRequest("GET", fmt.Sprintf("%s://%s/admin/system/config", u.Scheme, u.Host), nil)
	if err != nil {
		return nil, errors.Errorf("failed to build GET request for shuttle(%s) config: %s", hostname, err)
	}
	req.Header.Set("Authorization", "Bearer "+authToken)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, errors.Errorf("failed to request shuttle(%s) config: %s", hostname, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		bodyBytes, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return nil, errors.Errorf("failed to read shuttle(%s) config err resp: %s", hostname, err)
		}
		return nil, errors.Errorf("failed to get shuttle(%s) config: %s", hostname, bodyBytes)
	}

	var out interface{}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, errors.Errorf("failed to decode shuttle config response: %s", err)
	}
	return out, nil
}
