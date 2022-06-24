package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime/multipart"
	"net"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strings"
	"syscall"
	"time"

	"github.com/application-research/estuary/pinner/types"
	util "github.com/application-research/estuary/util"
	"github.com/cheggaaa/pb/v3"
	"github.com/ipfs/go-cid"
)

type EstClient struct {
	Host    string
	Shuttle string
	Tok     string

	DoProgress bool

	LogTimings bool
}

type httpStatusError struct {
	Status     string
	StatusCode int
	Extra      string
}

func (hse httpStatusError) Error() string {
	return fmt.Sprintf("received non-200 status: %s (%s)", hse.Status, hse.Extra)
}

func (c *EstClient) doRequest(ctx context.Context, method string, path string, body interface{}, resp interface{}) (int, error) {
	start := time.Now()
	defer func() {
		if c.LogTimings {
			fmt.Printf("%s %s took %s\n", method, path, time.Since(start))
		}
	}()

	var bodyr io.Reader
	if body != nil {
		data, err := json.Marshal(body)
		if err != nil {
			return 0, err
		}
		bodyr = bytes.NewReader(data)
	}

	req, err := http.NewRequest(method, c.Host+path, bodyr)
	if err != nil {
		return 0, err
	}

	req.Header.Set("Authorization", "Bearer "+c.Tok)
	if bodyr != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	r, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, err
	}

	defer r.Body.Close()

	if !(r.StatusCode >= 200 && r.StatusCode < 300) {
		var out map[string]interface{}
		if err := json.NewDecoder(r.Body).Decode(&out); err != nil {
			return r.StatusCode, &httpStatusError{
				StatusCode: r.StatusCode,
				Status:     r.Status,
				Extra:      "no error given",
			}
		}

		errstr, ok := out["error"]
		if !ok {
			return r.StatusCode, &httpStatusError{
				StatusCode: r.StatusCode,
				Status:     r.Status,
				Extra:      "unrecognized error format",
			}
		}

		var extra string
		switch es := errstr.(type) {
		case string:
			extra = es
		case map[string]interface{}:
			reason, _ := es["reason"].(string)
			details, _ := es["details"].(string)

			extra = reason
			if details != "" {
				extra += ": " + details
			}
		}

		return r.StatusCode, &httpStatusError{
			StatusCode: r.StatusCode,
			Status:     r.Status,
			Extra:      extra,
		}
	}

	if resp != nil {
		return r.StatusCode, json.NewDecoder(r.Body).Decode(resp)
	}
	return r.StatusCode, nil
}

func (c *EstClient) Viewer(ctx context.Context) (*util.ViewerResponse, error) {
	var vresp util.ViewerResponse
	_, err := c.doRequest(ctx, "GET", "/viewer", nil, &vresp)
	if err != nil {
		return nil, err
	}

	return &vresp, nil
}

func (c *EstClient) AddCar(fpath, name string) (*util.ContentAddResponse, error) {
	fi, err := os.Open(fpath)
	if err != nil {
		return nil, err
	}

	var rc io.ReadCloser = fi
	if c.DoProgress {
		finfo, err := fi.Stat()
		if err != nil {
			return nil, err
		}

		rc = pb.Start64(finfo.Size()).NewProxyReader(fi)
	}

	defer rc.Close()

	req, err := http.NewRequest("POST", fmt.Sprintf("%s/content/add-car", c.Shuttle), rc)
	if err != nil {
		return nil, err
	}

	//req.Header.Add("Content-Type", mw.FormDataContentType())
	req.Header.Set("Authorization", "Bearer "+c.Tok)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != 200 {
		var m map[string]interface{}
		if err := json.NewDecoder(resp.Body).Decode(&m); err != nil {
			fmt.Println(err)
		}
		return nil, fmt.Errorf("got invalid status code: %d", resp.StatusCode)
	}

	var rbody util.ContentAddResponse
	if err := json.NewDecoder(resp.Body).Decode(&rbody); err != nil {
		return nil, err
	}

	return &rbody, nil
}

func (c *EstClient) AddFile(fpath, filename string) (*util.ContentAddResponse, error) {
	r, w := io.Pipe()
	fi, err := os.Open(fpath)
	if err != nil {
		return nil, err
	}

	var rc io.ReadCloser = fi
	if c.DoProgress {
		finfo, err := fi.Stat()
		if err != nil {
			return nil, err
		}

		rc = pb.Start64(finfo.Size()).NewProxyReader(fi)
	}

	mw := multipart.NewWriter(w)

	go func() {
		var outerr error
		defer func() {
			if outerr != nil {
				w.CloseWithError(outerr)
			} else {
				w.Close()
			}
		}()

		part, err := mw.CreateFormFile("data", filename)
		if err != nil {
			outerr = err
			return
		}

		_, err = io.Copy(part, rc)
		if err != nil {
			outerr = err
			return
		}
		mw.Close()
	}()

	req, err := http.NewRequest("POST", fmt.Sprintf("%s/content/add", c.Shuttle), r)
	if err != nil {
		return nil, err
	}

	req.Header.Add("Content-Type", mw.FormDataContentType())
	req.Header.Set("Authorization", "Bearer "+c.Tok)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != 200 {
		var m map[string]interface{}
		if err := json.NewDecoder(resp.Body).Decode(&m); err != nil {
			fmt.Println(err)
		}
		return nil, fmt.Errorf("got invalid status code: %d", resp.StatusCode)
	}

	var rbody util.ContentAddResponse
	if err := json.NewDecoder(resp.Body).Decode(&rbody); err != nil {
		return nil, err
	}

	return &rbody, nil
}

// TODO: copied from main estuary codebase, should dedupe and use the same struct
type Collection struct {
	ID        uint      `json:"-"`
	CreatedAt time.Time `json:"createdAt"`

	UUID string `json:"uuid"`

	Name        string `json:"name"`
	Description string `json:"description"`
	UserID      uint   `json:"userId"`
}

func (c *EstClient) CollectionsList(ctx context.Context) ([]*Collection, error) {
	var out []*Collection
	_, err := c.doRequest(ctx, "GET", "/collections/list", nil, &out)
	if err != nil {
		return nil, err
	}

	return out, nil
}

func (c *EstClient) CollectionsCreate(ctx context.Context, name, desc string) (*Collection, error) {
	body := map[string]string{
		"name":        name,
		"description": desc,
	}

	var out Collection
	_, err := c.doRequest(ctx, "POST", "/collections/create", body, &out)
	if err != nil {
		return nil, err
	}

	return &out, nil
}

type collectionListResponse struct {
	Name   string `json:"name"`
	Dir    bool   `json:"dir"`
	Size   int64  `json:"size"`
	ContID uint   `json:"contId"`
}

func (c *EstClient) CollectionsListDir(ctx context.Context, coluuid, path string) ([]collectionListResponse, error) {
	var out []collectionListResponse
	_, err := c.doRequest(ctx, "GET", fmt.Sprintf("/collections/content?coluuid=%s&colpath=%s", coluuid, url.PathEscape(path)), nil, &out)
	if err != nil {
		return nil, err
	}

	sort.Slice(out, func(i, j int) bool {
		return out[i].Name < out[j].Name
	})

	return out, nil
}

func (c *EstClient) PinAdd(ctx context.Context, root cid.Cid, name string, origins []string, meta map[string]interface{}) (*types.IpfsPinStatusResponse, error) {
	p := &types.IpfsPin{
		CID:     root.String(),
		Name:    name,
		Origins: origins,
		Meta:    meta,
	}

	var resp types.IpfsPinStatusResponse
	_, err := c.doRequest(ctx, "POST", "/pinning/pins", p, &resp)
	if err != nil {
		return nil, err
	}

	return &resp, nil
}

func (c *EstClient) PinStatus(ctx context.Context, reqid string) (*types.IpfsPinStatusResponse, error) {
	var resp types.IpfsPinStatusResponse
	_, err := c.doRequest(ctx, "GET", "/pinning/pins/"+reqid, nil, &resp)
	if err != nil {
		return nil, err
	}

	return &resp, nil
}

type listPinsResp struct {
	Count   int
	Results []*types.IpfsPinStatusResponse
}

func shouldRetry(err error) bool {
	switch err := err.(type) {
	case net.Error:
		return err.Temporary() || err.Timeout()
	case *httpStatusError:
		return err.StatusCode == 502
	case syscall.Errno:
		if err == syscall.ENETUNREACH {
			return true
		}
	default:
		if uw := errors.Unwrap(err); uw != nil {
			return shouldRetry(uw)
		}

		return false
	}
	return false
}

func (c *EstClient) doRequestRetries(ctx context.Context, method, path string, body, resp interface{}, retries int) (int, error) {
	for i := 0; ; i++ {
		st, err := c.doRequest(ctx, method, path, body, resp)
		if err == nil {
			return st, nil
		}

		if i > retries {
			return st, err
		}

		if !shouldRetry(err) {
			return 0, err
		}

		time.Sleep(time.Second * 2)
	}
}

func (c *EstClient) PinStatuses(ctx context.Context, reqids []string) (map[string]*types.IpfsPinStatusResponse, error) {
	var resp listPinsResp
	_, err := c.doRequestRetries(ctx, "GET", "/pinning/pins?requestid="+strings.Join(reqids, ","), nil, &resp, 5)
	if err != nil {
		return nil, err
	}

	out := make(map[string]*types.IpfsPinStatusResponse)
	for _, res := range resp.Results {
		out[res.RequestID] = res
	}

	return out, nil
}

func (c *EstClient) PinStatusByCid(ctx context.Context, cids []string) (map[string]*types.IpfsPinStatusResponse, error) {
	var resp listPinsResp
	_, err := c.doRequest(ctx, "GET", "/pinning/pins?cid="+strings.Join(cids, ","), nil, &resp)
	if err != nil {
		return nil, err
	}

	out := make(map[string]*types.IpfsPinStatusResponse)
	for _, res := range resp.Results {
		out[res.Pin.CID] = res
	}

	return out, nil

}
