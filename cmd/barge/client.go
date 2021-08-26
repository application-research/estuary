package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"os"
	"time"

	"github.com/application-research/estuary/types"
	util "github.com/application-research/estuary/util"
	"github.com/ipfs/go-cid"
)

type EstClient struct {
	Host    string
	Shuttle string
	Tok     string
}

func (c *EstClient) doRequest(ctx context.Context, method string, path string, body interface{}, resp interface{}) (int, error) {
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

	if !(r.StatusCode >= 200 && r.StatusCode < 300) {
		return r.StatusCode, fmt.Errorf("received non-200 status: %s", r.Status)
	}

	defer r.Body.Close()

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

func (c *EstClient) AddFile(fpath, name string) (*util.AddFileResponse, error) {
	r, w := io.Pipe()
	fi, err := os.Open(fpath)
	if err != nil {
		return nil, err
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

		part, err := mw.CreateFormFile("data", name)
		if err != nil {
			outerr = err
			return
		}

		_, err = io.Copy(part, fi)
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

	var rbody util.AddFileResponse
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

func (c *EstClient) PinAdd(ctx context.Context, root cid.Cid, name string, origins []string, meta map[string]interface{}) (*types.IpfsPinStatus, error) {
	p := &types.IpfsPin{
		Cid:     root.String(),
		Name:    name,
		Origins: origins,
		Meta:    meta,
	}

	var resp types.IpfsPinStatus
	_, err := c.doRequest(ctx, "POST", "/pinning/pins", p, &resp)
	if err != nil {
		return nil, err
	}

	return &resp, nil
}

func (c *EstClient) PinStatus(ctx context.Context, reqid string) (*types.IpfsPinStatus, error) {
	var resp types.IpfsPinStatus
	_, err := c.doRequest(ctx, "GET", "/pinning/pins/"+reqid, nil, &resp)
	if err != nil {
		return nil, err
	}

	return &resp, nil
}
