package util

import (
	"context"

	"github.com/ipfs/go-cid"
	ipld "github.com/ipfs/go-ipld-format"
)

type ContentType int64

const (
	Unknown ContentType = iota
	File
	Directory
)

type ContentInCollection struct {
	CollectionID   string `json:"coluuid"`
	CollectionPath string `json:"colpath"`
}

type ContentAddIpfsBody struct {
	ContentInCollection

	Root     string   `json:"root"`
	Filename string   `json:"filename"`
	Peers    []string `json:"peers"`
}

type ContentAddResponse struct {
	Cid       string   `json:"cid"`
	EstuaryId uint     `json:"estuaryId"`
	Providers []string `json:"providers"`
}

type ContentCreateBody struct {
	ContentInCollection

	Root     string      `json:"root"`
	Name     string      `json:"name"`
	Location string      `json:"location"`
	Type     ContentType `json:"type"`
}

type ContentCreateResponse struct {
	ID uint `json:"id"`
}

// FindCIDType checks if a pinned CID (root) is a file, a dir or unknown
// Returns dbmgr.File or dbmgr.Directory on success
// Returns dbmgr.Unknown otherwise
func FindCIDType(ctx context.Context, root cid.Cid, dserv ipld.NodeGetter) (contentType ContentType) {
	contentType = Unknown
	nilCID := cid.Cid{}
	if root == nilCID || dserv == nil {
		return
	}

	nd, err := dserv.Get(ctx, root)
	if err != nil {
		return
	}

	contentType = File
	fsNode, err := TryExtractFSNode(nd)
	if err != nil {
		return
	}

	if fsNode.IsDir() {
		contentType = Directory
	}
	return
}
