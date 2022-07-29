package util

import (
	"context"
	"path/filepath"
	"strings"

	"github.com/ipfs/go-cid"
	format "github.com/ipfs/go-ipld-format"
	ipld "github.com/ipfs/go-ipld-format"
	"github.com/ipfs/go-merkledag"
	unixfs "github.com/ipfs/go-unixfs"
)

const DefaultContentSizeLimit = 34_000_000_000
const ContentLocationLocal = "local"

type ContentType int64

const (
	Unknown ContentType = iota
	File
	Directory
)

type ContentInCollection struct {
	CollectionID  string `json:"coluuid"`
	CollectionDir string `json:"dir"`
}

type ContentAddIpfsBody struct {
	ContentInCollection

	Root     string   `json:"root"`
	Filename string   `json:"filename"`
	Peers    []string `json:"peers"`
}

type ContentAddResponse struct {
	Cid          string   `json:"cid"`
	RetrievalURL string   `json:"RetrievalURL"`
	EstuaryId    uint     `json:"estuaryId"`
	Providers    []string `json:"providers"`
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

func removeEmptyStrings(strList []string) []string {
	var strListNoEmpty []string
	for _, str := range strList {
		if str != "" {
			strListNoEmpty = append(strListNoEmpty, str)
		}
	}
	return strListNoEmpty
}

// DirsFromPath splits a path into a list of directories
func DirsFromPath(collectionPath string, filename string) ([]string, error) {
	collectionPath = filepath.Clean(collectionPath)
	if dir, file := filepath.Split(collectionPath); file == filename { // path ends in the filename
		collectionPath = dir // only keep the part with dirs
	}
	dirs := strings.Split(collectionPath, "/")
	dirs = removeEmptyStrings(dirs)
	return dirs, nil
}

func EnsurePathIsLinked(dirs []string, rootNode *merkledag.ProtoNode, ds format.DAGService) (*merkledag.ProtoNode, error) {
	lookupNode := rootNode
	for _, dir := range dirs {
		// see if dir already created on DAG
		_, err := lookupNode.GetNodeLink(dir)
		if err == merkledag.ErrLinkNotFound { // if not create and link it
			dirNode := unixfs.EmptyDirNode()
			if err = lookupNode.AddNodeLink(dir, dirNode); err != nil {
				return nil, err
			}
		}
		ctx := context.Background()
		lookupNode, err = lookupNode.GetLinkedProtoNode(ctx, ds, dir)
		if err != nil {
			return nil, err
		}
	}
	return lookupNode, nil
}

func createRetrievalURL(cid string) string {
       return fmt.Sprintf("https://dweb.link/ipfs/%s", cid)
}
