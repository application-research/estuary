package util

import (
	"context"
	"fmt"
	"net/http"
	"path/filepath"
	"strings"
	"time"

	"github.com/ipfs/go-cid"
	format "github.com/ipfs/go-ipld-format"
	ipld "github.com/ipfs/go-ipld-format"
	"github.com/ipfs/go-merkledag"
	"github.com/ipfs/go-unixfs"
	"golang.org/x/xerrors"
	"gorm.io/gorm"
)

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

type ContentAddResponse struct {
	Cid                 string   `json:"cid"`
	RetrievalURL        string   `json:"retrieval_url"`
	EstuaryRetrievalURL string   `json:"estuary_retrieval_url"`
	EstuaryId           uint     `json:"estuaryId"`
	Providers           []string `json:"providers"`
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

type Content struct {
	ID        uint           `gorm:"primarykey" json:"id"`
	CreatedAt time.Time      `json:"createdAt"`
	UpdatedAt time.Time      `json:"updatedAt"`
	DeletedAt gorm.DeletedAt `gorm:"index" json:"-"`

	Cid         DbCID       `json:"cid"`
	Name        string      `json:"name"`
	UserID      uint        `json:"userId" gorm:"index"`
	Description string      `json:"description"`
	Size        int64       `json:"size"`
	Type        ContentType `json:"type"`
	Active      bool        `json:"active"`
	Offloaded   bool        `json:"offloaded"`
	Replication int         `json:"replication"`

	// TODO: shift most of the 'state' booleans in here into a single state
	// field, should make reasoning about things much simpler
	AggregatedIn uint `json:"aggregatedIn" gorm:"index:,option:CONCURRENTLY"`
	Aggregate    bool `json:"aggregate"`

	Pinning bool   `json:"pinning"`
	PinMeta string `json:"pinMeta"`
	Replace bool   `json:"replace" gorm:"default:0"`
	Origins string `json:"origins"`

	Failed bool `json:"failed"`

	Location string `json:"location"`
	// TODO: shift location tracking to just use the ID of the shuttle
	// Also move towards recording content movement intentions in the database,
	// making that process more resilient to failures
	// LocID     uint   `json:"locID"`
	// LocIntent uint   `json:"locIntent"`

	// If set, this content is part of a split dag.
	// In such a case, the 'root' content should be advertised on the dht, but
	// not have deals made for it, and the children should have deals made for
	// them (unlike with aggregates)
	DagSplit  bool `json:"dagSplit"`
	SplitFrom uint `json:"splitFrom"`

	PinningStatus string `json:"pinningStatus" gorm:"-"`
	DealStatus    string `json:"dealStatus" gorm:"-"`
}

type ContentWithPath struct {
	Content
	Path string `json:"path"`
}

type Object struct {
	ID         uint  `gorm:"primarykey"`
	Cid        DbCID `gorm:"index"`
	Size       int
	Reads      int
	LastAccess time.Time
}

type ObjRef struct {
	ID        uint `gorm:"primarykey"`
	Content   uint `gorm:"index:,option:CONCURRENTLY"`
	Object    uint `gorm:"index:,option:CONCURRENTLY"`
	Offloaded uint
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

func CreateDwebRetrievalURL(cid string) string {
	return fmt.Sprintf("https://dweb.link/ipfs/%s", cid)
}

func CreateEstuaryRetrievalURL(cid string) string {
	return fmt.Sprintf("https://api.estuary.tech/gw/ipfs/%s", cid)
}

func GetContent(contentid string, db *gorm.DB, u *User) (Content, error) {
	var content Content
	if err := db.First(&content, "id = ?", contentid).Error; err != nil {
		if xerrors.Is(err, gorm.ErrRecordNotFound) {
			return Content{}, &HttpError{
				Code:    http.StatusNotFound,
				Reason:  ERR_CONTENT_NOT_FOUND,
				Details: fmt.Sprintf("content with ID(%s) was not found", contentid),
			}
		}
	}
	if err := IsContentOwner(u.ID, content.UserID); err != nil {
		return Content{}, err
	}
	return content, nil
}
