package util

import (
	"context"
	"fmt"
	"net/http"
	"path/filepath"
	"strings"
	"time"

	"github.com/ipfs/go-cid"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	format "github.com/ipfs/go-ipld-format"
	ipld "github.com/ipfs/go-ipld-format"
	"github.com/ipfs/go-merkledag"
	"github.com/ipfs/go-unixfs"
	"github.com/ipld/go-car"
	"github.com/labstack/echo/v4"
	"github.com/libp2p/go-libp2p-core/peer"
	"golang.org/x/xerrors"
	"gorm.io/gorm"
)

type ContentType int64

const (
	Unknown ContentType = iota
	File
	Directory
)

type ContentInBucket struct {
	BucketID  string `json:"coluuid"`
	BucketDir string `json:"dir"`
}

type ContentAddIpfsBody struct {
	ContentInBucket
	Root  string   `json:"root"`
	Name  string   `json:"filename"`
	Peers []string `json:"peers"`
}

type ContentAddResponse struct {
	Cid                 string   `json:"cid"`
	RetrievalURL        string   `json:"retrieval_url"`
	EstuaryRetrievalURL string   `json:"estuary_retrieval_url"`
	EstuaryId           uint     `json:"estuaryId"`
	Providers           []string `json:"providers"`
}

type ContentCreateBody struct {
	ContentInBucket

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
	CreatedAt time.Time      `json:"-"`
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

type UploadedContent struct {
	Length   int64
	Filename string
	CID      cid.Cid
	Origins  []*peer.AddrInfo
}

// LoadContentFromRequest reads a POST /contents request and loads the content from it
// It treats every different case of content upload: file (formData, CID or CAR)
// Returns (UploadedContent, contentLen, filename, error)
func LoadContentFromRequest(c echo.Context, ctx context.Context, uploadType string, bs blockstore.Blockstore, dserv ipld.DAGService) (UploadedContent, error) {
	// for all three upload types
	// get len
	// get filename
	// import file and get cid
	content := UploadedContent{}
	switch uploadType {
	case "file":
		// get file from formData
		form, err := c.MultipartForm()
		if err != nil {
			return UploadedContent{}, xerrors.Errorf("invalid formData for 'file' upload option: %w", err)
		}
		defer form.RemoveAll()
		mpf, err := c.FormFile("data")
		if err != nil {
			return UploadedContent{}, xerrors.Errorf("invalid formData for 'file' upload option: %w", err)
		}

		// Get len
		content.Length = mpf.Size

		// Get filename
		content.Filename = mpf.Filename
		if fvname := c.FormValue("filename"); fvname != "" {
			content.Filename = fvname
		}

		// import file and get CID
		fi, err := mpf.Open()
		if err != nil {
			return UploadedContent{}, err
		}
		defer fi.Close()
		nd, err := ImportFile(dserv, fi)
		if err != nil {
			return UploadedContent{}, err
		}
		content.CID = nd.Cid()

	case "car":
		// get CAR file from request body
		// import file and get CID
		defer c.Request().Body.Close()
		header, err := car.LoadCar(ctx, bs, c.Request().Body)
		if err != nil {
			return UploadedContent{}, err
		}
		if len(header.Roots) != 1 {
			// if someone wants this feature, let me know
			return UploadedContent{}, xerrors.Errorf("cannot handle uploading car files with multiple roots")
		}
		content.CID = header.Roots[0]

		// Get filename
		// TODO: how to specify filename?
		content.Filename = content.CID.String()
		if qpname := c.QueryParam("filename"); qpname != "" {
			content.Filename = qpname
		}

		// Get len
		// TODO: uncomment and fix this
		// 	bdWriter := &bytes.Buffer{}
		// 	bdReader := io.TeeReader(c.Request().Body, bdWriter)

		// 	bdSize, err := io.Copy(ioutil.Discard, bdReader)
		// 	if err != nil {
		// 		return err
		// 	}

		// 	if bdSize > util.MaxDealContentSize {
		// 		return &util.HttpError{
		// 			Code:    http.StatusBadRequest,
		// 			Reason:  util.ERR_CONTENT_SIZE_OVER_LIMIT,
		// 			Details: fmt.Sprintf("content size %d bytes, is over upload size of limit %d bytes, and content splitting is not enabled, please reduce the content size", bdSize, util.MaxDealContentSize),
		// 		}
		// 	}

		// 	c.Request().Body = ioutil.NopCloser(bdWriter)
		content.Length = 0 // zero since we're not checking the length of this content so it doesn't break the limit check (bad)

	case "cid":
		// get CID from POST body
		var params ContentAddIpfsBody
		if err := c.Bind(&params); err != nil {
			return UploadedContent{}, err
		}

		// Get filename
		content.Filename = params.Name
		if content.Filename == "" {
			content.Filename = params.Root
		}

		// get CID
		cid, err := cid.Decode(params.Root)
		if err != nil {
			return UploadedContent{}, err
		}
		content.CID = cid

		// Can't get len (will be gotten during pinning)
		content.Length = 0

		// origins are needed for pinning later on
		var origins []*peer.AddrInfo
		for _, p := range params.Peers {
			ai, err := peer.AddrInfoFromString(p)
			if err != nil {
				return UploadedContent{}, err
			}
			origins = append(origins, ai)
		}
		content.Origins = origins

	default:
		return UploadedContent{}, xerrors.Errorf("invalid type, need 'file', 'cid' or 'car'. Got %s", uploadType)
	}
	return content, nil
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
func DirsFromPath(bucketPath string, filename string) ([]string, error) {
	bucketPath = filepath.Clean(bucketPath)
	if dir, file := filepath.Split(bucketPath); file == filename { // path ends in the filename
		bucketPath = dir // only keep the part with dirs
	}
	dirs := strings.Split(bucketPath, "/")
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
