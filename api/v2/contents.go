package api

import (
	"context"
	"fmt"
	"math/rand"
	"net/http"
	"net/http/httputil"
	"net/url"
	"path"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/application-research/estuary/collections"
	"github.com/application-research/estuary/model"
	"github.com/application-research/estuary/util"
	"github.com/application-research/filclient"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/lotus/chain/types"
	"github.com/ipfs/go-blockservice"
	"github.com/ipfs/go-cid"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	ipld "github.com/ipfs/go-ipld-format"
	"github.com/ipfs/go-merkledag"
	"github.com/ipld/go-car"
	"github.com/labstack/echo/v4"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/multiformats/go-multihash"
	"github.com/pkg/errors"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"gorm.io/gorm"

	"golang.org/x/xerrors"
)

type UploadedContent struct {
	Length   int64
	Filename string
	CID      cid.Cid
	Origins  []*peer.AddrInfo
}

type UploadType string

const (
	UploadTypeDefault UploadType = ""
	UploadTypeFile    UploadType = "file"
	UploadTypeCID     UploadType = "cid"
	UploadTypeCar     UploadType = "car"
	UploadTypeUrl     UploadType = "url"
)

type CidType string

const (
	Dir    CidType = "directory"
	File   CidType = "file"
	ColDir string  = "dir"
)

// handleAdd godoc
// @Summary      Add new content
// @Description  This endpoint is used to upload new content.
// @Tags         contents
// @Produce      json
// @Accept       multipart/form-data
// @Param		 type		   query     type	 false	 "Type of content to upload ('car', 'cid', 'file' or 'url'). Defaults to 'file'"
// @Param        car           body      string  false   "Car file to upload"
// @Param        body          body      util.ContentAddIpfsBody  false   "IPFS Body"
// @Param        data          formData  file    false   "File to upload"
// @Param        filename      formData  string  false   "Filename to use for upload"
// @Param        uuid		   query     string  false  "Collection UUID"
// @Param        replication   query     int     false  "Replication value"
// @Param        ignore-dupes  query     string  false  "Ignore Dupes true/false"
// @Param        lazy-provide  query     string  false  "Lazy Provide true/false"
// @Param        dir           query     string  false  "Directory in collection"
// @Success      200           {object}  util.ContentAddResponse
// @Failure      400           {object}  util.HttpError
// @Failure      500           {object}  util.HttpError
// @Router       /contents [post]
func (s *apiV2) handleAdd(c echo.Context, u *util.User) error {
	ctx, span := s.tracer.Start(c.Request().Context(), "handleAdd", trace.WithAttributes(attribute.Int("user", int(u.ID))))
	defer span.End()

	if err := util.ErrorIfContentAddingDisabled(s.isContentAddingDisabled(u)); err != nil {
		return err
	}

	if s.cfg.Content.DisableLocalAdding {
		return s.redirectContentAdding(c, u)
	}

	// replication from query params
	replication := s.cfg.Replication
	replVal := c.QueryParam("replication")
	if replVal != "" {
		parsed, err := strconv.Atoi(replVal)
		if err != nil {
			s.log.Errorf("failed to parse replication value in form data, assuming default for now: %s", err)
		} else {
			replication = parsed
		}
	}

	// collection uuid from query params
	collectionuuid := c.QueryParam("uuid")
	var collection *collections.Collection
	if collectionuuid != "" {
		var srchcollection collections.Collection
		if err := s.db.First(&srchcollection, "uuid = ? and user_id = ?", collectionuuid, u.ID).Error; err != nil {
			return err
		}
		collection = &srchcollection
	}

	path, err := util.ConstructDirectoryPath(c.QueryParam(ColDir))
	if err != nil {
		return err
	}

	uploadType := UploadType(c.QueryParam("type"))
	if uploadType == UploadTypeDefault {
		uploadType = UploadTypeFile
	}

	bsid, bs, err := s.stagingMgr.AllocNew()
	if err != nil {
		return err
	}

	defer func() {
		go func() {
			if err := s.stagingMgr.CleanUp(bsid); err != nil {
				s.log.Errorf("failed to clean up staging blockstore: %s", err)
			}
		}()
	}()

	bserv := blockservice.New(bs, nil)
	dserv := merkledag.NewDAGService(bserv)

	uploadedContent, err := s.loadContentFromRequest(c, ctx, uploadType, bs, dserv)
	if err != nil {
		return err
	}

	// if splitting is disabled and uploaded content size is greater than content size limit
	// reject the upload, as it will only get stuck and deals will never be made for it
	if !u.FlagSplitContent() && uploadedContent.Length > s.cfg.Content.MaxSize {
		return &util.HttpError{
			Code:    http.StatusBadRequest,
			Reason:  util.ERR_CONTENT_SIZE_OVER_LIMIT,
			Details: fmt.Sprintf("content size %d bytes, is over upload size limit of %d bytes, and content splitting is not enabled, please reduce the content size", uploadedContent.Length, s.cfg.Content.MaxSize),
		}
	}

	if c.QueryParam("ignore-dupes") == "true" {
		isDup, err := s.isDupCIDContent(c, uploadedContent.CID, u)
		if err != nil || isDup {
			return err
		}
	}

	// when pinning a CID we need to add a file to handle the special case
	// of calling PinContent on the content manager
	// TODO(gabe): PinContent adds to database tracking. decouple logic from that
	if uploadType == UploadTypeCID {
		makeDeal := true
		cols := []*collections.CollectionRef{
			{
				Path: &path,
			},
		}
		if collection != nil {
			cols[0].Collection = collection.ID
		}

		pinstatus, pinOp, err := s.cm.PinContent(ctx, u.ID, uploadedContent.CID, uploadedContent.Filename, cols, uploadedContent.Origins, 0, nil, makeDeal)
		if err != nil {
			return err
		}
		s.pinMgr.Add(pinOp)
		return c.JSON(http.StatusAccepted, pinstatus)
	}

	content, err := s.cm.AddDatabaseTracking(ctx, u, dserv, uploadedContent.CID, uploadedContent.Filename, replication)
	if err != nil {
		return xerrors.Errorf("encountered problem computing object references: %w", err)
	}
	fullPath := filepath.Join(path, content.Name)

	// create collection if need be
	if collection != nil {
		s.log.Debugf("COLLECTION CREATION: %d, %d", collection.ID, content.ID)
		if err := s.db.Create(&collections.CollectionRef{
			Collection: collection.ID,
			Content:    content.ID,
			Path:       &fullPath,
		}).Error; err != nil {
			s.log.Errorf("failed to add content to requested collection: %s", err)
		}
	}

	if err := util.DumpBlockstoreTo(ctx, s.tracer, bs, s.node.Blockstore); err != nil {
		return xerrors.Errorf("failed to move data from staging to main blockstore: %w", err)
	}

	go func() {
		s.cm.ToCheck(content.ID)
	}()

	if c.QueryParam("lazy-provide") != "true" {
		subctx, cancel := context.WithTimeout(ctx, time.Second*10)
		defer cancel()
		if err := s.node.FullRT.Provide(subctx, uploadedContent.CID, true); err != nil {
			span.RecordError(fmt.Errorf("provide error: %w", err))
			s.log.Errorf("fullrt provide call errored: %s", err)
		}
	}

	go func() {
		if err := s.node.Provider.Provide(uploadedContent.CID); err != nil {
			s.log.Warnf("failed to announce providers: %s", err)
		}
	}()

	return c.JSON(http.StatusOK, &util.ContentAddResponse{
		Cid:                 uploadedContent.CID.String(),
		RetrievalURL:        util.CreateDwebRetrievalURL(uploadedContent.CID.String()),
		EstuaryRetrievalURL: util.CreateEstuaryRetrievalURL(uploadedContent.CID.String()),
		EstuaryId:           content.ID,
		Providers:           s.cm.PinDelegatesForContent(*content),
	})
}

// LoadContentFromRequest reads a POST /contents request and loads the content from it
// It treats every different case of content upload: file (formData, CID, CAR or URL)
// Returns (UploadedContent, contentLen, filename, error)
func (s *apiV2) loadContentFromRequest(c echo.Context, ctx context.Context, uploadType UploadType, bs blockstore.Blockstore, dserv ipld.DAGService) (UploadedContent, error) {
	// for all three upload types
	// get len
	// get filename
	// import file and get cid
	content := UploadedContent{}
	switch uploadType {
	case UploadTypeFile:
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

		// import file and get UploadTypeCID
		fi, err := mpf.Open()
		if err != nil {
			return UploadedContent{}, err
		}
		defer fi.Close()
		nd, err := util.ImportFile(dserv, fi)
		if err != nil {
			return UploadedContent{}, err
		}
		content.CID = nd.Cid()

	case UploadTypeCar:
		// get CAR file from request body
		// import file and get UploadTypeCID
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

	case UploadTypeCID:
		// get UploadTypeCID from POST body
		var params util.ContentAddIpfsBody
		if err := c.Bind(&params); err != nil {
			return UploadedContent{}, err
		}

		// Get filename
		content.Filename = params.Name
		if content.Filename == "" {
			content.Filename = params.Root
		}

		// get UploadTypeCID
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

	case UploadTypeUrl:
		url := string(UploadTypeUrl)
		filename := path.Base(url)
		content.Filename = filename

		resp, err := http.Get(url)
		if err != nil {
			return UploadedContent{}, err
		}
		defer resp.Body.Close()

		nd, err := util.ImportFile(dserv, resp.Body)
		if err != nil {
			return UploadedContent{}, err
		}
		content.CID = nd.Cid()

	default:
		return UploadedContent{}, xerrors.Errorf("invalid type, need 'file', 'cid' or 'car'. Got %s", uploadType)
	}
	return content, nil
}

func (s *apiV2) isDupCIDContent(c echo.Context, rootCID cid.Cid, u *util.User) (bool, error) {
	var count int64
	if err := s.db.Model(util.Content{}).Where("cid = ? and user_id = ?", rootCID.Bytes(), u.ID).Count(&count).Error; err != nil {
		return false, err
	}
	if count > 0 {
		return true, c.JSON(409, map[string]string{"message": fmt.Sprintf("this content is already preserved under cid:%s", rootCID.String())})
	}
	return false, nil
}

func (s *apiV2) isContentAddingDisabled(u *util.User) bool {
	return (s.cfg.Content.DisableGlobalAdding && s.cfg.Content.DisableLocalAdding) || u.StorageDisabled
}

// redirectContentAdding is called when localContentAddingDisabled is true
// it finds available shuttles and adds the desired content in one of them
func (s *apiV2) redirectContentAdding(c echo.Context, u *util.User) error {
	uep, err := s.getPreferredUploadEndpoints(u)
	if err != nil {
		return fmt.Errorf("failed to get preferred upload endpoints: %s", err)
	}
	if len(uep) <= 0 {
		return &util.HttpError{
			Code:    http.StatusBadRequest,
			Reason:  util.ERR_CONTENT_ADDING_DISABLED,
			Details: "uploading content to this node is not allowed at the moment",
		}
	}

	//#nosec G404: ignore weak random number generator
	shURL, err := url.Parse(uep[rand.Intn(len(uep))])
	if err != nil {
		return err
	}
	shURL.Path = ""
	shURL.RawQuery = ""
	shURL.Fragment = ""

	proxy := httputil.NewSingleHostReverseProxy(shURL)
	proxy.ServeHTTP(c.Response(), c.Request())
	return nil
}

func (s *apiV2) getPreferredUploadEndpoints(u *util.User) ([]string, error) {
	// TODO: this should be a lotttttt smarter
	s.cm.ShuttlesLk.Lock()
	defer s.cm.ShuttlesLk.Unlock()
	var shuttles []model.Shuttle
	for hnd, sh := range s.cm.Shuttles {
		if sh.ContentAddingDisabled {
			s.log.Debugf("shuttle %+v content adding is disabled", sh)
			continue
		}

		if sh.Hostname == "" {
			s.log.Debugf("shuttle %+v has empty hostname", sh)
			continue
		}

		var shuttle model.Shuttle
		if err := s.db.First(&shuttle, "handle = ?", hnd).Error; err != nil {
			s.log.Errorf("failed to look up shuttle by handle: %s", err)
			continue
		}

		if !shuttle.Open {
			s.log.Debugf("shuttle %+v is not open, skipping", shuttle)
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

	if !s.cfg.Content.DisableLocalAdding {
		out = append(out, s.cfg.Hostname+"/content/add")
	}
	return out, nil
}

// handleListContent godoc
// @Summary      List all pinned content
// @Description  This endpoint lists all content
// @Tags         contents
// @Produce      json
// @Success      200   {object}  string
// @Failure      400   {object}  util.HttpError
// @Failure      500   {object}  util.HttpError
// @Param        deals   query     bool  false  "If 'true', only list content with deals made"
// @Param        cid   query     string  false  "CID of content to look for"
// @Router       /content [get]
func (s *apiV2) handleListContent(c echo.Context, u *util.User) error {
	if cidStr := c.QueryParam("cid"); cidStr != "" {
		out, err := s.getContentByCid(cidStr)
		if err != nil {
			return err
		}
		return c.JSON(http.StatusOK, out)
	}
	if deals := c.QueryParam("deals"); deals == "true" {
		return s.handleListContentWithDeals(c, u)
	}
	var contents []util.Content
	if err := s.db.Find(&contents, "active and user_id = ?", u.ID).Error; err != nil {
		return err
	}

	return c.JSON(http.StatusOK, contents)
}

type expandedContent struct {
	util.Content
	AggregatedFiles int64 `json:"aggregatedFiles"`
}

// handleListContentWithDeals godoc
// @Summary      Content with deals
// @Description  This endpoint lists all content with deals
// @Tags         contents
// @Produce      json
// @Success      200     {object}  string
// @Failure      400      {object}  util.HttpError
// @Failure      500      {object}  util.HttpError
// @Param        limit   query     int  false  "Limit"
// @Param        offset  query     int  false  "Offset"
// @Router       /content/deals [get]
func (s *apiV2) handleListContentWithDeals(c echo.Context, u *util.User) error {

	var limit = 20
	if limstr := c.QueryParam("limit"); limstr != "" {
		l, err := strconv.Atoi(limstr)
		if err != nil {
			return err
		}
		limit = l
	}

	var offset int
	if offstr := c.QueryParam("offset"); offstr != "" {
		o, err := strconv.Atoi(offstr)
		if err != nil {
			return err
		}
		offset = o
	}

	var contents []util.Content
	err := s.db.Model(&util.Content{}).
		Limit(limit).
		Offset(offset).
		Order("contents.id desc").
		Joins("inner join content_deals on contents.id = content_deals.content").
		Where("contents.active and contents.user_id = ? and not contents.aggregated_in > 0", u.ID).
		Group("contents.id").
		Scan(&contents).Error

	if err != nil {
		return err
	}

	out := make([]expandedContent, 0, len(contents))
	for _, content := range contents {
		ec := expandedContent{
			Content: content,
		}
		if content.Aggregate {
			if err := s.db.Model(util.Content{}).Where("aggregated_in = ?", content.ID).Count(&ec.AggregatedFiles).Error; err != nil {
				return err
			}

		}
		out = append(out, ec)
	}

	return c.JSON(http.StatusOK, out)
}

type getContentResponse struct {
	Content      *util.Content        `json:"content"`
	AggregatedIn *util.Content        `json:"aggregatedIn,omitempty"`
	Selector     string               `json:"selector,omitempty"`
	Deals        []*model.ContentDeal `json:"deals"`
}

func (s *apiV2) getContentByCid(cidStr string) ([]getContentResponse, error) {
	obj, err := cid.Decode(cidStr)
	if err != nil {
		return []getContentResponse{}, errors.Wrapf(err, "invalid cid")
	}

	v0 := cid.Undef
	dec, err := multihash.Decode(obj.Hash())
	if err == nil {
		if dec.Code == multihash.SHA2_256 || dec.Length == 32 {
			v0 = cid.NewCidV0(obj.Hash())
		}
	}
	v1 := cid.NewCidV1(obj.Prefix().Codec, obj.Hash())

	var contents []util.Content
	if err := s.db.Find(&contents, "(cid=? or cid=?) and active", v0.Bytes(), v1.Bytes()).Error; err != nil {
		return []getContentResponse{}, err
	}

	out := make([]getContentResponse, 0)
	for i, content := range contents {
		resp := getContentResponse{
			Content: &contents[i],
		}

		id := content.ID

		if content.AggregatedIn > 0 {
			var aggr util.Content
			if err := s.db.First(&aggr, "id = ?", content.AggregatedIn).Error; err != nil {
				return []getContentResponse{}, err
			}

			resp.AggregatedIn = &aggr

			// no need to early return here, the selector is mostly cosmetic atm
			if selector, err := s.calcSelector(content.AggregatedIn, content.ID); err == nil {
				resp.Selector = selector
			}

			id = content.AggregatedIn
		}

		var deals []*model.ContentDeal
		if err := s.db.Find(&deals, "content = ? and deal_id > 0 and not failed", id).Error; err != nil {
			return []getContentResponse{}, err
		}

		resp.Deals = deals

		out = append(out, resp)
	}

	return out, nil
}

func (s *apiV2) calcSelector(aggregatedIn uint, contentID uint) (string, error) {
	// sort the known content IDs aggregated in a CAR, and use the index in the sorted list
	// to build the CAR sub-selector

	var ordinal uint
	result := s.db.Raw(`SELECT ordinal - 1 FROM (
				SELECT
					id, ROW_NUMBER() OVER ( ORDER BY CAST(id AS TEXT) ) AS ordinal
				FROM contents
				WHERE aggregated_in = ?
			) subq
				WHERE id = ?
			`, aggregatedIn, contentID).Scan(&ordinal)

	if result.Error != nil {
		return "", result.Error
	}

	return fmt.Sprintf("/Links/%d/Hash", ordinal), nil
}

// handleGetContentByCid godoc
// @Summary      Get Content by Cid
// @Description  This endpoint returns the content record associated with a CID
// @Tags         public
// @Produce      json
// @Success      200      {object}  string
// @Failure      400     {object}  util.HttpError
// @Failure      500     {object}  util.HttpError
// @Param        cid  path      string  true  "Cid"
// @Router       /public/by-cid/{cid} [get]
func (s *apiV2) handleGetContentByCid(c echo.Context) error {
	cidStr := c.Param("cid")
	out, err := s.getContentByCid(cidStr)
	if err != nil {
		return err
	}
	return c.JSON(http.StatusOK, out)
}

// handleGetContent godoc
// @Summary      Content
// @Description  This endpoint returns a content by its ID
// @Tags         contents
// @Produce      json
// @Success      200    {object}  string
// @Failure      400      {object}  util.HttpError
// @Failure      500      {object}  util.HttpError
// @Param        contentid   path      int  true  "Content ID"
// @Router       /contents/{contentid} [get]
func (s *apiV2) handleGetContent(c echo.Context, u *util.User) error {
	contID, err := strconv.Atoi(c.Param("contentid"))
	if err != nil {
		return err
	}

	var content util.Content
	if err := s.db.First(&content, "id = ?", contID).Error; err != nil {
		if xerrors.Is(err, gorm.ErrRecordNotFound) {
			return &util.HttpError{
				Code:    http.StatusNotFound,
				Reason:  util.ERR_CONTENT_NOT_FOUND,
				Details: fmt.Sprintf("content: %d was not found", contID),
			}
		}
		return err
	}

	if err := util.IsContentOwner(u.ID, content.UserID); err != nil {
		return err
	}

	return c.JSON(http.StatusOK, content)
}

type EnsureReplicationResponse struct {
	Content util.Content `json:"content"`
}

// handleEnsureReplication godoc
// @Summary      Ensure Replication
// @Description  This endpoint ensures that the content is replicated to the specified number of providers
// @Tags         contents
// @Produce      json
// @Success      200   {object}  string
// @Failure      400    {object}  util.HttpError
// @Failure      500    {object}  util.HttpError
// @Param        cid  path      string  true  "CID"
// @Router       /content/{cid}/ensure-replication [get]
func (s *apiV2) handleEnsureReplication(c echo.Context) error {
	cid, err := cid.Decode(c.Param("cid"))
	if err != nil {
		return err
	}

	var content util.Content
	if err := s.db.Find(&content, "cid = ?", cid.Bytes()).Error; err != nil {
		return err
	}

	fmt.Println("Content: ", content.Cid.CID, cid)

	s.cm.ToCheck(content.ID)
	return c.JSON(http.StatusOK, EnsureReplicationResponse{Content: content})
}

type onChainDealState struct {
	SectorStartEpoch abi.ChainEpoch `json:"sectorStartEpoch"`
	LastUpdatedEpoch abi.ChainEpoch `json:"lastUpdatedEpoch"`
	SlashEpoch       abi.ChainEpoch `json:"slashEpoch"`
}

type dealStatus struct {
	Deal           model.ContentDeal       `json:"deal"`
	TransferStatus *filclient.ChannelState `json:"transfer"`
	OnChainState   *onChainDealState       `json:"onChainState"`
}

type ContentStatusResponse struct {
	Content       util.Content      `json:"content"`
	Deals         []dealStatus      `json:"deals"`
	FailuresCount int64             `json:"failuresCount"`
	Failures      []model.DfeRecord `json:"failures"`
	TotalOutBw    int64             `json:"totalOutBw"`
	Aggregated    []util.Content    `json:"aggregated"`
	Offloaded     bool              `json:"offloaded"` // TODO(gabe): this needs to be set on handleContentStatus, but we're not offloading data as of now
}

// handleContentStatus godoc
// @Summary      Content Status
// @Description  This endpoint returns the status of a content
// @Tags         contents
// @Produce      json
// @Success      200            {object} ContentStatusResponse
// @Failure      400      {object}  util.HttpError
// @Failure      500      {object}  util.HttpError
// @Param        contentid   path      int  true  "Content ID"
// @Router       /contents/{contentid}/status [get]
func (s *apiV2) handleContentStatus(c echo.Context, u *util.User) error {
	ctx := c.Request().Context()
	contID, err := strconv.Atoi(c.Param("contentid"))
	if err != nil {
		return err
	}

	var content util.Content
	if err := s.db.First(&content, "id = ?", contID).Error; err != nil {
		if xerrors.Is(err, gorm.ErrRecordNotFound) {
			return &util.HttpError{
				Code:    http.StatusNotFound,
				Reason:  util.ERR_CONTENT_NOT_FOUND,
				Details: fmt.Sprintf("content: %d was not found", contID),
			}
		}
		return err
	}

	if err := util.IsContentOwner(u.ID, content.UserID); err != nil {
		return err
	}

	var deals []model.ContentDeal
	if err := s.db.Find(&deals, "content = ?", content.ID).Error; err != nil {
		return err
	}

	ds := make([]dealStatus, len(deals))
	var wg sync.WaitGroup
	for i, d := range deals {
		dl := d
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			d := deals[i]
			dstatus := dealStatus{
				Deal: d,
			}

			chanst, err := s.cm.GetTransferStatus(ctx, &dl, content.Cid.CID, content.Location)
			if err != nil {
				s.log.Errorf("failed to get transfer status: %s", err)
				// the UI needs to display a transfer state even for intermittent errors
				chanst = &filclient.ChannelState{
					StatusStr: "Error",
				}
			}

			// the transfer state is yet to be been announced - the UI needs to display a transfer state
			if chanst == nil && d.DTChan == "" {
				chanst = &filclient.ChannelState{
					StatusStr: "Initializing",
				}
			}

			dstatus.TransferStatus = chanst

			if d.DealID > 0 {
				markDeal, err := s.api.StateMarketStorageDeal(ctx, abi.DealID(d.DealID), types.EmptyTSK)
				if err != nil {
					s.log.Warnw("failed to get deal info from market actor", "dealID", d.DealID, "error", err)
				} else {
					dstatus.OnChainState = &onChainDealState{
						SectorStartEpoch: markDeal.State.SectorStartEpoch,
						LastUpdatedEpoch: markDeal.State.LastUpdatedEpoch,
						SlashEpoch:       markDeal.State.SlashEpoch,
					}
				}
			}
			ds[i] = dstatus
		}(i)
	}

	wg.Wait()

	sort.Slice(ds, func(i, j int) bool {
		return ds[i].Deal.CreatedAt.Before(ds[j].Deal.CreatedAt)
	})

	var failCount int64
	var errs []model.DfeRecord
	if err := s.db.Model(&model.DfeRecord{}).Where("content = ?", content.ID).Find(&errs).Count(&failCount).Error; err != nil {
		return err
	}

	var bw int64
	if err := s.db.Model(util.ObjRef{}).
		Select("SUM(size * reads)").
		Where("obj_refs.content = ?", content.ID).
		Joins("left join objects on obj_refs.object = objects.id").
		Scan(&bw).Error; err != nil {
		return err
	}

	var sub []util.Content
	if err := s.db.Find(&sub, "aggregated_in = ?", content.ID).Error; err != nil {
		return err
	}

	return c.JSON(http.StatusOK, ContentStatusResponse{
		Content:       content,
		Deals:         ds,
		FailuresCount: failCount,
		Failures:      errs,
		TotalOutBw:    bw,
		Aggregated:    sub,
	})
}
