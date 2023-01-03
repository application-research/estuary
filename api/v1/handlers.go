package api

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"net/http"
	"net/http/httputil"
	httpprof "net/http/pprof"
	"net/url"
	"path/filepath"
	"runtime/pprof"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	logging "github.com/ipfs/go-log/v2"

	"golang.org/x/crypto/bcrypt"

	"github.com/application-research/estuary/collections"
	"github.com/application-research/estuary/constants"
	"github.com/application-research/estuary/miner"
	"github.com/application-research/estuary/model"
	"github.com/application-research/estuary/node/modules/peering"
	"github.com/libp2p/go-libp2p/core/network"

	"github.com/application-research/estuary/autoretrieve"
	pinningtypes "github.com/application-research/estuary/pinner/types"
	"github.com/application-research/estuary/util"
	"github.com/application-research/estuary/util/gateway"
	"github.com/application-research/filclient"
	"github.com/filecoin-project/go-address"
	datatransfer "github.com/filecoin-project/go-data-transfer"
	"github.com/filecoin-project/go-padreader"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/lotus/chain/types"
	"github.com/filecoin-project/specs-actors/v6/actors/builtin/market"
	"github.com/google/uuid"
	"github.com/ipfs/go-blockservice"
	"github.com/ipfs/go-cid"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	exchange "github.com/ipfs/go-ipfs-exchange-interface"
	offline "github.com/ipfs/go-ipfs-exchange-offline"
	ipld "github.com/ipfs/go-ipld-format"
	"github.com/ipfs/go-merkledag"
	"github.com/ipfs/go-unixfs"
	uio "github.com/ipfs/go-unixfs/io"
	"github.com/ipld/go-car"
	"github.com/labstack/echo/v4"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"
	"github.com/pkg/errors"
	"golang.org/x/sys/unix"
	"golang.org/x/xerrors"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	_ "github.com/application-research/estuary/docs"
)

func serveCpuProfile(c echo.Context) error {
	if err := pprof.StartCPUProfile(c.Response()); err != nil {
		return err
	}

	defer pprof.StopCPUProfile()

	select {
	case <-c.Request().Context().Done():
		return c.Request().Context().Err()
	case <-time.After(time.Second * 30):
	}

	return nil
}

func serveProfile(c echo.Context) error {
	httpprof.Handler(c.Param("prof")).ServeHTTP(c.Response().Writer, c.Request())
	return nil
}

type statsResp struct {
	ID              uint                       `json:"id"`
	Cid             cid.Cid                    `json:"cid"`
	Filename        string                     `json:"name"`
	Size            int64                      `json:"size"`
	CreatedAt       time.Time                  `json:"createdAt"`
	BWUsed          int64                      `json:"bwUsed"`
	TotalRequests   int64                      `json:"totalRequests"`
	Offloaded       bool                       `json:"offloaded"`
	AggregatedFiles int64                      `json:"aggregatedFiles"`
	PinningStatus   pinningtypes.PinningStatus `json:"pinningStatus"`
}

func withUser(f func(echo.Context, *util.User) error) func(echo.Context) error {
	return func(c echo.Context) error {
		u, ok := c.Get("user").(*util.User)
		if !ok {
			return &util.HttpError{
				Code:    http.StatusUnauthorized,
				Reason:  util.ERR_INVALID_AUTH,
				Details: "endpoint not called with proper authentication",
			}
		}
		return f(c, u)
	}
}

// handleStats godoc
// @Summary      Get content statistics
// @Description  This endpoint is used to get content statistics. Every content stored in the network (estuary) is tracked by a unique ID which can be used to get information about the content. This endpoint will allow the consumer to get the collected stats of a content
// @Tags         content
// @Param        limit   query  string  true  "limit"
// @Param        offset  query  string  true  "offset"
// @Produce      json
// @Success      200          {object}  string
// @Failure      400      {object}  util.HttpError
// @Failure      500      {object}  util.HttpError
// @Router       /content/stats [get]
func (s *apiV1) handleStats(c echo.Context, u *util.User) error {
	limit, offset, err := s.getLimitAndOffset(c, 500, 0)
	if err != nil {
		return err
	}

	var contents []util.Content
	if err := s.DB.Limit(limit).Offset(offset).Order("created_at desc").Find(&contents, "user_id = ? and not aggregate", u.ID).Error; err != nil {
		return err
	}

	out := make([]statsResp, 0, len(contents))
	for _, c := range contents {
		st := statsResp{
			ID:            c.ID,
			Cid:           c.Cid.CID,
			Filename:      c.Name,
			Size:          c.Size,
			CreatedAt:     c.CreatedAt,
			PinningStatus: pinningtypes.GetContentPinningStatus(c),
		}
		out = append(out, st)
	}

	return c.JSON(http.StatusOK, out)
}

// cacheKey returns a key based on the request being made, the user associated to it, and optional tags
// this key is used when calling Get or Add from a cache
func cacheKey(c echo.Context, u *util.User, tags ...string) string {
	paramNames := strings.Join(c.ParamNames(), ",")
	paramVals := strings.Join(c.ParamValues(), ",")
	tagsString := strings.Join(tags, ",")
	if u != nil {
		return fmt.Sprintf("URL=%s ParamNames=%s ParamVals=%s user=%d tags=%s", c.Request().URL, paramNames, paramVals, u.ID, tagsString)
	} else {
		return fmt.Sprintf("URL=%s ParamNames=%s ParamVals=%s tags=%s", c.Request().URL, paramNames, paramVals, tagsString)
	}
}

// handleGetUserContents godoc
// @Summary      Get user contents
// @Description  This endpoint is used to get user contents
// @Tags         content
// @Param        limit   query  string  true  "limit"
// @Param        offset  query  string  true  "offset"
// @Produce      json
// @Success      200          {object}  string
// @Failure      400      {object}  util.HttpError
// @Failure      500      {object}  util.HttpError
// @Router       /content/contents [get]
func (s *apiV1) handleGetUserContents(c echo.Context, u *util.User) error {
	limit, offset, err := s.getLimitAndOffset(c, 500, 0)
	if err != nil {
		return err
	}

	var contents []util.Content
	if err := s.DB.Limit(limit).Offset(offset).Order("created_at desc").Find(&contents, "user_id = ? and not aggregate", u.ID).Error; err != nil {
		return err
	}

	for i, c := range contents {
		contents[i].PinningStatus = string(pinningtypes.GetContentPinningStatus(c))
	}

	return c.JSON(http.StatusOK, contents)
}

// handlePeeringPeersAdd godoc
// @Summary      Add peers on Peering Service
// @Description  This endpoint can be used to add a Peer from the Peering Service
// @Tags         admin
// @Produce      json
// @Param        req           body      []peering.PeeringPeer true   "Peering Peer array"
// @Success      200     {object}  string
// @Failure      400      {object}  util.HttpError
// @Failure      500      {object}  util.HttpError
// @Router       /admin/peering/peers [post]
func (s *apiV1) handlePeeringPeersAdd(c echo.Context) error {
	var params []peering.PeeringPeer
	if err := c.Bind(&params); err != nil {
		return &util.HttpError{
			Code:   http.StatusBadRequest,
			Reason: util.ERR_INVALID_INPUT,
		}
	}

	//	validate the IDs and Addrs here
	var validPeersAddInfo []peer.AddrInfo
	for _, peerParam := range params {
		//	validate the PeerID
		peerParamId, err := peer.Decode(peerParam.ID)
		if err != nil {
			return &util.HttpError{
				Code:    http.StatusBadRequest,
				Reason:  util.ERR_INVALID_INPUT,
				Details: "Adding Peer(s) on Peering failed, the peerID is invalid: " + peerParam.ID,
			}
		}

		//	validate the Addrs for each PeerID
		var multiAddrs []multiaddr.Multiaddr
		for _, addr := range peerParam.Addrs {
			a, err := multiaddr.NewMultiaddr(addr)
			if err != nil {
				return &util.HttpError{
					Code:    http.StatusBadRequest,
					Reason:  util.ERR_INVALID_INPUT,
					Details: "Adding Peer(s) on Peering failed, the addr is invalid: " + addr,
				}
			}
			multiAddrs = append(multiAddrs, a)
		}

		//	Only add it here if all is valid.
		validPeersAddInfo = append(validPeersAddInfo,
			peer.AddrInfo{
				ID:    peerParamId,
				Addrs: multiAddrs,
			})
	}

	//	if no error return from the validation, go thru the validPeers here and add each of them
	//	to Peering.
	for _, validPeerAddInfo := range validPeersAddInfo {
		s.Node.Peering.AddPeer(validPeerAddInfo)
	}
	return c.JSON(http.StatusOK, util.PeeringPeerAddMessage{Message: "Added the following Peers on Peering", PeersAdd: params})
}

// handlePeeringPeersRemove godoc
// @Summary      Remove peers on Peering Service
// @Description  This endpoint can be used to remove a Peer from the Peering Service
// @Tags         admin
// @Produce      json
// @Success      200      {object}  string
// @Failure      400     {object}  util.HttpError
// @Failure      500     {object}  util.HttpError
// @Param        peerIds  body      []peer.ID  true  "Peer ids"
// @Router       /admin/peering/peers [delete]
func (s *apiV1) handlePeeringPeersRemove(c echo.Context) error {
	var params []peer.ID

	if err := c.Bind(&params); err != nil {
		return &util.HttpError{
			Code:   http.StatusBadRequest,
			Reason: util.ERR_INVALID_INPUT,
		}
	}

	for _, peerId := range params {
		s.Node.Peering.RemovePeer(peerId)
	}
	return c.JSON(http.StatusOK, util.PeeringPeerRemoveMessage{Message: "Removed the following Peers from Peering", PeersRemove: params})
}

// handlePeeringPeersList godoc
// @Summary      List all Peering peers
// @Description  This endpoint can be used to list all peers on Peering Service
// @Tags         admin
// @Produce      json
// @Success      200      {object}  string
// @Failure      400   {object}  util.HttpError
// @Failure      500   {object}  util.HttpError
// @Router       /admin/peering/peers [get]
func (s *apiV1) handlePeeringPeersList(c echo.Context) error {
	var connectionCheck []peering.PeeringPeer
	for _, peerAddrInfo := range s.Node.Peering.ListPeers() {

		var peerAddrInfoAddrsStr []string
		for _, addrInfo := range peerAddrInfo.Addrs {
			peerAddrInfoAddrsStr = append(peerAddrInfoAddrsStr, addrInfo.String())
		}
		connectionCheck = append(connectionCheck, peering.PeeringPeer{
			ID:        peerAddrInfo.ID.Pretty(),
			Addrs:     peerAddrInfoAddrsStr,
			Connected: s.Node.Host.Network().Connectedness(peerAddrInfo.ID) == network.Connected,
		})
	}
	return c.JSON(http.StatusOK, connectionCheck)
}

// handlePeeringStart godoc
// @Summary      Start Peering
// @Description  This endpoint can be used to start the Peering Service
// @Tags         admin
// @Produce      json
// @Success      200     {object}  string
// @Failure      400    {object}  util.HttpError
// @Failure      500    {object}  util.HttpError
// @Router       /admin/peering/start [post]
func (s *apiV1) handlePeeringStart(c echo.Context) error {
	err := s.Node.Peering.Start()
	if err != nil {
		return &util.HttpError{
			Code:   http.StatusBadRequest,
			Reason: util.ERR_PEERING_PEERS_START_ERROR,
		}
	}
	return c.JSON(http.StatusOK, util.GenericResponse{Message: "Peering Started."})
}

// handlePeeringStop godoc
// @Summary      Stop Peering
// @Description  This endpoint can be used to stop the Peering Service
// @Tags         admin
// @Produce      json
// @Success      200   {object}  string
// @Failure      400    {object}  util.HttpError
// @Failure      500    {object}  util.HttpError
// @Router       /admin/peering/stop [post]
func (s *apiV1) handlePeeringStop(c echo.Context) error {
	err := s.Node.Peering.Stop()
	if err != nil {
		return &util.HttpError{
			Code:   http.StatusBadRequest,
			Reason: util.ERR_PEERING_PEERS_STOP_ERROR,
		}
	}
	return c.JSON(http.StatusOK, util.GenericResponse{Message: "Peering Stopped."})
}

// handlePeeringStatus godoc
// @Summary      Check Peering Status
// @Description  This endpoint can be used to check the Peering status
// @Tags         admin
// @Produce      json
// @Success      200    {object}  string
// @Failure      400            {object}  util.HttpError
// @Failure      500            {object}  util.HttpError
// @Router       /admin/peering/status [get]
func (s *apiV1) handlePeeringStatus(c echo.Context) error {
	type StateResponse struct {
		State string `json:"state"`
	}
	return c.JSON(http.StatusOK, StateResponse{State: ""})
}

// handleAddIpfs godoc
// @Summary      Add IPFS object
// @Description  This endpoint is used to add an IPFS object to the network. The object can be a file or a directory.
// @Tags         content
// @Produce      json
// @Success      200           {object}  string
// @Failure      400           {object}  util.HttpError
// @Failure      500           {object}  util.HttpError
// @Param        body          body      types.IpfsPin  true   "IPFS Body"
// @Param        ignore-dupes  query     string                   false  "Ignore Dupes"
// @Router       /content/add-ipfs [post]
func (s *apiV1) handleAddIpfs(c echo.Context, u *util.User) error {
	return s.handleAddPin(c, u)
}

// handleAddCar godoc
// @Summary      Add Car object
// @Description  This endpoint is used to add a car object to the network. The object can be a file or a directory.
// @Tags         content
// @Produce      json
// @Success      200           {object}  util.ContentAddResponse
// @Failure      400           {object}  util.HttpError
// @Failure      500           {object}  util.HttpError
// @Param        body          body      string  true   "Car"
// @Param        ignore-dupes  query     string  false  "Ignore Dupes"
// @Param        filename      query     string  false  "Filename"
// @Router       /content/add-car [post]
func (s *apiV1) handleAddCar(c echo.Context, u *util.User) error {
	ctx := c.Request().Context()

	if err := util.ErrorIfContentAddingDisabled(s.isContentAddingDisabled(u)); err != nil {
		return err
	}

	if s.cfg.Content.DisableLocalAdding {
		return s.redirectContentAdding(c, u)
	}

	// if splitting is disabled and uploaded content size is greater than content size limit
	// reject the upload, as it will only get stuck and deals will never be made for it
	// if !u.FlagSplitContent() {
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
	// }

	bsid, sbs, err := s.StagingMgr.AllocNew()
	if err != nil {
		return err
	}

	defer func() {
		go func() {
			if err := s.StagingMgr.CleanUp(bsid); err != nil {
				s.log.Errorf("failed to clean up staging blockstore: %s", err)
			}
		}()
	}()

	defer c.Request().Body.Close()
	header, err := s.loadCar(ctx, sbs, c.Request().Body)
	if err != nil {
		return err
	}

	if len(header.Roots) != 1 {
		// if someone wants this feature, let me know
		return c.JSON(400, map[string]string{"error": "cannot handle uploading car files with multiple roots"})
	}
	rootCID := header.Roots[0]

	if c.QueryParam("ignore-dupes") == "true" {
		isDup, err := s.isDupCIDContent(c, rootCID, u)
		if err != nil || isDup {
			return err
		}
	}

	// TODO: how to specify filename?
	filename := rootCID.String()
	if qpname := c.QueryParam("filename"); qpname != "" {
		filename = qpname
	}

	bserv := blockservice.New(sbs, nil)
	dserv := merkledag.NewDAGService(bserv)

	cont, err := s.CM.AddDatabaseTracking(ctx, u, dserv, rootCID, filename, s.cfg.Replication)
	if err != nil {
		return err
	}

	if err := util.DumpBlockstoreTo(ctx, s.tracer, sbs, s.Node.Blockstore); err != nil {
		return xerrors.Errorf("failed to move data from staging to main blockstore: %w", err)
	}

	go func() {
		if err := s.Node.Provider.Provide(rootCID); err != nil {
			s.log.Warnf("failed to announce providers: %s", err)
		}
	}()

	return c.JSON(http.StatusOK, &util.ContentAddResponse{
		Cid:                 rootCID.String(),
		RetrievalURL:        util.CreateDwebRetrievalURL(rootCID.String()),
		EstuaryRetrievalURL: util.CreateEstuaryRetrievalURL(rootCID.String()),
		EstuaryId:           cont.ID,
		Providers:           s.CM.PinDelegatesForContent(*cont),
	})
}

func (s *apiV1) loadCar(ctx context.Context, bs blockstore.Blockstore, r io.Reader) (*car.CarHeader, error) {
	_, span := s.tracer.Start(ctx, "loadCar")
	defer span.End()

	return car.LoadCar(ctx, bs, r)
}

// handleAdd godoc
// @Summary      Add new content
// @Description  This endpoint is used to upload new content.
// @Tags         content
// @Produce      json
// @Accept       multipart/form-data
// @Param        data          formData  file    true   "File to upload"
// @Param        filename      formData  string  false  "Filename to use for upload"
// @Param        coluuid       query     string  false  "Collection UUID"
// @Param        replication   query     int     false  "Replication value"
// @Param        ignore-dupes  query     string  false  "Ignore Dupes true/false"
// @Param        lazy-provide  query     string  false  "Lazy Provide true/false"
// @Param        dir           query     string  false  "Directory"
// @Success      200           {object}  util.ContentAddResponse
// @Failure      400           {object}  util.HttpError
// @Failure      500           {object}  util.HttpError
// @Router       /content/add [post]
func (s *apiV1) handleAdd(c echo.Context, u *util.User) error {
	ctx, span := s.tracer.Start(c.Request().Context(), "handleAdd", trace.WithAttributes(attribute.Int("user", int(u.ID))))
	defer span.End()

	if err := util.ErrorIfContentAddingDisabled(s.isContentAddingDisabled(u)); err != nil {
		return err
	}

	if s.cfg.Content.DisableLocalAdding {
		return s.redirectContentAdding(c, u)
	}

	form, err := c.MultipartForm()
	if err != nil {
		return err
	}
	defer form.RemoveAll()

	mpf, err := c.FormFile("data")
	if err != nil {
		return err
	}

	// if splitting is disabled and uploaded content size is greater than content size limit
	// reject the upload, as it will only get stuck and deals will never be made for it
	if !u.FlagSplitContent() && mpf.Size > s.cfg.Content.MaxSize {
		return &util.HttpError{
			Code:    http.StatusBadRequest,
			Reason:  util.ERR_CONTENT_SIZE_OVER_LIMIT,
			Details: fmt.Sprintf("content size %d bytes, is over upload size limit of %d bytes, and content splitting is not enabled, please reduce the content size", mpf.Size, s.cfg.Content.MaxSize),
		}
	}

	filename := mpf.Filename
	if fvname := c.FormValue("filename"); fvname != "" {
		filename = fvname
	}

	fi, err := mpf.Open()
	if err != nil {
		return err
	}

	defer fi.Close()

	replication := s.cfg.Replication
	replVal := c.FormValue("replication")
	if replVal != "" {
		parsed, err := strconv.Atoi(replVal)
		if err != nil {
			s.log.Errorf("failed to parse replication value in form data, assuming default for now: %s", err)
		} else {
			replication = parsed
		}
	}

	coluuid := c.QueryParam("coluuid")
	var col *collections.Collection
	if coluuid != "" {
		var srchCol collections.Collection
		if err := s.DB.First(&srchCol, "uuid = ? and user_id = ?", coluuid, u.ID).Error; err != nil {
			return err
		}

		col = &srchCol
	}

	path, err := constructDirectoryPath(c.QueryParam(ColDir))
	if err != nil {
		return err
	}

	bsid, bs, err := s.StagingMgr.AllocNew()
	if err != nil {
		return err
	}

	defer func() {
		go func() {
			if err := s.StagingMgr.CleanUp(bsid); err != nil {
				s.log.Errorf("failed to clean up staging blockstore: %s", err)
			}
		}()
	}()

	bserv := blockservice.New(bs, nil)
	dserv := merkledag.NewDAGService(bserv)

	nd, err := s.importFile(ctx, dserv, fi)
	if err != nil {
		return err
	}

	if c.QueryParam("ignore-dupes") == "true" {
		isDup, err := s.isDupCIDContent(c, nd.Cid(), u)
		if err != nil || isDup {
			return err
		}
	}

	content, err := s.CM.AddDatabaseTracking(ctx, u, dserv, nd.Cid(), filename, replication)
	if err != nil {
		return xerrors.Errorf("encountered problem computing object references: %w", err)
	}
	fullPath := filepath.Join(path, content.Name)

	if col != nil {
		s.log.Infof("COLLECTION CREATION: %d, %d", col.ID, content.ID)
		if err := s.DB.Create(&collections.CollectionRef{
			Collection: col.ID,
			Content:    content.ID,
			Path:       &fullPath,
		}).Error; err != nil {
			s.log.Errorf("failed to add content to requested collection: %s", err)
		}
	}

	if err := util.DumpBlockstoreTo(ctx, s.tracer, bs, s.Node.Blockstore); err != nil {
		return xerrors.Errorf("failed to move data from staging to main blockstore: %w", err)
	}

	s.CM.ToCheck(content.ID, content.Size)

	if c.QueryParam("lazy-provide") != "true" {
		subctx, cancel := context.WithTimeout(ctx, time.Second*10)
		defer cancel()
		if err := s.Node.FullRT.Provide(subctx, nd.Cid(), true); err != nil {
			span.RecordError(fmt.Errorf("provide error: %w", err))
			s.log.Errorf("fullrt provide call errored: %s", err)
		}
	}

	go func() {
		if err := s.Node.Provider.Provide(nd.Cid()); err != nil {
			s.log.Warnf("failed to announce providers: %s", err)
		}
	}()

	return c.JSON(http.StatusOK, &util.ContentAddResponse{
		Cid:                 nd.Cid().String(),
		RetrievalURL:        util.CreateDwebRetrievalURL(nd.Cid().String()),
		EstuaryRetrievalURL: util.CreateEstuaryRetrievalURL(nd.Cid().String()),
		EstuaryId:           content.ID,
		Providers:           s.CM.PinDelegatesForContent(*content),
	})
}

func constructDirectoryPath(dir string) (string, error) {
	defaultPath := "/"
	path := defaultPath
	if cp := dir; cp != "" {
		sp, err := sanitizePath(cp)
		if err != nil {
			return "", err
		}

		path = sp
	}
	return path, nil
}

// redirectContentAdding is called when localContentAddingDisabled is true
// it finds available shuttles and adds the desired content in one of them
func (s *apiV1) redirectContentAdding(c echo.Context, u *util.User) error {
	uep, err := s.shuttleMgr.GetPreferredUploadEndpoints(u)
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

func (s *apiV1) importFile(ctx context.Context, dserv ipld.DAGService, fi io.Reader) (ipld.Node, error) {
	_, span := s.tracer.Start(ctx, "importFile")
	defer span.End()

	return util.ImportFile(dserv, fi)
}

// handleEnsureReplication godoc
// @Summary      Ensure Replication
// @Description  This endpoint ensures that the content is replicated to the specified number of providers
// @Tags         content
// @Produce      json
// @Success      200   {object}  string
// @Failure      400    {object}  util.HttpError
// @Failure      500    {object}  util.HttpError
// @Param        datacid  path      string  true  "Data CID"
// @Router       /content/ensure-replication/{datacid} [get]
func (s *apiV1) handleEnsureReplication(c echo.Context) error {
	data, err := cid.Decode(c.Param("datacid"))
	if err != nil {
		return err
	}

	var content util.Content
	if err := s.DB.Find(&content, "cid = ?", data.Bytes()).Error; err != nil {
		return err
	}

	fmt.Println("Content: ", content.Cid.CID, data)

	s.CM.ToCheck(content.ID, content.Size)
	return nil
}

// handleListContent godoc
// @Summary      List all pinned content
// @Description  This endpoint lists all content
// @Tags         content
// @Produce      json
// @Success      200   {object}  string
// @Failure      400   {object}  util.HttpError
// @Failure      500   {object}  util.HttpError
// @Router       /content/list [get]
func (s *apiV1) handleListContent(c echo.Context, u *util.User) error {
	var contents []util.Content
	if err := s.DB.Find(&contents, "active and user_id = ?", u.ID).Error; err != nil {
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
// @Tags         content
// @Produce      json
// @Success      200     {object}  string
// @Failure      400      {object}  util.HttpError
// @Failure      500      {object}  util.HttpError
// @Param        limit   query     int  false  "Limit"
// @Param        offset  query     int  false  "Offset"
// @Router       /content/deals [get]
func (s *apiV1) handleListContentWithDeals(c echo.Context, u *util.User) error {

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
	err := s.DB.Model(&util.Content{}).
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
	for _, cont := range contents {
		ec := expandedContent{
			Content: cont,
		}
		if cont.Aggregate {
			if err := s.DB.Model(util.Content{}).Where("aggregated_in = ?", cont.ID).Count(&ec.AggregatedFiles).Error; err != nil {
				return err
			}

		}
		out = append(out, ec)
	}

	return c.JSON(http.StatusOK, out)
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

// handleGetContent godoc
// @Summary      Content
// @Description  This endpoint returns a content by its ID
// @Tags         content
// @Produce      json
// @Success      200    {object}  string
// @Failure      400      {object}  util.HttpError
// @Failure      500      {object}  util.HttpError
// @Param        id   path      int  true  "Content ID"
// @Router       /content/{id} [get]
func (s *apiV1) handleGetContent(c echo.Context, u *util.User) error {
	contID, err := strconv.Atoi(c.Param("cont_id"))
	if err != nil {
		return err
	}

	var content util.Content
	if err := s.DB.First(&content, "id = ?", contID).Error; err != nil {
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

// handleContentStatus godoc
// @Summary      Content Status
// @Description  This endpoint returns the status of a content
// @Tags         content
// @Produce      json
// @Success      200            {object}  string
// @Failure      400      {object}  util.HttpError
// @Failure      500      {object}  util.HttpError
// @Param        id   path      int  true  "Content ID"
// @Router       /content/status/{id} [get]
func (s *apiV1) handleContentStatus(c echo.Context, u *util.User) error {
	ctx := c.Request().Context()
	contID, err := strconv.Atoi(c.Param("id"))
	if err != nil {
		return err
	}

	var content util.Content
	if err := s.DB.First(&content, "id = ?", contID).Error; err != nil {
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
	if err := s.DB.Find(&deals, "content = ?", content.ID).Error; err != nil {
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

			chanst, err := s.transferMgr.GetTransferStatus(ctx, &dl, content.Cid.CID, content.Location)
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
				markDeal, err := s.Api.StateMarketStorageDeal(ctx, abi.DealID(d.DealID), types.EmptyTSK)
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
	if err := s.DB.Model(&model.DfeRecord{}).Where("content = ?", content.ID).Count(&failCount).Error; err != nil {
		return err
	}

	return c.JSON(http.StatusOK, map[string]interface{}{
		"content":       content,
		"deals":         ds,
		"failuresCount": failCount,
	})
}

// handleGetDealStatus godoc
// @Summary      Get Deal Status
// @Description  This endpoint returns the status of a deal
// @Tags         deals
// @Produce      json
// @Success      200      {object}  string
// @Failure      400     {object}  util.HttpError
// @Failure      500     {object}  util.HttpError
// @Param        deal  path      int  true  "Deal ID"
// @Router       /deals/status/{deal} [get]
func (s *apiV1) handleGetDealStatus(c echo.Context, u *util.User) error {
	ctx := c.Request().Context()

	val, err := strconv.Atoi(c.Param("deal"))
	if err != nil {
		return err
	}

	dstatus, err := s.dealStatusByID(ctx, uint(val))
	if err != nil {
		return err
	}

	return c.JSON(http.StatusOK, dstatus)
}

// handleGetDealStatusByPropCid godoc
// @Summary      Get Deal Status by PropCid
// @Description  Get Deal Status by PropCid
// @Tags         deals
// @Produce      json
// @Success      200      {object}  string
// @Failure      400      {object}  util.HttpError
// @Failure      500      {object}  util.HttpError
// @Param        propcid  path      string  true  "PropCid"
// @Router       /deal/status-by-proposal/{propcid} [get]
func (s *apiV1) handleGetDealStatusByPropCid(c echo.Context, u *util.User) error {
	ctx := c.Request().Context()

	propcid, err := cid.Decode(c.Param("propcid"))
	if err != nil {
		return err
	}

	var deal model.ContentDeal
	if err := s.DB.First(&deal, "prop_cid = ?", propcid.Bytes()).Error; err != nil {
		if xerrors.Is(err, gorm.ErrRecordNotFound) {
			return &util.HttpError{
				Code:    http.StatusNotFound,
				Reason:  util.ERR_RECORD_NOT_FOUND,
				Details: fmt.Sprintf("deal was not found for prop_cid: %s", propcid),
			}
		}
		return err
	}

	dstatus, err := s.dealStatusByID(ctx, deal.ID)
	if err != nil {
		return err
	}

	return c.JSON(http.StatusOK, dstatus)
}

func (s *apiV1) dealStatusByID(ctx context.Context, dealid uint) (*dealStatus, error) {
	var deal model.ContentDeal
	if err := s.DB.First(&deal, "id = ?", dealid).Error; err != nil {
		if xerrors.Is(err, gorm.ErrRecordNotFound) {
			return nil, &util.HttpError{
				Code:    http.StatusNotFound,
				Reason:  util.ERR_RECORD_NOT_FOUND,
				Details: fmt.Sprintf("deal: %d was not found", dealid),
			}
		}
		return nil, err
	}

	var content util.Content
	if err := s.DB.First(&content, "id = ?", deal.Content).Error; err != nil {
		return nil, err
	}

	chanst, err := s.transferMgr.GetTransferStatus(ctx, &deal, content.Cid.CID, content.Location)
	if err != nil {
		s.log.Errorf("failed to get transfer status: %s", err)
		// the UI needs to display a transfer state even for intermittent errors
		chanst = &filclient.ChannelState{
			StatusStr: "Error",
		}
	}

	// the transfer state is yet to be been announced - the UI needs to display a transfer state
	if chanst == nil && deal.DTChan == "" {
		chanst = &filclient.ChannelState{
			StatusStr: "Initializing",
		}
	}

	dstatus := dealStatus{
		Deal:           deal,
		TransferStatus: chanst,
	}

	if deal.DealID > 0 {
		markDeal, err := s.Api.StateMarketStorageDeal(ctx, abi.DealID(deal.DealID), types.EmptyTSK)
		if err != nil {
			s.log.Warnw("failed to get deal info from market actor", "dealID", deal.DealID, "error", err)
		} else {
			dstatus.OnChainState = &onChainDealState{
				SectorStartEpoch: markDeal.State.SectorStartEpoch,
				LastUpdatedEpoch: markDeal.State.LastUpdatedEpoch,
				SlashEpoch:       markDeal.State.SlashEpoch,
			}
		}
	}
	return &dstatus, nil
}

type getContentResponse struct {
	Content      *util.Content        `json:"content"`
	AggregatedIn *util.Content        `json:"aggregatedIn,omitempty"`
	Selector     string               `json:"selector,omitempty"`
	Deals        []*model.ContentDeal `json:"deals"`
}

func (s *apiV1) calcSelector(aggregatedIn uint, contentID uint) (string, error) {
	// sort the known content IDs aggregated in a CAR, and use the index in the sorted list
	// to build the CAR sub-selector

	var ordinal uint
	result := s.DB.Raw(`SELECT ordinal - 1 FROM (
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
func (s *apiV1) handleGetContentByCid(c echo.Context) error {
	obj, err := cid.Decode(c.Param("cid"))
	if err != nil {
		return errors.Wrapf(err, "invalid cid")
	}

	v0 := cid.Undef

	if err == nil {
		if obj.Prefix().Version == 0 {
			v0 = cid.NewCidV0(obj.Hash())
		}
	}
	v1 := cid.NewCidV1(obj.Prefix().Codec, obj.Hash())

	var contents []util.Content
	if err := s.DB.Find(&contents, "(cid=? or cid=?) and active", v0.Bytes(), v1.Bytes()).Error; err != nil {
		return err
	}

	out := make([]getContentResponse, 0)
	for i, cont := range contents {
		resp := getContentResponse{
			Content: &contents[i],
		}

		id := cont.ID

		if cont.AggregatedIn > 0 {
			var aggr util.Content
			if err := s.DB.First(&aggr, "id = ?", cont.AggregatedIn).Error; err != nil {
				return err
			}

			resp.AggregatedIn = &aggr

			// no need to early return here, the selector is mostly cosmetic atm
			if selector, err := s.calcSelector(cont.AggregatedIn, cont.ID); err == nil {
				resp.Selector = selector
			}

			id = cont.AggregatedIn
		}

		var deals []*model.ContentDeal
		if err := s.DB.Find(&deals, "content = ? and deal_id > 0 and not failed", id).Error; err != nil {
			return err
		}

		resp.Deals = deals

		out = append(out, resp)
	}

	return c.JSON(http.StatusOK, out)
}

// handleGetFullContentbyCid godoc
// @Summary      Get Full Content by Cid
// @Description  This endpoint returns the content associated with a CID
// @Tags         public
// @Produce      json
// @Success      307      {object}  string
// @Failure      400     {object}  util.HttpError
// @Failure      500     {object}  util.HttpError
// @Param        cid  path      string  true  "Cid"
// @Router       /get/{cid} [get]
func (s *apiV1) handleGetFullContentbyCid(c echo.Context) error {
	obj, err := cid.Decode(c.Param("cid"))
	if err != nil {
		return errors.Wrapf(err, "invalid cid")
	}
	cidStr := cid.NewCidV1(obj.Prefix().Codec, obj.Hash()).String()
	return c.Redirect(http.StatusTemporaryRedirect, "/gw/ipfs/"+cidStr)
}

// handleQueryAsk godoc
// @Summary      Query Ask
// @Description  This endpoint returns the ask for a given CID
// @Tags         deals
// @Produce      json
// @Success      200    {object}  string
// @Failure      400   {object}  util.HttpError
// @Failure      500   {object}  util.HttpError
// @Param        miner  path      string  true  "CID"
// @Router       /deal/query/{miner} [get]
// @router       /public/miners/storage/query/{miner} [get]
func (s *apiV1) handleQueryAsk(c echo.Context) error {
	addr, err := address.NewFromString(c.Param("miner"))
	if err != nil {
		return err
	}

	ask, err := s.minerManager.GetAsk(c.Request().Context(), addr, 0)
	if err != nil {
		return c.JSON(500, map[string]string{"error": err.Error()})
	}
	return c.JSON(http.StatusOK, ask)
}

type dealRequest struct {
	ContentID uint `json:"content_id"`
}

// handleMakeDeal godoc
// @Summary      Make Deal
// @Description  This endpoint makes a deal for a given content and miner
// @Tags         deals
// @Produce      json
// @Success      200      {object}  string
// @Failure      400          {object}  util.HttpError
// @Failure      500          {object}  util.HttpError
// @Param        miner        path      string  true  "Miner"
// @Param        dealRequest  body      string  true  "Deal Request"
// @Router       /deals/make/{miner} [post]
func (s *apiV1) handleMakeDeal(c echo.Context, u *util.User) error {
	ctx := c.Request().Context()

	if u.Perm < util.PermLevelAdmin {
		return &util.HttpError{
			Code:    http.StatusForbidden,
			Reason:  util.ERR_NOT_AUTHORIZED,
			Details: "user not authorized",
		}
	}

	addr, err := address.NewFromString(c.Param("miner"))
	if err != nil {
		return errors.Wrapf(err, "invalid miner address")
	}

	var req dealRequest
	if err := c.Bind(&req); err != nil {
		return err
	}

	if req.ContentID == 0 {
		return &util.HttpError{
			Code:    http.StatusBadRequest,
			Reason:  util.ERR_INVALID_INPUT,
			Details: "supply a valid value for content_id",
		}
	}

	var cont util.Content
	if err := s.DB.First(&cont, "id = ?", req.ContentID).Error; err != nil {
		if xerrors.Is(err, gorm.ErrRecordNotFound) {
			return &util.HttpError{
				Code:    http.StatusNotFound,
				Reason:  util.ERR_CONTENT_NOT_FOUND,
				Details: fmt.Sprintf("content: %d was not found", req.ContentID),
			}
		}
		return err
	}

	if err := s.CM.CheckContentReadyForDealMaking(ctx, cont); err != nil {
		return err
	}

	cd, err := s.CM.MakeDealWithMiner(ctx, cont, addr)
	if err != nil {
		return err
	}

	return c.JSON(http.StatusOK, map[string]interface{}{
		"deal": cd.ID,
	})
}

// from datatransfer.ChannelID and used for swagger docs
// if we don't redefine this here, we'll need to enable parse dependences for swagger and it will take a really long time
type ChannelIDParam struct {
	Initiator string
	Responder string
	ID        uint64
}

// handleTransferStatus godoc
// @Summary      Transfer Status
// @Description  This endpoint returns the status of a transfer
// @Tags         deals
// @Produce      json
// @Success      200      {object}  string
// @Failure      400      {object}  util.HttpError
// @Failure      500      {object}  util.HttpError
// @Param        chanid  body      ChannelIDParam  true  "Channel ID"
// @Router       /deal/transfer/status [post]
func (s *apiV1) handleTransferStatus(c echo.Context) error {
	var chanid datatransfer.ChannelID
	if err := c.Bind(&chanid); err != nil {
		return err
	}

	var deal model.ContentDeal
	if err := s.DB.First(&deal, "dt_chan = ?", chanid.ID).Error; err != nil {
		if xerrors.Is(err, gorm.ErrRecordNotFound) {
			return &util.HttpError{
				Code:    http.StatusNotFound,
				Reason:  util.ERR_RECORD_NOT_FOUND,
				Details: fmt.Sprintf("deal: %d was not found", chanid.ID),
			}
		}
		return err
	}

	var cont util.Content
	if err := s.DB.First(&cont, "id = ?", deal.Content).Error; err != nil {
		return err
	}

	status, err := s.transferMgr.GetTransferStatus(c.Request().Context(), &deal, cont.Cid.CID, cont.Location)
	if err != nil {
		return err
	}

	return c.JSON(http.StatusOK, status)
}

func (s *apiV1) handleTransferStatusByID(c echo.Context) error {
	transferID := c.Param("id")

	var deal model.ContentDeal
	if err := s.DB.First(&deal, "dt_chan = ?", transferID).Error; err != nil {
		if xerrors.Is(err, gorm.ErrRecordNotFound) {
			return &util.HttpError{
				Code:    http.StatusNotFound,
				Reason:  util.ERR_RECORD_NOT_FOUND,
				Details: fmt.Sprintf("deal: %s was not found", transferID),
			}
		}
		return err
	}

	var cont util.Content
	if err := s.DB.First(&cont, "id = ?", deal.Content).Error; err != nil {
		return err
	}

	status, err := s.transferMgr.GetTransferStatus(c.Request().Context(), &deal, cont.Cid.CID, cont.Location)
	if err != nil {
		return err
	}
	return c.JSON(http.StatusOK, status)
}

// handleTransferInProgress godoc
// @Summary      Transfer In Progress
// @Description  This endpoint returns the in-progress transfers
// @Tags         deals
// @Produce      json
// @Success      200      {object}  string
// @Failure      400     {object}  util.HttpError
// @Failure      500     {object}  util.HttpError
// @Router       /deal/transfer/in-progress [get]
func (s *apiV1) handleTransferInProgress(c echo.Context) error {
	ctx := context.TODO()

	transfers, err := s.FilClient.TransfersInProgress(ctx)
	if err != nil {
		return err
	}

	return c.JSON(http.StatusOK, transfers)
}

func (s *apiV1) handleMinerTransferDiagnostics(c echo.Context) error {
	m, err := address.NewFromString(c.Param("miner"))
	if err != nil {
		return err
	}

	minerTransferDiagnostics, err := s.FilClient.MinerTransferDiagnostics(c.Request().Context(), m)
	if err != nil {
		return err
	}

	return c.JSON(http.StatusOK, minerTransferDiagnostics)
}

func (s *apiV1) handleTransferRestart(c echo.Context) error {
	ctx := c.Request().Context()

	dealid, err := strconv.Atoi(c.Param("deal"))
	if err != nil {
		return err
	}

	var deal model.ContentDeal
	if err := s.DB.First(&deal, "id = ?", dealid).Error; err != nil {
		if xerrors.Is(err, gorm.ErrRecordNotFound) {
			return &util.HttpError{
				Code:    http.StatusNotFound,
				Reason:  util.ERR_RECORD_NOT_FOUND,
				Details: fmt.Sprintf("deal: %d was not found", dealid),
			}
		}
		return err
	}

	var cont util.Content
	if err := s.DB.First(&cont, "id = ?", deal.Content).Error; err != nil {
		return err
	}

	if deal.Failed {
		return fmt.Errorf("cannot restart transfer, deal failed")
	}

	if deal.DealID > 0 {
		return fmt.Errorf("cannot restart transfer, already finished")
	}

	if deal.DTChan == "" {
		return fmt.Errorf("cannot restart transfer, no channel id")
	}

	chanid, err := deal.ChannelID()
	if err != nil {
		return err
	}

	if err := s.transferMgr.RestartTransfer(ctx, cont.Location, chanid, deal); err != nil {
		return err
	}
	return nil
}

// handleDealStatus godoc
// @Summary      Deal Status
// @Description  This endpoint returns the status of a deal
// @Tags         deals
// @Produce      json
// @Success      200       {object}  string
// @Failure      400   {object}  util.HttpError
// @Failure      500   {object}  util.HttpError
// @Param        miner    path      string  true  "Miner"
// @Param        propcid  path      string  true  "Proposal CID"
// @Router       /deal/status/{miner}/{propcid} [get]
func (s *apiV1) handleDealStatus(c echo.Context) error {
	ctx := c.Request().Context()

	addr, err := address.NewFromString(c.Param("miner"))
	if err != nil {
		return err
	}

	propCid, err := cid.Decode(c.Param("propcid"))
	if err != nil {
		return err
	}

	var d model.ContentDeal
	if err := s.DB.First(&d, "prop_cid = ?", propCid.Bytes()).Error; err != nil {
		if xerrors.Is(err, gorm.ErrRecordNotFound) {
			return &util.HttpError{
				Code:    http.StatusNotFound,
				Reason:  util.ERR_RECORD_NOT_FOUND,
				Details: fmt.Sprintf("deal was not found for prop_cid: %s", propCid),
			}
		}
		return err
	}

	// Get deal UUID, if there is one for the deal.
	// (There should be a UUID for deals made with deal protocol v1.2.0)
	var dealUUID *uuid.UUID
	if d.DealUUID != "" {
		parsed, err := uuid.Parse(d.DealUUID)
		if err != nil {
			return fmt.Errorf("parsing deal uuid %s: %w", d.DealUUID, err)
		}
		dealUUID = &parsed
	}

	status, err := s.FilClient.DealStatus(ctx, addr, propCid, dealUUID)
	if err != nil {
		return xerrors.Errorf("getting deal status: %w", err)
	}
	return c.JSON(http.StatusOK, status)
}

// handleGetProposal godoc
// @Summary      Get Proposal
// @Description  This endpoint returns the proposal for a deal
// @Tags         deals
// @Produce      json
// @Success      200           {object}  string
// @Failure      400         {object}  util.HttpError
// @Failure      500         {object}  util.HttpError
// @Param        propcid  path      string  true  "Proposal CID"
// @Router       /deal/proposal/{propcid} [get]
func (s *apiV1) handleGetProposal(c echo.Context) error {
	propCid, err := cid.Decode(c.Param("propcid"))
	if err != nil {
		return err
	}

	var proprec model.ProposalRecord
	if err := s.DB.First(&proprec, "prop_cid = ?", propCid.Bytes()).Error; err != nil {
		if xerrors.Is(err, gorm.ErrRecordNotFound) {
			return &util.HttpError{
				Code:    http.StatusNotFound,
				Reason:  util.ERR_RECORD_NOT_FOUND,
				Details: fmt.Sprintf("proposal: %s was not found", propCid),
			}
		}
		return err
	}

	var prop market.ClientDealProposal
	if err := prop.UnmarshalCBOR(bytes.NewReader(proprec.Data)); err != nil {
		return err
	}

	return c.JSON(http.StatusOK, prop)
}

// handleGetDealInfo godoc
// @Summary      Get Deal Info
// @Description  This endpoint returns the deal info for a deal
// @Tags         deals
// @Produce      json
// @Success      200  {object}  string
// @Failure      400      {object}  util.HttpError
// @Failure      500      {object}  util.HttpError
// @Param        dealid  path      int  true  "Deal ID"
// @Router       /deal/info/{dealid} [get]
func (s *apiV1) handleGetDealInfo(c echo.Context) error {
	dealid, err := strconv.ParseInt(c.Param("dealid"), 10, 64)
	if err != nil {
		return err
	}

	deal, err := s.Api.StateMarketStorageDeal(c.Request().Context(), abi.DealID(dealid), types.EmptyTSK)
	if err != nil {
		return err
	}

	return c.JSON(http.StatusOK, deal)
}

type getInvitesResp struct {
	Code      string `json:"code"`
	Username  string `json:"createdBy"`
	ClaimedBy string `json:"claimedBy"`
	CreatedAt string `json:"createdAt"`
}

// handleAdminGetInvites godoc
// @Summary      Get Estuary invites
// @Description  This endpoint is used to list all estuary invites.
// @Tags         content
// @Produce      json
// @Success      200           {object}  string
// @Failure      400           {object}  util.HttpError
// @Failure      500           {object}  util.HttpError
// @Router       /admin/invites [get]
func (s *apiV1) handleAdminGetInvites(c echo.Context) error {
	var invites []getInvitesResp
	if err := s.DB.Model(&util.InviteCode{}).
		Select("code, username, invite_codes.created_at, (?) as claimed_by", s.DB.Table("users").Select("username").Where("id = invite_codes.claimed_by")).
		//Where("claimed_by IS NULL").
		Joins("left join users on users.id = invite_codes.created_by").
		Order("invite_codes.created_at ASC").
		Scan(&invites).Error; err != nil {
		return err
	}

	return c.JSON(http.StatusOK, invites)
}

// handleAdminCreateInvite godoc
// @Summary      Create an Estuary invite
// @Description  This endpoint is used to create an estuary invite.
// @Tags         content
// @Produce      json
// @Success      200           {object}  string
// @Failure      400           {object}  util.HttpError
// @Failure      500           {object}  util.HttpError
// @Param        code  path      string  false  "Invite code to be created"
// @Router       /admin/invites/{code} [post]
func (s *apiV1) handleAdminCreateInvite(c echo.Context, u *util.User) error {
	code := c.Param("code")
	invite := &util.InviteCode{
		Code:      code,
		CreatedBy: u.ID,
	}
	if err := s.DB.Create(invite).Error; err != nil {
		return err
	}

	return c.JSON(http.StatusOK, map[string]string{
		"code": invite.Code,
	})
}

func (s *apiV1) handleAdminFilAddress(c echo.Context) error {
	return c.JSON(http.StatusOK, s.FilClient.ClientAddr)
}

func (s *apiV1) handleAdminBalance(c echo.Context) error {
	balance, err := s.FilClient.Balance(c.Request().Context())
	if err != nil {
		return err
	}

	return c.JSON(http.StatusOK, balance)
}

func (s *apiV1) handleAdminAddEscrow(c echo.Context) error {
	amt, err := types.ParseFIL(c.Param("amt"))
	if err != nil {
		return err
	}

	resp, err := s.FilClient.LockMarketFunds(context.TODO(), amt)
	if err != nil {
		return err
	}

	return c.JSON(http.StatusOK, resp)
}

type adminStatsResponse struct {
	TotalDealAttempted   int64 `json:"totalDealsAttempted"`
	TotalDealsSuccessful int64 `json:"totalDealsSuccessful"`
	TotalDealsFailed     int64 `json:"totalDealsFailed"`

	NumMiners int64 `json:"numMiners"`
	NumUsers  int64 `json:"numUsers"`
	NumFiles  int64 `json:"numFiles"`

	NumRetrievals      int64 `json:"numRetrievals"`
	NumRetrFailures    int64 `json:"numRetrievalFailures"`
	NumStorageFailures int64 `json:"numStorageFailures"`

	PinQueueSize int `json:"pinQueueSize"`
}

func (s *apiV1) handleAdminStats(c echo.Context) error {

	var dealsTotal int64
	if err := s.DB.Model(&model.ContentDeal{}).Count(&dealsTotal).Error; err != nil {
		return err
	}

	var dealsSuccessful int64
	if err := s.DB.Model(&model.ContentDeal{}).Where("deal_id > 0").Count(&dealsSuccessful).Error; err != nil {
		return err
	}

	var dealsFailed int64
	if err := s.DB.Model(&model.ContentDeal{}).Where("failed").Count(&dealsFailed).Error; err != nil {
		return err
	}

	var numMiners int64
	if err := s.DB.Model(&model.StorageMiner{}).Count(&numMiners).Error; err != nil {
		return err
	}

	var numUsers int64
	if err := s.DB.Model(&util.User{}).Count(&numUsers).Error; err != nil {
		return err
	}

	var numFiles int64
	if err := s.DB.Model(&util.Content{}).Where("active").Count(&numFiles).Error; err != nil {
		return err
	}

	var numRetrievals int64
	if err := s.DB.Model(&model.RetrievalSuccessRecord{}).Count(&numRetrievals).Error; err != nil {
		return err
	}

	var numRetrievalFailures int64
	if err := s.DB.Model(&util.RetrievalFailureRecord{}).Count(&numRetrievalFailures).Error; err != nil {
		return err
	}

	var numStorageFailures int64
	if err := s.DB.Model(&model.DfeRecord{}).Count(&numStorageFailures).Error; err != nil {
		return err
	}

	return c.JSON(http.StatusOK, &adminStatsResponse{
		TotalDealAttempted:   dealsTotal,
		TotalDealsSuccessful: dealsSuccessful,
		TotalDealsFailed:     dealsFailed,
		NumMiners:            numMiners,
		NumUsers:             numUsers,
		NumFiles:             numFiles,
		NumRetrievals:        numRetrievals,
		NumRetrFailures:      numRetrievalFailures,
		NumStorageFailures:   numStorageFailures,
		PinQueueSize:         s.pinMgr.PinQueueSize(),
	})
}

// handleGetSystemConfig godoc
// @Summary      Get systems(estuary/shuttle) config
// @Description  This endpoint is used to get system configs.
// @Tags         admin
// @Produce      json
// @Success      200  {object}  string
// @Failure      400       {object}  util.HttpError
// @Failure      500       {object}  util.HttpError
// @Router       /admin/system/config [get]
func (s *apiV1) handleGetSystemConfig(c echo.Context, u *util.User) error {
	shConfigs, err := s.shuttleMgr.GetShuttlesConfig(u)
	if err != nil {
		return err
	}

	resp := map[string]interface{}{
		"data": map[string]interface{}{
			"primary":  s.cfg,
			"shuttles": shConfigs,
		},
	}
	return c.JSON(http.StatusOK, resp)
}

type minerResp struct {
	Addr            address.Address `json:"addr"`
	Name            string          `json:"name"`
	Suspended       bool            `json:"suspended"`
	SuspendedReason string          `json:"suspendedReason,omitempty"`
	Version         string          `json:"version"`
}

// handleAdminGetMiners godoc
// @Summary      Get all miners
// @Description  This endpoint returns all miners
// @Tags         public,net
// @Produce      json
// @Success      200  {object}  string
// @Failure      400           {object}  util.HttpError
// @Failure      500           {object}  util.HttpError
// @Router       /public/miners [get]
func (s *apiV1) handleAdminGetMiners(c echo.Context) error {
	var miners []model.StorageMiner
	if err := s.DB.Find(&miners).Error; err != nil {
		return err
	}

	out := make([]minerResp, len(miners))
	for i, m := range miners {
		out[i].Addr = m.Address.Addr
		out[i].Suspended = m.Suspended
		out[i].SuspendedReason = m.SuspendedReason
		out[i].Name = m.Name
		out[i].Version = m.Version
	}

	return c.JSON(http.StatusOK, out)
}

func (s *apiV1) handlePublicGetMinerStats(c echo.Context) error {
	_, stats, err := s.minerManager.SortedMinerList()
	if err != nil {
		return err
	}
	return c.JSON(http.StatusOK, stats)
}

func (s *apiV1) handleAdminGetMinerStats(c echo.Context) error {
	sml, err := s.minerManager.ComputeSortedMinerList()
	if err != nil {
		return err
	}
	return c.JSON(http.StatusOK, sml)
}

// handleMinersSetInfo godoc
// @Summary      Set Miner Info
// @Description  This endpoint lets a user set miner info.
// @Tags         miner
// @Produce      json
// @Success      200  {object}  emptyResp
// @Failure      400  {object}  util.HttpError
// @Failure      500  {object}  util.HttpError
// @Param        params           body      miner.MinerSetInfoParams  true   "Miner set info params"
// @Param        miner           path      string  true   "Miner to set info for"
// @Router       /miner/set-info/{miner} [put]
func (s *apiV1) handleMinersSetInfo(c echo.Context, u *util.User) error {
	m, err := address.NewFromString(c.Param("miner"))
	if err != nil {
		return err
	}

	var params miner.MinerSetInfoParams
	if err := c.Bind(&params); err != nil {
		return err
	}

	if err := s.minerManager.SetMinerInfo(m, params, u); err != nil {
		return err
	}
	return c.JSON(http.StatusOK, emptyResp{})
}

func (s *apiV1) handleAdminRemoveMiner(c echo.Context) error {
	m, err := address.NewFromString(c.Param("miner"))
	if err != nil {
		return err
	}

	if err := s.DB.Unscoped().Where("address = ?", m.String()).Delete(&model.StorageMiner{}).Error; err != nil {
		return err
	}
	return c.JSON(http.StatusOK, map[string]string{})
}

type emptyResp struct{}

// handleSuspendMiner godoc
// @Summary      Suspend Miner
// @Description  This endpoint lets a user suspend a miner.
// @Tags         miner
// @Produce      json
// @Success      200  {object}  emptyResp
// @Failure      400  {object}  util.HttpError
// @Failure      500  {object}  util.HttpError
// @Param        req           body      miner.SuspendMinerBody  true   "Suspend Miner Body"
// @Param        miner           path      string  true   "Miner to suspend"
// @Router       /miner/suspend/{miner} [post]
func (s *apiV1) handleSuspendMiner(c echo.Context, u *util.User) error {
	var body miner.SuspendMinerBody
	if err := c.Bind(&body); err != nil {
		return err
	}

	m, err := address.NewFromString(c.Param("miner"))
	if err != nil {
		return err
	}

	if err := s.minerManager.SuspendMiner(m, body, u); err != nil {
		return err
	}
	return c.JSON(http.StatusOK, emptyResp{})
}

// handleUnsuspendMiner godoc
// @Summary      Unuspend Miner
// @Description  This endpoint lets a user unsuspend a miner.
// @Tags         miner
// @Produce      json
// @Success      200  {object}  emptyResp
// @Failure      400  {object}  util.HttpError
// @Failure      500  {object}  util.HttpError
// @Param        miner           path      string  true   "Miner to unsuspend"
// @Router       /miner/unsuspend/{miner} [put]
func (s *apiV1) handleUnsuspendMiner(c echo.Context, u *util.User) error {
	m, err := address.NewFromString(c.Param("miner"))
	if err != nil {
		return err
	}

	if err := s.minerManager.UnSuspendMiner(m, u); err != nil {
		return err
	}
	return c.JSON(http.StatusOK, emptyResp{})
}

func (s *apiV1) handleAdminAddMiner(c echo.Context) error {
	m, err := address.NewFromString(c.Param("miner"))
	if err != nil {
		return err
	}

	name := c.QueryParam("name")
	if err := s.DB.Clauses(&clause.OnConflict{UpdateAll: true}).Create(&model.StorageMiner{
		Address: util.DbAddr{Addr: m},
		Name:    name,
	}).Error; err != nil {
		return err
	}

	return c.JSON(http.StatusOK, map[string]string{})
}

type contentDealStats struct {
	NumDeals     int `json:"numDeals"`
	NumConfirmed int `json:"numConfirmed"`
	NumFailed    int `json:"numFailed"`

	TotalSpending     abi.TokenAmount `json:"totalSpending"`
	ConfirmedSpending abi.TokenAmount `json:"confirmedSpending"`
}

func (s *apiV1) handleDealStats(c echo.Context) error {
	ctx, span := s.tracer.Start(c.Request().Context(), "handleDealStats")
	defer span.End()

	var alldeals []model.ContentDeal
	if err := s.DB.Find(&alldeals).Error; err != nil {
		return err
	}

	sbc := make(map[uint]*contentDealStats)

	for _, d := range alldeals {
		maddr, err := d.MinerAddr()
		if err != nil {
			return err
		}

		// Get deal UUID, if there is one for the deal.
		// (There should be a UUID for deals made with deal protocol v1.2.0)
		var dealUUID *uuid.UUID
		if d.DealUUID != "" {
			parsed, err := uuid.Parse(d.DealUUID)
			if err != nil {
				return fmt.Errorf("parsing deal uuid %s: %w", d.DealUUID, err)
			}
			dealUUID = &parsed
		}
		st, err := s.FilClient.DealStatus(ctx, maddr, d.PropCid.CID, dealUUID)
		if err != nil {
			s.log.Errorf("checking deal status failed (%s): %s", maddr, err)
			continue
		}
		if st.Proposal == nil {
			s.log.Errorf("deal status proposal is empty (%s): %s", maddr, d.PropCid.CID)
			continue
		}

		fee := st.Proposal.TotalStorageFee()

		cds, ok := sbc[d.Content]
		if !ok {
			cds = &contentDealStats{
				TotalSpending:     abi.NewTokenAmount(0),
				ConfirmedSpending: abi.NewTokenAmount(0),
			}
			sbc[d.Content] = cds
		}

		if d.Failed {
			cds.NumFailed++
			continue
		}

		cds.TotalSpending = types.BigAdd(cds.TotalSpending, fee)
		cds.NumDeals++

		if d.DealID != 0 {
			cds.ConfirmedSpending = types.BigAdd(cds.ConfirmedSpending, fee)
			cds.NumConfirmed++
		}
	}

	return c.JSON(http.StatusOK, sbc)
}

type lmdbStat struct {
	PSize         uint   `json:"pSize"`
	Depth         uint   `json:"depth"`
	BranchPages   uint64 `json:"branchPages"`
	LeafPages     uint64 `json:"leafPages"`
	OverflowPages uint64 `json:"overflowPages"`
	Entries       uint64 `json:"entries"`
}

type diskSpaceInfo struct {
	BstoreSize uint64 `json:"bstoreSize"`
	BstoreFree uint64 `json:"bstoreFree"`

	LmdbUsage uint64 `json:"lmdbUsage"`

	LmdbStat lmdbStat `json:"lmdbStat"`
}

func (s *apiV1) handleDiskSpaceCheck(c echo.Context) error {
	/*
		lmst, err := s.Node.Lmdb.Stat()
		if err != nil {
			return err
		}
	*/

	var st unix.Statfs_t
	if err := unix.Statfs(s.Node.Config.Blockstore, &st); err != nil {
		return err
	}

	return c.JSON(http.StatusOK, &diskSpaceInfo{
		BstoreSize: st.Blocks * uint64(st.Bsize),
		BstoreFree: st.Bavail * uint64(st.Bsize),
		/*
			LmdbUsage:  uint64(lmst.PSize) * (lmst.BranchPages + lmst.OverflowPages + lmst.LeafPages),
			LmdbStat: lmdbStat{
				PSize:         lmst.PSize,
				Depth:         lmst.Depth,
				BranchPages:   lmst.BranchPages,
				LeafPages:     lmst.LeafPages,
				OverflowPages: lmst.OverflowPages,
				Entries:       lmst.Entries,
			},
		*/
	})
}

func (s *apiV1) handleGetRetrievalInfo(c echo.Context) error {
	var infos []model.RetrievalSuccessRecord
	if err := s.DB.Find(&infos).Error; err != nil {
		return err
	}

	var failures []util.RetrievalFailureRecord
	if err := s.DB.Find(&failures).Error; err != nil {
		return err
	}

	return c.JSON(http.StatusOK, map[string]interface{}{
		"records":  infos,
		"failures": failures,
	})
}

func (s *apiV1) handleRetrievalCheck(c echo.Context) error {
	ctx := c.Request().Context()
	contid, err := strconv.Atoi(c.Param("content"))
	if err != nil {
		return err
	}
	if err := s.retrieveContent(ctx, uint(contid)); err != nil {
		return err
	}

	return c.JSON(http.StatusOK, "We did a thing")

}

type estimateDealBody struct {
	Size         uint64 `json:"size"`
	Replication  int    `json:"replication"`
	DurationBlks int    `json:"durationBlks"`
	Verified     bool   `json:"verified"`
}

type priceEstimateResponse struct {
	TotalStr string `json:"totalFil"`
	Total    string `json:"totalAttoFil"`
	Asks     []*model.MinerStorageAsk
}

// handleEstimateDealCost godoc
// @Summary      Estimate the cost of a deal
// @Description  This endpoint estimates the cost of a deal
// @Tags         deals
// @Produce      json
// @Success      200  {object}  string
// @Failure      400  {object}  util.HttpError
// @Failure      500  {object}  util.HttpError
// @Param        body  body      estimateDealBody  true  "The size of the deal in bytes, the replication factor, and the duration of the deal in blocks"
// @Router       /deal/estimate [post]
func (s *apiV1) handleEstimateDealCost(c echo.Context) error {
	ctx := c.Request().Context()

	var body estimateDealBody
	if err := c.Bind(&body); err != nil {
		return err
	}

	pieceSize := padreader.PaddedSize(body.Size)

	estimate, err := s.minerManager.EstimatePrice(ctx, body.Replication, pieceSize.Padded(), abi.ChainEpoch(body.DurationBlks), body.Verified)
	if err != nil {
		return err
	}

	return c.JSON(http.StatusOK, &priceEstimateResponse{
		TotalStr: types.FIL(*estimate.Total).String(),
		Total:    estimate.Total.String(),
		Asks:     estimate.Asks,
	})
}

// handleGetMinerFailures godoc
// @Summary      Get all miners
// @Description  This endpoint returns all miners
// @Tags         public,net
// @Produce      json
// @Success      200  {object}  string
// @Failure      400  {object}  util.HttpError
// @Failure      500  {object}  util.HttpError
// @Param        miner  path      string  true  "Filter by miner"
// @Router       /public/miners/failures/{miner} [get]
func (s *apiV1) handleGetMinerFailures(c echo.Context) error {
	maddr, err := address.NewFromString(c.Param("miner"))
	if err != nil {
		return err
	}

	var merrs []model.DfeRecord
	if err := s.DB.Limit(1000).Order("created_at desc").Find(&merrs, "miner = ?", maddr.String()).Error; err != nil {
		return err
	}
	return c.JSON(http.StatusOK, merrs)
}

type minerStatsResp struct {
	Miner           address.Address `json:"miner"`
	Name            string          `json:"name"`
	Version         string          `json:"version"`
	UsedByEstuary   bool            `json:"usedByEstuary"`
	DealCount       int64           `json:"dealCount"`
	ErrorCount      int64           `json:"errorCount"`
	Suspended       bool            `json:"suspended"`
	SuspendedReason string          `json:"suspendedReason"`

	ChainInfo *minerChainInfo `json:"chainInfo"`
}

type minerChainInfo struct {
	PeerID    string   `json:"peerId"`
	Addresses []string `json:"addresses"`

	Owner  string `json:"owner"`
	Worker string `json:"worker"`
}

// handleGetMinerStats godoc
// @Summary      Get miner stats
// @Description  This endpoint returns miner stats
// @Tags         public,miner
// @Produce      json
// @Success      200  {object}  string
// @Failure      400  {object}  util.HttpError
// @Failure      500  {object}  util.HttpError
// @Param        miner  path      string  true  "Filter by miner"
// @Router       /public/miners/stats/{miner} [get]
func (s *apiV1) handleGetMinerStats(c echo.Context) error {
	ctx, span := s.tracer.Start(c.Request().Context(), "handleGetMinerStats")
	defer span.End()

	maddr, err := address.NewFromString(c.Param("miner"))
	if err != nil {
		return err
	}

	minfo, err := s.Api.StateMinerInfo(ctx, maddr, types.EmptyTSK)
	if err != nil {
		return err
	}

	ci := minerChainInfo{
		Owner:  minfo.Owner.String(),
		Worker: minfo.Worker.String(),
	}

	if minfo.PeerId != nil {
		ci.PeerID = minfo.PeerId.String()
	}
	for _, a := range minfo.Multiaddrs {
		ma, err := multiaddr.NewMultiaddrBytes(a)
		if err != nil {
			return err
		}
		ci.Addresses = append(ci.Addresses, ma.String())
	}

	var m model.StorageMiner
	if err := s.DB.First(&m, "address = ?", maddr.String()).Error; err != nil {
		if xerrors.Is(err, gorm.ErrRecordNotFound) {
			return c.JSON(http.StatusOK, &minerStatsResp{
				Miner:         maddr,
				UsedByEstuary: false,
			})
		}
		return err
	}

	var dealscount int64
	if err := s.DB.Model(&model.ContentDeal{}).Where("miner = ?", maddr.String()).Count(&dealscount).Error; err != nil {
		return err
	}

	var errorcount int64
	if err := s.DB.Model(&model.DfeRecord{}).Where("miner = ?", maddr.String()).Count(&errorcount).Error; err != nil {
		return err
	}

	return c.JSON(http.StatusOK, &minerStatsResp{
		Miner:           maddr,
		UsedByEstuary:   true,
		DealCount:       dealscount,
		ErrorCount:      errorcount,
		Suspended:       m.Suspended,
		SuspendedReason: m.SuspendedReason,
		Name:            m.Name,
		Version:         m.Version,
		ChainInfo:       &ci,
	})
}

type minerDealsResp struct {
	ID               uint       `json:"id"`
	CreatedAt        time.Time  `json:"created_at"`
	UpdatedAt        time.Time  `json:"updated_at"`
	Content          uint       `json:"content"`
	PropCid          util.DbCID `json:"propCid"`
	Miner            string     `json:"miner"`
	DealID           int64      `json:"dealId"`
	Failed           bool       `json:"failed"`
	Verified         bool       `json:"verified"`
	FailedAt         time.Time  `json:"failedAt,omitempty"`
	DTChan           string     `json:"dtChan"`
	TransferStarted  time.Time  `json:"transferStarted"`
	TransferFinished time.Time  `json:"transferFinished"`
	OnChainAt        time.Time  `json:"onChainAt"`
	SealedAt         time.Time  `json:"sealedAt"`
	ContentCid       util.DbCID `json:"contentCid"`
}

// handleGetMinerDeals godoc
// @Summary      Get all miners deals
// @Description  This endpoint returns all miners deals
// @Tags         public,miner
// @Produce      json
// @Success      200  {object}  string
// @Failure      400  {object}  util.HttpError
// @Failure      500  {object}  util.HttpError
// @Param        miner          path      string  true   "Filter by miner"
// @Param        ignore-failed  query     string  false  "Ignore Failed"
// @Router       /public/miners/deals/{miner} [get]
func (s *apiV1) handleGetMinerDeals(c echo.Context) error {
	maddr, err := address.NewFromString(c.Param("miner"))
	if err != nil {
		return err
	}

	q := s.DB.Model(model.ContentDeal{}).Order("created_at desc").
		Joins("left join contents on contents.id = content_deals.content").
		Where("miner = ?", maddr.String())

	if c.QueryParam("ignore-failed") != "" {
		q = q.Where("not content_deals.failed")
	}

	var deals []minerDealsResp
	if err := q.Select("contents.cid as content_cid, content_deals.*").Scan(&deals).Error; err != nil {
		return err
	}

	return c.JSON(http.StatusOK, deals)
}

type bandwidthResponse struct {
	TotalOut int64 `json:"totalOut"`
}

// handleGetContentBandwidth godoc
// @Summary      Get content bandwidth
// @Description  This endpoint returns content bandwidth
// @Tags         content
// @Produce      json
// @Success      200  {object}  string
// @Failure      400  {object}  util.HttpError
// @Failure      500  {object}  util.HttpError
// @Param        content  path      string  true  "Content ID"
// @Router       /content/bw-usage/{content} [get]
func (s *apiV1) handleGetContentBandwidth(c echo.Context, u *util.User) error {
	contID, err := strconv.Atoi(c.Param("content"))
	if err != nil {
		return err
	}

	var content util.Content
	if err := s.DB.First(&content, contID).Error; err != nil {
		if xerrors.Is(err, gorm.ErrRecordNotFound) {
			return &util.HttpError{
				Code:    http.StatusNotFound,
				Reason:  util.ERR_RECORD_NOT_FOUND,
				Details: fmt.Sprintf("content: %d was not found", contID),
			}
		}
		return err
	}

	if err := util.IsContentOwner(u.ID, content.UserID); err != nil {
		return err
	}

	// select SUM(size * reads) from obj_refs left join objects on obj_refs.object = objects.id where obj_refs.content = 42;
	var bw int64
	if err := s.DB.Model(util.ObjRef{}).
		Select("SUM(size * reads)").
		Where("obj_refs.content = ?", content.ID).
		Joins("left join objects on obj_refs.object = objects.id").
		Scan(&bw).Error; err != nil {
		return err
	}

	return c.JSON(http.StatusOK, &bandwidthResponse{
		TotalOut: bw,
	})
}

// handleGetAggregatedForContent godoc
// @Summary      Get aggregated content stats
// @Description  This endpoint returns aggregated content stats
// @Tags         content
// @Produce      json
// @Success      200  {object}  string
// @Failure      400  {object}  util.HttpError
// @Failure      500  {object}  util.HttpError
// @Param        content  path      string  true  "Content ID"
// @Router       /content/aggregated/{content} [get]
func (s *apiV1) handleGetAggregatedForContent(c echo.Context, u *util.User) error {
	contID, err := strconv.Atoi(c.Param("content"))
	if err != nil {
		return err
	}

	var content util.Content
	if err := s.DB.First(&content, "id = ?", contID).Error; err != nil {
		if xerrors.Is(err, gorm.ErrRecordNotFound) {
			return &util.HttpError{
				Code:    http.StatusNotFound,
				Reason:  util.ERR_RECORD_NOT_FOUND,
				Details: fmt.Sprintf("miner: %d was not found", contID),
			}
		}
		return err
	}

	if err := util.IsContentOwner(u.ID, content.UserID); err != nil {
		return err
	}

	var sub []util.Content
	if err := s.DB.Find(&sub, "aggregated_in = ?", contID).Error; err != nil {
		return err
	}
	return c.JSON(http.StatusOK, sub)
}

// handleGetContentFailures godoc
// @Summary      List all failures for a content
// @Description  This endpoint returns all failures for a content
// @Tags         content
// @Produce      json
// @Success      200  {object}  string
// @Failure      400  {object}  util.HttpError
// @Failure      500  {object}  util.HttpError
// @Param        content  path      string  true  "Content ID"
// @Router       /content/failures/{content} [get]
func (s *apiV1) handleGetContentFailures(c echo.Context, u *util.User) error {
	cont, err := strconv.Atoi(c.Param("content"))
	if err != nil {
		return err
	}

	var errs []model.DfeRecord
	if err := s.DB.Find(&errs, "content = ?", cont).Error; err != nil {
		return err
	}

	return c.JSON(http.StatusOK, errs)
}

func (s *apiV1) handleGetOffloadingCandidates(c echo.Context) error {
	conts, err := s.CM.GetRemovalCandidates(c.Request().Context(), c.QueryParam("all") == "true", c.QueryParam("location"), nil)
	if err != nil {
		return err
	}
	return c.JSON(http.StatusOK, conts)
}

func (s *apiV1) handleRunOffloadingCollection(c echo.Context) error {
	var body struct {
		Execute        bool   `json:"execute"`
		SpaceRequested int64  `json:"spaceRequested"`
		Location       string `json:"location"`
		Users          []uint `json:"users"`
	}

	if err := c.Bind(&body); err != nil {
		return err
	}

	res, err := s.CM.ClearUnused(c.Request().Context(), body.SpaceRequested, body.Location, body.Users, !body.Execute)
	if err != nil {
		return err
	}

	return c.JSON(http.StatusOK, res)
}

func (s *apiV1) handleOffloadContent(c echo.Context) error {
	cont, err := strconv.Atoi(c.Param("content"))
	if err != nil {
		return err
	}

	removed, err := s.CM.OffloadContents(c.Request().Context(), []uint{uint(cont)})
	if err != nil {
		return err
	}

	return c.JSON(http.StatusOK, map[string]interface{}{
		"blocksRemoved": removed,
	})
}

type moveContentBody struct {
	Contents    []uint `json:"contents"`
	Destination string `json:"destination"`
}

func (s *apiV1) handleMoveContent(c echo.Context) error {
	ctx := c.Request().Context()
	var body moveContentBody
	if err := c.Bind(&body); err != nil {
		return err
	}

	var contents []util.Content
	if err := s.DB.Find(&contents, "id in ?", body.Contents).Error; err != nil {
		return err
	}

	if len(contents) != len(body.Contents) {
		s.log.Warnf("got back fewer contents than requested: %d != %d", len(contents), len(body.Contents))
	}

	var shuttle model.Shuttle
	if err := s.DB.First(&shuttle, "handle = ?", body.Destination).Error; err != nil {
		return err
	}

	if err := s.shuttleMgr.ConsolidateContent(ctx, shuttle.Handle, contents); err != nil {
		return err
	}

	return c.JSON(http.StatusOK, map[string]string{})
}

func (s *apiV1) handleRefreshContent(c echo.Context) error {
	cont, err := strconv.Atoi(c.Param("content"))
	if err != nil {
		return err
	}

	if err := s.CM.RefreshContent(c.Request().Context(), uint(cont)); err != nil {
		return c.JSON(500, map[string]string{"error": err.Error()})
	}
	return c.JSON(http.StatusOK, map[string]string{})
}

func (s *apiV1) handleReadLocalContent(c echo.Context) error {
	cont, err := strconv.Atoi(c.Param("content"))
	if err != nil {
		return err
	}

	var content util.Content
	if err := s.DB.First(&content, "id = ?", cont).Error; err != nil {
		if xerrors.Is(err, gorm.ErrRecordNotFound) {
			return &util.HttpError{
				Code:    http.StatusNotFound,
				Reason:  util.ERR_RECORD_NOT_FOUND,
				Details: fmt.Sprintf("content: %d was not found", cont),
			}
		}
		return err
	}

	bserv := blockservice.New(s.Node.Blockstore, offline.Exchange(s.Node.Blockstore))
	dserv := merkledag.NewDAGService(bserv)

	ctx := context.Background()
	nd, err := dserv.Get(ctx, content.Cid.CID)
	if err != nil {
		return c.JSON(400, map[string]string{
			"error": err.Error(),
		})
	}
	r, err := uio.NewDagReader(ctx, nd, dserv)
	if err != nil {
		return c.JSON(400, map[string]string{
			"error": err.Error(),
		})
	}

	_, err = io.Copy(c.Response(), r)
	if err != nil {
		return err
	}
	return nil
}

func (s *apiV1) checkTokenAuth(token string) (*util.User, error) {
	cached, ok := s.cacher.Get(token)
	if ok && cached != nil {
		user, ok := cached.(*util.User)
		if !ok {
			return nil, xerrors.Errorf("value in user auth cache was not a user (got %T)", cached)
		}
		if user.AuthToken.Expiry.Before(time.Now()) {
			s.cacher.Remove(token)
		} else {
			return user, nil
		}
	}
	var authToken util.AuthToken
	tokenHash := util.GetTokenHash(token)
	if err := s.DB.First(&authToken, "token = ? OR token_hash = ?", token, tokenHash).Error; err != nil {
		if xerrors.Is(err, gorm.ErrRecordNotFound) {
			return nil, &util.HttpError{
				Code:    http.StatusUnauthorized,
				Reason:  util.ERR_INVALID_TOKEN,
				Details: "api key does not exist",
			}
		}
		return nil, err
	}

	if authToken.Expiry.Before(time.Now()) {
		return nil, &util.HttpError{
			Code:    http.StatusUnauthorized,
			Reason:  util.ERR_TOKEN_EXPIRED,
			Details: fmt.Sprintf("token for user %d expired %s", authToken.User, authToken.Expiry),
		}
	}

	var user util.User
	if err := s.DB.First(&user, "id = ?", authToken.User).Error; err != nil {
		if xerrors.Is(err, gorm.ErrRecordNotFound) {
			return nil, &util.HttpError{
				Code:    http.StatusUnauthorized,
				Reason:  util.ERR_INVALID_TOKEN,
				Details: "no user exists for the spicified api key",
			}
		}
		return nil, err
	}

	user.AuthToken = authToken
	s.cacher.Add(token, &user)
	return &user, nil
}

func (s *apiV1) AuthRequired(level int) echo.MiddlewareFunc {
	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c echo.Context) error {

			//	Check first if the Token is available. We should not continue if the
			//	token isn't even available.
			auth, err := util.ExtractAuth(c)
			if err != nil {
				return err
			}

			ctx, span := s.tracer.Start(c.Request().Context(), "authCheck")
			defer span.End()
			c.SetRequest(c.Request().WithContext(ctx))

			u, err := s.checkTokenAuth(auth)
			if err != nil {
				return err
			}

			span.SetAttributes(attribute.Int("user", int(u.ID)))

			if u.AuthToken.UploadOnly && level >= util.PermLevelUser {
				s.log.Warnw("api key is upload only", "user", u.ID, "perm", u.Perm, "required", level)

				return &util.HttpError{
					Code:    http.StatusForbidden,
					Reason:  util.ERR_NOT_AUTHORIZED,
					Details: "api key is upload only",
				}
			}

			if u.Perm >= level {
				c.Set("user", u)
				return next(c)
			}

			s.log.Warnw("user not authorized", "user", u.ID, "perm", u.Perm, "required", level)

			return &util.HttpError{
				Code:    http.StatusForbidden,
				Reason:  util.ERR_NOT_AUTHORIZED,
				Details: "user not authorized",
			}
		}
	}
}

type registerBody struct {
	Username   string `json:"username"`
	Password   string `json:"passwordHash"`
	InviteCode string `json:"inviteCode"`
}

const TOKEN_LABEL_ON_REGISTER = "on-register"

func (s *apiV1) handleRegisterUser(c echo.Context) error {
	var reg registerBody
	if err := c.Bind(&reg); err != nil {
		return err
	}

	var invite util.InviteCode
	if err := s.DB.First(&invite, "code = ?", reg.InviteCode).Error; err != nil {
		if xerrors.Is(err, gorm.ErrRecordNotFound) {
			return &util.HttpError{
				Code:    http.StatusNotFound,
				Reason:  util.ERR_INVALID_INVITE,
				Details: "no such invite code was found",
			}
		}
		return err
	}

	if invite.ClaimedBy != 0 {
		return &util.HttpError{
			Code:    http.StatusBadRequest,
			Reason:  util.ERR_INVITE_ALREADY_USED,
			Details: "the invite code as already been claimed",
		}
	}

	username := strings.ToLower(reg.Username)

	var exist *util.User
	if err := s.DB.First(&exist, "username = ?", username).Error; err != nil {
		if !xerrors.Is(err, gorm.ErrRecordNotFound) {
			return err
		}
		exist = nil
	}

	if exist != nil {
		return &util.HttpError{
			Code:    http.StatusBadRequest,
			Reason:  util.ERR_USERNAME_TAKEN,
			Details: "username already exist",
		}
	}

	salt := uuid.New().String()

	newUser := &util.User{
		Username: username,
		UUID:     uuid.New().String(),
		Salt:     salt,
		PassHash: util.GetPasswordHash(reg.Password, salt, s.DB.Config.Dialector.Name()),
		Perm:     util.PermLevelUser,
	}

	if err := s.DB.Create(newUser).Error; err != nil {
		return &util.HttpError{
			Code:   http.StatusInternalServerError,
			Reason: util.ERR_USER_CREATION_FAILED,
		}
	}

	authToken, err := s.newAuthTokenForUser(newUser, time.Now().Add(constants.TokenExpiryDurationRegister), nil, TOKEN_LABEL_ON_REGISTER)
	if err != nil {
		return err
	}

	invite.ClaimedBy = newUser.ID
	if err := s.DB.Save(&invite).Error; err != nil {
		return err
	}

	return c.JSON(http.StatusOK, &loginResponse{
		Token:  authToken.Token,
		Expiry: authToken.Expiry,
	})
}

type loginBody struct {
	Username string `json:"username"`
	Password string `json:"passwordHash"`
}

type loginResponse struct {
	Token  string    `json:"token"`
	Expiry time.Time `json:"expiry"`
}

const TOKEN_LABEL_ON_LOGIN = "on-login"

func (s *apiV1) handleLoginUser(c echo.Context) error {
	var body loginBody
	if err := c.Bind(&body); err != nil {
		return err
	}

	var user util.User
	if err := s.DB.First(&user, "username = ?", strings.ToLower(body.Username)).Error; err != nil {
		if xerrors.Is(err, gorm.ErrRecordNotFound) {
			return &util.HttpError{
				Code:    http.StatusForbidden,
				Reason:  util.ERR_USER_NOT_FOUND,
				Details: "no such user exist",
			}
		}
		return err
	}

	//	validate password
	//	SQLlite and Postgres has incompatibility in hashing and even though we are dropping support for sqlite later,
	//	we still need to accommodate those who chooses to use SQLite for experimentation purposes.
	var valid = true
	var dbDialect = s.DB.Config.Dialector.Name()

	//	check password hash (this is the way).
	if (user.Salt != "" && (user.PassHash != util.GetPasswordHash(body.Password, user.Salt, dbDialect))) || (user.Salt == "" && user.PassHash != body.Password) {
		valid = false                                                                           //	assume it's not valid.
		if bcrypt.CompareHashAndPassword([]byte(user.PassHash), []byte(body.Password)) == nil { //	we are using bcrypt, so we need to rehash it.
			valid = true
		}
	}

	if !valid {
		return &util.HttpError{
			Code:   http.StatusForbidden,
			Reason: util.ERR_INVALID_PASSWORD,
		}
	}

	authToken, err := s.newAuthTokenForUser(&user, time.Now().Add(constants.TokenExpiryDurationLogin), nil, TOKEN_LABEL_ON_LOGIN)
	if err != nil {
		return err
	}

	return c.JSON(http.StatusOK, &loginResponse{
		Token:  authToken.Token,
		Expiry: authToken.Expiry,
	})
}

type changePasswordParams struct {
	NewPassword string `json:"newPasswordHash"`
}

func (s *apiV1) handleUserChangePassword(c echo.Context, u *util.User) error {
	var params changePasswordParams
	if err := c.Bind(&params); err != nil {
		return err
	}

	salt := uuid.New().String()

	updatedUserColumns := &util.User{
		Salt:     salt,
		PassHash: util.GetPasswordHash(params.NewPassword, salt, s.DB.Config.Dialector.Name()),
	}

	if err := s.DB.Model(util.User{}).Where("id = ?", u.ID).Updates(updatedUserColumns).Error; err != nil {
		return err
	}

	return c.JSON(http.StatusOK, map[string]string{})
}

type changeAddressParams struct {
	Address string `json:"address"`
}

func (s *apiV1) handleUserChangeAddress(c echo.Context, u *util.User) error {
	var params changeAddressParams
	if err := c.Bind(&params); err != nil {
		return err
	}

	addr, err := address.NewFromString(params.Address)
	if err != nil {
		s.log.Warnf("invalid filecoin address in change address request body: %w", err)

		return &util.HttpError{
			Code:   http.StatusUnauthorized,
			Reason: "invalid address in request body",
		}
	}

	if err := s.DB.Model(util.User{}).Where("id = ?", u.ID).Update("address", addr.String()).Error; err != nil {
		return err
	}

	return c.JSON(http.StatusOK, map[string]string{})
}

type userStatsResponse struct {
	TotalSize int64 `json:"totalSize"`
	NumPins   int64 `json:"numPins"`
}

// handleGetUserStats godoc
// @Summary      Get stats for the current user
// @Description  This endpoint is used to get stats for the current user.
// @Tags         User
// @Produce      json
// @Success      200  {object}  string
// @Failure      400  {object}  util.HttpError
// @Failure      500  {object}  util.HttpError
// @Router       /user/stats [get]
func (s *apiV1) handleGetUserStats(c echo.Context, u *util.User) error {
	var stats userStatsResponse
	if err := s.DB.Raw(` SELECT
						(SELECT SUM(size) FROM contents where user_id = ? AND NOT aggregate AND active AND deleted_at IS NULL) as total_size,
						(SELECT COUNT(1) FROM contents where user_id = ? AND NOT aggregate AND active AND deleted_at IS NULL) as num_pins`,
		u.ID, u.ID).Scan(&stats).Error; err != nil {
		return err
	}

	return c.JSON(http.StatusOK, stats)
}

func (s *apiV1) newAuthTokenForUser(user *util.User, expiry time.Time, perms []string, label string) (*util.AuthToken, error) {
	if len(perms) > 1 {
		return nil, fmt.Errorf("invalid perms")
	}

	var uploadOnly bool
	if len(perms) == 1 {
		switch perms[0] {
		case "all":
			uploadOnly = false
		case "upload":
			uploadOnly = true
		default:
			return nil, fmt.Errorf("invalid perm: %q", perms[0])
		}
	}

	token := "EST" + uuid.New().String() + "ARY"
	authToken := &util.AuthToken{
		Token:      token,
		TokenHash:  util.GetTokenHash(token),
		Label:      label,
		User:       user.ID,
		Expiry:     expiry,
		UploadOnly: uploadOnly,
	}
	if err := s.DB.Create(authToken).Error; err != nil {
		return nil, err
	}

	return authToken, nil
}

// handleGetViewer godoc
// @Summary Fetch viewer details
// @Description This endpoint fetches viewer details such as username, permissions, address, owned miners, user settings etc.
// @Produce json
// @Success 200 {object} util.ViewerResponse
// @Failure 401 {object} util.HttpError
// @Failure 500 {object} util.HttpError
// @Router /viewer [get]
func (s *apiV1) handleGetViewer(c echo.Context, u *util.User) error {
	key := cacheKey(c, u)
	cached, ok := s.cacher.Get(key)
	if ok {
		return c.JSON(http.StatusOK, cached)
	}

	uep, err := s.shuttleMgr.GetPreferredUploadEndpoints(u)
	if err != nil {
		return err
	}

	viewer := &util.ViewerResponse{
		ID:       u.ID,
		Username: u.Username,
		Perms:    u.Perm,
		Address:  u.Address.Addr.String(),
		Miners:   s.getMinersOwnedByUser(u),
		Settings: util.UserSettings{
			Replication:           s.cfg.Replication,
			Verified:              s.cfg.Deal.IsVerified,
			DealDuration:          s.cfg.Deal.Duration,
			FileStagingThreshold:  s.cfg.Content.MinSize,
			ContentAddingDisabled: s.isContentAddingDisabled(u),
			DealMakingDisabled:    s.CM.DealMakingDisabled(),
			UploadEndpoints:       uep,
			Flags:                 u.Flags,
		},
		AuthExpiry: u.AuthToken.Expiry,
	}

	s.cacher.Add(key, viewer)
	return c.JSON(http.StatusOK, viewer)
}

func (s *apiV1) getMinersOwnedByUser(u *util.User) []string {
	var miners []model.StorageMiner
	if err := s.DB.Find(&miners, "owner = ?", u.ID).Error; err != nil {
		s.log.Errorf("failed to query miners for user %d: %s", u.ID, err)
		return nil
	}

	var out []string
	for _, m := range miners {
		out = append(out, m.Address.Addr.String())
	}

	return out
}

func (s *apiV1) handleHealth(c echo.Context) error {
	return c.JSON(http.StatusOK, map[string]string{
		"status": "ok",
	})
}

type getApiKeysResp struct {
	Token     string    `json:"token"`
	TokenHash string    `json:"tokenHash"`
	Label     string    `json:"label"`
	Expiry    time.Time `json:"expiry"`
}

// handleUserRevokeApiKey godoc
// @Summary      Revoke a User API Key.
// @Description  This endpoint is used to revoke a user API key. In estuary, every user is assigned with an API key, this API key is generated and issued for each user and is primarily used to access all estuary features. This endpoint can be used to revoke the API key that's assigned to the user. Revoked API keys are completely deleted and are not recoverable.
// @Tags         User
// @Produce      json
// @Success      200  {object}  string
// @Failure      400  {object}  util.HttpError
// @Failure      500  {object}  util.HttpError
// @Param        key_or_hash path string true "Key or Hash"
// @Router       /user/api-keys/{key_or_hash} [delete]
func (s *apiV1) handleUserRevokeApiKey(c echo.Context, u *util.User) error {
	kval := c.Param("key_or_hash")
	// need to check the kvalHash in case someone is revoking their token by the token itself, but only its hash is stored
	kvalHash := util.GetTokenHash(kval)
	if err := s.DB.Delete(&util.AuthToken{}, "\"user\" = ? AND (token = ? OR token_hash = ? OR token_hash = ?)", u.ID, kval, kval, kvalHash).Error; err != nil {
		return err
	}

	return c.NoContent(200)
}

// handleUserCreateApiKey godoc
// @Summary      Create API keys for a user
// @Description  This endpoint is used to create API keys for a user. In estuary, each user is given an API key to access all features.
// @Tags         User
// @Produce      json
// @Param        expiry  query     string  false  "Expiration - Expiration - Valid time units are ns, us (or µs),  ms,  s,  m,  h.  for  example  300h"
// @Param        perms   query     string  false  "Permissions -- currently unused"
// @Success      200     {object}  getApiKeysResp
// @Failure      400  {object}  util.HttpError
// @Failure      404     {object}  util.HttpError
// @Failure      500  {object}  util.HttpError
// @Router       /user/api-keys [post]
func (s *apiV1) handleUserCreateApiKey(c echo.Context, u *util.User) error {
	expiry := time.Now().Add(constants.TokenExpiryDurationDefault)
	if exp := c.QueryParam("expiry"); exp != "" {
		if exp == "false" {
			expiry = time.Now().Add(constants.TokenExpiryDurationPermanent)
		} else {
			dur, err := time.ParseDuration(exp)
			if err != nil {
				return err
			}
			expiry = time.Now().Add(dur)
		}
	}

	var perms []string
	if p := c.QueryParam("perms"); p != "" {
		perms = strings.Split(p, ",")
	}

	label := c.QueryParam("label")

	authToken, err := s.newAuthTokenForUser(u, expiry, perms, label)
	if err != nil {
		return err
	}

	return c.JSON(http.StatusOK, &getApiKeysResp{
		Token:     authToken.Token,
		TokenHash: authToken.TokenHash,
		Label:     authToken.Label,
		Expiry:    authToken.Expiry,
	})
}

// handleUserGetApiKeys godoc
// @Summary      Get API keys for a user
// @Description  This endpoint is used to get API keys for a user. In estuary, each user can be given multiple API keys (tokens). This endpoint can be used to retrieve all available API keys for a given user.
// @Tags         User
// @Produce      json
// @Success      200  {array}   getApiKeysResp
// @Failure      400  {object}  util.HttpError
// @Failure      404   {object}  util.HttpError
// @Failure      500  {object}  util.HttpError
// @Router       /user/api-keys [get]
func (s *apiV1) handleUserGetApiKeys(c echo.Context, u *util.User) error {
	var keys []util.AuthToken
	if err := s.DB.Find(&keys, "auth_tokens.user = ?", u.ID).Error; err != nil {
		return err
	}

	out := []getApiKeysResp{}
	for _, k := range keys {
		out = append(out, getApiKeysResp{
			Token:     k.Token,
			TokenHash: k.TokenHash,
			Label:     k.Label,
			Expiry:    k.Expiry,
		})
	}

	return c.JSON(http.StatusOK, out)
}

type createCollectionBody struct {
	Name        string `json:"name"`
	Description string `json:"description"`
}

// handleCreateCollection godoc
// @Summary      Create a new collection
// @Description  This endpoint is used to create a new collection. A collection is a representaion of a group of objects added on the estuary. This endpoint can be used to create a new collection.
// @Tags         collections
// @Produce      json
// @Param        body  body      createCollectionBody  true  "Collection name and description"
// @Success      200   {object}  collections.Collection
// @Failure      400  {object}  util.HttpError
// @Failure      404  {object}  util.HttpError
// @Failure      500  {object}  util.HttpError
// @Router       /collections/ [post]
func (s *apiV1) handleCreateCollection(c echo.Context, u *util.User) error {
	var body createCollectionBody
	if err := c.Bind(&body); err != nil {
		return err
	}

	col := &collections.Collection{
		UUID:        uuid.New().String(),
		Name:        body.Name,
		Description: body.Description,
		UserID:      u.ID,
	}

	if err := s.DB.Create(col).Error; err != nil {
		return err
	}

	return c.JSON(http.StatusOK, col)
}

// handleListCollections godoc
// @Summary      List all collections
// @Description  This endpoint is used to list all collections. Whenever a user logs on estuary, it will list all collections that the user has access to. This endpoint provides a way to list all collections to the user.
// @Tags         collections
// @Produce      json
// @Success      200  {array}   collections.Collection
// @Failure      400  {object}  util.HttpError
// @Failure      404  {object}  util.HttpError
// @Failure      500  {object}  util.HttpError
// @Router       /collections/ [get]
func (s *apiV1) handleListCollections(c echo.Context, u *util.User) error {
	var cols []collections.Collection
	if err := s.DB.Find(&cols, "user_id = ?", u.ID).Error; err != nil {
		return err
	}

	return c.JSON(http.StatusOK, cols)
}

type addContentsToCollectionBody struct {
	ContentIDs []uint `json:"contentids"`
}

// handleAddContentsToCollection godoc
// @Summary      Add contents to a collection
// @Description  This endpoint adds already-pinned contents (that have ContentIDs) to a collection.
// @Tags         collections
// @Accept       json
// @Produce      json
// @Param        coluuid     path      string  true  "Collection UUID"
// @Param        contentIDs  body      []uint  true  "Content IDs to add to collection"
// @Param		 dir		 query	   string  false  "Directory"
// @Success      200         {object}  string
// @Failure      400  {object}  util.HttpError
// @Failure      500  {object}  util.HttpError
// @Router       /collections/{coluuid} [post]
func (s *apiV1) handleAddContentsToCollection(c echo.Context, u *util.User) error {
	coluuid := c.Param("coluuid")

	// we accept both {"contentids": [1, 2]} and [1, 2] as json payloads
	var body addContentsToCollectionBody // {"contentids": [1, 2]}
	var contentIDs []uint                // [1, 2]

	// Save the body of the request so that we can try to unmarshal it twice (if first bind fails)
	// We can't simply c.Bind() because that reads directly from the socket
	// And so if we try c.Bind()ing twice, the second call will just return EOF
	bodyBytes, err := ioutil.ReadAll(c.Request().Body)
	if err != nil {
		return err
	}

	if err = json.Unmarshal([]byte(bodyBytes), &contentIDs); err != nil {
		// Failed to bind to [1, 10] payload, try {"contentids": [1, 10]}
		if err = json.Unmarshal([]byte(bodyBytes), &body); err != nil {
			return err
		}
		contentIDs = body.ContentIDs
	}

	// no contents
	if len(contentIDs) == 0 {
		return &util.HttpError{
			Code:    http.StatusBadRequest,
			Reason:  util.ERR_INVALID_INPUT,
			Details: fmt.Sprintf("no contents specified, need at least one"),
		}
	}

	if len(contentIDs) > 128 {
		return &util.HttpError{
			Code:    http.StatusBadRequest,
			Reason:  util.ERR_INVALID_INPUT,
			Details: fmt.Sprintf("too many contents specified: %d (max 128)", len(contentIDs)),
		}
	}

	var col collections.Collection
	if err := s.DB.First(&col, "uuid = ?", coluuid).Error; err != nil {
		if xerrors.Is(err, gorm.ErrRecordNotFound) {
			return &util.HttpError{
				Code:    http.StatusNotFound,
				Reason:  util.ERR_RECORD_NOT_FOUND,
				Details: fmt.Sprintf("collection: %s was not found", coluuid),
			}
		}
		return err
	}

	if err := util.IsCollectionOwner(u.ID, col.UserID); err != nil {
		return err
	}

	var contents []util.Content
	if err := s.DB.Find(&contents, "id in ? and user_id = ?", contentIDs, u.ID).Error; err != nil {
		return err
	}

	if len(contents) != len(contentIDs) {
		return fmt.Errorf("%d specified content(s) were not found or user missing permissions", len(contentIDs)-len(contents))
	}

	path, err := constructDirectoryPath(c.QueryParam(ColDir))
	var colrefs []collections.CollectionRef
	for _, cont := range contents {
		fullPath := filepath.Join(path, cont.Name)
		colrefs = append(colrefs, collections.CollectionRef{
			Collection: col.ID,
			Content:    cont.ID,
			Path:       &fullPath,
		})
	}

	if err := s.DB.Create(colrefs).Error; err != nil {
		return err
	}

	return c.JSON(http.StatusOK, map[string]string{})
}

// handleCommitCollection godoc
// @Summary      Produce a CID of the collection contents
// @Description  This endpoint is used to save the contents in a collection, producing a top-level CID that references all the current CIDs in the collection.
// @Param        coluuid  path  string  true  "coluuid"
// @Tags         collections
// @Produce      json
// @Success      200  {object}  string
// @Failure      400  {object}  util.HttpError
// @Failure      500  {object}  util.HttpError
// @Router       /collections/{coluuid}/commit [post]
func (s *apiV1) handleCommitCollection(c echo.Context, u *util.User) error {
	colid := c.Param("coluuid")

	var col collections.Collection
	if err := s.DB.First(&col, "uuid = ?", colid).Error; err != nil {
		if xerrors.Is(err, gorm.ErrRecordNotFound) {
			return &util.HttpError{
				Code:    http.StatusNotFound,
				Reason:  util.ERR_RECORD_NOT_FOUND,
				Details: fmt.Sprintf("collection: %s was not found", colid),
			}
		}
		return err
	}

	if err := util.IsCollectionOwner(u.ID, col.UserID); err != nil {
		return err
	}

	contents := []util.ContentWithPath{}
	if err := s.DB.Model(collections.CollectionRef{}).
		Where("collection = ?", col.ID).
		Joins("left join contents on contents.id = collection_refs.content").
		Select("contents.*, collection_refs.path").
		Scan(&contents).Error; err != nil {
		return err
	}

	// transform listen addresses (/ip/1.2.3.4/tcp/80) into full p2p multiaddresses
	// e.g. /ip/1.2.3.4/tcp/80/p2p/12D3KooWCVTKbuvrZ9ton6zma5LNhCEeZyuFtxcDzDTmWh2qPtWM
	fullP2pMultiAddrs := []multiaddr.Multiaddr{}
	for _, listenAddr := range s.Node.Host.Addrs() {
		fullP2pAddr := fmt.Sprintf("%s/p2p/%s", listenAddr, s.Node.Host.ID())
		fullP2pMultiAddr, err := multiaddr.NewMultiaddr(fullP2pAddr)
		if err != nil {
			return err
		}
		fullP2pMultiAddrs = append(fullP2pMultiAddrs, fullP2pMultiAddr)
	}

	// transform multiaddresses into AddrInfo objects
	var origins []*peer.AddrInfo
	for _, p := range fullP2pMultiAddrs {
		ai, err := peer.AddrInfoFromP2pAddr(p)
		if err != nil {
			return err
		}
		origins = append(origins, ai)
	}

	bserv := blockservice.New(s.Node.Blockstore, nil)
	dserv := merkledag.NewDAGService(bserv)

	// create DAG respecting directory structure
	collectionNode := unixfs.EmptyDirNode()
	for _, c := range contents {
		dirs, err := util.DirsFromPath(c.Path, c.Name)
		if err != nil {
			return err
		}

		lastDirNode, err := util.EnsurePathIsLinked(dirs, collectionNode, dserv)
		if err != nil {
			return err
		}
		err = lastDirNode.AddRawLink(c.Name, &ipld.Link{
			Size: uint64(c.Size),
			Cid:  c.Cid.CID,
		})
		if err != nil {
			return err
		}
	}

	if err := dserv.Add(context.Background(), collectionNode); err != nil {
		return err
	} // add new CID to local blockstore

	// update DB with new collection CID
	col.CID = collectionNode.Cid().String()
	if err := s.DB.Model(collections.Collection{}).Where("id = ?", col.ID).UpdateColumn("c_id", collectionNode.Cid().String()).Error; err != nil {
		return err
	}

	ctx := c.Request().Context()
	makeDeal := false

	pinstatus, pinOp, err := s.CM.PinContent(ctx, u.ID, collectionNode.Cid(), collectionNode.Cid().String(), nil, origins, 0, nil, makeDeal)
	if err != nil {
		return err
	}
	s.pinMgr.Add(pinOp)

	return c.JSON(http.StatusOK, pinstatus)
}

// handleGetCollectionContents godoc
// @Summary      Get contents in a collection
// @Description  This endpoint is used to get contents in a collection. If no colpath query param is passed
// @Tags         collections
// @Produce      json
// @Success      200  {object}  []collections.CollectionListResponse
// @Failure      400  {object}  util.HttpError
// @Failure      500  {object}  util.HttpError
// @Param        coluuid  path      string  true   "coluuid"
// @Param        dir      query     string  false  "Directory"
// @Router       /collections/{coluuid} [get]
func (s *apiV1) handleGetCollectionContents(c echo.Context, u *util.User) error {
	coluuid := c.Param("coluuid")

	var col collections.Collection
	if err := s.DB.First(&col, "uuid = ?", coluuid).Error; err != nil {
		if xerrors.Is(err, gorm.ErrRecordNotFound) {
			return &util.HttpError{
				Code:    http.StatusNotFound,
				Reason:  util.ERR_RECORD_NOT_FOUND,
				Details: fmt.Sprintf("collection: %s was not found", coluuid),
			}
		}
		return err
	}

	if err := util.IsCollectionOwner(u.ID, col.UserID); err != nil {
		return err
	}

	// TODO: optimize this a good deal
	var refs []util.ContentWithPath
	if err := s.DB.Model(collections.CollectionRef{}).
		Where("collection = ?", col.ID).
		Joins("left join contents on contents.id = collection_refs.content").
		Select("contents.*, collection_refs.path as path").
		Scan(&refs).Error; err != nil {
		return err
	}

	queryDir := c.QueryParam(ColDir)
	if queryDir == "" {
		return c.JSON(http.StatusOK, refs)
	}

	// if queryDir is set, do the content listing
	queryDir = filepath.Clean(queryDir)

	out, err := collections.GetDirectoryContents(refs, queryDir, coluuid)
	if err != nil {
		return err
	}

	return c.JSON(http.StatusOK, out)
}

// handleDeleteCollection godoc
// @Summary      Deletes a collection
// @Description  This endpoint is used to delete an existing collection.
// @Tags         collections
// @Param        coluuid  path  string  true  "Collection ID"
// @Router       /collections/{coluuid} [delete]
// @Success      200  {string}  string  ""
// @Failure      400  {object}  util.HttpError
// @Failure      500  {object}  util.HttpError
func (s *apiV1) handleDeleteCollection(c echo.Context, u *util.User) error {
	coluuid := c.Param("coluuid")

	var col collections.Collection
	if err := s.DB.First(&col, "uuid = ?", coluuid).Error; err != nil {
		if xerrors.Is(err, gorm.ErrRecordNotFound) {
			return &util.HttpError{
				Code:    http.StatusNotFound,
				Reason:  util.ERR_RECORD_NOT_FOUND,
				Details: fmt.Sprintf("collection with ID(%s) was not found", coluuid),
			}
		}
		return err
	}

	if err := util.IsCollectionOwner(u.ID, col.UserID); err != nil {
		return err
	}

	if err := s.DB.Delete(&col).Error; err != nil {
		return err
	}
	return c.NoContent(http.StatusOK)
}

type deleteContentFromCollectionBody struct {
	By    string `json:"by"`
	Value string `json:"value"`
}

// handleDeleteContentFromCollection godoc
// @Summary      Deletes a content from a collection
// @Description  This endpoint is used to delete an existing content from an existing collection. If two or more files with the same contentid exist in the collection, delete the one in the specified path
// @Tags         collections
// @Param        coluuid    path  string                           true  "Collection ID"
// @Param        body       body  deleteContentFromCollectionBody  true  "Variable to use when filtering for files (must be either 'path' or 'content_id')"
// @Produce      json
// @Success      200  {object}  string
// @Failure      400  {object}  util.HttpError
// @Failure      500  {object}  util.HttpError
// @Router       /collections/{coluuid}/contents [delete]
func (s *apiV1) handleDeleteContentFromCollection(c echo.Context, u *util.User) error {
	var body deleteContentFromCollectionBody
	if err := c.Bind(&body); err != nil {
		return err
	}
	coluuid := c.Param("coluuid")

	// check if 'by' is either 'path' or 'content_id'
	if body.By != "path" && body.By != "content_id" {
		return &util.HttpError{
			Code:    http.StatusNotFound,
			Reason:  util.ERR_INVALID_FILTER,
			Details: fmt.Sprintf("invalid 'by' value, must be either 'content_id' or 'path', got %s", body.By),
		}
	}

	if len(body.Value) == 0 {
		return &util.HttpError{
			Code:    http.StatusNotFound,
			Reason:  util.ERR_VALUE_REQUIRED,
			Details: fmt.Sprintf("invalid 'value' field, must not be empty"),
		}
	}

	col, err := collections.GetCollection(coluuid, s.DB, u)
	if err != nil {
		return err
	}

	refs := []collections.CollectionRef{}
	if body.By == "path" {
		path := body.Value
		refs, err = collections.GetContentsInPath(coluuid, path, s.DB, u)
		if err != nil {
			return err
		}
	} else if body.By == "content_id" {
		contentid := body.Value
		content, err := util.GetContent(contentid, s.DB, u)
		if err != nil {
			return err
		}
		if err := s.DB.Model(collections.CollectionRef{}).
			Where("collection = ?", col.ID).
			Where("content = ?", content.ID).
			Scan(&refs).Error; err != nil {
			return err
		}
	}

	// delete found refs
	if err := s.DB.Delete(&refs).Error; err != nil {
		return err
	}
	return c.NoContent(http.StatusOK)
}

type adminUserResponse struct {
	Id       uint   `json:"id"`
	Username string `json:"username"`

	SpaceUsed int `json:"spaceUsed"`
	NumFiles  int `json:"numFiles"`
}

// handleAdminGetUsers godoc
// @Summary      Get all users
// @Description  This endpoint is used to get all users.
// @Tags         admin
// @Produce      json
// @Success      200  {object}  string
// @Failure      400  {object}  util.HttpError
// @Failure      500  {object}  util.HttpError
// @Router       /admin/users [get]
func (s *apiV1) handleAdminGetUsers(c echo.Context) error {
	var resp []adminUserResponse
	if err := s.DB.Model(util.Content{}).
		Select("user_id as id,(?) as username,SUM(size) as space_used,count(*) as num_files", s.DB.Model(&util.User{}).Select("username").Where("id = user_id")).
		Group("user_id").Scan(&resp).Error; err != nil {
		return err
	}

	sort.Slice(resp, func(i, j int) bool {
		return resp[i].Id < resp[j].Id
	})

	return c.JSON(http.StatusOK, resp)
}

type publicStatsResponse struct {
	TotalStorage       sql.NullInt64 `json:"totalStorage"`
	TotalFilesStored   sql.NullInt64 `json:"totalFiles"`
	DealsOnChain       sql.NullInt64 `json:"dealsOnChain"`
	TotalObjectsRef    sql.NullInt64 `json:"totalObjectsRef"`
	TotalBytesUploaded sql.NullInt64 `json:"totalBytesUploaded"`
	TotalUsers         sql.NullInt64 `json:"totalUsers"`
	TotalStorageMiner  sql.NullInt64 `json:"totalStorageMiners"`
}

// handlePublicStats godoc
// @Summary      Public stats
// @Description  This endpoint is used to get public stats.
// @Tags         public
// @Produce      json
// @Success      200  {object}  string
// @Failure      400  {object}  util.HttpError
// @Failure      500  {object}  util.HttpError
// @Router       /public/stats [get]
func (s *apiV1) handlePublicStats(c echo.Context) error {
	key := cacheKey(c, nil)
	val, ok := s.cacher.Get(key)
	if !ok {
		computedVal, err := s.computePublicStats()
		val = computedVal
		if err != nil {
			return err
		}
		s.cacher.Add(key, val)
	}

	keyExt := cacheKey(c, nil, "ext")
	valExt, ok := s.extendedCacher.Get(keyExt)
	if !ok {
		computedValExt, err := s.computePublicStatsWithExtensiveLookups()
		valExt = computedValExt
		if err != nil {
			return err
		}
		s.extendedCacher.Add(keyExt, valExt)
	}

	// reuse the original stats and add the ones from the extensive lookup function.
	val.(*publicStatsResponse).TotalObjectsRef = valExt.(*publicStatsResponse).TotalObjectsRef
	val.(*publicStatsResponse).TotalBytesUploaded = valExt.(*publicStatsResponse).TotalBytesUploaded
	val.(*publicStatsResponse).TotalUsers = valExt.(*publicStatsResponse).TotalUsers
	val.(*publicStatsResponse).TotalStorageMiner = valExt.(*publicStatsResponse).TotalStorageMiner

	jsonResponse := map[string]interface{}{
		"totalStorage":       val.(*publicStatsResponse).TotalStorage.Int64,
		"totalFilesStored":   val.(*publicStatsResponse).TotalFilesStored.Int64,
		"dealsOnChain":       val.(*publicStatsResponse).DealsOnChain.Int64,
		"totalObjectsRef":    val.(*publicStatsResponse).TotalObjectsRef.Int64,
		"totalBytesUploaded": val.(*publicStatsResponse).TotalBytesUploaded.Int64,
		"totalUsers":         val.(*publicStatsResponse).TotalUsers.Int64,
		"totalStorageMiner":  val.(*publicStatsResponse).TotalStorageMiner.Int64,
	}

	return c.JSON(http.StatusOK, jsonResponse)
}

func (s *apiV1) computePublicStats() (*publicStatsResponse, error) {
	var stats publicStatsResponse
	if err := s.DB.Model(util.Content{}).Where("active and not aggregated_in > 0").Select("SUM(size) as total_storage").Scan(&stats).Error; err != nil {
		return nil, err
	}

	if err := s.DB.Model(util.Content{}).Where("active and not aggregate").Count(&stats.TotalFilesStored.Int64).Error; err != nil {
		return nil, err
	}

	if err := s.DB.Model(model.ContentDeal{}).Where("not failed and deal_id > 0").Count(&stats.DealsOnChain.Int64).Error; err != nil {
		return nil, err
	}

	return &stats, nil
}

func (s *apiV1) computePublicStatsWithExtensiveLookups() (*publicStatsResponse, error) {
	var stats publicStatsResponse

	//	this can be resource expensive but we are already caching it.
	if err := s.DB.Table("obj_refs").Count(&stats.TotalObjectsRef.Int64).Error; err != nil {
		return nil, err
	}

	if err := s.DB.Table("objects").Select("SUM(size)").Find(&stats.TotalBytesUploaded.Int64).Error; err != nil {
		return nil, err
	}

	if err := s.DB.Model(util.User{}).Count(&stats.TotalUsers.Int64).Error; err != nil {
		return nil, err
	}

	if err := s.DB.Table("storage_miners").Count(&stats.TotalStorageMiner.Int64).Error; err != nil {
		return nil, err
	}

	return &stats, nil
}

// handleGetStagingZonesForUser godoc
// @Summary      Get staging zone for user, excluding its contents
// @Description  This endpoint is used to get staging zone for user, excluding its contents.
// @Tags         content
// @Produce      json
// @Success      200  {object}  string
// @Failure      400  {object}  util.HttpError
// @Failure      500  {object}  util.HttpError
// @Router       /content/staging-zones [get]
func (s *apiV1) handleGetStagingZonesForUser(c echo.Context, u *util.User) error {
	limit, offset, err := s.getLimitAndOffset(c, 500, 0)
	if err != nil {
		return err
	}

	zones, err := s.CM.GetStagingZonesForUser(c.Request().Context(), u.ID, limit, offset)
	if err != nil {
		return err
	}
	return c.JSON(http.StatusOK, zones)
}

// handleGetStagingZoneWithoutContents godoc
// @Summary      Get staging zone without its contents field populated
// @Description  This endpoint is used to get a staging zone, excluding its contents.
// @Tags         content
// @Produce      json
// @Success      200  {object}  string
// @Failure      400  {object}  util.HttpError
// @Failure      500  {object}  util.HttpError
// @Param        staging_zone   path      int  true  "Staging Zone Content ID"
// @Router       /content/staging-zones/{staging_zone} [get]
func (s *apiV1) handleGetStagingZoneWithoutContents(c echo.Context, u *util.User) error {
	zoneID, err := strconv.Atoi(c.Param("staging_zone"))
	if err != nil {
		return err
	}
	zone, err := s.CM.GetStagingZoneWithoutContents(c.Request().Context(), u.ID, uint(zoneID))
	if err != nil {
		return err
	}
	return c.JSON(http.StatusOK, zone)
}

// handleGetStagingZoneContents godoc
// @Summary      Get contents for a staging zone
// @Description  This endpoint is used to get the contents for a staging zone
// @Tags         content
// @Produce      json
// @Success      200  {object}  string
// @Failure      400  {object}  util.HttpError
// @Failure      500  {object}  util.HttpError
// @Param        staging_zone   path      int  true  "Staging Zone Content ID"
// @Param        limit   query  string  true  "limit"
// @Param        offset  query  string  true  "offset"
// @Router       /content/staging-zones/{staging_zone}/contents [get]
func (s *apiV1) handleGetStagingZoneContents(c echo.Context, u *util.User) error {
	zoneID, err := strconv.Atoi(c.Param("staging_zone"))
	if err != nil {
		return err
	}

	limit, offset, err := s.getLimitAndOffset(c, 500, 0)
	if err != nil {
		return err
	}

	contents, err := s.CM.GetStagingZoneContents(c.Request().Context(), u.ID, uint(zoneID), limit, offset)
	if err != nil {
		return err
	}
	return c.JSON(http.StatusOK, contents)
}

// handleUserExportData godoc
// @Summary      Export user data
// @Description  This endpoint is used to get API keys for a user.
// @Tags         User
// @Produce      json
// @Success      200  {object}  string
// @Failure      400  {object}  util.HttpError
// @Failure      500  {object}  util.HttpError
// @Router       /user/export [get]
func (s *apiV1) handleUserExportData(c echo.Context, u *util.User) error {
	export, err := s.exportUserData(u.ID)
	if err != nil {
		return err
	}

	return c.JSON(http.StatusOK, export)
}

// handleNetPeers godoc
// @Summary      Net Peers
// @Description  This endpoint is used to get net peers
// @Tags         public,net
// @Produce      json
// @Success      200  {array}   string
// @Failure      400  {object}  util.HttpError
// @Failure      500  {object}  util.HttpError
// @Router       /public/net/peers [get]
func (s *apiV1) handleNetPeers(c echo.Context) error {
	return c.JSON(http.StatusOK, s.Node.Host.Network().Peers())
}

// handleNetAddrs godoc
// @Summary      Net Addrs
// @Description  This endpoint is used to get net addrs
// @Tags         public,net
// @Produce      json
// @Success      200  {array}  string
// @Router       /public/net/addrs [get]
func (s *apiV1) handleNetAddrs(c echo.Context) error {
	id := s.Node.Host.ID()
	addrs := s.Node.Host.Addrs()

	return c.JSON(http.StatusOK, map[string]interface{}{
		"id":        id,
		"addresses": addrs,
	})
}

type dealMetricsInfo struct {
	Time              time.Time `json:"time"`
	DealsOnChain      int       `json:"dealsOnChain"`
	DealsOnChainBytes int64     `json:"dealsOnChainBytes"`
	DealsAttempted    int       `json:"dealsAttempted"`
	DealsSealed       int       `json:"dealsSealed"`
	DealsSealedBytes  int64     `json:"dealsSealedBytes"`
	DealsFailed       int       `json:"dealsFailed"`
}

type metricsDealJoin struct {
	CreatedAt        time.Time `json:"created_at"`
	Failed           bool      `json:"failed"`
	FailedAt         time.Time `json:"failed_at"`
	DealID           int64     `json:"deal_id"`
	Size             int64     `json:"size"`
	TransferStarted  time.Time `json:"transferStarted"`
	TransferFinished time.Time `json:"transferFinished"`
	OnChainAt        time.Time `json:"onChainAt"`
	SealedAt         time.Time `json:"sealedAt"`
}

// handleMetricsDealOnChain godoc
// @Summary      Get deal metrics
// @Description  This endpoint is used to get deal metrics
// @Tags         public,metrics
// @Produce      json
// @Success      200  {object}  string
// @Failure      400  {object}  util.HttpError
// @Failure      500  {object}  util.HttpError
// @Router       /public/metrics/deals-on-chain [get]
func (s *apiV1) handleMetricsDealOnChain(c echo.Context) error {
	key := cacheKey(c, nil)
	cached, ok := s.extendedCacher.Get(key)
	if ok {
		return c.JSON(http.StatusOK, cached)
	}
	val, err := s.computeDealMetrics()
	if err != nil {
		return err
	}
	//	Make sure we don't return a nil val.
	if val == nil {
		val = []*dealMetricsInfo{}
	}

	s.extendedCacher.Add(key, val)
	return c.JSON(http.StatusOK, val)
}

func (s *apiV1) computeDealMetrics() ([]*dealMetricsInfo, error) {
	var deals []*metricsDealJoin
	if err := s.DB.Model(model.ContentDeal{}).
		Joins("left join contents on content_deals.content = contents.id").
		Select("content_deals.failed as failed, failed_at, deal_id, size, transfer_started, transfer_finished, on_chain_at, sealed_at").
		Scan(&deals).Error; err != nil {
		return nil, err
	}

	coll := make(map[time.Time]*dealMetricsInfo)
	onchainbuckets := make(map[time.Time][]*metricsDealJoin)
	attempts := make(map[time.Time][]*metricsDealJoin)
	sealed := make(map[time.Time][]*metricsDealJoin)
	beginning := time.Now().Add(time.Hour * -100000)
	failed := make(map[time.Time][]*metricsDealJoin)

	for _, d := range deals {
		created := d.CreatedAt.Round(time.Hour * 24)
		attempts[created] = append(attempts[created], d)

		if !(d.DealID == 0 || d.Failed) {
			if d.OnChainAt.Before(beginning) {
				d.OnChainAt = time.Time{}
			}

			btime := d.OnChainAt.Round(time.Hour * 24)
			onchainbuckets[btime] = append(onchainbuckets[btime], d)
		}

		if d.SealedAt.After(beginning) {
			sbuck := d.SealedAt.Round(time.Hour * 24)
			sealed[sbuck] = append(sealed[sbuck], d)
		}

		if d.Failed {
			fbuck := d.FailedAt.Round(time.Hour * 24)
			failed[fbuck] = append(failed[fbuck], d)
		}
	}

	for bt, deals := range onchainbuckets {
		dmi := &dealMetricsInfo{
			Time:         bt,
			DealsOnChain: len(deals),
		}
		for _, d := range deals {
			dmi.DealsOnChainBytes += d.Size
		}

		coll[bt] = dmi
	}

	for bt, deals := range attempts {
		dmi, ok := coll[bt]
		if !ok {
			dmi = &dealMetricsInfo{
				Time: bt,
			}
			coll[bt] = dmi
		}

		dmi.DealsAttempted = len(deals)
	}

	for bt, deals := range sealed {
		dmi, ok := coll[bt]
		if !ok {
			dmi = &dealMetricsInfo{
				Time: bt,
			}
			coll[bt] = dmi
		}

		dmi.DealsSealed = len(deals)
		for _, d := range deals {
			dmi.DealsSealedBytes += d.Size
		}
	}

	for bt, deals := range failed {
		dmi, ok := coll[bt]
		if !ok {
			dmi = &dealMetricsInfo{
				Time: bt,
			}
			coll[bt] = dmi
		}

		dmi.DealsFailed = len(deals)
	}

	var out []*dealMetricsInfo
	for _, dmi := range coll {
		out = append(out, dmi)
	}

	sort.Slice(out, func(i, j int) bool {
		return out[i].Time.Before(out[j].Time)
	})

	return out, nil
}

type dealQuery struct {
	DealID    int64
	Contentid uint
	Cid       util.DbCID
	Aggregate bool
}

type dealPairs struct {
	Deals []int64   `json:"deals"`
	Cids  []cid.Cid `json:"cids"`
}

// handleGetAllDealsForUser godoc
// @Summary      Get all deals for a user
// @Description  This endpoint is used to get all deals for a user
// @Tags         content
// @Produce      json
// @Success      200  {object}  string
// @Failure      400  {object}  util.HttpError
// @Failure      500  {object}  util.HttpError
// @Param        begin     query     string  true  "Begin"
// @Param        duration  query     string  true  "Duration"
// @Param        all       query     string  true  "All"
// @Router       /content/all-deals [get]
func (s *apiV1) handleGetAllDealsForUser(c echo.Context, u *util.User) error {

	begin := time.Now().Add(time.Hour * 24)
	duration := time.Hour * 24

	if beg := c.QueryParam("begin"); beg != "" {
		ts, err := time.Parse("2006-01-02T15:04", beg)
		if err != nil {
			return err
		}
		begin = ts
	}

	if dur := c.QueryParam("duration"); dur != "" {
		dur, err := time.ParseDuration(dur)
		if err != nil {
			return err
		}

		duration = dur
	}

	all := c.QueryParam("all") != ""

	var deals []dealQuery
	if err := s.DB.Model(model.ContentDeal{}).
		Where("deal_id > 0 AND (? OR (on_chain_at >= ? AND on_chain_at <= ?)) AND content_deals.user_id = ?", all, begin, begin.Add(duration), u.ID).
		Joins("left join contents on content_deals.content = contents.id").
		Select("deal_id, contents.id as contentid, cid, aggregate").
		Scan(&deals).Error; err != nil {
		return err
	}

	contmap := make(map[uint][]dealQuery, len(deals))
	for _, d := range deals {
		contmap[d.Contentid] = append(contmap[d.Contentid], d)
	}

	out := make([]dealPairs, 0)
	for cont, deals := range contmap {
		var dp dealPairs
		if deals[0].Aggregate {
			var conts []util.Content
			if err := s.DB.Model(util.Content{}).Where("aggregated_in = ?", cont).Select("cid").Scan(&conts).Error; err != nil {
				return err
			}

			for _, c := range conts {
				dp.Cids = append(dp.Cids, c.Cid.CID)
			}
		} else {
			dp.Cids = []cid.Cid{deals[0].Cid.CID}
		}

		for _, d := range deals {
			dp.Deals = append(dp.Deals, d.DealID)
		}
		out = append(out, dp)
	}

	return c.JSON(http.StatusOK, out)
}

type setDealMakingBody struct {
	Enabled bool `json:"enabled"`
}

func (s *apiV1) handleSetDealMaking(c echo.Context) error {
	var body setDealMakingBody
	if err := c.Bind(&body); err != nil {
		return err
	}

	s.CM.SetDealMakingEnabled(body.Enabled)
	return c.JSON(http.StatusOK, map[string]string{})
}

func (s *apiV1) handleContentHealthCheck(c echo.Context) error {
	ctx := c.Request().Context()
	val, err := strconv.Atoi(c.Param("id"))
	if err != nil {
		return err
	}

	var cont util.Content
	if err := s.DB.First(&cont, "id = ?", val).Error; err != nil {
		if xerrors.Is(err, gorm.ErrRecordNotFound) {
			return &util.HttpError{
				Code:    http.StatusNotFound,
				Reason:  util.ERR_RECORD_NOT_FOUND,
				Details: fmt.Sprintf("content: %d was not found", val),
			}
		}
		return err
	}

	var u util.User
	if err := s.DB.First(&u, "id = ?", cont.UserID).Error; err != nil {
		return err
	}

	var deals []model.ContentDeal
	if err := s.DB.Find(&deals, "content = ? and not failed", cont.ID).Error; err != nil {
		return err
	}

	var aggr []util.Content
	if err := s.DB.Find(&aggr, "aggregated_in = ?", cont.ID).Error; err != nil {
		return err
	}

	var aggrLocs map[string]string
	for _, child := range aggr {
		aggrLocs[child.Location] = child.Location
	}

	var fixedAggregateSize bool
	if cont.Aggregate && cont.Size == 0 && cont.Active {
		// if this is an active aggregate and its size is zero, then that means we
		// failed at some point while updating the aggregate, we can fix that

		switch len(aggrLocs) {
		case 0:
			s.log.Warnf("content %d has nothing aggregated in it", cont.ID)
		case 1:
			var aggrLoc string
			for loc := range aggrLocs {
				aggrLoc = loc
				break
			}

			var zone *model.StagingZone
			if err := s.DB.First(&zone, "cont_id = ?", cont.AggregatedIn).Error; err != nil {
				if errors.Is(err, gorm.ErrRecordNotFound) {
					s.log.Errorf("content %d's aggregatedIn zone %d not found in DB", cont.ID, cont.AggregatedIn)
				}
				return err
			}

			if err := s.CM.AggregateStagingZone(ctx, zone, cont, aggrLoc); err != nil {
				return err
			}
			fixedAggregateSize = true

		default:
			// well that sucks, this will need migration
			s.log.Warnf("content %d has messed up aggregation", cont.ID)
		}
	}

	if cont.Location != constants.ContentLocationLocal {
		return c.JSON(http.StatusOK, map[string]interface{}{
			"deals":              deals,
			"content":            cont,
			"error":              "requested content was not local to this instance, cannot check health right now",
			"fixedAggregateSize": fixedAggregateSize,
		})
	}

	_, rootFetchErr := s.Node.Blockstore.Get(ctx, cont.Cid.CID)
	if rootFetchErr != nil {
		s.log.Errorf("failed to fetch root: %s", rootFetchErr)
	}

	if cont.Aggregate && rootFetchErr != nil {
		// if this is an aggregate and we dont have the root, thats funky, but we can regenerate the root
		nd, err := s.CM.CreateAggregate(ctx, aggr)
		if err != nil {
			return fmt.Errorf("failed to create aggregate: %w", err)
		}

		if nd.Cid() != cont.Cid.CID {
			return fmt.Errorf("recreated aggregate cid does not match one recorded in db: %s != %s", nd.Cid(), cont.Cid.CID)
		}

		if err := s.Node.Blockstore.Put(ctx, nd); err != nil {
			return err
		}
	}

	var fixedAggregateLocation bool
	if c.QueryParam("check-locations") != "" && cont.Aggregate {
		// TODO: check if the contents of the aggregate are somewhere other than where the aggregate root is
		switch len(aggrLocs) {
		case 0:
			s.log.Warnf("content %d has nothing aggregated in it", cont.ID)
		case 1:
			loc := aggr[0].Location
			if loc != cont.Location {
				// should be safe to send a re-aggregate command to the shuttle in question
				if err := s.shuttleMgr.AggregateContent(ctx, loc, cont, aggr); err != nil {
					return err
				}
				fixedAggregateLocation = true
			}
		default:
			// well that sucks, this will need migration
			s.log.Warnf("content %d has messed up aggregation", cont.ID)
		}
	}

	var exch exchange.Interface
	if c.QueryParam("fetch") != "" {
		exch = s.Node.Bitswap
	}

	bserv := blockservice.New(s.Node.Blockstore, exch)
	dserv := merkledag.NewDAGService(bserv)

	cset := cid.NewSet()
	err = merkledag.Walk(ctx, func(ctx context.Context, c cid.Cid) ([]*ipld.Link, error) {
		node, err := dserv.Get(ctx, c)
		if err != nil {
			return nil, err
		}

		if c.Type() == cid.Raw {
			return nil, nil
		}

		return util.FilterUnwalkableLinks(node.Links()), nil
	}, cont.Cid.CID, cset.Visit, merkledag.Concurrent())

	errstr := ""
	if err != nil {
		errstr = err.Error()
	}

	out := map[string]interface{}{
		"user":               u.Username,
		"content":            cont,
		"deals":              deals,
		"traverseError":      errstr,
		"foundBlocks":        cset.Len(),
		"fixedAggregateSize": fixedAggregateSize,
	}
	if aggrLocs != nil {
		out["aggregatedContentLocations"] = aggrLocs
		out["fixedAggregateLocation"] = fixedAggregateLocation
	}
	return c.JSON(http.StatusOK, out)
}

func (s *apiV1) handleContentHealthCheckByCid(c echo.Context) error {
	ctx := c.Request().Context()
	cc, err := cid.Decode(c.Param("cid"))
	if err != nil {
		return err
	}

	var roots []util.Content
	if err := s.DB.Find(&roots, "cid = ?", cc.Bytes()).Error; err != nil {
		return err
	}

	var obj util.Object
	if err := s.DB.First(&obj, "cid = ?", cc.Bytes()).Error; err != nil {
		return c.JSON(404, map[string]interface{}{
			"error":                "object not found in database",
			"cid":                  cc.String(),
			"matchingRootContents": roots,
		})
	}

	var contents []util.Content
	if err := s.DB.Model(util.ObjRef{}).Joins("left join contents on obj_refs.content = contents.id").Where("object = ?", obj.ID).Select("contents.*").Scan(&contents).Error; err != nil {
		s.log.Errorf("failed to find contents for cid: %s", err)
	}

	_, rootFetchErr := s.Node.Blockstore.Get(ctx, cc)
	if rootFetchErr != nil {
		s.log.Errorf("failed to fetch root: %s", rootFetchErr)
	}

	var exch exchange.Interface
	if c.QueryParam("fetch") != "" {
		exch = s.Node.Bitswap
	}

	bserv := blockservice.New(s.Node.Blockstore, exch)
	dserv := merkledag.NewDAGService(bserv)

	cset := cid.NewSet()
	err = merkledag.Walk(ctx, func(ctx context.Context, c cid.Cid) ([]*ipld.Link, error) {
		node, err := dserv.Get(ctx, c)
		if err != nil {
			return nil, err
		}

		if c.Type() == cid.Raw {
			return nil, nil
		}

		return util.FilterUnwalkableLinks(node.Links()), nil
	}, cc, cset.Visit, merkledag.Concurrent())

	errstr := ""
	if err != nil {
		errstr = err.Error()
	}

	rferrstr := ""
	if rootFetchErr != nil {
		rferrstr = rootFetchErr.Error()
	}

	return c.JSON(http.StatusOK, map[string]interface{}{
		"contents":             contents,
		"cid":                  cc,
		"traverseError":        errstr,
		"foundBlocks":          cset.Len(),
		"rootFetchErr":         rferrstr,
		"matchingRootContents": roots,
	})
}

func (s *apiV1) handleShuttleInit(c echo.Context) error {
	shuttle := &model.Shuttle{
		Handle: "SHUTTLE" + uuid.New().String() + "HANDLE",
		Token:  "SECRET" + uuid.New().String() + "SECRET",
		Open:   false,
	}
	if err := s.DB.Create(shuttle).Error; err != nil {
		return err
	}

	return c.JSON(http.StatusOK, &util.InitShuttleResponse{
		Handle: shuttle.Handle,
		Token:  shuttle.Token,
	})
}

func (s *apiV1) handleShuttleList(c echo.Context) error {
	var shuttles []model.Shuttle
	if err := s.DB.Find(&shuttles).Error; err != nil {
		return err
	}

	var out []util.ShuttleListResponse
	for _, d := range shuttles {
		isOnline, err := s.shuttleMgr.IsOnline(d.Handle)
		if err != nil {
			return err
		}

		addInf, err := s.shuttleMgr.AddrInfo(d.Handle)
		if err != nil {
			return err
		}

		hn, err := s.shuttleMgr.HostName(d.Handle)
		if err != nil {
			return err
		}

		sts, err := s.shuttleMgr.StorageStats(d.Handle)
		if err != nil {
			return err
		}

		out = append(out, util.ShuttleListResponse{
			Handle:         d.Handle,
			Token:          d.Token,
			LastConnection: d.LastConnection,
			Online:         isOnline,
			AddrInfo:       addInf,
			Hostname:       hn,
			StorageStats:   sts,
		})
	}
	return c.JSON(http.StatusOK, out)
}

func (s *apiV1) handleShuttleConnection(c echo.Context) error {
	auth, err := util.ExtractAuth(c)
	if err != nil {
		return err
	}

	shuttle, err := s.shuttleMgr.GetByAuth(auth)
	if err != nil {
		return err
	}

	done := make(chan struct{})
	go s.transferMgr.RestartAllTransfersForLocation(context.TODO(), shuttle.Handle, done)
	return s.shuttleMgr.Connect(c, shuttle.Handle, done)
}

// handleAutoretrieveInit godoc
// @Summary      Register autoretrieve server
// @Description  This endpoint registers a new autoretrieve server
// @Tags         autoretrieve
// @Param        addresses  formData  string  true  "Autoretrieve's comma-separated list of addresses"
// @Param        pubKey     formData  string  true  "Autoretrieve's public key"
// @Produce      json
// @Success      200  {object}  string
// @Failure      400  {object}  util.HttpError
// @Failure      500  {object}  util.HttpError
// @Router       /admin/autoretrieve/init [post]
func (s *apiV1) handleAutoretrieveInit(c echo.Context) error {

	err := func() error {
		// If there's already an Autoretrieve database entry under the requested pub
		// key, delete it first
		if err := s.DB.Unscoped().Delete(&autoretrieve.Autoretrieve{}, "pub_key = ?", c.FormValue("pubKey")).Error; err != nil {
			return err
		}

		// Initialize Autoretrieve database entry
		ar := &autoretrieve.Autoretrieve{
			Handle:            "AUTORETRIEVE" + uuid.New().String() + "HANDLE",
			Token:             "SECRET" + uuid.New().String() + "SECRET",
			LastConnection:    time.Now(),
			LastAdvertisement: time.Time{},
			PubKey:            c.FormValue("pubKey"),
			Addresses:         c.FormValue("addresses"),
		}
		if err := s.DB.Create(ar).Error; err != nil {
			return err
		}

		addrInfo, err := ar.AddrInfo()
		if err != nil {
			return err
		}

		s.log.Infof("Added autoretrieve with addr info %s", addrInfo)

		return c.JSON(200, &autoretrieve.AutoretrieveInitResponse{
			Handle:            ar.Handle,
			Token:             ar.Token,
			LastConnection:    ar.LastConnection,
			AddrInfo:          addrInfo,
			AdvertiseInterval: s.Node.Config.IndexerAdvertisementInterval.String(),
		})
	}()

	if err != nil {
		s.log.Errorf("Failed to register estuary: %v", err)
	}

	return err
}

// handleAutoretrieveList godoc
// @Summary      List autoretrieve servers
// @Description  This endpoint lists all registered autoretrieve servers
// @Tags         autoretrieve
// @Produce      json
// @Success      200  {object}  string
// @Failure      400  {object}  util.HttpError
// @Failure      500  {object}  util.HttpError
// @Router       /admin/autoretrieve/list [get]
func (s *apiV1) handleAutoretrieveList(c echo.Context) error {
	var autoretrieves []autoretrieve.Autoretrieve
	if err := s.DB.Find(&autoretrieves).Error; err != nil {
		return err
	}

	var out []autoretrieve.AutoretrieveListResponse

	for _, ar := range autoretrieves {
		addrInfo, err := ar.AddrInfo()
		if err != nil {
			return err
		}

		out = append(out, autoretrieve.AutoretrieveListResponse{
			Handle:            ar.Handle,
			LastConnection:    ar.LastConnection,
			LastAdvertisement: ar.LastAdvertisement,
			AddrInfo:          addrInfo,
		})
	}
	return c.JSON(http.StatusOK, out)
}

// handleAutoretrieveHeartbeat godoc
// @Summary      Marks autoretrieve server as up
// @Description  This endpoint updates the lastConnection field for autoretrieve
// @Tags         autoretrieve
// @Param        token  header  string  true  "Autoretrieve's auth token"
// @Produce      json
// @Success      200  {object}  string
// @Failure      400  {object}  util.HttpError
// @Failure      500  {object}  util.HttpError
// @Router       /autoretrieve/heartbeat [post]
func (s *apiV1) handleAutoretrieveHeartbeat(c echo.Context) error {
	auth, err := util.ExtractAuth(c)
	if err != nil {
		return err
	}

	var ar autoretrieve.Autoretrieve
	if err := s.DB.First(&ar, "token = ?", auth).Error; err != nil {
		return err
	}

	ar.LastConnection = time.Now()
	if err := s.DB.Save(&ar).Error; err != nil {
		return err
	}

	addrInfo, err := ar.AddrInfo()
	if err != nil {
		return err
	}

	out := autoretrieve.HeartbeatAutoretrieveResponse{
		Handle:            ar.Handle,
		LastConnection:    ar.LastConnection,
		LastAdvertisement: ar.LastAdvertisement,
		AddrInfo:          addrInfo,
		AdvertiseInterval: s.Node.Config.IndexerAdvertisementInterval.String(),
	}
	return c.JSON(http.StatusOK, out)
}

type allDealsQuery struct {
	Miner  string
	Cid    util.DbCID
	DealID int64
}

func (s *apiV1) handleDebugGetAllDeals(c echo.Context) error {
	var out []allDealsQuery
	if err := s.DB.Model(model.ContentDeal{}).Where("deal_id > 0 and not content_deals.failed").
		Joins("left join contents on content_deals.content = contents.id").
		Select("miner, contents.cid as cid, deal_id").
		Scan(&out).
		Error; err != nil {
		return err
	}
	return c.JSON(http.StatusOK, out)
}

type logLevelBody struct {
	System string `json:"system"`
	Level  string `json:"level"`
}

func (s *apiV1) handleLogLevel(c echo.Context) error {
	var body logLevelBody
	if err := c.Bind(&body); err != nil {
		return err
	}

	//#nosec G104 - it's not common to treat SetLogLevel error return
	logging.SetLogLevel(body.System, body.Level)

	return c.JSON(http.StatusOK, map[string]interface{}{})
}

// handlePublicStorageFailures godoc
// @Summary      Get storage failures
// @Description  This endpoint returns a list of storage failures
// @Tags         deals
// @Produce      json
// @Success      200  {object}  string
// @Failure      400  {object}  util.HttpError
// @Failure      500  {object}  util.HttpError
// @Router       /public/deals/failures [get]
func (s *apiV1) handlePublicStorageFailures(c echo.Context) error {
	recs, err := s.getStorageFailure(c, nil)
	if err != nil {
		return err
	}
	return c.JSON(http.StatusOK, recs)
}

// handleStorageFailures godoc
// @Summary      Get storage failures for user
// @Description  This endpoint returns a list of storage failures for user
// @Tags         deals
// @Produce      json
// @Success      200  {object}  string
// @Failure      400  {object}  util.HttpError
// @Failure      500  {object}  util.HttpError
// @Router       /deals/failures [get]
func (s *apiV1) handleStorageFailures(c echo.Context, u *util.User) error {
	recs, err := s.getStorageFailure(c, u)
	if err != nil {
		return err
	}
	return c.JSON(http.StatusOK, recs)
}

func (s *apiV1) getStorageFailure(c echo.Context, u *util.User) ([]model.DfeRecord, error) {
	limit, _, err := s.getLimitAndOffset(c, 500, 0)
	if err != nil {
		return nil, err
	}

	q := s.DB.Model(model.DfeRecord{}).Limit(limit).Order("created_at desc")
	if u != nil {
		q = q.Where("user_id=?", u.ID)
	}

	if bef := c.QueryParam("before"); bef != "" {
		beftime, err := time.Parse(time.RFC3339, bef)
		if err != nil {
			return nil, err
		}
		q = q.Where("created_at <= ?", beftime)
	}

	var recs []model.DfeRecord
	if err := q.Scan(&recs).Error; err != nil {
		return nil, err
	}
	return recs, nil
}

// handleCreateContent godoc
// @Summary      Add a new content
// @Description  This endpoint adds a new content
// @Tags         content
// @Produce      json
// @Success      200  {object}  string
// @Failure      400  {object}  util.HttpError
// @Failure      500  {object}  util.HttpError
// @Param        req           body      util.ContentCreateBody  true   "Content"
// @Param        ignore-dupes  query     string                  false  "Ignore Dupes"
// @Router       /content/create [post]
func (s *apiV1) handleCreateContent(c echo.Context, u *util.User) error {
	var req util.ContentCreateBody
	if err := c.Bind(&req); err != nil {
		return err
	}

	rootCID, err := cid.Decode(req.Root)
	if err != nil {
		return err
	}

	if c.QueryParam("ignore-dupes") == "true" {
		isDup, err := s.isDupCIDContent(c, rootCID, u)
		if err != nil || isDup {
			return err
		}
	}

	var col collections.Collection
	if req.CollectionID != "" {
		if err := s.DB.First(&col, "uuid = ?", req.CollectionID).Error; err != nil {
			return err
		}

		if err := util.IsCollectionOwner(u.ID, col.UserID); err != nil {
			return err
		}
	}

	content := &util.Content{
		Cid:         util.DbCID{CID: rootCID},
		Name:        req.Name,
		Active:      false,
		Pinning:     true,
		UserID:      u.ID,
		Replication: s.cfg.Replication,
		Location:    req.Location,
	}

	if err := s.DB.Create(content).Error; err != nil {
		return err
	}

	if req.CollectionID != "" {
		if req.CollectionDir == "" {
			req.CollectionDir = "/"
		}

		sp, err := sanitizePath(req.CollectionDir)
		if err != nil {
			return err
		}

		path := &sp
		if err := s.DB.Create(&collections.CollectionRef{
			Collection: col.ID,
			Content:    content.ID,
			Path:       path,
		}).Error; err != nil {
			return err
		}
	}

	s.CM.ToCheck(content.ID, content.Size)

	return c.JSON(http.StatusOK, util.ContentCreateResponse{
		ID: content.ID,
	})
}

type claimResponse struct {
	Success bool `json:"success"`
}

// handleUserClaimMiner godoc
// @Summary      Claim Miner
// @Description  This endpoint lets a user claim a miner
// @Tags         miner
// @Produce      json
// @Success      200  {object}  claimResponse
// @Failure      400  {object}  util.HttpError
// @Failure      500  {object}  util.HttpError
// @Param        req           body      miner.ClaimMinerBody  true   "Claim Miner Body"
// @Router       /miner/claim [post]
func (s *apiV1) handleUserClaimMiner(c echo.Context, u *util.User) error {
	ctx := c.Request().Context()

	var cmb miner.ClaimMinerBody
	if err := c.Bind(&cmb); err != nil {
		return err
	}

	if err := s.minerManager.ClaimMiner(ctx, cmb, u); err != nil {
		return err
	}
	return c.JSON(http.StatusOK, claimResponse{Success: true})
}

type claimMsgResponse struct {
	Hexmsg string `json:"hexmsg"`
}

// handleUserGetClaimMinerMsg godoc
// @Summary      Get Claim Miner Message
// @Description  This endpoint lets a user get the message in order to claim a miner
// @Tags         miner
// @Produce      json
// @Success      200    {object}  claimMsgResponse
// @Failure      400  {object}  util.HttpError
// @Failure      500  {object}  util.HttpError
// @Param        miner  path     string  true  "Miner claim message"
// @Router       /miner/claim/{miner} [get]
func (s *apiV1) handleUserGetClaimMinerMsg(c echo.Context, u *util.User) error {
	m, err := address.NewFromString(c.Param("miner"))
	if err != nil {
		return err
	}

	return c.JSON(http.StatusOK, claimMsgResponse{
		Hexmsg: hex.EncodeToString(s.minerManager.GetMsgForMinerClaim(m, u.ID)),
	})
}

type progressResponse struct {
	GoodContents []uint
	InProgress   []uint
	NoDeals      []uint

	TotalTopLevel int64
	TotalPinning  int64
}

type contCheck struct {
	ID       uint
	NumDeals int
}

func (s *apiV1) handleAdminGetProgress(c echo.Context) error {
	var out progressResponse
	if err := s.DB.Model(util.Content{}).Where("not aggregated_in > 0 AND (pinning OR active) AND not failed").Count(&out.TotalTopLevel).Error; err != nil {
		return err
	}

	if err := s.DB.Model(util.Content{}).Where("pinning and not failed").Count(&out.TotalPinning).Error; err != nil {
		return err
	}

	var conts []contCheck
	if err := s.DB.Model(util.Content{}).Where("not aggregated_in > 0 and active").
		Select("id, (?) as num_deals",
			s.DB.Model(model.ContentDeal{}).
				Where("content = contents.id and deal_id > 0 and not failed").
				Select("count(1)"),
		).Scan(&conts).Error; err != nil {
		return err
	}

	for _, c := range conts {
		if c.NumDeals >= s.cfg.Replication {
			out.GoodContents = append(out.GoodContents, c.ID)
		} else if c.NumDeals > 0 {
			out.InProgress = append(out.InProgress, c.ID)
		} else {
			out.NoDeals = append(out.NoDeals, c.ID)
		}
	}

	return c.JSON(http.StatusOK, out)
}

func (s *apiV1) handleAdminBreakAggregate(c echo.Context) error {
	ctx := c.Request().Context()
	aggr, err := strconv.Atoi(c.Param("content"))
	if err != nil {
		return err
	}

	var cont util.Content
	if err := s.DB.First(&cont, "id = ?", aggr).Error; err != nil {
		if xerrors.Is(err, gorm.ErrRecordNotFound) {
			return &util.HttpError{
				Code:    http.StatusNotFound,
				Reason:  util.ERR_RECORD_NOT_FOUND,
				Details: fmt.Sprintf("content: %d was not found", aggr),
			}
		}
		return err
	}

	if !cont.Aggregate {
		return fmt.Errorf("content %d is not an aggregate", aggr)
	}

	var children []util.Content
	if err := s.DB.Find(&children, "aggregated_in = ?", aggr).Error; err != nil {
		return err
	}

	if c.QueryParam("check-missing-children") != "" {
		var childRes []map[string]interface{}
		bserv := blockservice.New(s.Node.Blockstore, nil)
		dserv := merkledag.NewDAGService(bserv)

		for _, c := range children {

			cset := cid.NewSet()
			err := merkledag.Walk(ctx, func(ctx context.Context, c cid.Cid) ([]*ipld.Link, error) {
				node, err := dserv.Get(ctx, c)
				if err != nil {
					return nil, err
				}

				if c.Type() == cid.Raw {
					return nil, nil
				}

				return util.FilterUnwalkableLinks(node.Links()), nil
			}, cont.Cid.CID, cset.Visit, merkledag.Concurrent())
			res := map[string]interface{}{
				"content":     c,
				"foundBlocks": cset.Len(),
			}
			if err != nil {
				res["walkErr"] = err.Error()
			}
			childRes = append(childRes, res)
		}

		return c.JSON(http.StatusOK, map[string]interface{}{
			"children": childRes,
		})
	}

	if err := s.DB.Model(util.Content{}).Where("aggregated_in = ?", aggr).UpdateColumns(map[string]interface{}{
		"aggregated_in": 0,
	}).Error; err != nil {
		return err
	}

	if err := s.DB.Model(util.Content{}).Where("id = ?", aggr).UpdateColumns(map[string]interface{}{
		"active": false,
	}).Error; err != nil {
		return err
	}

	return c.JSON(http.StatusOK, map[string]string{})
}

type publicNodeInfo struct {
	PrimaryAddress address.Address `json:"primaryAddress"`
}

// handleGetPublicNodeInfo godoc
// @Summary      Get public node info
// @Description  This endpoint returns information about the node
// @Tags         public
// @Produce      json
// @Success      200  {object}  publicNodeInfo
// @Failure      400  {object}  util.HttpError
// @Failure      500  {object}  util.HttpError
// @Router       /public/info [get]
func (s *apiV1) handleGetPublicNodeInfo(c echo.Context) error {
	return c.JSON(http.StatusOK, &publicNodeInfo{
		PrimaryAddress: s.FilClient.ClientAddr,
	})
}

type retrievalCandidate struct {
	Miner   address.Address
	RootCid cid.Cid
	DealID  uint
}

func (s *apiV1) handleGetRetrievalCandidates(c echo.Context) error {
	// Read the cid from the client request
	cid, err := cid.Decode(c.Param("cid"))
	if err != nil {
		return c.JSON(http.StatusBadRequest, map[string]string{
			"error": "invalid cid",
		})
	}

	var candidateInfos []struct {
		Miner  string
		Cid    util.DbCID
		DealID uint
	}
	if err := s.DB.
		Table("content_deals").
		Where("content IN (?) AND NOT content_deals.failed",
			s.DB.Table("contents").Select("CASE WHEN @aggregated_in = 0 THEN id ELSE aggregated_in END").Where("id in (?)",
				s.DB.Table("obj_refs").Select("content").Where(
					"object IN (?)", s.DB.Table("objects").Select("id").Where("cid = ?", util.DbCID{CID: cid}),
				),
			),
		).
		Joins("JOIN contents ON content_deals.content = contents.id").
		Select("miner, cid, deal_id").
		Scan(&candidateInfos).Error; err != nil {
		return err
	}

	var candidates []retrievalCandidate
	for _, candidateInfo := range candidateInfos {
		maddr, err := address.NewFromString(candidateInfo.Miner)
		if err != nil {
			return err
		}

		candidates = append(candidates, retrievalCandidate{
			Miner:   maddr,
			RootCid: candidateInfo.Cid.CID,
			DealID:  candidateInfo.DealID,
		})
	}

	return c.JSON(http.StatusOK, candidates)
}

func (s *apiV1) handleShuttleCreateContent(c echo.Context) error {
	var req util.ShuttleCreateContentBody
	if err := c.Bind(&req); err != nil {
		return err
	}

	s.log.Debugw("handle shuttle create content", "root", req.Root, "user", req.User, "dsr", req.DagSplitRoot, "name", req.Name)

	root, err := cid.Decode(req.Root)
	if err != nil {
		return c.JSON(http.StatusBadRequest, map[string]interface{}{
			"error": map[string]interface{}{
				"reason": err,
			},
		})
	}

	content := &util.Content{
		Cid:         util.DbCID{CID: root},
		Name:        req.Name,
		Active:      false,
		Pinning:     true,
		UserID:      req.User,
		Replication: s.cfg.Replication,
		Location:    req.Location,
	}

	if req.DagSplitRoot != 0 {
		content.DagSplit = true
		content.SplitFrom = req.DagSplitRoot
	}

	if err := s.DB.Create(content).Error; err != nil {
		return err
	}

	return c.JSON(http.StatusOK, util.ContentCreateResponse{
		ID: content.ID,
	})
}

func (s *apiV1) withAutoretrieveAuth() echo.MiddlewareFunc {
	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c echo.Context) error {
			auth, err := util.ExtractAuth(c)
			if err != nil {
				return err
			}

			var ar autoretrieve.Autoretrieve
			if err := s.DB.First(&ar, "token = ?", auth).Error; err != nil {
				s.log.Warnw("Autoretrieve server not authorized", "token", auth)
				if xerrors.Is(err, gorm.ErrRecordNotFound) {
					return &util.HttpError{
						Code:    http.StatusUnauthorized,
						Reason:  util.ERR_NOT_AUTHORIZED,
						Details: "token was not found",
					}
				}
				return err
			}
			return next(c)
		}
	}
}

func (s *apiV1) withShuttleAuth() echo.MiddlewareFunc {
	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c echo.Context) error {
			auth, err := util.ExtractAuth(c)
			if err != nil {
				return err
			}

			var sh model.Shuttle
			if err := s.DB.First(&sh, "token = ?", auth).Error; err != nil {
				s.log.Warnw("Shuttle not authorized", "token", auth)
				if xerrors.Is(err, gorm.ErrRecordNotFound) {
					return &util.HttpError{
						Code:    http.StatusUnauthorized,
						Reason:  util.ERR_NOT_AUTHORIZED,
						Details: "shuttle token was not found",
					}
				}
			}
			return next(c)
		}
	}
}

func (s *apiV1) handleShuttleRepinAll(c echo.Context) error {
	handle := c.Param("shuttle")

	rows, err := s.DB.Model(util.Content{}).Where("location = ? and not offloaded", handle).Rows()
	if err != nil {
		return err
	}

	defer rows.Close()
	for rows.Next() {
		var cont util.Content
		if err := s.DB.ScanRows(rows, &cont); err != nil {
			return err
		}

		var origins []*peer.AddrInfo
		// when refreshing pinning queue, use content origins if available
		if cont.Origins != "" {
			_ = json.Unmarshal([]byte(cont.Origins), &origins) // no need to handle or log err, its just a nice to have
		}

		if err := s.shuttleMgr.PinContent(c.Request().Context(), handle, cont, origins); err != nil {
			return err
		}
	}
	return nil
}

type CidType string

const (
	ColDir string = "dir"
)

func sanitizePath(p string) (string, error) {
	if len(p) == 0 {
		return "", fmt.Errorf("can't sanitize empty path")
	}

	if p[0] != '/' {
		return "", fmt.Errorf("paths must start with /")
	}

	// TODO: prevent use of special weird characters

	cleanPath := filepath.Clean(p)

	// if original path ends in /, append / to cleaned path
	// needed for full path vs dir+filename magic to work in handleAddIpfs
	if strings.HasSuffix(p, "/") {
		cleanPath = cleanPath + "/"
	}
	return cleanPath, nil
}

// handleColfsAdd godoc
// @Summary      Add a file to a collection
// @Description  This endpoint adds a file to a collection
// @Tags         collections
// @Param        coluuid  query  string  true  "Collection ID"
// @Param        content  query  string  true  "Content"
// @Param        path     query  string  true  "Path to file"
// @Produce      json
// @Success      200  {object}  string
// @Failure      400  {object}  util.HttpError
// @Failure      500  {object}  util.HttpError
// @Router       /collections/fs/add [post]
func (s *apiV1) handleColfsAdd(c echo.Context, u *util.User) error {
	coluuid := c.QueryParam("coluuid")
	contid := c.QueryParam("content")
	npath := c.QueryParam("path")

	var col collections.Collection
	if err := s.DB.First(&col, "uuid = ?", coluuid).Error; err != nil {
		if xerrors.Is(err, gorm.ErrRecordNotFound) {
			return &util.HttpError{
				Code:    http.StatusNotFound,
				Reason:  util.ERR_RECORD_NOT_FOUND,
				Details: fmt.Sprintf("collection: %s was not found", coluuid),
			}
		}
		return err
	}

	if err := util.IsCollectionOwner(u.ID, col.UserID); err != nil {
		return err
	}

	var content util.Content
	if err := s.DB.First(&content, "id = ?", contid).Error; err != nil {
		if xerrors.Is(err, gorm.ErrRecordNotFound) {
			return &util.HttpError{
				Code:    http.StatusNotFound,
				Reason:  util.ERR_RECORD_NOT_FOUND,
				Details: fmt.Sprintf("collection content: %s was not found", contid),
			}
		}
		return err
	}

	if err := util.IsContentOwner(u.ID, content.UserID); err != nil {
		return err
	}

	var path *string
	if npath != "" {
		p, err := sanitizePath(npath)
		if err != nil {
			return err
		}
		path = &p
	}

	if err := s.DB.Create(&collections.CollectionRef{Collection: col.ID, Content: content.ID, Path: path}).Error; err != nil {
		return errors.Wrap(err, "failed to add content to requested collection")
	}
	return c.JSON(http.StatusOK, map[string]string{})
}

func (s *apiV1) handleRunGc(c echo.Context) error {
	if err := s.CM.GarbageCollect(c.Request().Context()); err != nil {
		return err
	}
	return nil
}

func (s *apiV1) handleGateway(c echo.Context) error {
	npath := "/" + c.Param("path")
	proto, cc, segs, err := gateway.ParsePath(npath)
	if err != nil {
		return err
	}

	redir, err := s.checkGatewayRedirect(proto, cc, segs)
	if err != nil {
		return err
	}

	if redir == "" {

		req := c.Request().Clone(c.Request().Context())
		req.URL.Path = npath

		s.gwayHandler.ServeHTTP(c.Response().Writer, req)
		return nil
	}
	return c.Redirect(307, redir)
}

const bestGateway = "dweb.link"

func (s *apiV1) checkGatewayRedirect(proto string, cc cid.Cid, segs []string) (string, error) {
	if proto != "ipfs" {
		return fmt.Sprintf("https://%s/%s/%s/%s", bestGateway, proto, cc, strings.Join(segs, "/")), nil
	}

	var cont util.Content
	if err := s.DB.First(&cont, "cid = ? and active and not offloaded", &util.DbCID{CID: cc}).Error; err != nil {
		if xerrors.Is(err, gorm.ErrRecordNotFound) {
			// if not pinned on any shuttle or local, check dweb
			return fmt.Sprintf("https://%s/%s/%s/%s", bestGateway, proto, cc, strings.Join(segs, "/")), nil
		}
		return "", err
	}

	if cont.Location == constants.ContentLocationLocal {
		return "", nil
	}

	isOnline, err := s.shuttleMgr.IsOnline(cont.Location)
	if err != nil {
		return "", err
	}

	if !isOnline {
		return fmt.Sprintf("https://%s/%s/%s/%s", bestGateway, proto, cc, strings.Join(segs, "/")), nil
	}

	var shuttle model.Shuttle
	if err := s.DB.First(&shuttle, "handle = ?", cont.Location).Error; err != nil {
		return "", err
	}
	return fmt.Sprintf("https://%s/gw/%s/%s/%s", shuttle.Host, proto, cc, strings.Join(segs, "/")), nil
}

func (s *apiV1) isDupCIDContent(c echo.Context, rootCID cid.Cid, u *util.User) (bool, error) {
	var count int64
	if err := s.DB.Model(util.Content{}).Where("cid = ? and user_id = ?", rootCID.Bytes(), u.ID).Count(&count).Error; err != nil {
		return false, err
	}
	if count > 0 {
		return true, c.JSON(409, map[string]string{"message": fmt.Sprintf("this content is already preserved under cid:%s", rootCID.String())})
	}
	return false, nil
}

func (s *apiV1) isContentAddingDisabled(u *util.User) bool {
	return (s.cfg.Content.DisableGlobalAdding && s.cfg.Content.DisableLocalAdding) || u.StorageDisabled
}

func (s *apiV1) handleFixupDeals(c echo.Context) error {
	ctx := context.Background()
	var deals []model.ContentDeal
	if err := s.DB.Order("deal_id desc").Find(&deals, "deal_id > 0 AND on_chain_at < ?", time.Now().Add(time.Hour*24*-100)).Error; err != nil {
		return err
	}

	gentime, err := time.Parse("2006-01-02 15:04:05", "2020-08-24 15:00:00")
	if err != nil {
		return err
	}

	head, err := s.Api.ChainHead(ctx)
	if err != nil {
		return err
	}

	sem := make(chan struct{}, 50)
	for _, dll := range deals {
		sem <- struct{}{}
		go func(d model.ContentDeal) {
			defer func() {
				<-sem
			}()
			miner, err := d.MinerAddr()
			if err != nil {
				s.log.Error(err)
				return
			}

			subctx, cancel := context.WithTimeout(ctx, time.Second*5)
			defer cancel()

			// Get deal UUID, if there is one for the deal.
			// (There should be a UUID for deals made with deal protocol v1.2.0)
			var dealUUID *uuid.UUID
			if d.DealUUID != "" {
				parsed, err := uuid.Parse(d.DealUUID)
				if err != nil {
					s.log.Errorf("failed to get deal status: parsing deal uuid %s: %d %s: %s",
						d.DealUUID, d.ID, miner, err)
					return
				}
				dealUUID = &parsed
			}

			provds, _, err := s.CM.GetProviderDealStatus(subctx, &d, miner, dealUUID)
			if err != nil {
				s.log.Errorf("failed to get deal status: %d %s: %s", d.ID, miner, err)
				return
			}

			// this should not happen, but be safe
			if provds == nil {
				s.log.Errorf("failed to lookup provider deal state for deal: %d", d.DealID)
				return
			}

			if provds.PublishCid == nil {
				s.log.Errorf("no publish cid for deal: %d", d.DealID)
				return
			}

			subctx2, cancel2 := context.WithTimeout(ctx, time.Second*20)
			defer cancel2()
			wait, err := s.Api.StateSearchMsg(subctx2, head.Key(), *provds.PublishCid, 100000, true)
			if err != nil {
				s.log.Errorf("failed to search message: %s", err)
				return
			}

			if wait == nil {
				s.log.Errorf("failed to find message: %d %s", d.ID, *provds.PublishCid)
				return
			}

			ontime := gentime.Add(time.Second * 30 * time.Duration(wait.Height))
			s.log.Debugf("updating onchainat time for deal %d %d to %s", d.ID, d.DealID, ontime)
			if err := s.DB.Model(model.ContentDeal{}).Where("id = ?", d.ID).Update("on_chain_at", ontime).Error; err != nil {
				s.log.Error(err)
				return
			}
		}(dll)
	}
	return nil
}

func (s *apiV1) getLimitAndOffset(c echo.Context, defaultLimit int, defaultOffset int) (int, int, error) {
	limit := defaultLimit
	offset := defaultOffset
	if limstr := c.QueryParam("limit"); limstr != "" {
		nlim, err := strconv.Atoi(limstr)
		if err != nil {
			return limit, offset, err
		}

		if nlim > 0 {
			limit = nlim
		}
	}

	if offstr := c.QueryParam("offset"); offstr != "" {
		noff, err := strconv.Atoi(offstr)
		if err != nil {
			return limit, offset, err
		}

		if noff > 0 {
			offset = noff
		}
	}
	return limit, offset, nil
}
