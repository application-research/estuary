package main

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
	httpprof "net/http/pprof"
	"net/url"
	"os"
	"path/filepath"
	"runtime/pprof"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/application-research/estuary/constants"
	"github.com/application-research/estuary/node/modules/peering"
	"github.com/libp2p/go-libp2p-core/network"

	"github.com/application-research/estuary/autoretrieve"
	drpc "github.com/application-research/estuary/drpc"
	esmetrics "github.com/application-research/estuary/metrics"
	"github.com/application-research/estuary/util"
	"github.com/application-research/estuary/util/gateway"
	"github.com/application-research/filclient"
	"github.com/filecoin-project/go-address"
	datatransfer "github.com/filecoin-project/go-data-transfer"
	"github.com/filecoin-project/go-padreader"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/big"
	"github.com/filecoin-project/go-state-types/crypto"
	"github.com/filecoin-project/lotus/chain/types"
	"github.com/filecoin-project/lotus/lib/sigs"
	"github.com/filecoin-project/specs-actors/v6/actors/builtin/market"
	"github.com/google/uuid"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-blockservice"
	"github.com/ipfs/go-cid"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	exchange "github.com/ipfs/go-ipfs-exchange-interface"
	offline "github.com/ipfs/go-ipfs-exchange-offline"
	ipld "github.com/ipfs/go-ipld-format"
	logging "github.com/ipfs/go-log/v2"
	"github.com/ipfs/go-merkledag"
	"github.com/ipfs/go-unixfs"
	uio "github.com/ipfs/go-unixfs/io"
	"github.com/ipld/go-car"
	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/multiformats/go-multiaddr"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"golang.org/x/net/websocket"
	"golang.org/x/sys/unix"
	"golang.org/x/xerrors"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	semconv "go.opentelemetry.io/otel/semconv/v1.7.0"
	"go.opentelemetry.io/otel/trace"

	echoSwagger "github.com/swaggo/echo-swagger"

	_ "github.com/application-research/estuary/docs"
	"github.com/multiformats/go-multihash"
)

// @title Estuary API
// @version 0.0.0
// @description This is the API for the Estuary application.
// @termsOfService http://estuary.tech

// @contact.name API Support
// @contact.url https://docs.estuary.tech/feedback

// @license.name Apache 2.0 Apache-2.0 OR MIT
// @license.url https://github.com/application-research/estuary/blob/master/LICENSE.md

// @host api.estuary.tech
// @BasePath  /
// @securityDefinitions.Bearer
// @securityDefinitions.Bearer.type apiKey
// @securityDefinitions.Bearer.in header
// @securityDefinitions.Bearer.name Authorization
func (s *Server) ServeAPI() error {

	e := echo.New()

	e.Binder = new(binder)

	if s.cfg.Logging.ApiEndpointLogging {
		e.Use(middleware.Logger())
	}

	e.Use(s.tracingMiddleware)
	e.Use(util.AppVersionMiddleware(s.cfg.AppVersion))
	e.HTTPErrorHandler = util.ErrorHandler

	e.GET("/debug/pprof/:prof", serveProfile)
	e.GET("/debug/cpuprofile", serveCpuProfile)

	phandle := promhttp.Handler()
	e.GET("/debug/metrics/prometheus", func(e echo.Context) error {
		phandle.ServeHTTP(e.Response().Writer, e.Request())
		return nil
	})

	exporter := esmetrics.Exporter()
	e.GET("/debug/metrics/opencensus", func(e echo.Context) error {
		exporter.ServeHTTP(e.Response().Writer, e.Request())
		return nil
	})

	e.Use(middleware.CORS())

	e.POST("/register", s.handleRegisterUser)
	e.POST("/login", s.handleLoginUser)
	e.GET("/health", s.handleHealth)

	e.GET("/viewer", withUser(s.handleGetViewer), s.AuthRequired(util.PermLevelUpload))

	e.GET("/retrieval-candidates/:cid", s.handleGetRetrievalCandidates)

	e.GET("/gw/:path", s.handleGateway)

	user := e.Group("/user")
	user.Use(s.AuthRequired(util.PermLevelUser))
	user.GET("/api-keys", withUser(s.handleUserGetApiKeys))
	user.POST("/api-keys", withUser(s.handleUserCreateApiKey))
	user.DELETE("/api-keys/:key", withUser(s.handleUserRevokeApiKey))
	user.GET("/export", withUser(s.handleUserExportData))
	user.PUT("/password", withUser(s.handleUserChangePassword))
	user.PUT("/address", withUser(s.handleUserChangeAddress))
	user.GET("/stats", withUser(s.handleGetUserStats))

	userMiner := user.Group("/miner")
	userMiner.POST("/claim", withUser(s.handleUserClaimMiner))
	userMiner.GET("/claim/:miner", withUser(s.handleUserGetClaimMinerMsg))
	userMiner.POST("/suspend/:miner", withUser(s.handleSuspendMiner))
	userMiner.PUT("/unsuspend/:miner", withUser(s.handleUnsuspendMiner))
	userMiner.PUT("/set-info/:miner", withUser(s.handleMinersSetInfo))

	contmeta := e.Group("/content")
	uploads := contmeta.Group("", s.AuthRequired(util.PermLevelUpload))
	uploads.POST("/add", withUser(s.handleAdd))
	uploads.POST("/add-ipfs", withUser(s.handleAddIpfs))
	uploads.POST("/add-car", util.WithContentLengthCheck(withUser(s.handleAddCar)))
	uploads.POST("/create", withUser(s.handleCreateContent))

	content := contmeta.Group("", s.AuthRequired(util.PermLevelUser))
	content.GET("/by-cid/:cid", s.handleGetContentByCid)
	content.GET("/stats", withUser(s.handleStats))
	content.GET("/ensure-replication/:datacid", s.handleEnsureReplication)
	content.GET("/status/:id", withUser(s.handleContentStatus))
	content.GET("/list", withUser(s.handleListContent))
	content.GET("/deals", withUser(s.handleListContentWithDeals))
	content.GET("/failures/:content", withUser(s.handleGetContentFailures))
	content.GET("/bw-usage/:content", withUser(s.handleGetContentBandwidth))
	content.GET("/staging-zones", withUser(s.handleGetStagingZoneForUser))
	content.GET("/aggregated/:content", withUser(s.handleGetAggregatedForContent))
	content.GET("/all-deals", withUser(s.handleGetAllDealsForUser))

	// TODO: the commented out routes here are still fairly useful, but maybe
	// need to have some sort of 'super user' permission level in order to use
	// them? Can easily cause harm using them
	deals := e.Group("/deals")
	deals.Use(s.AuthRequired(util.PermLevelUser))
	deals.GET("/status/:deal", withUser(s.handleGetDealStatus))
	deals.GET("/status-by-proposal/:propcid", withUser(s.handleGetDealStatusByPropCid))
	deals.GET("/query/:miner", s.handleQueryAsk)
	deals.POST("/make/:miner", withUser(s.handleMakeDeal))
	//deals.POST("/transfer/start/:miner/:propcid/:datacid", s.handleTransferStart)
	deals.GET("/transfer/status/:id", s.handleTransferStatusByID)
	deals.POST("/transfer/status", s.handleTransferStatus)
	deals.GET("/transfer/in-progress", s.handleTransferInProgress)
	deals.GET("/status/:miner/:propcid", s.handleDealStatus)
	deals.POST("/estimate", s.handleEstimateDealCost)
	deals.GET("/proposal/:propcid", s.handleGetProposal)
	deals.GET("/info/:dealid", s.handleGetDealInfo)
	deals.GET("/failures", withUser(s.handleStorageFailures))

	cols := e.Group("/collections")
	cols.Use(s.AuthRequired(util.PermLevelUser))
	cols.GET("/list", withUser(s.handleListCollections))
	cols.DELETE("/:coluuid", withUser(s.handleDeleteCollection))
	cols.POST("/create", withUser(s.handleCreateCollection))
	cols.POST("/add-content", withUser(s.handleAddContentsToCollection))
	cols.GET("/content", withUser(s.handleGetCollectionContents))
	cols.POST("/:coluuid/commit", withUser(s.handleCommitCollection))

	colfs := cols.Group("/fs")
	colfs.POST("/add", withUser(s.handleColfsAdd))

	pinning := e.Group("/pinning")
	pinning.Use(openApiMiddleware)
	pinning.Use(s.AuthRequired(util.PermLevelUser))
	pinning.GET("/pins", withUser(s.handleListPins))
	pinning.POST("/pins", withUser(s.handleAddPin))
	pinning.GET("/pins/:pinid", withUser(s.handleGetPin))
	pinning.POST("/pins/:pinid", withUser(s.handleReplacePin))
	pinning.DELETE("/pins/:pinid", withUser(s.handleDeletePin))

	// explicitly public, for now
	public := e.Group("/public")

	public.GET("/stats", s.handlePublicStats)
	public.GET("/by-cid/:cid", s.handleGetContentByCid)
	public.GET("/deals/failures", s.handlePublicStorageFailures)
	public.GET("/info", s.handleGetPublicNodeInfo)
	public.GET("/miners", s.handlePublicGetMinerStats)

	metrics := public.Group("/metrics")
	metrics.GET("/deals-on-chain", s.handleMetricsDealOnChain)

	netw := public.Group("/net")
	netw.GET("/peers", s.handleNetPeers)
	netw.GET("/addrs", s.handleNetAddrs)

	miners := public.Group("/miners")
	miners.GET("", s.handleAdminGetMiners)
	miners.GET("/failures/:miner", s.handleGetMinerFailures)
	miners.GET("/deals/:miner", s.handleGetMinerDeals)
	miners.GET("/stats/:miner", s.handleGetMinerStats)
	miners.GET("/storage/query/:miner", s.handleQueryAsk)

	admin := e.Group("/admin")
	admin.Use(s.AuthRequired(util.PermLevelAdmin))
	admin.GET("/balance", s.handleAdminBalance)
	admin.POST("/add-escrow/:amt", s.handleAdminAddEscrow)
	admin.GET("/dealstats", s.handleDealStats)
	admin.GET("/disk-info", s.handleDiskSpaceCheck)
	admin.GET("/stats", s.handleAdminStats)
	admin.GET("/system/config", withUser(s.handleGetSystemConfig))

	// miners
	admin.POST("/miners/add/:miner", s.handleAdminAddMiner)
	admin.POST("/miners/rm/:miner", s.handleAdminRemoveMiner)
	admin.POST("/miners/suspend/:miner", withUser(s.handleSuspendMiner))
	admin.PUT("/miners/unsuspend/:miner", withUser(s.handleUnsuspendMiner))
	admin.PUT("/miners/set-info/:miner", withUser(s.handleMinersSetInfo))
	admin.GET("/miners", s.handleAdminGetMiners)
	admin.GET("/miners/stats", s.handleAdminGetMinerStats)
	admin.GET("/miners/transfers/:miner", s.handleMinerTransferDiagnostics)

	admin.GET("/cm/progress", s.handleAdminGetProgress)
	admin.GET("/cm/all-deals", s.handleDebugGetAllDeals)
	admin.GET("/cm/read/:content", s.handleReadLocalContent)
	admin.GET("/cm/staging/all", s.handleAdminGetStagingZones)
	admin.GET("/cm/offload/candidates", s.handleGetOffloadingCandidates)
	admin.POST("/cm/offload/:content", s.handleOffloadContent)
	admin.POST("/cm/offload/collect", s.handleRunOffloadingCollection)
	admin.GET("/cm/refresh/:content", s.handleRefreshContent)
	admin.POST("/cm/gc", s.handleRunGc)
	admin.POST("/cm/move", s.handleMoveContent)
	admin.GET("/cm/buckets", s.handleGetBucketDiag)
	admin.GET("/cm/health/:id", s.handleContentHealthCheck)
	admin.GET("/cm/health-by-cid/:cid", s.handleContentHealthCheckByCid)
	admin.POST("/cm/dealmaking", s.handleSetDealMaking)
	admin.POST("/cm/break-aggregate/:content", s.handleAdminBreakAggregate)
	admin.POST("/cm/transfer/restart/:chanid", s.handleTransferRestart)
	admin.POST("/cm/repinall/:shuttle", s.handleShuttleRepinAll)

	//	peering
	adminPeering := admin.Group("/peering")
	adminPeering.POST("/peers", s.handlePeeringPeersAdd)
	adminPeering.DELETE("/peers", s.handlePeeringPeersRemove)
	adminPeering.GET("/peers", s.handlePeeringPeersList)
	adminPeering.POST("/start", s.handlePeeringStart)
	adminPeering.POST("/stop", s.handlePeeringStop)
	adminPeering.GET("/status", s.handlePeeringStatus)

	admnetw := admin.Group("/net")
	admnetw.GET("/peers", s.handleNetPeers)

	admin.GET("/retrieval/querytest/:content", s.handleRetrievalCheck)
	admin.GET("/retrieval/stats", s.handleGetRetrievalInfo)

	admin.POST("/invite/:code", withUser(s.handleAdminCreateInvite))
	admin.GET("/invites", s.handleAdminGetInvites)

	admin.GET("/fixdeals", s.handleFixupDeals)
	admin.POST("/loglevel", s.handleLogLevel)

	users := admin.Group("/users")
	users.GET("", s.handleAdminGetUsers)

	shuttle := admin.Group("/shuttle")
	shuttle.POST("/init", s.handleShuttleInit)
	shuttle.GET("/list", s.handleShuttleList)

	ar := admin.Group("/autoretrieve")
	ar.POST("/init", s.handleAutoretrieveInit)
	ar.GET("/list", s.handleAutoretrieveList)

	e.POST("/autoretrieve/heartbeat", s.handleAutoretrieveHeartbeat, s.withAutoretrieveAuth())

	e.GET("/shuttle/conn", s.handleShuttleConnection)
	e.POST("/shuttle/content/create", s.handleShuttleCreateContent, s.withShuttleAuth())

	if os.Getenv("ENABLE_SWAGGER_ENDPOINT") == "true" {
		e.GET("/swagger/*", echoSwagger.WrapHandler)
	}
	return e.Start(s.cfg.ApiListen)
}

type binder struct{}

func (b binder) Bind(i interface{}, c echo.Context) error {
	defer c.Request().Body.Close()
	return json.NewDecoder(c.Request().Body).Decode(i)
}

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
	ID              uint    `json:"id"`
	Cid             cid.Cid `json:"cid"`
	Filename        string  `json:"name"`
	BWUsed          int64   `json:"bwUsed"`
	TotalRequests   int64   `json:"totalRequests"`
	Offloaded       bool    `json:"offloaded"`
	AggregatedFiles int64   `json:"aggregatedFiles"`
}

func withUser(f func(echo.Context, *User) error) func(echo.Context) error {
	return func(c echo.Context) error {
		u, ok := c.Get("user").(*User)
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
// @Description  This endpoint is used to get content statistics. Every content stored in the network (estuary) is tracked by a unique ID which can be used to get information about the content. This endpoint will allow the consumer to get the collected stats of a conten
// @Tags         content
// @Param        limit path string true "limit"
// @Produce      json
// @Router       /content/stats [get]
func (s *Server) handleStats(c echo.Context, u *User) error {
	limit := 500
	if limstr := c.QueryParam("limit"); limstr != "" {
		nlim, err := strconv.Atoi(limstr)
		if err != nil {
			return err
		}

		if nlim > 0 {
			limit = nlim
		}
	}

	offset := 0
	if offstr := c.QueryParam("offset"); offstr != "" {
		noff, err := strconv.Atoi(offstr)
		if err != nil {
			return err
		}

		if noff > 0 {
			offset = noff
		}
	}

	var contents []util.Content
	if err := s.DB.Limit(limit).Offset(offset).Order("created_at desc").Find(&contents, "user_id = ? and active", u.ID).Error; err != nil {
		return err
	}

	out := make([]statsResp, 0, len(contents))
	for _, c := range contents {
		st := statsResp{
			ID:       c.ID,
			Cid:      c.Cid.CID,
			Filename: c.Name,
		}

		if false {
			var res struct {
				Bw         int64
				TotalReads int64
			}

			if err := s.DB.Model(util.ObjRef{}).
				Select("SUM(size * reads) as bw, SUM(reads) as total_reads").
				Where("obj_refs.content = ?", c.ID).
				Joins("left join objects on obj_refs.object = objects.id").
				Scan(&res).Error; err != nil {
				return err
			}

			st.TotalRequests = res.TotalReads
			st.BWUsed = res.Bw
		}

		if c.Aggregate {
			if err := s.DB.Model(util.Content{}).Where("aggregated_in = ?", c.ID).Count(&st.AggregatedFiles).Error; err != nil {
				return err
			}
		}

		out = append(out, st)
	}

	return c.JSON(http.StatusOK, out)
}

// handlePeeringPeersAdd godoc
// @Summary      Add peers on Peering Service
// @Description  This endpoint can be used to add a Peer from the Peering Service
// @Tags         admin,peering,peers
// @Produce      json
// @Router       /admin/peering/peers [post]
func (s *Server) handlePeeringPeersAdd(c echo.Context) error {
	var params []peering.PeeringPeer
	if err := c.Bind(&params); err != nil {
		return err
	}

	//	validate the IDs and Addrs here
	var validPeersAddInfo []peer.AddrInfo
	for _, peerParam := range params {
		//	validate the PeerID
		peerParamId, err := peer.Decode(peerParam.ID)

		if err != nil {
			log.Errorf("handlePeeringPeersAdd error on Decode: %s", err)
			return c.JSON(http.StatusBadRequest,
				util.PeeringPeerAddMessage{
					Message:  "Adding Peer(s) on Peering failed, the peerID is invalid: " + peerParam.ID,
					PeersAdd: params,
				},
			)
		}

		//	validate the Addrs for each PeerID
		var multiAddrs []multiaddr.Multiaddr
		for _, addr := range peerParam.Addrs {
			a, err := multiaddr.NewMultiaddr(addr)
			if err != nil {
				log.Errorf("handlePeeringPeersAdd error: %s", err)
				return c.JSON(http.StatusBadRequest,
					util.PeeringPeerAddMessage{
						Message:  "Adding Peer(s) on Peering failed, the addr is invalid: " + addr,
						PeersAdd: params,
					},
				)
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
// @Tags         admin,peering,peers
// @Produce      json
// @Router       /admin/peering/peers [delete]
func (s *Server) handlePeeringPeersRemove(c echo.Context) error {
	var params []peer.ID

	if err := c.Bind(&params); err != nil {
		log.Errorf("handlePeeringPeersRemove error: %s", err)
		return &util.HttpError{
			Code:   http.StatusBadRequest,
			Reason: util.ERR_PEERING_PEERS_REMOVE_ERROR,
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
// @Tags         admin,peering,peers
// @Produce      json
// @Router       /admin/peering/peers [get]
func (s *Server) handlePeeringPeersList(c echo.Context) error {
	var connectionCheck []peering.PeeringPeer
	for _, peerAddrInfo := range s.Node.Peering.ListPeers() {

		var peerAddrInfoAddrsStr []string
		for _, addrInfo := range peerAddrInfo.Addrs {
			peerAddrInfoAddrsStr = append(peerAddrInfoAddrsStr, addrInfo.String())
		}
		connectionCheck = append(connectionCheck, peering.PeeringPeer{
			ID:        peerAddrInfo.ID.Pretty(),
			Addrs:     peerAddrInfoAddrsStr,
			Connected: (s.Node.Host.Network().Connectedness(peerAddrInfo.ID) == network.Connected),
		})
	}
	return c.JSON(http.StatusOK, connectionCheck)
}

// handlePeeringStart godoc
// @Summary      Start Peering
// @Description  This endpoint can be used to start the Peering Service
// @Tags         admin,peering,peers
// @Produce      json
// @Router       /admin/peering/start [post]
func (s *Server) handlePeeringStart(c echo.Context) error {
	err := s.Node.Peering.Start()
	if err != nil {
		log.Errorf("handlePeeringStart error: %s", err)
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
// @Tags         admin,peering,peers
// @Produce      json
// @Router       /admin/peering/stop [post]
func (s *Server) handlePeeringStop(c echo.Context) error {
	err := s.Node.Peering.Stop()
	if err != nil {
		log.Errorf("handlePeeringStop error: %s", err)
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
// @Tags         admin,peering,peers
// @Produce      json
// @Router       /admin/peering/status [get]
func (s *Server) handlePeeringStatus(c echo.Context) error {
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
// @Param        body body util.ContentAddIpfsBody true "IPFS Body"
// @Router       /content/add-ipfs [post]
func (s *Server) handleAddIpfs(c echo.Context, u *User) error {
	ctx := c.Request().Context()

	if err := util.ErrorIfContentAddingDisabled(s.isContentAddingDisabled(u)); err != nil {
		return err
	}

	var params util.ContentAddIpfsBody
	if err := c.Bind(&params); err != nil {
		return err
	}

	filename := params.Name
	if filename == "" {
		filename = params.Root
	}

	var cols []*CollectionRef
	if params.CollectionID != "" {
		var srchCol Collection
		if err := s.DB.First(&srchCol, "uuid = ? and user_id = ?", params.CollectionID, u.ID).Error; err != nil {
			return err
		}

		// if dir is "" or nil, put the file on the root dir (/filename)
		defaultPath := "/" + filename
		colp := defaultPath
		if params.CollectionDir != "" {
			p, err := sanitizePath(params.CollectionDir)
			if err != nil {
				return err
			}
			colp = p
		}

		// default: colp ends in / (does not include filename e.g. /hello/)
		path := colp + filename

		// if path does not end in /, it includes the filename
		if !strings.HasSuffix(colp, "/") {
			path = colp
			filename = filepath.Base(colp)
		}

		cols = []*CollectionRef{
			{
				Collection: srchCol.ID,
				Path:       &path,
			},
		}
	}

	var origins []*peer.AddrInfo
	for _, p := range params.Peers {
		ai, err := peer.AddrInfoFromString(p)
		if err != nil {
			return err
		}
		origins = append(origins, ai)
	}

	rcid, err := cid.Decode(params.Root)
	if err != nil {
		return err
	}

	if c.QueryParam("ignore-dupes") == "true" {
		var count int64
		if err := s.DB.Model(util.Content{}).Where("cid = ? and user_id = ?", rcid.Bytes(), u.ID).Count(&count).Error; err != nil {
			return err
		}
		if count > 0 {
			return c.JSON(302, map[string]string{"message": "content with given cid already preserved"})
		}
	}

	makeDeal := true
	pinstatus, err := s.CM.pinContent(ctx, u.ID, rcid, filename, cols, origins, 0, nil, makeDeal)
	if err != nil {
		return err
	}
	return c.JSON(http.StatusAccepted, pinstatus)
}

// handleAddCar godoc
// @Summary      Add Car object
// @Description  This endpoint is used to add a car object to the network. The object can be a file or a directory.
// @Tags         content
// @Produce      json
// @Param        body body string true "Car"
// @Param 		 filename query string false "Filename"
// @Param 		 commp query string false "Commp"
// @Param 		 size query string false "Size"
// @Router       /content/add-car [post]
func (s *Server) handleAddCar(c echo.Context, u *User) error {
	ctx := c.Request().Context()

	if err := util.ErrorIfContentAddingDisabled(s.isContentAddingDisabled(u)); err != nil {
		return err
	}
	if s.CM.localContentAddingDisabled {
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

	// 	if bdSize > util.DefaultContentSizeLimit {
	// 		return &util.HttpError{
	// 			Code:    http.StatusBadRequest,
	// 			Reason:  util.ERR_CONTENT_SIZE_OVER_LIMIT,
	// 			Details: fmt.Sprintf("content size %d bytes, is over upload size of limit %d bytes, and content splitting is not enabled, please reduce the content size", bdSize, util.DefaultContentSizeLimit),
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
				log.Errorf("failed to clean up staging blockstore: %s", err)
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

	cont, err := s.CM.addDatabaseTracking(ctx, u, dserv, rootCID, filename, s.CM.Replication)
	if err != nil {
		return err
	}

	if err := s.dumpBlockstoreTo(ctx, sbs, s.Node.Blockstore); err != nil {
		return xerrors.Errorf("failed to move data from staging to main blockstore: %w", err)
	}

	go func() {
		// TODO: we should probably have a queue to throw these in instead of putting them out in goroutines...
		s.CM.toCheck(cont.ID)
	}()

	go func() {
		if err := s.Node.Provider.Provide(rootCID); err != nil {
			log.Warnf("failed to announce providers: %s", err)
		}
	}()

	return c.JSON(http.StatusOK, &util.ContentAddResponse{
		Cid:          rootCID.String(),
		RetrievalURL: util.CreateRetrievalURL(rootCID.String()),
		EstuaryId:    cont.ID,
		Providers:    s.CM.pinDelegatesForContent(*cont),
	})
}

func (s *Server) loadCar(ctx context.Context, bs blockstore.Blockstore, r io.Reader) (*car.CarHeader, error) {
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
// @Param        file formData file true "File to upload"
// @Param        coluuid path string false "Collection UUID"
// @Param        dir path string false "Directory"
// @Router       /content/add [post]
func (s *Server) handleAdd(c echo.Context, u *User) error {
	ctx, span := s.tracer.Start(c.Request().Context(), "handleAdd", trace.WithAttributes(attribute.Int("user", int(u.ID))))
	defer span.End()

	if err := util.ErrorIfContentAddingDisabled(s.isContentAddingDisabled(u)); err != nil {
		return err
	}

	if s.CM.localContentAddingDisabled {
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
	if !u.FlagSplitContent() && mpf.Size > s.CM.contentSizeLimit {
		return &util.HttpError{
			Code:    http.StatusBadRequest,
			Reason:  util.ERR_CONTENT_SIZE_OVER_LIMIT,
			Details: fmt.Sprintf("content size %d bytes, is over upload size limit of %d bytes, and content splitting is not enabled, please reduce the content size", mpf.Size, s.CM.contentSizeLimit),
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

	replication := s.CM.Replication
	replVal := c.FormValue("replication")
	if replVal != "" {
		parsed, err := strconv.Atoi(replVal)
		if err != nil {
			log.Errorf("failed to parse replication value in form data, assuming default for now: %s", err)
		} else {
			replication = parsed
		}
	}

	coluuid := c.QueryParam("coluuid")
	var col *Collection
	if coluuid != "" {
		var srchCol Collection
		if err := s.DB.First(&srchCol, "uuid = ? and user_id = ?", coluuid, u.ID).Error; err != nil {
			return err
		}

		col = &srchCol
	}

	defaultPath := "/"
	path := defaultPath
	if cp := c.QueryParam(ColDir); cp != "" {
		sp, err := sanitizePath(cp)
		if err != nil {
			return err
		}

		path = sp
	}

	bsid, bs, err := s.StagingMgr.AllocNew()
	if err != nil {
		return err
	}

	defer func() {
		go func() {
			if err := s.StagingMgr.CleanUp(bsid); err != nil {
				log.Errorf("failed to clean up staging blockstore: %s", err)
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

	content, err := s.CM.addDatabaseTracking(ctx, u, dserv, nd.Cid(), filename, replication)
	if err != nil {
		return xerrors.Errorf("encountered problem computing object references: %w", err)
	}
	fullPath := filepath.Join(path, content.Name)

	if col != nil {
		log.Infof("COLLECTION CREATION: %d, %d", col.ID, content.ID)
		if err := s.DB.Create(&CollectionRef{
			Collection: col.ID,
			Content:    content.ID,
			Path:       &fullPath,
		}).Error; err != nil {
			log.Errorf("failed to add content to requested collection: %s", err)
		}
	}

	if err := s.dumpBlockstoreTo(ctx, bs, s.Node.Blockstore); err != nil {
		return xerrors.Errorf("failed to move data from staging to main blockstore: %w", err)
	}

	go func() {
		s.CM.toCheck(content.ID)
	}()

	if c.QueryParam("lazy-provide") != "true" {
		subctx, cancel := context.WithTimeout(ctx, time.Second*10)
		defer cancel()
		if err := s.Node.FullRT.Provide(subctx, nd.Cid(), true); err != nil {
			span.RecordError(fmt.Errorf("provide error: %w", err))
			log.Errorf("fullrt provide call errored: %s", err)
		}
	}

	go func() {
		if err := s.Node.Provider.Provide(nd.Cid()); err != nil {
			log.Warnf("failed to announce providers: %s", err)
		}
	}()

	return c.JSON(http.StatusOK, &util.ContentAddResponse{
		Cid:          nd.Cid().String(),
		RetrievalURL: util.CreateRetrievalURL(nd.Cid().String()),
		EstuaryId:    content.ID,
		Providers:    s.CM.pinDelegatesForContent(*content),
	})
}

// redirectContentAdding is called when localContentAddingDisabled is true
// it finds available shuttles and adds the desired content in one of them
func (s *Server) redirectContentAdding(c echo.Context, u *User) error {
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
	// propagate any query params
	//#nosec G404: ignore weak random number generator
	req, err := http.NewRequest("POST", uep[rand.Intn(len(uep))], c.Request().Body)
	if err != nil {
		return err
	}
	req.Header = c.Request().Header.Clone()
	req.URL.RawQuery = c.Request().URL.Query().Encode()

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}

	c.Response().WriteHeader(resp.StatusCode)

	_, err = io.Copy(c.Response().Writer, resp.Body)
	if err != nil {
		return err
	}

	return nil
}

func (s *Server) importFile(ctx context.Context, dserv ipld.DAGService, fi io.Reader) (ipld.Node, error) {
	_, span := s.tracer.Start(ctx, "importFile")
	defer span.End()

	return util.ImportFile(dserv, fi)
}

var noDataTimeout = time.Minute * 10

func (cm *ContentManager) addDatabaseTrackingToContent(ctx context.Context, cont uint, dserv ipld.NodeGetter, root cid.Cid, cb func(int64)) error {
	ctx, span := cm.tracer.Start(ctx, "computeObjRefsUpdate")
	defer span.End()

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	gotData := make(chan struct{}, 1)
	go func() {
		nodata := time.NewTimer(noDataTimeout)
		defer nodata.Stop()

		for {
			select {
			case <-nodata.C:
				cancel()
			case <-gotData:
				nodata.Reset(noDataTimeout)
			case <-ctx.Done():
				return
			}
		}
	}()

	var objlk sync.Mutex
	var objects []*util.Object
	cset := cid.NewSet()

	defer func() {
		cm.inflightCidsLk.Lock()
		_ = cset.ForEach(func(c cid.Cid) error {
			v, ok := cm.inflightCids[c]
			if !ok || v <= 0 {
				log.Errorf("cid should be inflight but isn't: %s", c)
			}

			cm.inflightCids[c]--
			if cm.inflightCids[c] == 0 {
				delete(cm.inflightCids, c)
			}
			return nil
		})
		cm.inflightCidsLk.Unlock()
	}()

	err := merkledag.Walk(ctx, func(ctx context.Context, c cid.Cid) ([]*ipld.Link, error) {
		// cset.Visit gets called first, so if we reach here we should immediately track the CID
		cm.inflightCidsLk.Lock()
		cm.inflightCids[c]++
		cm.inflightCidsLk.Unlock()

		node, err := dserv.Get(ctx, c)
		if err != nil {
			return nil, err
		}

		cb(int64(len(node.RawData())))

		select {
		case gotData <- struct{}{}:
		case <-ctx.Done():
		}

		objlk.Lock()
		objects = append(objects, &util.Object{
			Cid:  util.DbCID{CID: c},
			Size: len(node.RawData()),
		})
		objlk.Unlock()

		if c.Type() == cid.Raw {
			return nil, nil
		}

		return util.FilterUnwalkableLinks(node.Links()), nil
	}, root, cset.Visit, merkledag.Concurrent())

	if err != nil {
		return err
	}
	return cm.addObjectsToDatabase(ctx, cont, dserv, root, objects, constants.ContentLocationLocal)
}

func (cm *ContentManager) addDatabaseTracking(ctx context.Context, u *User, dserv ipld.NodeGetter, root cid.Cid, filename string, replication int) (*util.Content, error) {
	ctx, span := cm.tracer.Start(ctx, "computeObjRefs")
	defer span.End()

	content := &util.Content{
		Cid:         util.DbCID{CID: root},
		Name:        filename,
		Active:      false,
		Pinning:     true,
		UserID:      u.ID,
		Replication: replication,
		Location:    constants.ContentLocationLocal,
	}

	if err := cm.DB.Create(content).Error; err != nil {
		return nil, xerrors.Errorf("failed to track new content in database: %w", err)
	}

	if err := cm.addDatabaseTrackingToContent(ctx, content.ID, dserv, root, func(int64) {}); err != nil {
		return nil, err
	}

	return content, nil
}

func (s *Server) dumpBlockstoreTo(ctx context.Context, from, to blockstore.Blockstore) error {
	ctx, span := s.tracer.Start(ctx, "blockstoreCopy")
	defer span.End()

	// TODO: smarter batching... im sure ive written this logic before, just gotta go find it
	keys, err := from.AllKeysChan(ctx)
	if err != nil {
		return err
	}

	var batch []blocks.Block

	for k := range keys {
		blk, err := from.Get(ctx, k)
		if err != nil {
			return err
		}

		batch = append(batch, blk)

		if len(batch) > 500 {
			if err := to.PutMany(ctx, batch); err != nil {
				return err
			}
			batch = batch[:0]
		}
	}

	if len(batch) > 0 {
		if err := to.PutMany(ctx, batch); err != nil {
			return err
		}
	}
	return nil
}

// handleEnsureReplication godoc
// @Summary      Ensure Replication
// @Description  This endpoint ensures that the content is replicated to the specified number of providers
// @Tags         content
// @Produce      json
// @Param        datacid path string true "Data CID"
// @Router       /content/ensure-replication/{datacid} [get]
func (s *Server) handleEnsureReplication(c echo.Context) error {
	data, err := cid.Decode(c.Param("datacid"))
	if err != nil {
		return err
	}

	var content util.Content
	if err := s.DB.Find(&content, "cid = ?", data.Bytes()).Error; err != nil {
		return err
	}

	fmt.Println("Content: ", content.Cid.CID, data)

	s.CM.toCheck(content.ID)
	return nil
}

// handleListContent godoc
// @Summary      List all pinned content
// @Description  This endpoint lists all content
// @Tags         content
// @Produce      json
// @Success 	200 {array} string
// @Router       /content/list [get]
func (s *Server) handleListContent(c echo.Context, u *User) error {
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
// @Param limit query int false "Limit"
// @Param offset query int false "Offset"
// @Router       /content/deals [get]
func (s *Server) handleListContentWithDeals(c echo.Context, u *User) error {

	var limit int = 20
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
	if err := s.DB.Limit(limit).Offset(offset).Order("id desc").Find(&contents, "active and user_id = ? and not aggregated_in > 0", u.ID).Error; err != nil {
		return err
	}

	out := make([]expandedContent, 0, len(contents))
	for _, cont := range contents {
		if !s.CM.contentInStagingZone(c.Request().Context(), cont) {
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
	}

	return c.JSON(http.StatusOK, out)
}

type onChainDealState struct {
	SectorStartEpoch abi.ChainEpoch `json:"sectorStartEpoch"`
	LastUpdatedEpoch abi.ChainEpoch `json:"lastUpdatedEpoch"`
	SlashEpoch       abi.ChainEpoch `json:"slashEpoch"`
}

type dealStatus struct {
	Deal           contentDeal             `json:"deal"`
	TransferStatus *filclient.ChannelState `json:"transfer"`
	OnChainState   *onChainDealState       `json:"onChainState"`
}

// handleContentStatus godoc
// @Summary      Content Status
// @Description  This endpoint returns the status of a content
// @Tags         content
// @Produce      json
// @Param id path int true "Content ID"
// @Router       /content/status/{id} [get]
func (s *Server) handleContentStatus(c echo.Context, u *User) error {
	ctx := c.Request().Context()
	contID, err := strconv.Atoi(c.Param("id"))
	if err != nil {
		return err
	}

	var content util.Content
	if err := s.DB.First(&content, "id = ?", contID).Error; err != nil {
		return err
	}

	if err := util.IsContentOwner(u.ID, content.UserID); err != nil {
		return err
	}

	var deals []contentDeal
	if err := s.DB.Find(&deals, "content = ?", content.ID).Error; err != nil {
		return err
	}

	ds := make([]dealStatus, len(deals))
	var wg sync.WaitGroup
	for i := range deals {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			d := deals[i]
			dstatus := dealStatus{
				Deal: d,
			}

			chanst, err := s.CM.GetTransferStatus(ctx, &d, &content)
			if err != nil {
				log.Errorf("failed to get transfer status: %s", err)
			}

			dstatus.TransferStatus = chanst

			if d.DealID > 0 {
				markDeal, err := s.Api.StateMarketStorageDeal(ctx, abi.DealID(d.DealID), types.EmptyTSK)
				if err != nil {
					log.Warnw("failed to get deal info from market actor", "dealID", d.DealID, "error", err)
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
	if err := s.DB.Model(&dfeRecord{}).Where("content = ?", content.ID).Count(&failCount).Error; err != nil {
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
// @Param deal path int true "Deal ID"
// @Router       /deals/status/{deal} [get]
func (s *Server) handleGetDealStatus(c echo.Context, u *User) error {
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
// @Param 		propcid path string true "PropCid"
// @Router       /deal/status-by-proposal/{propcid} [get]
func (s *Server) handleGetDealStatusByPropCid(c echo.Context, u *User) error {
	ctx := c.Request().Context()

	propcid, err := cid.Decode(c.Param("propcid"))
	if err != nil {
		return err
	}

	var deal contentDeal
	if err := s.DB.First(&deal, "prop_cid = ?", propcid.Bytes()).Error; err != nil {
		return err
	}

	dstatus, err := s.dealStatusByID(ctx, deal.ID)
	if err != nil {
		return err
	}

	return c.JSON(http.StatusOK, dstatus)
}

func (s *Server) dealStatusByID(ctx context.Context, dealid uint) (*dealStatus, error) {
	var deal contentDeal
	if err := s.DB.First(&deal, "id = ?", dealid).Error; err != nil {
		return nil, err
	}

	var content util.Content
	if err := s.DB.First(&content, "id = ?", deal.Content).Error; err != nil {
		return nil, err
	}

	chanst, err := s.CM.GetTransferStatus(ctx, &deal, &content)
	if err != nil {
		log.Errorf("failed to get transfer status: %s", err)
	}

	dstatus := dealStatus{
		Deal:           deal,
		TransferStatus: chanst,
	}

	if deal.DealID > 0 {
		markDeal, err := s.Api.StateMarketStorageDeal(ctx, abi.DealID(deal.DealID), types.EmptyTSK)
		if err != nil {
			log.Warnw("failed to get deal info from market actor", "dealID", deal.DealID, "error", err)
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
	Content      *util.Content  `json:"content"`
	AggregatedIn *util.Content  `json:"aggregatedIn,omitempty"`
	Selector     string         `json:"selector,omitempty"`
	Deals        []*contentDeal `json:"deals"`
}

func (s *Server) calcSelector(aggregatedIn uint, contentID uint) (string, error) {
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
// @Description  This endpoint returns the content associated with a CID
// @Tags         public
// @Produce      json
// @Param 		cid path string true "Cid"
// @Router       /public/by-cid/{cid} [get]
func (s *Server) handleGetContentByCid(c echo.Context) error {
	obj, err := cid.Decode(c.Param("cid"))
	if err != nil {
		return errors.Wrapf(err, "invalid cid")
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

		var deals []*contentDeal
		if err := s.DB.Find(&deals, "content = ? and deal_id > 0 and not failed", id).Error; err != nil {
			return err
		}

		resp.Deals = deals

		out = append(out, resp)
	}

	return c.JSON(http.StatusOK, out)
}

// handleQueryAsk godoc
// @Summary      Query Ask
// @Description  This endpoint returns the ask for a given CID
// @Tags         deals
// @Produce      json
// @Param 		 miner path string true "CID"
// @Router       /deal/query/{miner} [get]
func (s *Server) handleQueryAsk(c echo.Context) error {
	addr, err := address.NewFromString(c.Param("miner"))
	if err != nil {
		return err
	}

	ask, err := s.CM.getAsk(c.Request().Context(), addr, 0)
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
// @Param miner path string true "Miner"
// @Param dealRequest body string true "Deal Request"
// @Router       /deals/make/{miner} [post]
func (s *Server) handleMakeDeal(c echo.Context, u *User) error {
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
	}

	id, err := s.CM.makeDealWithMiner(ctx, cont, addr)
	if err != nil {
		return err
	}

	return c.JSON(http.StatusOK, map[string]interface{}{
		"deal": id,
	})
}

// handleTransferStatus godoc
// @Summary      Transfer Status
// @Description  This endpoint returns the status of a transfer
// @Tags         deals
// @Produce      json
// @Router       /deal/transfer/status [post]
func (s *Server) handleTransferStatus(c echo.Context) error {
	var chanid datatransfer.ChannelID
	if err := c.Bind(&chanid); err != nil {
		return err
	}

	status, err := s.FilClient.TransferStatus(context.TODO(), &chanid)
	if err != nil {
		return err
	}

	return c.JSON(http.StatusOK, status)
}

func (s *Server) handleTransferStatusByID(c echo.Context) error {
	status, err := s.FilClient.TransferStatusByID(context.TODO(), c.Param("id"))
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
// @Router       /deal/transfer/in-progress [get]
func (s *Server) handleTransferInProgress(c echo.Context) error {
	ctx := context.TODO()

	transfers, err := s.FilClient.TransfersInProgress(ctx)
	if err != nil {
		return err
	}

	return c.JSON(http.StatusOK, transfers)
}

func (s *Server) handleMinerTransferDiagnostics(c echo.Context) error {
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

func (s *Server) handleTransferRestart(c echo.Context) error {
	ctx := c.Request().Context()

	dealid, err := strconv.Atoi(c.Param("deal"))
	if err != nil {
		return err
	}

	var deal contentDeal
	if err := s.DB.First(&deal, "id = ?", dealid).Error; err != nil {
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

	if err := s.CM.RestartTransfer(ctx, cont.Location, chanid, deal.ID); err != nil {
		return err
	}
	return nil
}

// handleDealStatus godoc
// @Summary      Deal Status
// @Description  This endpoint returns the status of a deal
// @Tags         deals
// @Produce      json
// @Param miner path string true "Miner"
// @Param propcid path string true "Proposal CID"
// @Router       /deal/status/{miner}/{propcid} [get]
func (s *Server) handleDealStatus(c echo.Context) error {
	ctx := c.Request().Context()

	addr, err := address.NewFromString(c.Param("miner"))
	if err != nil {
		return err
	}

	propCid, err := cid.Decode(c.Param("propcid"))
	if err != nil {
		return err
	}

	var d contentDeal
	if err := s.DB.First(&d, "prop_cid = ?", propCid.Bytes()).Error; err != nil {
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
// @Param propcid path string true "Proposal CID"
// @Router       /deal/proposal/{propcid} [get]
func (s *Server) handleGetProposal(c echo.Context) error {
	propCid, err := cid.Decode(c.Param("propcid"))
	if err != nil {
		return err
	}

	var proprec proposalRecord
	if err := s.DB.First(&proprec, "prop_cid = ?", propCid.Bytes()).Error; err != nil {
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
// @Param 	  	 dealid path int true "Deal ID"
// @Router       /deal/info/{dealid} [get]
func (s *Server) handleGetDealInfo(c echo.Context) error {
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
}

func (s *Server) handleAdminGetInvites(c echo.Context) error {
	var invites []getInvitesResp
	if err := s.DB.Model(&InviteCode{}).
		Select("code, username, (?) as claimed_by", s.DB.Table("users").Select("username").Where("id = invite_codes.claimed_by")).
		//Where("claimed_by IS NULL").
		Joins("left join users on users.id = invite_codes.created_by").
		Scan(&invites).Error; err != nil {
		return err
	}

	return c.JSON(http.StatusOK, invites)
}

func (s *Server) handleAdminCreateInvite(c echo.Context, u *User) error {
	code := c.Param("code")
	invite := &InviteCode{
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

func (s *Server) handleAdminBalance(c echo.Context) error {
	balance, err := s.FilClient.Balance(c.Request().Context())
	if err != nil {
		return err
	}

	return c.JSON(http.StatusOK, balance)
}

func (s *Server) handleAdminAddEscrow(c echo.Context) error {
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

func (s *Server) handleAdminStats(c echo.Context) error {

	var dealsTotal int64
	if err := s.DB.Model(&contentDeal{}).Count(&dealsTotal).Error; err != nil {
		return err
	}

	var dealsSuccessful int64
	if err := s.DB.Model(&contentDeal{}).Where("deal_id > 0").Count(&dealsSuccessful).Error; err != nil {
		return err
	}

	var dealsFailed int64
	if err := s.DB.Model(&contentDeal{}).Where("failed").Count(&dealsFailed).Error; err != nil {
		return err
	}

	var numMiners int64
	if err := s.DB.Model(&storageMiner{}).Count(&numMiners).Error; err != nil {
		return err
	}

	var numUsers int64
	if err := s.DB.Model(&User{}).Count(&numUsers).Error; err != nil {
		return err
	}

	var numFiles int64
	if err := s.DB.Model(&util.Content{}).Where("active").Count(&numFiles).Error; err != nil {
		return err
	}

	var numRetrievals int64
	if err := s.DB.Model(&retrievalSuccessRecord{}).Count(&numRetrievals).Error; err != nil {
		return err
	}

	var numRetrievalFailures int64
	if err := s.DB.Model(&util.RetrievalFailureRecord{}).Count(&numRetrievalFailures).Error; err != nil {
		return err
	}

	var numStorageFailures int64
	if err := s.DB.Model(&dfeRecord{}).Count(&numStorageFailures).Error; err != nil {
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
		PinQueueSize:         s.CM.pinMgr.PinQueueSize(),
	})
}

// handleGetSystemConfig godoc
// @Summary      Get systems(estuary/shuttle) config
// @Description  This endpoint is used to get system configs.
// @Tags       	 admin
// @Produce      json
// @Router       /admin/system/config [get]
func (s *Server) handleGetSystemConfig(c echo.Context, u *User) error {
	var shts []interface{}
	for _, sh := range s.CM.shuttles {
		if sh.hostname == "" {
			log.Warnf("failed to get shuttle(%s) config, shuttle hostname is not set", sh.handle)
			continue
		}

		out, err := s.getShuttleConfig(sh.hostname, u.authToken.Token)
		if err != nil {
			log.Warnf("failed to get shuttle config: %s", err)
			continue
		}
		shts = append(shts, out)
	}

	resp := map[string]interface{}{
		"data": map[string]interface{}{
			"primary":  s.cfg,
			"shuttles": shts,
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
// @Router       /public/miners [get]
func (s *Server) handleAdminGetMiners(c echo.Context) error {
	var miners []storageMiner
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

func (s *Server) handlePublicGetMinerStats(c echo.Context) error {
	_, stats, err := s.CM.sortedMinerList()
	if err != nil {
		return err
	}

	return c.JSON(http.StatusOK, stats)
}

func (s *Server) handleAdminGetMinerStats(c echo.Context) error {
	sml, err := s.CM.computeSortedMinerList()
	if err != nil {
		return err
	}

	return c.JSON(http.StatusOK, sml)
}

type minerSetInfoParams struct {
	Name string `json:"name"`
}

func (s *Server) handleMinersSetInfo(c echo.Context, u *User) error {
	m, err := address.NewFromString(c.Param("miner"))
	if err != nil {
		return err
	}

	var sm storageMiner
	if err := s.DB.First(&sm, "address = ?", m.String()).Error; err != nil {
		return err
	}

	if !(u.Perm >= util.PermLevelAdmin || sm.Owner == u.ID) {
		return &util.HttpError{
			Code:   http.StatusUnauthorized,
			Reason: util.ERR_MINER_NOT_OWNED,
		}
	}

	var params minerSetInfoParams
	if err := c.Bind(&params); err != nil {
		return err
	}

	if err := s.DB.Model(storageMiner{}).Where("address = ?", m.String()).Update("name", params.Name).Error; err != nil {
		return err
	}

	return c.JSON(http.StatusOK, map[string]string{})
}

func (s *Server) handleAdminRemoveMiner(c echo.Context) error {
	m, err := address.NewFromString(c.Param("miner"))
	if err != nil {
		return err
	}

	if err := s.DB.Unscoped().Where("address = ?", m.String()).Delete(&storageMiner{}).Error; err != nil {
		return err
	}

	return c.JSON(http.StatusOK, map[string]string{})
}

type suspendMinerBody struct {
	Reason string `json:"reason"`
}

func (s *Server) handleSuspendMiner(c echo.Context, u *User) error {
	m, err := address.NewFromString(c.Param("miner"))
	if err != nil {
		return err
	}

	var sm storageMiner
	if err := s.DB.First(&sm, "address = ?", m.String()).Error; err != nil {
		return err
	}

	if !(u.Perm >= util.PermLevelAdmin || sm.Owner == u.ID) {
		return &util.HttpError{
			Code:   http.StatusUnauthorized,
			Reason: util.ERR_MINER_NOT_OWNED,
		}
	}

	var body suspendMinerBody
	if err := c.Bind(&body); err != nil {
		return err
	}

	if err := s.DB.Model(&storageMiner{}).Where("address = ?", m.String()).Updates(map[string]interface{}{
		"suspended":        true,
		"suspended_reason": body.Reason,
	}).Error; err != nil {
		return err
	}

	return c.JSON(http.StatusOK, map[string]string{})
}

func (s *Server) handleUnsuspendMiner(c echo.Context, u *User) error {
	m, err := address.NewFromString(c.Param("miner"))
	if err != nil {
		return err
	}

	var sm storageMiner
	if err := s.DB.First(&sm, "address = ?", m.String()).Error; err != nil {
		return err
	}

	if !(u.Perm >= util.PermLevelAdmin || sm.Owner == u.ID) {
		return &util.HttpError{
			Code:   http.StatusUnauthorized,
			Reason: util.ERR_MINER_NOT_OWNED,
		}
	}

	if err := s.DB.Model(&storageMiner{}).Where("address = ?", m.String()).Update("suspended", false).Error; err != nil {
		return err
	}

	return c.JSON(http.StatusOK, map[string]string{})
}

func (s *Server) handleAdminAddMiner(c echo.Context) error {
	m, err := address.NewFromString(c.Param("miner"))
	if err != nil {
		return err
	}

	name := c.QueryParam("name")

	if err := s.DB.Clauses(&clause.OnConflict{UpdateAll: true}).Create(&storageMiner{
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

func (s *Server) handleDealStats(c echo.Context) error {
	ctx, span := s.tracer.Start(c.Request().Context(), "handleDealStats")
	defer span.End()

	var alldeals []contentDeal
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
			log.Errorf("checking deal status failed (%s): %s", maddr, err)
			continue
		}
		if st.Proposal == nil {
			log.Errorf("deal status proposal is empty (%s): %s", maddr, d.PropCid.CID)
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

func (s *Server) handleDiskSpaceCheck(c echo.Context) error {
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

func (s *Server) handleGetRetrievalInfo(c echo.Context) error {
	var infos []retrievalSuccessRecord
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

func (s *Server) handleRetrievalCheck(c echo.Context) error {
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
	Asks     []*minerStorageAsk
}

// handleEstimateDealCost godoc
// @Summary      Estimate the cost of a deal
// @Description  This endpoint estimates the cost of a deal
// @Tags         deals
// @Produce      json
// @Param body body main.estimateDealBody true "The size of the deal in bytes, the replication factor, and the duration of the deal in blocks"
// @Router       /deal/estimate [post]
func (s *Server) handleEstimateDealCost(c echo.Context) error {
	ctx := c.Request().Context()

	var body estimateDealBody
	if err := c.Bind(&body); err != nil {
		return err
	}

	pieceSize := padreader.PaddedSize(body.Size)

	estimate, err := s.CM.estimatePrice(ctx, body.Replication, pieceSize.Padded(), abi.ChainEpoch(body.DurationBlks), body.Verified)
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
// @Param miner query string false "Filter by miner"
// @Router       /public/miners/failures/{miner} [get]
func (s *Server) handleGetMinerFailures(c echo.Context) error {
	maddr, err := address.NewFromString(c.Param("miner"))
	if err != nil {
		return err
	}

	var merrs []dfeRecord
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
// @Param miner path string false "Filter by miner"
// @Router       /public/miners/stats/{miner} [get]
func (s *Server) handleGetMinerStats(c echo.Context) error {
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

	var m storageMiner
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
	if err := s.DB.Model(&contentDeal{}).Where("miner = ?", maddr.String()).Count(&dealscount).Error; err != nil {
		return err
	}

	var errorcount int64
	if err := s.DB.Model(&dfeRecord{}).Where("miner = ?", maddr.String()).Count(&errorcount).Error; err != nil {
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
// @Param miner path string false "Filter by miner"
// @Router       /public/miners/deals/{miner} [get]
func (s *Server) handleGetMinerDeals(c echo.Context) error {
	maddr, err := address.NewFromString(c.Param("miner"))
	if err != nil {
		return err
	}

	q := s.DB.Model(contentDeal{}).Order("created_at desc").
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
// @Param 		 content path string true "Content ID"
// @Router       /content/bw-usage/{content} [get]
func (s *Server) handleGetContentBandwidth(c echo.Context, u *User) error {
	contID, err := strconv.Atoi(c.Param("content"))
	if err != nil {
		return err
	}

	var content util.Content
	if err := s.DB.First(&content, contID).Error; err != nil {
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
// @Param content path string true "Content ID"
// @Success 	200 {object} string
// @Router       /content/aggregated/{content} [get]
func (s *Server) handleGetAggregatedForContent(c echo.Context, u *User) error {
	contID, err := strconv.Atoi(c.Param("content"))
	if err != nil {
		return err
	}

	var content util.Content
	if err := s.DB.First(&content, "id = ?", contID).Error; err != nil {
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
// @Param content path string true "Content ID"
// @Success 	200 {object} string
// @Router       /content/failures/{content} [get]
func (s *Server) handleGetContentFailures(c echo.Context, u *User) error {
	cont, err := strconv.Atoi(c.Param("content"))
	if err != nil {
		return err
	}

	var errs []dfeRecord
	if err := s.DB.Find(&errs, "content = ?", cont).Error; err != nil {
		return err
	}

	return c.JSON(http.StatusOK, errs)
}

func (s *Server) handleAdminGetStagingZones(c echo.Context) error {
	s.CM.bucketLk.Lock()
	defer s.CM.bucketLk.Unlock()

	return c.JSON(http.StatusOK, s.CM.buckets)
}

func (s *Server) handleGetOffloadingCandidates(c echo.Context) error {
	conts, err := s.CM.getRemovalCandidates(c.Request().Context(), c.QueryParam("all") == "true", c.QueryParam("location"), nil)
	if err != nil {
		return err
	}

	return c.JSON(http.StatusOK, conts)
}

func (s *Server) handleRunOffloadingCollection(c echo.Context) error {
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

func (s *Server) handleOffloadContent(c echo.Context) error {
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

func (s *Server) handleMoveContent(c echo.Context) error {
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
		log.Warnf("got back fewer contents than requested: %d != %d", len(contents), len(body.Contents))
	}

	var shuttle Shuttle
	if err := s.DB.First(&shuttle, "handle = ?", body.Destination).Error; err != nil {
		return err
	}

	if err := s.CM.sendConsolidateContentCmd(ctx, shuttle.Handle, contents); err != nil {
		return err
	}

	return c.JSON(http.StatusOK, map[string]string{})
}

func (s *Server) handleRefreshContent(c echo.Context) error {
	cont, err := strconv.Atoi(c.Param("content"))
	if err != nil {
		return err
	}

	if err := s.CM.RefreshContent(c.Request().Context(), uint(cont)); err != nil {
		return c.JSON(500, map[string]string{"error": err.Error()})
	}

	return c.JSON(http.StatusOK, map[string]string{})
}

func (s *Server) handleReadLocalContent(c echo.Context) error {
	cont, err := strconv.Atoi(c.Param("content"))
	if err != nil {
		return err
	}

	var content util.Content
	if err := s.DB.First(&content, "id = ?", cont).Error; err != nil {
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

func (s *Server) checkTokenAuth(token string) (*User, error) {
	var authToken AuthToken
	if err := s.DB.First(&authToken, "token = ?", token).Error; err != nil {
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

	var user User
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

	user.authToken = authToken
	return &user, nil
}

func (s *Server) AuthRequired(level int) echo.MiddlewareFunc {
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

			if u.authToken.UploadOnly && level >= util.PermLevelUser {
				log.Warnw("api key is upload only", "user", u.ID, "perm", u.Perm, "required", level)

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

			log.Warnw("user not authorized", "user", u.ID, "perm", u.Perm, "required", level)

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

func (s *Server) handleRegisterUser(c echo.Context) error {
	var reg registerBody
	if err := c.Bind(&reg); err != nil {
		return err
	}

	var invite InviteCode
	if err := s.DB.First(&invite, "code = ?", reg.InviteCode).Error; err != nil {
		if xerrors.Is(err, gorm.ErrRecordNotFound) {
			return &util.HttpError{
				Code:   http.StatusNotFound,
				Reason: util.ERR_INVALID_INVITE,
			}
		}
		return err
	}

	if invite.ClaimedBy != 0 {
		return &util.HttpError{
			Code:   http.StatusBadRequest,
			Reason: util.ERR_INVITE_ALREADY_USED,
		}
	}

	username := strings.ToLower(reg.Username)

	var exist *User
	if err := s.DB.First(&exist, "username = ?", username).Error; err != nil {
		if !xerrors.Is(err, gorm.ErrRecordNotFound) {
			return err
		}
		exist = nil
	}

	if exist != nil {
		return &util.HttpError{
			Code:   http.StatusBadRequest,
			Reason: util.ERR_USERNAME_TAKEN,
		}
	}

	salt := uuid.New().String()

	newUser := &User{
		Username: username,
		UUID:     uuid.New().String(),
		Salt:     salt,
		PassHash: util.GetPasswordHash(reg.Password, salt),
		Perm:     util.PermLevelUser,
	}

	if err := s.DB.Create(newUser).Error; err != nil {
		return &util.HttpError{
			Code:   http.StatusInternalServerError,
			Reason: util.ERR_USER_CREATION_FAILED,
		}
	}

	authToken := &AuthToken{
		Token:  "EST" + uuid.New().String() + "ARY",
		User:   newUser.ID,
		Expiry: time.Now().Add(time.Hour * 24 * 7),
	}

	if err := s.DB.Create(authToken).Error; err != nil {
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

func (s *Server) handleLoginUser(c echo.Context) error {
	var body loginBody
	if err := c.Bind(&body); err != nil {
		return err
	}

	var user User
	if err := s.DB.First(&user, "username = ?", strings.ToLower(body.Username)).Error; err != nil {
		if xerrors.Is(err, gorm.ErrRecordNotFound) {
			return &util.HttpError{
				Code:   http.StatusForbidden,
				Reason: util.ERR_USER_NOT_FOUND,
			}
		}
		return err
	}

	//	validate password
	if ((user.Salt != "") && user.PassHash != util.GetPasswordHash(body.Password, user.Salt)) ||
		((user.Salt == "") && (user.PassHash != body.Password)) {
		return &util.HttpError{
			Code:   http.StatusForbidden,
			Reason: util.ERR_INVALID_PASSWORD,
		}
	}

	authToken, err := s.newAuthTokenForUser(&user, time.Now().Add(time.Hour*24*30), nil)
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

func (s *Server) handleUserChangePassword(c echo.Context, u *User) error {
	var params changePasswordParams
	if err := c.Bind(&params); err != nil {
		return err
	}

	salt := uuid.New().String()

	updatedUserColumns := &User{
		Salt:     salt,
		PassHash: util.GetPasswordHash(params.NewPassword, salt),
	}

	if err := s.DB.Model(User{}).Where("id = ?", u.ID).Updates(updatedUserColumns).Error; err != nil {
		return err
	}

	return c.JSON(http.StatusOK, map[string]string{})
}

type changeAddressParams struct {
	Address string `json:"address"`
}

func (s *Server) handleUserChangeAddress(c echo.Context, u *User) error {
	var params changeAddressParams
	if err := c.Bind(&params); err != nil {
		return err
	}

	addr, err := address.NewFromString(params.Address)
	if err != nil {
		log.Warnf("invalid filecoin address in change address request body: %w", err)

		return &util.HttpError{
			Code:   http.StatusUnauthorized,
			Reason: "invalid address in request body",
		}
	}

	if err := s.DB.Model(User{}).Where("id = ?", u.ID).Update("address", addr.String()).Error; err != nil {
		return err
	}

	return c.JSON(http.StatusOK, map[string]string{})
}

type userStatsResponse struct {
	TotalSize int64 `json:"totalSize"`
	NumPins   int64 `json:"numPins"`
}

// handleGetUserStats godoc
// @Summary      Create API keys for a user
// @Description  This endpoint is used to create API keys for a user.
// @Tags         User
// @Produce      json
// @Success      200  {object}  userStatsResponse
// @Router       /user/stats [get]
func (s *Server) handleGetUserStats(c echo.Context, u *User) error {
	var stats userStatsResponse
	if err := s.DB.Raw(` SELECT
						(SELECT SUM(size) FROM contents where user_id = ? AND aggregated_in = 0 AND active) as total_size,
						(SELECT COUNT(1) FROM contents where user_id = ? AND active) as num_pins`,
		u.ID, u.ID).Scan(&stats).Error; err != nil {
		return err
	}

	return c.JSON(http.StatusOK, stats)
}

func (s *Server) newAuthTokenForUser(user *User, expiry time.Time, perms []string) (*AuthToken, error) {
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

	authToken := &AuthToken{
		Token:      "EST" + uuid.New().String() + "ARY",
		User:       user.ID,
		Expiry:     expiry,
		UploadOnly: uploadOnly,
	}

	if err := s.DB.Create(authToken).Error; err != nil {
		return nil, err
	}

	return authToken, nil
}

func (s *Server) handleGetViewer(c echo.Context, u *User) error {
	uep, err := s.getPreferredUploadEndpoints(u)
	if err != nil {
		return err
	}

	return c.JSON(http.StatusOK, &util.ViewerResponse{
		ID:       u.ID,
		Username: u.Username,
		Perms:    u.Perm,
		Address:  u.Address.Addr.String(),
		Miners:   s.getMinersOwnedByUser(u),
		Settings: util.UserSettings{
			Replication:           s.CM.Replication,
			Verified:              s.cfg.Deal.IsVerified,
			DealDuration:          s.cfg.Deal.Duration,
			MaxStagingWait:        s.cfg.StagingBucket.MaxLifeTime,
			FileStagingThreshold:  s.cfg.StagingBucket.IndividualDealThreshold,
			ContentAddingDisabled: s.isContentAddingDisabled(u),
			DealMakingDisabled:    s.CM.dealMakingDisabled(),
			UploadEndpoints:       uep,
			Flags:                 u.Flags,
		},
		AuthExpiry: u.authToken.Expiry,
	})
}

func (s *Server) getMinersOwnedByUser(u *User) []string {
	var miners []storageMiner
	if err := s.DB.Find(&miners, "owner = ?", u.ID).Error; err != nil {
		log.Errorf("failed to query miners for user %d: %s", u.ID, err)
		return nil
	}

	var out []string
	for _, m := range miners {
		out = append(out, m.Address.Addr.String())
	}

	return out
}

func (s *Server) getPreferredUploadEndpoints(u *User) ([]string, error) {

	// TODO: this should be a lotttttt smarter
	s.CM.shuttlesLk.Lock()
	defer s.CM.shuttlesLk.Unlock()
	var shuttles []Shuttle
	for hnd, sh := range s.CM.shuttles {
		if sh.hostname == "" {
			log.Debugf("shuttle %+v has empty hostname", sh)
			continue
		}

		var shuttle Shuttle
		if err := s.DB.First(&shuttle, "handle = ?", hnd).Error; err != nil {
			log.Errorf("failed to look up shuttle by handle: %s", err)
			continue
		}

		if !shuttle.Open {
			log.Debugf("shuttle %+v is not open, skipping", shuttle)
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
	if !s.CM.localContentAddingDisabled {
		out = append(out, s.CM.hostname+"/content/add")
	}

	return out, nil
}

func (s *Server) handleHealth(c echo.Context) error {
	return c.JSON(http.StatusOK, map[string]string{
		"status": "ok",
	})
}

type getApiKeysResp struct {
	Token  string    `json:"token"`
	Expiry time.Time `json:"expiry"`
}

// handleUserRevokeApiKey godoc
// @Summary      Revoke a User API Key.
// @Description  This endpoint is used to revoke a user API key. In estuary, every user is assigned with an API key, this API key is generated and issued for each user and is primarily use to access all estuary features. This endpoint can be used to revoke the API key thats assigned to the user.
// @Tags         User
// @Produce      json
// @Param        key path string true "Key"
// @Router       /user/api-keys/{key} [delete]
func (s *Server) handleUserRevokeApiKey(c echo.Context, u *User) error {
	kval := c.Param("key")

	if err := s.DB.Delete(&AuthToken{}, "\"user\" = ? AND token = ?", u.ID, kval).Error; err != nil {
		return err
	}

	return c.NoContent(200)
}

// handleUserCreateApiKey godoc
// @Summary      Create API keys for a user
// @Description  This endpoint is used to create API keys for a user. In estuary, each user is given an API key to access all features.
// @Tags         User
// @Produce      json
// @Success      200  {object}  getApiKeysResp
// @Failure      400  {object}  util.HttpError
// @Failure      404  {object}  util.HttpError
// @Failure      500  {object}  util.HttpError
// @Router       /user/api-keys [post]
func (s *Server) handleUserCreateApiKey(c echo.Context, u *User) error {
	expiry := time.Now().Add(time.Hour * 24 * 30)
	if exp := c.QueryParam("expiry"); exp != "" {
		if exp == "false" {
			expiry = time.Now().Add(time.Hour * 24 * 365 * 100) // 100 years is forever enough
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

	authToken, err := s.newAuthTokenForUser(u, expiry, perms)
	if err != nil {
		return err
	}

	return c.JSON(http.StatusOK, &getApiKeysResp{
		Token:  authToken.Token,
		Expiry: authToken.Expiry,
	})
}

// handleUserGetApiKeys godoc
// @Summary      Get API keys for a user
// @Description  This endpoint is used to get API keys for a user. In estuary, each user can be given multiple API keys (tokens). This endpoint can be used to retrieve all available API keys for a given user.
// @Tags         User
// @Produce      json
// @Success      200  {object}  []getApiKeysResp
// @Failure      400  {object}  util.HttpError
// @Failure      404  {object}  util.HttpError
// @Failure      500  {object}  util.HttpError
// @Router       /user/api-keys [get]
func (s *Server) handleUserGetApiKeys(c echo.Context, u *User) error {
	var keys []AuthToken
	if err := s.DB.Find(&keys, "auth_tokens.user = ?", u.ID).Error; err != nil {
		return err
	}

	out := []getApiKeysResp{}
	for _, k := range keys {
		out = append(out, getApiKeysResp{
			Token:  k.Token,
			Expiry: k.Expiry,
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
// @Param        body     body     createCollectionBody  true        "Collection name and description"
// @Success      200  {object}  Collection
// @Failure      400  {object}  util.HttpError
// @Failure      404  {object}  util.HttpError
// @Failure      500  {object}  util.HttpError
// @Router       /collections/create [post]
func (s *Server) handleCreateCollection(c echo.Context, u *User) error {
	var body createCollectionBody
	if err := c.Bind(&body); err != nil {
		return err
	}

	col := &Collection{
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
// @Param        id   path      int  true  "User ID"
// @Success      200  {object}  []main.Collection
// @Failure      400  {object}  util.HttpError
// @Failure      404  {object}  util.HttpError
// @Failure      500  {object}  util.HttpError
// @Router       /collections/list [get]
func (s *Server) handleListCollections(c echo.Context, u *User) error {
	var cols []Collection
	if err := s.DB.Find(&cols, "user_id = ?", u.ID).Error; err != nil {
		return err
	}

	return c.JSON(http.StatusOK, cols)
}

type addContentToCollectionParams struct {
	Contents     []uint   `json:"contents"`
	CollectionID string   `json:"coluuid"`
	Cids         []string `json:"cids"`
}

// handleAddContentsToCollection godoc
// @Summary      Add contents to a collection
// @Description  When a collection is created, users with valid API keys can add contents to the collection. This endpoint can be used to add contents to a collection.
// @Tags         collections
// @Accept       json
// @Produce      json
// @Param        body     body     main.addContentToCollectionParams  true     "Contents to add to collection"
// @Success      200  {object}  map[string]string
// @Router       /collections/add-content [post]
func (s *Server) handleAddContentsToCollection(c echo.Context, u *User) error {
	var params addContentToCollectionParams
	if err := c.Bind(&params); err != nil {
		return err
	}

	if len(params.Contents) > 128 {
		return fmt.Errorf("too many contents specified: %d (max 128)", len(params.Contents))
	}

	if len(params.Cids) > 128 {
		return fmt.Errorf("too many cids specified: %d (max 128)", len(params.Cids))
	}

	var col Collection
	if err := s.DB.First(&col, "uuid = ? and user_id = ?", params.CollectionID, u.ID).Error; err != nil {
		return fmt.Errorf("no collection found by that uuid for your user: %w", err)
	}

	var contents []util.Content
	if err := s.DB.Find(&contents, "id in ? and user_id = ?", params.Contents, u.ID).Error; err != nil {
		return err
	}

	for _, c := range params.Cids {
		cc, err := cid.Decode(c)
		if err != nil {
			return fmt.Errorf("cid in params was improperly formatted: %w", err)
		}

		var cont util.Content
		if err := s.DB.First(&cont, "cid = ? and user_id = ?", util.DbCID{CID: cc}, u.ID).Error; err != nil {
			return fmt.Errorf("failed to find content by given cid %s: %w", cc, err)
		}

		contents = append(contents, cont)
	}

	if len(contents) != len(params.Contents)+len(params.Cids) {
		return fmt.Errorf("%d specified content(s) were not found or user missing permissions", len(params.Contents)-len(contents))
	}

	var colrefs []CollectionRef
	for _, cont := range contents {
		colrefs = append(colrefs, CollectionRef{
			Collection: col.ID,
			Content:    cont.ID,
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
// @Param        coluuid     path     string  true     "coluuid"
// @Tags         collections
// @Produce      json
// @Success      200  {object}  string
// @Router       /collections/{coluuid}/commit [post]
func (s *Server) handleCommitCollection(c echo.Context, u *User) error {
	colid := c.Param("coluuid")

	var col Collection
	if err := s.DB.First(&col, "uuid = ? and user_id = ?", colid, u.ID).Error; err != nil {
		return err
	}

	contents := []util.ContentWithPath{}
	if err := s.DB.Model(CollectionRef{}).
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
	if err := s.DB.Model(Collection{}).Where("id = ?", col.ID).UpdateColumn("c_id", collectionNode.Cid().String()).Error; err != nil {
		return err
	}

	ctx := c.Request().Context()
	makeDeal := false

	pinstatus, err := s.CM.pinContent(ctx, u.ID, collectionNode.Cid(), collectionNode.Cid().String(), nil, origins, 0, nil, makeDeal)
	if err != nil {
		return err
	}
	return c.JSON(http.StatusOK, pinstatus)
}

// handleGetCollectionContents godoc
// @Summary      Get contents in a collection
// @Description  This endpoint is used to get contents in a collection. If no colpath query param is passed
// @Tags         collections
// @Produce      json
// @Success      200  {object}  string
// @Param        coluuid query string true "Collection UUID"
// @Param        dir query string false "Directory"
// @Router       /collections/content [get]
func (s *Server) handleGetCollectionContents(c echo.Context, u *User) error {
	coluuid := c.QueryParam("coluuid")

	var col Collection
	if err := s.DB.First(&col, "uuid = ? and user_id = ?", coluuid, u.ID).Error; err != nil {
		return err
	}

	// TODO: optimize this a good deal
	var refs []util.ContentWithPath
	if err := s.DB.Model(CollectionRef{}).
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

	dirs := make(map[string]bool)
	var out []collectionListResponse
	for _, r := range refs {
		if r.Path == "" || r.Name == "" {
			continue
		}

		relp, err := getRelativePath(r, queryDir)
		if err != nil {
			return c.JSON(http.StatusInternalServerError, fmt.Errorf("errored while calculating relative contentPath queryDir=%s, contentPath=%s", queryDir, r.Path))
		}

		// if the relative contentPath requires pathing up, its definitely not in this queryDir
		if strings.HasPrefix(relp, "..") {
			continue
		}

		if relp == "." { // Query directory is the complete path containing the content.
			// trying to list a CID queryDir, not allowed
			if r.Type == util.Directory {
				return c.JSON(http.StatusBadRequest, fmt.Errorf("listing CID directories is not allowed"))
			}

			out = append(out, collectionListResponse{
				Name:      r.Name,
				Size:      r.Size,
				ContID:    r.ID,
				Cid:       &util.DbCID{CID: r.Cid.CID},
				Dir:       queryDir,
				ColUuid:   coluuid,
				UpdatedAt: r.UpdatedAt,
			})
		} else { // Query directory has a subdirectory, which contains the actual content.

			// if CID is a queryDir, set type as Dir and mark Dir as listed so we don't list it again
			//if r.Type == util.Directory {
			//	if !dirs[relp] {
			//		dirs[relp] = true
			//		out = append(out, collectionListResponse{
			//			Name:    relp,
			//			Type:    Dir,
			//			Size:    r.Size,
			//			ContID:  r.ID,
			//			Cid:     &r.Cid,
			//			Dir:     queryDir,
			//			ColUuid: coluuid,
			//		})
			//	}
			//	continue
			//}

			// if relative contentPath has a /, the file is in a subdirectory
			// print the directory the file is in if we haven't already
			var subDir string
			if strings.Contains(relp, "/") {
				parts := strings.Split(relp, "/")
				subDir = parts[0]
			} else {
				subDir = relp
			}
			if !dirs[subDir] {
				dirs[subDir] = true
				out = append(out, collectionListResponse{
					Name:    subDir,
					Type:    Dir,
					Dir:     queryDir,
					ColUuid: coluuid,
				})
				continue
			}
		}

		//var contentType CidType
		//contentType = File
		//if r.Type == util.Directory {
		//	contentType = Dir
		//}
		//out = append(out, collectionListResponse{
		//	Name:    r.Name,
		//	Type:    contentType,
		//	Size:    r.Size,
		//	ContID:  r.ID,
		//	Cid:     &util.DbCID{CID: r.Cid.CID},
		//	Dir:     queryDir,
		//	ColUuid: coluuid,
		//})
	}
	return c.JSON(http.StatusOK, out)
}

func getRelativePath(r util.ContentWithPath, queryDir string) (string, error) {
	contentPath := r.Path
	relp, err := filepath.Rel(queryDir, contentPath)
	return relp, err
}

// handleDeleteCollection godoc
// @Summary      Deletes a collection
// @Description  This endpoint is used to delete an existing collection.
// @Tags         collections
// @Param        coluuid path string true "Collection ID"
// @Router       /collections/{coluuid} [delete]
func (s *Server) handleDeleteCollection(c echo.Context, u *User) error {
	coluuid := c.Param("coluuid")

	var col Collection
	if err := s.DB.First(&col, "uuid = ?", coluuid).Error; err != nil {
		if xerrors.Is(err, gorm.ErrRecordNotFound) {
			return &util.HttpError{
				Code:    http.StatusNotFound,
				Reason:  util.ERR_CONTENT_NOT_FOUND,
				Details: fmt.Sprintf("collection with ID(%s) was not found", coluuid),
			}
		}
	}

	if err := util.IsCollectionOwner(u.ID, col.UserID); err != nil {
		return err
	}

	if err := s.DB.Delete(&col).Error; err != nil {
		return err
	}
	return c.NoContent(http.StatusOK)
}

func (s *Server) tracingMiddleware(next echo.HandlerFunc) echo.HandlerFunc {
	return func(c echo.Context) error {

		r := c.Request()

		attrs := []attribute.KeyValue{
			semconv.HTTPMethodKey.String(r.Method),
			semconv.HTTPRouteKey.String(r.URL.Path),
			semconv.HTTPClientIPKey.String(r.RemoteAddr),
			semconv.HTTPRequestContentLengthKey.Int64(c.Request().ContentLength),
		}

		if reqid := r.Header.Get("EstClientReqID"); reqid != "" {
			if len(reqid) > 64 {
				reqid = reqid[:64]
			}
			attrs = append(attrs, attribute.String("ClientReqID", reqid))
		}

		tctx, span := s.tracer.Start(context.Background(),
			"HTTP "+r.Method+" "+c.Path(),
			trace.WithAttributes(attrs...),
		)
		defer span.End()

		r = r.WithContext(tctx)
		c.SetRequest(r)

		err := next(c)
		if err != nil {
			span.SetStatus(codes.Error, err.Error())
			span.RecordError(err)
		} else {
			span.SetStatus(codes.Ok, "")
		}

		span.SetAttributes(
			semconv.HTTPStatusCodeKey.Int(c.Response().Status),
			semconv.HTTPResponseContentLengthKey.Int64(c.Response().Size),
		)

		return err
	}
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
// @Tags       	 admin
// @Produce      json
// @Router       /admin/users [get]
func (s *Server) handleAdminGetUsers(c echo.Context) error {
	var resp []adminUserResponse
	if err := s.DB.Model(util.Content{}).
		Select("user_id as id,(?) as username,SUM(size) as space_used,count(*) as num_files", s.DB.Model(&User{}).Select("username").Where("id = user_id")).
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
// @Router       /public/stats [get]
func (s *Server) handlePublicStats(c echo.Context) error {
	val, err := s.cacher.Get("public/stats", time.Minute*2, func() (interface{}, error) {
		return s.computePublicStats()
	})
	if err != nil {
		return err
	}

	//	handle the extensive looks up differently. Cache them for 1 hour.
	valExt, err := s.cacher.Get("public/stats/ext", time.Minute*60, func() (interface{}, error) {
		return s.computePublicStatsWithExtensiveLookups()
	})

	// reuse the original stats and add the ones from the extensive lookup function.
	val.(*publicStatsResponse).TotalObjectsRef = valExt.(*publicStatsResponse).TotalObjectsRef
	val.(*publicStatsResponse).TotalBytesUploaded = valExt.(*publicStatsResponse).TotalBytesUploaded
	val.(*publicStatsResponse).TotalUsers = valExt.(*publicStatsResponse).TotalUsers
	val.(*publicStatsResponse).TotalStorageMiner = valExt.(*publicStatsResponse).TotalStorageMiner

	if err != nil {
		return err
	}

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

func (s *Server) computePublicStats() (*publicStatsResponse, error) {
	var stats publicStatsResponse
	if err := s.DB.Model(util.Content{}).Where("active and not aggregated_in > 0").Select("SUM(size) as total_storage").Scan(&stats).Error; err != nil {
		return nil, err
	}

	if err := s.DB.Model(util.Content{}).Where("active and not aggregate").Count(&stats.TotalFilesStored.Int64).Error; err != nil {
		return nil, err
	}

	if err := s.DB.Model(contentDeal{}).Where("not failed and deal_id > 0").Count(&stats.DealsOnChain.Int64).Error; err != nil {
		return nil, err
	}

	return &stats, nil
}

func (s *Server) computePublicStatsWithExtensiveLookups() (*publicStatsResponse, error) {
	var stats publicStatsResponse

	//	this can be resource expensive but we are already caching it.
	if err := s.DB.Table("obj_refs").Count(&stats.TotalObjectsRef.Int64).Error; err != nil {
		return nil, err
	}

	if err := s.DB.Table("objects").Select("SUM(size)").Find(&stats.TotalBytesUploaded.Int64).Error; err != nil {
		return nil, err
	}

	if err := s.DB.Model(User{}).Count(&stats.TotalUsers.Int64).Error; err != nil {
		return nil, err
	}

	if err := s.DB.Table("storage_miners").Count(&stats.TotalStorageMiner.Int64).Error; err != nil {
		return nil, err
	}

	return &stats, nil
}

func (s *Server) handleGetBucketDiag(c echo.Context) error {
	return c.JSON(http.StatusOK, s.CM.getStagingZoneSnapshot(c.Request().Context()))
}

// handleGetStagingZoneForUser godoc
// @Summary      Get staging zone for user
// @Description  This endpoint is used to get staging zone for user.
// @Tags         content
// @Produce      json
// @Router       /content/staging-zones [get]
func (s *Server) handleGetStagingZoneForUser(c echo.Context, u *User) error {
	return c.JSON(http.StatusOK, s.CM.getStagingZonesForUser(c.Request().Context(), u.ID))
}

// handleUserExportData godoc
// @Summary      Export user data
// @Description  This endpoint is used to get API keys for a user.
// @Tags         User
// @Produce      json
// @Success      200  {object}  string
// @Router       /user/export [get]
func (s *Server) handleUserExportData(c echo.Context, u *User) error {
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
// @Success      200  {array}  string
// @Router       /public/net/peers [get]
func (s *Server) handleNetPeers(c echo.Context) error {
	return c.JSON(http.StatusOK, s.Node.Host.Network().Peers())
}

// handleNetAddrs godoc
// @Summary      Net Addrs
// @Description  This endpoint is used to get net addrs
// @Tags         public,net
// @Produce      json
// @Success      200  {array}  string
// @Router       /public/net/addrs [get]
func (s *Server) handleNetAddrs(c echo.Context) error {
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
// @Router       /public/metrics/deals-on-chain [get]
func (s *Server) handleMetricsDealOnChain(c echo.Context) error {
	val, err := s.cacher.Get("public/metrics", time.Minute*2, func() (interface{}, error) {
		return s.computeDealMetrics()
	})

	if err != nil {
		return err
	}

	//	Make sure we don't return a nil val.
	dealMetrics := val.([]*dealMetricsInfo)
	if len(dealMetrics) < 1 {
		return c.JSON(http.StatusOK, []*dealMetricsInfo{})
	}

	return c.JSON(http.StatusOK, val)
}

func (s *Server) computeDealMetrics() ([]*dealMetricsInfo, error) {
	var deals []*metricsDealJoin
	if err := s.DB.Model(contentDeal{}).
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
// @Param        begin query string true "Begin"
// @Param        duration query string true "Duration"
// @Param        all query string true "All"
// @Router       /content/all-deals [get]
func (s *Server) handleGetAllDealsForUser(c echo.Context, u *User) error {

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

	all := (c.QueryParam("all") != "")

	var deals []dealQuery
	if err := s.DB.Model(contentDeal{}).
		Where("deal_id > 0 AND (? OR (on_chain_at >= ? AND on_chain_at <= ?)) AND user_id = ?", all, begin, begin.Add(duration), u.ID).
		Joins("left join contents on content_deals.content = contents.id").
		Select("deal_id, contents.id as contentid, cid, aggregate").
		Scan(&deals).Error; err != nil {
		return err
	}

	contmap := make(map[uint][]dealQuery)
	for _, d := range deals {
		contmap[d.Contentid] = append(contmap[d.Contentid], d)
	}

	var out []dealPairs
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

func (s *Server) handleSetDealMaking(c echo.Context) error {
	var body setDealMakingBody
	if err := c.Bind(&body); err != nil {
		return err
	}

	s.CM.setDealMakingEnabled(body.Enabled)
	return c.JSON(http.StatusOK, map[string]string{})
}

func (s *Server) handleContentHealthCheck(c echo.Context) error {
	ctx := c.Request().Context()
	val, err := strconv.Atoi(c.Param("id"))
	if err != nil {
		return err
	}

	var cont util.Content
	if err := s.DB.First(&cont, "id = ?", val).Error; err != nil {
		return err
	}

	var u User
	if err := s.DB.First(&u, "id = ?", cont.UserID).Error; err != nil {
		return err
	}

	var deals []contentDeal
	if err := s.DB.Find(&deals, "content = ? and not failed", cont.ID).Error; err != nil {
		return err
	}

	var fixedAggregateSize bool
	if cont.Aggregate && cont.Size == 0 {
		// if this is an aggregate and its size is zero, then that means we
		// failed at some point while updating the aggregate, we can fix that
		var children []util.Content
		if err := s.DB.Find(&children, "aggregated_in = ?", cont.ID).Error; err != nil {
			return err
		}

		nd, err := s.CM.createAggregate(ctx, children)
		if err != nil {
			return fmt.Errorf("failed to create aggregate: %w", err)
		}

		// just to be safe, put it into the blockstore again
		if err := s.Node.Blockstore.Put(ctx, nd); err != nil {
			return err
		}

		size, err := nd.Size()
		if err != nil {
			return err
		}

		// now, update size and cid
		if err := s.DB.Model(util.Content{}).Where("id = ?", cont.ID).UpdateColumns(map[string]interface{}{
			"cid":  util.DbCID{CID: nd.Cid()},
			"size": size,
		}).Error; err != nil {
			return err
		}
		fixedAggregateSize = true
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
		log.Errorf("failed to fetch root: %s", rootFetchErr)
	}

	if cont.Aggregate && rootFetchErr != nil {
		// if this is an aggregate and we dont have the root, thats funky, but we can regenerate the root
		var children []util.Content
		if err := s.DB.Find(&children, "aggregated_in = ?", cont.ID).Error; err != nil {
			return err
		}

		nd, err := s.CM.createAggregate(ctx, children)
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

	var aggrLocs map[string]int
	var fixedAggregateLocation bool
	if c.QueryParam("check-locations") != "" && cont.Aggregate {
		// TODO: check if the contents of the aggregate are somewhere other than where the aggregate root is
		var aggr []util.Content
		if err := s.DB.Find(&aggr, "aggregated_in = ?", cont.ID).Error; err != nil {
			return err
		}

		aggrLocs = make(map[string]int)
		for _, child := range aggr {
			aggrLocs[child.Location]++
		}

		switch len(aggrLocs) {
		case 0:
			log.Warnf("content %d has nothing aggregated in it", cont.ID)
		case 1:
			loc := aggr[0].Location

			if loc != cont.Location {
				// should be safe to send a re-aggregate command to the shuttle in question
				var ids []uint
				for _, c := range aggr {
					ids = append(ids, c.ID)
				}

				dir, err := s.CM.createAggregate(ctx, aggr)
				if err != nil {
					return err
				}

				if err := s.CM.sendAggregateCmd(ctx, loc, cont, ids, dir.RawData()); err != nil {
					return err
				}

				fixedAggregateLocation = true
			}
		default:
			// well that sucks
			log.Warnf("content %d has messed up aggregation", cont.ID)
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

func (s *Server) handleContentHealthCheckByCid(c echo.Context) error {
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
		log.Errorf("failed to find contents for cid: %s", err)
	}

	_, rootFetchErr := s.Node.Blockstore.Get(ctx, cc)
	if rootFetchErr != nil {
		log.Errorf("failed to fetch root: %s", rootFetchErr)
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

func (s *Server) handleShuttleInit(c echo.Context) error {
	shuttle := &Shuttle{
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

func (s *Server) handleShuttleList(c echo.Context) error {
	var shuttles []Shuttle
	if err := s.DB.Find(&shuttles).Error; err != nil {
		return err
	}

	var out []util.ShuttleListResponse
	for _, d := range shuttles {
		out = append(out, util.ShuttleListResponse{
			Handle:         d.Handle,
			Token:          d.Token,
			LastConnection: d.LastConnection,
			Online:         s.CM.shuttleIsOnline(d.Handle),
			AddrInfo:       s.CM.shuttleAddrInfo(d.Handle),
			Hostname:       s.CM.shuttleHostName(d.Handle),
			StorageStats:   s.CM.shuttleStorageStats(d.Handle),
		})
	}

	return c.JSON(http.StatusOK, out)
}

func (s *Server) handleShuttleConnection(c echo.Context) error {
	auth, err := util.ExtractAuth(c)
	if err != nil {
		return err
	}

	var shuttle Shuttle
	if err := s.DB.First(&shuttle, "token = ?", auth).Error; err != nil {
		return err
	}

	websocket.Handler(func(ws *websocket.Conn) {
		ws.MaxPayloadBytes = 128 << 20

		done := make(chan struct{})
		defer close(done)
		defer ws.Close()
		var hello drpc.Hello
		if err := websocket.JSON.Receive(ws, &hello); err != nil {
			log.Errorf("failed to read hello message from client: %s", err)
			return
		}

		cmds, unreg, err := s.CM.registerShuttleConnection(shuttle.Handle, &hello)
		if err != nil {
			log.Errorf("failed to register shuttle: %s", err)
			return
		}
		defer unreg()

		go func() {
			for {
				select {
				case cmd := <-cmds:
					// Write
					err := websocket.JSON.Send(ws, cmd)
					if err != nil {
						log.Errorf("failed to write command to shuttle: %s", err)
						return
					}
				case <-done:
					return
				}
			}
		}()

		go s.RestartAllTransfersForLocation(context.TODO(), shuttle.Handle)

		for {
			var msg drpc.Message
			if err := websocket.JSON.Receive(ws, &msg); err != nil {
				log.Errorf("failed to read message from shuttle: %s", err)
				return
			}

			go func(msg *drpc.Message) {
				msg.Handle = shuttle.Handle
				s.CM.IncomingRPCMessages <- msg
			}(&msg)
		}
	}).ServeHTTP(c.Response(), c.Request())
	return nil
}

// handleAutoretrieveInit godoc
// @Summary      Register autoretrieve server
// @Description  This endpoint registers a new autoretrieve server
// @Tags         autoretrieve
// @Param        addresses body string true "Autoretrieve's comma-separated list of addresses"
// @Param        pubKey body string true "Autoretrieve's public key"
// @Produce      json
// @Router       /admin/autoretrieve/init [post]
func (s *Server) handleAutoretrieveInit(c echo.Context) error {
	// validate peerid and peer multi addresses
	addresses := strings.Split(c.FormValue("addresses"), ",")
	addrInfo, err := autoretrieve.ValidatePeerInfo(c.FormValue("pubKey"), addresses)
	if err != nil {
		return err
	}

	ar := &autoretrieve.Autoretrieve{
		Handle:            "AUTORETRIEVE" + uuid.New().String() + "HANDLE",
		Token:             "SECRET" + uuid.New().String() + "SECRET",
		LastConnection:    time.Now(),
		LastAdvertisement: time.Time{},
		PubKey:            c.FormValue("pubKey"),
		Addresses:         c.FormValue("addresses"), // cant store []string in gorm
	}
	if err := s.DB.Create(ar).Error; err != nil {
		return err
	}

	return c.JSON(200, &autoretrieve.AutoretrieveInitResponse{
		Handle:            ar.Handle,
		Token:             ar.Token,
		LastConnection:    ar.LastConnection,
		AddrInfo:          addrInfo,
		AdvertiseInterval: s.Node.ArEngine.TickInterval.String(),
	})
}

// handleAutoretrieveList godoc
// @Summary      List autoretrieve servers
// @Description  This endpoint lists all registered autoretrieve servers
// @Tags         autoretrieve
// @Produce      json
// @Router       /admin/autoretrieve/list [get]
func (s *Server) handleAutoretrieveList(c echo.Context) error {
	var autoretrieves []autoretrieve.Autoretrieve
	if err := s.DB.Find(&autoretrieves).Error; err != nil {
		return err
	}

	var out []autoretrieve.AutoretrieveListResponse

	for _, ar := range autoretrieves {
		// any of the multiaddresses of the peer should work to get addrInfo
		// we get the first one
		addresses := strings.Split(ar.Addresses, ",")
		addrInfo, err := peer.AddrInfoFromString(addresses[0])
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
// @Param        token header string true "Autoretrieve's auth token"
// @Produce      json
// @Router       /autoretrieve/heartbeat [post]
func (s *Server) handleAutoretrieveHeartbeat(c echo.Context) error {
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

	// any of the multiaddresses of the peer should work to get addrInfo
	// we get the first one
	addresses := strings.Split(ar.Addresses, ",")
	addrInfo, err := peer.AddrInfoFromString(addresses[0])
	if err != nil {
		return err
	}

	out := autoretrieve.HeartbeatAutoretrieveResponse{
		Handle:            ar.Handle,
		LastConnection:    ar.LastConnection,
		LastAdvertisement: ar.LastAdvertisement,
		AddrInfo:          addrInfo,
		AdvertiseInterval: s.Node.ArEngine.TickInterval.String(),
	}

	return c.JSON(http.StatusOK, out)
}

type allDealsQuery struct {
	Miner  string
	Cid    util.DbCID
	DealID int64
}

func (s *Server) handleDebugGetAllDeals(c echo.Context) error {
	var out []allDealsQuery
	if err := s.DB.Model(contentDeal{}).Where("deal_id > 0 and not content_deals.failed").
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

func (s *Server) handleLogLevel(c echo.Context) error {
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
// @Router       /public/deals/failures [get]
func (s *Server) handlePublicStorageFailures(c echo.Context) error {
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
// @Router       /deals/failures [get]
func (s *Server) handleStorageFailures(c echo.Context, u *User) error {
	recs, err := s.getStorageFailure(c, u)
	if err != nil {
		return err
	}
	return c.JSON(http.StatusOK, recs)
}

func (s *Server) getStorageFailure(c echo.Context, u *User) ([]dfeRecord, error) {
	limit := 2000
	if limstr := c.QueryParam("limit"); limstr != "" {
		nlim, err := strconv.Atoi(limstr)
		if err != nil {
			return nil, err
		}
		limit = nlim
	}

	q := s.DB.Model(dfeRecord{}).Limit(limit).Order("created_at desc")
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

	var recs []dfeRecord
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
// @Param        body body string true "Content"
// @Router       /content/create [post]
func (s *Server) handleCreateContent(c echo.Context, u *User) error {
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

	var col Collection
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
		Replication: s.CM.Replication,
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
		if err := s.DB.Create(&CollectionRef{
			Collection: col.ID,
			Content:    content.ID,
			Path:       path,
		}).Error; err != nil {
			return err
		}
	}

	return c.JSON(http.StatusOK, util.ContentCreateResponse{
		ID: content.ID,
	})
}

type claimMinerBody struct {
	Miner address.Address `json:"miner"`
	Claim string          `json:"claim"`
	Name  string          `json:"name"`
}

func (s *Server) handleUserClaimMiner(c echo.Context, u *User) error {
	ctx := c.Request().Context()

	var cmb claimMinerBody
	if err := c.Bind(&cmb); err != nil {
		return err
	}

	var sm []storageMiner
	if err := s.DB.Find(&sm, "address = ?", cmb.Miner.String()).Error; err != nil {
		return err
	}

	minfo, err := s.Api.StateMinerInfo(ctx, cmb.Miner, types.EmptyTSK)
	if err != nil {
		return err
	}

	acckey, err := s.Api.StateAccountKey(ctx, minfo.Worker, types.EmptyTSK)
	if err != nil {
		return err
	}

	sigb, err := hex.DecodeString(cmb.Claim)
	if err != nil {
		return err
	}

	if len(sigb) < 2 {
		return &util.HttpError{
			Code:   http.StatusBadRequest,
			Reason: util.ERR_INVALID_INPUT,
		}
	}

	sig := &crypto.Signature{
		Type: crypto.SigType(sigb[0]),
		Data: sigb[1:],
	}

	msg := s.msgForMinerClaim(cmb.Miner, u.ID)

	if err := sigs.Verify(sig, acckey, msg); err != nil {
		return err
	}

	if len(sm) == 0 {
		// This is a new miner, need to run some checks first
		if err := s.checkNewMiner(ctx, cmb.Miner); err != nil {
			return c.JSON(http.StatusBadRequest, map[string]interface{}{
				"success": false,
				"error":   err.Error(),
			})
		}

		if err := s.DB.Create(&storageMiner{
			Address: util.DbAddr{Addr: cmb.Miner},
			Name:    cmb.Name,
			Owner:   u.ID,
		}).Error; err != nil {
			return err
		}

	} else {
		if err := s.DB.Model(storageMiner{}).Where("id = ?", sm[0].ID).UpdateColumn("owner", u.ID).Error; err != nil {
			return err
		}
	}

	return c.JSON(http.StatusOK, map[string]interface{}{
		"success": true,
	})
}

func (s *Server) checkNewMiner(ctx context.Context, addr address.Address) error {
	minfo, err := s.Api.StateMinerInfo(ctx, addr, types.EmptyTSK)
	if err != nil {
		return err
	}

	if minfo.PeerId == nil {
		return fmt.Errorf("miner has no peer ID set")
	}

	if len(minfo.Multiaddrs) == 0 {
		return fmt.Errorf("miner has no addresses set on chain")
	}

	pow, err := s.Api.StateMinerPower(ctx, addr, types.EmptyTSK)
	if err != nil {
		return fmt.Errorf("could not check miners power: %w", err)
	}

	if types.BigCmp(pow.MinerPower.QualityAdjPower, types.NewInt(1<<40)) < 0 {
		return fmt.Errorf("miner must have at least 1TiB of power to be considered by estuary")
	}

	ask, err := s.FilClient.GetAsk(ctx, addr)
	if err != nil {
		return fmt.Errorf("failed to get ask from miner: %w", err)
	}

	if !ask.Ask.Ask.VerifiedPrice.Equals(big.NewInt(0)) {
		return fmt.Errorf("miners verified deal price is not zero")
	}

	return nil
}

func (s *Server) handleUserGetClaimMinerMsg(c echo.Context, u *User) error {
	m, err := address.NewFromString(c.Param("miner"))
	if err != nil {
		return err
	}

	return c.JSON(http.StatusOK, map[string]string{
		"hexmsg": hex.EncodeToString(s.msgForMinerClaim(m, u.ID)),
	})
}

func (s *Server) msgForMinerClaim(miner address.Address, uid uint) []byte {
	return []byte(fmt.Sprintf("---- user %d owns miner %s ----", uid, miner))
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

func (s *Server) handleAdminGetProgress(c echo.Context) error {
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
			s.DB.Model(contentDeal{}).
				Where("content = contents.id and deal_id > 0 and not failed").
				Select("count(1)"),
		).Scan(&conts).Error; err != nil {
		return err
	}

	for _, c := range conts {
		if c.NumDeals >= s.CM.Replication {
			out.GoodContents = append(out.GoodContents, c.ID)
		} else if c.NumDeals > 0 {
			out.InProgress = append(out.InProgress, c.ID)
		} else {
			out.NoDeals = append(out.NoDeals, c.ID)
		}
	}

	return c.JSON(http.StatusOK, out)
}

func (s *Server) handleAdminBreakAggregate(c echo.Context) error {
	ctx := c.Request().Context()
	aggr, err := strconv.Atoi(c.Param("content"))
	if err != nil {
		return err
	}

	var cont util.Content
	if err := s.DB.First(&cont, "id = ?", aggr).Error; err != nil {
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
// @Router       /public/info [get]
func (s *Server) handleGetPublicNodeInfo(c echo.Context) error {
	return c.JSON(http.StatusOK, &publicNodeInfo{
		PrimaryAddress: s.FilClient.ClientAddr,
	})
}

type retrievalCandidate struct {
	Miner   address.Address
	RootCid cid.Cid
	DealID  uint
}

func (s *Server) handleGetRetrievalCandidates(c echo.Context) error {
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

func (s *Server) handleShuttleCreateContent(c echo.Context) error {
	var req util.ShuttleCreateContentBody
	if err := c.Bind(&req); err != nil {
		return err
	}

	log.Debugw("handle shuttle create content", "root", req.Root, "user", req.User, "dsr", req.DagSplitRoot, "name", req.Name)

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
		Replication: s.CM.Replication,
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

func (s *Server) withAutoretrieveAuth() echo.MiddlewareFunc {
	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c echo.Context) error {
			auth, err := util.ExtractAuth(c)
			if err != nil {
				return err
			}

			var ar autoretrieve.Autoretrieve
			if err := s.DB.First(&ar, "token = ?", auth).Error; err != nil {
				log.Warnw("Autoretrieve server not authorized", "token", auth)
				return &util.HttpError{
					Code:    http.StatusUnauthorized,
					Reason:  util.ERR_NOT_AUTHORIZED,
					Details: "token not authorized",
				}
			}
			return next(c)
		}
	}
}

func (s *Server) withShuttleAuth() echo.MiddlewareFunc {
	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c echo.Context) error {
			auth, err := util.ExtractAuth(c)
			if err != nil {
				return err
			}

			var sh Shuttle
			if err := s.DB.First(&sh, "token = ?", auth).Error; err != nil {
				log.Warnw("Shuttle not authorized", "token", auth)
				return &util.HttpError{
					Code:   http.StatusUnauthorized,
					Reason: util.ERR_NOT_AUTHORIZED,
				}
			}
			return next(c)
		}
	}
}

func (s *Server) handleShuttleRepinAll(c echo.Context) error {
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

		if err := s.CM.sendShuttleCommand(c.Request().Context(), handle, &drpc.Command{
			Op: drpc.CMD_AddPin,
			Params: drpc.CmdParams{
				AddPin: &drpc.AddPin{
					DBID:   cont.ID,
					UserId: cont.UserID,
					Cid:    cont.Cid.CID,
				},
			},
		}); err != nil {
			return err
		}
	}
	return nil
}

// this is required as ipfs pinning spec has strong requirements on response format
func openApiMiddleware(next echo.HandlerFunc) echo.HandlerFunc {
	return func(c echo.Context) error {
		err := next(c)
		if err == nil {
			return nil
		}

		var httpRespErr *util.HttpError
		if xerrors.As(err, &httpRespErr) {
			log.Errorf("handler error: %s", err)
			return c.JSON(httpRespErr.Code, &util.HttpErrorResponse{
				Error: util.HttpError{
					Reason:  httpRespErr.Reason,
					Details: httpRespErr.Details,
				},
			})
		}

		var echoErr *echo.HTTPError
		if xerrors.As(err, &echoErr) {
			return c.JSON(echoErr.Code, &util.HttpErrorResponse{
				Error: util.HttpError{
					Reason:  http.StatusText(echoErr.Code),
					Details: echoErr.Message.(string),
				},
			})
		}

		log.Errorf("handler error: %s", err)
		return c.JSON(http.StatusInternalServerError, &util.HttpErrorResponse{
			Error: util.HttpError{
				Reason:  http.StatusText(http.StatusInternalServerError),
				Details: err.Error(),
			},
		})
	}
}

type CidType string

const (
	Raw    CidType = "raw"
	File   CidType = "file"
	Dir    CidType = "directory"
	ColDir string  = "dir"
)

type collectionListResponse struct {
	Name      string      `json:"name"`
	Type      CidType     `json:"type"`
	Size      int64       `json:"size"`
	ContID    uint        `json:"contId"`
	Cid       *util.DbCID `json:"cid,omitempty"`
	Dir       string      `json:"dir"`
	ColUuid   string      `json:"coluuid"`
	UpdatedAt time.Time   `json:"updatedAt"`
}

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
// @Param        coluuid query string true "Collection ID"
// @Param        content query string true "Content"
// @Param        path query string true "Path to file"
// @Produce      json
// @Router       /collections/fs/add [post]
func (s *Server) handleColfsAdd(c echo.Context, u *User) error {
	coluuid := c.QueryParam("coluuid")
	contid := c.QueryParam("content")
	npath := c.QueryParam("path")

	var col Collection
	if err := s.DB.First(&col, "uuid = ?", coluuid).Error; err != nil {
		return err
	}

	if err := util.IsCollectionOwner(u.ID, col.UserID); err != nil {
		return err
	}

	var content util.Content
	if err := s.DB.First(&content, "id = ?", contid).Error; err != nil {
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

	if err := s.DB.Create(&CollectionRef{Collection: col.ID, Content: content.ID, Path: path}).Error; err != nil {
		return errors.Wrap(err, "failed to add content to requested collection")
	}
	return c.JSON(http.StatusOK, map[string]string{})
}

func (s *Server) handleRunGc(c echo.Context) error {
	if err := s.CM.GarbageCollect(c.Request().Context()); err != nil {
		return err
	}

	return nil
}

func (s *Server) handleGateway(c echo.Context) error {
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

func (s *Server) checkGatewayRedirect(proto string, cc cid.Cid, segs []string) (string, error) {
	if proto != "ipfs" {
		return fmt.Sprintf("https://%s/%s/%s/%s", bestGateway, proto, cc, strings.Join(segs, "/")), nil
	}

	var cont util.Content
	if err := s.DB.First(&cont, "cid = ? and active and not offloaded", &util.DbCID{CID: cc}).Error; err != nil {
		if xerrors.Is(err, gorm.ErrRecordNotFound) {
			return "", nil
		}
		return "", err
	}

	if cont.Location == constants.ContentLocationLocal {
		return "", nil
	}

	if !s.CM.shuttleIsOnline(cont.Location) {
		return fmt.Sprintf("https://%s/%s/%s/%s", bestGateway, proto, cc, strings.Join(segs, "/")), nil
	}

	var shuttle Shuttle
	if err := s.DB.First(&shuttle, "handle = ?", cont.Location).Error; err != nil {
		return "", err
	}

	return fmt.Sprintf("https://%s/gw/%s/%s/%s", shuttle.Host, proto, cc, strings.Join(segs, "/")), nil
}

func (s *Server) isDupCIDContent(c echo.Context, rootCID cid.Cid, u *User) (bool, error) {
	var count int64
	if err := s.DB.Model(util.Content{}).Where("cid = ? and user_id = ?", rootCID.Bytes(), u.ID).Count(&count).Error; err != nil {
		return false, err
	}
	if count > 0 {
		return true, c.JSON(409, map[string]string{"message": fmt.Sprintf("this content is already preserved under cid:%s", rootCID.String())})
	}
	return false, nil
}

func (s *Server) getShuttleConfig(hostname string, authToken string) (interface{}, error) {
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

func (s *Server) isContentAddingDisabled(u *User) bool {
	return (s.CM.globalContentAddingDisabled && s.CM.localContentAddingDisabled) || u.StorageDisabled
}
