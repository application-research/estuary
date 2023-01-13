package api

import (
	"github.com/application-research/estuary/config"
	contentmgr "github.com/application-research/estuary/content"
	"github.com/application-research/estuary/deal/transfer"
	"github.com/application-research/estuary/miner"
	"github.com/application-research/estuary/node"
	"github.com/application-research/estuary/pinner"
	"github.com/application-research/estuary/shuttle"
	"github.com/application-research/estuary/stagingbs"
	"github.com/application-research/estuary/util"
	"github.com/application-research/estuary/util/gateway"
	"github.com/application-research/filclient"
	"github.com/filecoin-project/lotus/api"
	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
	explru "github.com/paskal/golang-lru/simplelru"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

type apiV1 struct {
	cfg            *config.Estuary
	DB             *gorm.DB
	tracer         trace.Tracer
	Node           *node.Node
	FilClient      *filclient.FilClient
	Api            api.Gateway
	CM             *contentmgr.ContentManager
	StagingMgr     *stagingbs.StagingBSMgr
	gwayHandler    *gateway.GatewayHandler
	cacher         *explru.ExpirableLRU
	extendedCacher *explru.ExpirableLRU
	minerManager   miner.IMinerManager
	pinMgr         *pinner.EstuaryPinManager
	log            *zap.SugaredLogger
	shuttleMgr     shuttle.IManager
	transferMgr    transfer.IManager
}

func NewAPIV1(
	cfg *config.Estuary,
	db *gorm.DB,
	nd *node.Node,
	fc *filclient.FilClient,
	gwApi api.Gateway,
	sbm *stagingbs.StagingBSMgr,
	cm *contentmgr.ContentManager,
	cacher *explru.ExpirableLRU,
	extendedCacher *explru.ExpirableLRU,
	mm miner.IMinerManager,
	pinMgr *pinner.EstuaryPinManager,
	log *zap.SugaredLogger,
	trc trace.Tracer,
	shuttleMgr shuttle.IManager,
	transferMgr transfer.IManager,
) *apiV1 {
	return &apiV1{
		cfg:            cfg,
		DB:             db,
		tracer:         trc,
		Node:           nd,
		FilClient:      fc,
		Api:            gwApi,
		CM:             cm,
		StagingMgr:     sbm,
		gwayHandler:    gateway.NewGatewayHandler(nd.Blockstore),
		cacher:         cacher,
		extendedCacher: extendedCacher,
		minerManager:   mm,
		pinMgr:         pinMgr,
		log:            log,
		shuttleMgr:     shuttleMgr,
		transferMgr:    transferMgr,
	}
}

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
func (s *apiV1) RegisterRoutes(e *echo.Echo) {

	e.Use(middleware.RateLimiterWithConfig(util.ConfigureRateLimiter(s.cfg.RateLimit)))
	e.POST("/register", s.handleRegisterUser)
	e.POST("/login", s.handleLoginUser)
	e.GET("/health", s.handleHealth)
	e.GET("/viewer", util.WithUser(s.handleGetViewer), s.AuthRequired(util.PermLevelUpload))
	e.GET("/retrieval-candidates/:cid", s.handleGetRetrievalCandidates)
	e.GET("/gw/:path", s.handleGateway)

	e.POST("/put", util.WithMultipartFormDataChecker(util.WithUser(s.handleAdd)), s.AuthRequired(util.PermLevelUpload))
	e.GET("/get/:cid", s.handleGetFullContentbyCid)
	// e.HEAD("/get/:cid", s.handleGetContentByCid)

	user := e.Group("/user")
	user.Use(s.AuthRequired(util.PermLevelUser))
	user.GET("/api-keys", util.WithUser(s.handleUserGetApiKeys))
	user.POST("/api-keys", util.WithUser(s.handleUserCreateApiKey))
	user.DELETE("/api-keys/:key_or_hash", util.WithUser(s.handleUserRevokeApiKey))
	user.GET("/export", util.WithUser(s.handleUserExportData))
	user.PUT("/password", util.WithUser(s.handleUserChangePassword))
	user.PUT("/address", util.WithUser(s.handleUserChangeAddress))
	user.GET("/stats", util.WithUser(s.handleGetUserStats))

	userMiner := user.Group("/miner")
	userMiner.POST("/claim", util.WithUser(s.handleUserClaimMiner))
	userMiner.GET("/claim/:miner", util.WithUser(s.handleUserGetClaimMinerMsg))
	userMiner.POST("/suspend/:miner", util.WithUser(s.handleSuspendMiner))
	userMiner.PUT("/unsuspend/:miner", util.WithUser(s.handleUnsuspendMiner))
	userMiner.PUT("/set-info/:miner", util.WithUser(s.handleMinersSetInfo))

	contmeta := e.Group("/content")
	uploads := contmeta.Group("", s.AuthRequired(util.PermLevelUpload))
	uploads.POST("/add", util.WithMultipartFormDataChecker(util.WithUser(s.handleAdd)))
	uploads.POST("/add-ipfs", util.WithUser(s.handleAddIpfs))
	uploads.POST("/add-car", util.WithContentLengthCheck(util.WithUser(s.handleAddCar)))
	uploads.POST("/create", util.WithUser(s.handleCreateContent))

	content := contmeta.Group("", s.AuthRequired(util.PermLevelUser))
	content.GET("/by-cid/:cid", s.handleGetContentByCid)
	content.GET("/:cont_id", util.WithUser(s.handleGetContent))
	content.GET("/stats", util.WithUser(s.handleStats))
	content.GET("/contents", util.WithUser(s.handleGetUserContents))
	content.GET("/ensure-replication/:datacid", s.handleEnsureReplication)
	content.GET("/status/:id", util.WithUser(s.handleContentStatus))
	content.GET("/list", util.WithUser(s.handleListContent))
	content.GET("/deals", util.WithUser(s.handleListContentWithDeals))
	content.GET("/failures/:content", util.WithUser(s.handleGetContentFailures))
	content.GET("/bw-usage/:content", util.WithUser(s.handleGetContentBandwidth))
	content.GET("/staging-zones", util.WithUser(s.handleGetStagingZonesForUser))
	content.GET("/staging-zones/:staging_zone", util.WithUser(s.handleGetStagingZoneWithoutContents))
	content.GET("/staging-zones/:staging_zone/contents", util.WithUser(s.handleGetStagingZoneContents))
	content.GET("/aggregated/:content", util.WithUser(s.handleGetAggregatedForContent))
	content.GET("/all-deals", util.WithUser(s.handleGetAllDealsForUser))

	// TODO: the commented out routes here are still fairly useful, but maybe
	// need to have some sort of 'super user' permission level in order to use
	// them? Can easily cause harm using them
	deals := e.Group("/deals")
	deals.Use(s.AuthRequired(util.PermLevelUser))
	deals.GET("/status/:deal", util.WithUser(s.handleGetDealStatus))
	deals.GET("/status-by-proposal/:propcid", util.WithUser(s.handleGetDealStatusByPropCid))
	deals.GET("/query/:miner", s.handleQueryAsk)
	deals.POST("/make/:miner", util.WithUser(s.handleMakeDeal))
	//deals.POST("/transfer/start/:miner/:propcid/:datacid", s.handleTransferStart)
	deals.GET("/transfer/status/:id", s.handleTransferStatusByID)
	deals.POST("/transfer/status", s.handleTransferStatus)
	deals.GET("/transfer/in-progress", s.handleTransferInProgress)
	deals.GET("/status/:miner/:propcid", s.handleDealStatus)
	deals.POST("/estimate", s.handleEstimateDealCost)
	deals.GET("/proposal/:propcid", s.handleGetProposal)
	deals.GET("/info/:dealid", s.handleGetDealInfo)
	deals.GET("/failures", util.WithUser(s.handleStorageFailures))

	cols := e.Group("/collections")
	cols.Use(s.AuthRequired(util.PermLevelUser))
	cols.GET("", util.WithUser(s.handleListCollections))
	cols.POST("", util.WithUser(s.handleCreateCollection))
	cols.DELETE("/:coluuid", util.WithUser(s.handleDeleteCollection))
	cols.POST("/:coluuid", util.WithUser(s.handleAddContentsToCollection))
	cols.GET("/:coluuid", util.WithUser(s.handleGetCollectionContents))
	cols.DELETE("/:coluuid/contents", util.WithUser(s.handleDeleteContentFromCollection))
	cols.POST("/:coluuid/commit", util.WithUser(s.handleCommitCollection))
	colfs := cols.Group("/fs")
	colfs.POST("/add", util.WithUser(s.handleColfsAdd))

	pinning := e.Group("/pinning")
	pinning.Use(util.OpenApiMiddleware(s.log))
	pinning.Use(s.AuthRequired(util.PermLevelUser))
	pinning.GET("/pins", util.WithUser(s.handleListPins))
	pinning.GET("/pins/:pinid", util.WithUser(s.handleGetPin))
	pinning.DELETE("/pins/:pinid", util.WithUser(s.handleDeletePin))
	pinning.Use(util.JSONPayloadMiddleware)
	pinning.POST("/pins", util.WithUser(s.handleAddPin))
	pinning.POST("/batched-pins", util.WithUser(s.handleAddPins))
	pinning.POST("/pins/:pinid", util.WithUser(s.handleReplacePin))

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
	admin.GET("/fil-address", s.handleAdminFilAddress)
	admin.GET("/balance", s.handleAdminBalance)
	admin.POST("/add-escrow/:amt", s.handleAdminAddEscrow)
	admin.GET("/dealstats", s.handleDealStats)
	admin.GET("/disk-info", s.handleDiskSpaceCheck)
	admin.GET("/stats", s.handleAdminStats)
	admin.GET("/system/config", util.WithUser(s.handleGetSystemConfig))

	// miners
	admin.POST("/miners/add/:miner", s.handleAdminAddMiner)
	admin.POST("/miners/rm/:miner", s.handleAdminRemoveMiner)
	admin.POST("/miners/suspend/:miner", util.WithUser(s.handleSuspendMiner))
	admin.PUT("/miners/unsuspend/:miner", util.WithUser(s.handleUnsuspendMiner))
	admin.PUT("/miners/set-info/:miner", util.WithUser(s.handleMinersSetInfo))
	admin.GET("/miners", s.handleAdminGetMiners)
	admin.GET("/miners/stats", s.handleAdminGetMinerStats)
	admin.GET("/miners/transfers/:miner", s.handleMinerTransferDiagnostics)

	admin.GET("/cm/progress", s.handleAdminGetProgress)
	admin.GET("/cm/all-deals", s.handleDebugGetAllDeals)
	admin.GET("/cm/read/:content", s.handleReadLocalContent)
	admin.GET("/cm/offload/candidates", s.handleGetOffloadingCandidates)
	admin.POST("/cm/offload/:content", s.handleOffloadContent)
	admin.POST("/cm/offload/collect", s.handleRunOffloadingCollection)
	admin.GET("/cm/refresh/:content", s.handleRefreshContent)
	admin.POST("/cm/gc", s.handleRunGc)
	admin.POST("/cm/move", s.handleMoveContent)
	admin.GET("/cm/health/:id", s.handleContentHealthCheck)
	admin.GET("/cm/health-by-cid/:cid", s.handleContentHealthCheckByCid)
	admin.POST("/cm/dealmaking", s.handleSetDealMaking)
	admin.POST("/cm/break-aggregate/:content", s.handleAdminBreakAggregate)
	admin.POST("/cm/transfer/restart/:chanid", s.handleTransferRestart)
	admin.POST("/cm/repinall/:shuttle", s.handleShuttleRepinAll)

	//	peering
	admin.POST("/peering/peers", s.handlePeeringPeersAdd)
	admin.DELETE("/peering/peers", s.handlePeeringPeersRemove)
	admin.GET("/peering/peers", s.handlePeeringPeersList)
	admin.POST("/peering/start", s.handlePeeringStart)
	admin.POST("/peering/stop", s.handlePeeringStop)
	admin.GET("/peering/status", s.handlePeeringStatus)

	admnetw := admin.Group("/net")
	admnetw.GET("/peers", s.handleNetPeers)

	admin.GET("/retrieval/querytest/:content", s.handleRetrievalCheck)
	admin.GET("/retrieval/stats", s.handleGetRetrievalInfo)

	admin.POST("/invite/:code", util.WithUser(s.handleAdminCreateInvite))
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
}
