package api

import (
	"github.com/application-research/estuary/config"
	content "github.com/application-research/estuary/content"
	"github.com/application-research/estuary/miner"
	"github.com/application-research/estuary/node"
	"github.com/application-research/estuary/pinner"
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

type apiV2 struct {
	cfg            *config.Estuary
	db             *gorm.DB
	tracer         trace.Tracer
	nd             *node.Node
	fc             *filclient.FilClient
	api            api.Gateway
	cm             content.IManager
	stagingBsMgr   *stagingbs.StagingBSMgr
	gwayHandler    *gateway.GatewayHandler
	cacher         *explru.ExpirableLRU
	extendedCacher *explru.ExpirableLRU
	minerManager   miner.IMinerManager
	pinMgr         *pinner.EstuaryPinManager
	log            *zap.SugaredLogger
}

func NewAPIV2(
	cfg *config.Estuary,
	db *gorm.DB,
	nd *node.Node,
	fc *filclient.FilClient,
	gwApi api.Gateway,
	sbm *stagingbs.StagingBSMgr,
	cm content.IManager,
	cacher *explru.ExpirableLRU,
	minerManager miner.IMinerManager,
	extendedCacher *explru.ExpirableLRU,
	pinMgr *pinner.EstuaryPinManager,
	log *zap.SugaredLogger,
	trc trace.Tracer,
) *apiV2 {
	return &apiV2{
		cfg:            cfg,
		db:             db,
		tracer:         trc,
		nd:             nd,
		fc:             fc,
		api:            gwApi,
		cm:             cm,
		stagingBsMgr:   sbm,
		gwayHandler:    gateway.NewGatewayHandler(nd.Blockstore),
		cacher:         cacher,
		extendedCacher: extendedCacher,
		minerManager:   minerManager,
		pinMgr:         pinMgr,
		log:            log,
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
func (s *apiV2) RegisterRoutes(e *echo.Echo) {
	api := e.Group("/v2")
	api.Use(middleware.RateLimiterWithConfig(util.ConfigureRateLimiter(s.cfg.RateLimit)))

	// Storage Provider Endpoints
	storageProvider := api.Group("/storage-providers")
	storageProvider.POST("/add/:sp", s.handleAddStorageProvider, s.AuthRequired(util.PermLevelAdmin))
	storageProvider.POST("/rm/:sp", s.handleRemoveStorageProvider, s.AuthRequired(util.PermLevelAdmin))
	storageProvider.POST("/suspend/:sp", util.WithUser(s.handleSuspendStorageProvider))
	storageProvider.PUT("/unsuspend/:sp", util.WithUser(s.handleUnsuspendStorageProvider))
	storageProvider.PUT("/set-info/:sp", util.WithUser(s.handleStorageProvidersSetInfo))
	storageProvider.GET("/stats", s.handleGetStorageProviderDealStats, s.AuthRequired(util.PermLevelAdmin))
	storageProvider.GET("/transfers/:sp", s.handleStorageProviderTransferDiagnostics, s.AuthRequired(util.PermLevelAdmin))
	storageProvider.GET("", s.handleGetStorageProviders)
	storageProvider.GET("/failures/:sp", s.handleGetStorageProviderFailures)
	storageProvider.GET("/deals/:sp", s.handleGetStorageProviderDeals)
	storageProvider.GET("/stats/:sp", s.handleGetStorageProviderStats)
	storageProvider.GET("/storage/query/:cid", s.handleStorageProviderQueryAsk)
	storageProvider.POST("/claim", util.WithUser(s.handleClaimStorageProvider))
	storageProvider.GET("/claim/:sp", util.WithUser(s.handleGetClaimStorageProviderMsg))

	// Pinning
	pinning := api.Group("/pinning")
	pinning.Use(s.AuthRequired(util.PermLevelUser))
	pinning.POST("/batched-pins", util.WithUser(s.handleAddBatchedPins))
	pinning.GET("/batched-pins", util.WithUser(s.handleGetBatchedPins))
}

func (s *apiV2) isContentAddingDisabled(u *util.User) bool {
	return (s.cfg.Content.DisableGlobalAdding && s.cfg.Content.DisableLocalAdding) || u.StorageDisabled
}
