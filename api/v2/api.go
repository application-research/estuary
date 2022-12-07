package api

import (
	"github.com/application-research/estuary/config"
	"github.com/application-research/estuary/contentmgr"
	"github.com/application-research/estuary/miner"
	"github.com/application-research/estuary/node"
	"github.com/application-research/estuary/pinner"
	"github.com/application-research/estuary/stagingbs"
	"github.com/application-research/estuary/util"
	"github.com/application-research/estuary/util/gateway"
	"github.com/application-research/filclient"
	"github.com/filecoin-project/lotus/api"
	"github.com/labstack/echo/v4"
	"github.com/whyrusleeping/memo"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

type apiV2 struct {
	cfg          *config.Estuary
	db           *gorm.DB
	tracer       trace.Tracer
	node         *node.Node
	filClient    *filclient.FilClient
	api          api.Gateway
	cm           *contentmgr.ContentManager
	stagingMgr   *stagingbs.StagingBSMgr
	gwayHandler  *gateway.GatewayHandler
	cacher       *memo.Cacher
	minerManager miner.IMinerManager
	pinMgr       *pinner.PinManager
	log          *zap.SugaredLogger
}

func NewAPIV2(
	cfg *config.Estuary,
	db *gorm.DB,
	nd *node.Node,
	fc *filclient.FilClient,
	gwApi api.Gateway,
	sbm *stagingbs.StagingBSMgr,
	cm *contentmgr.ContentManager,
	mm miner.IMinerManager,
	pinMgr *pinner.PinManager,
	log *zap.SugaredLogger,
	trc trace.Tracer,
) *apiV2 {
	return &apiV2{
		cfg:          cfg,
		db:           db,
		tracer:       trc,
		node:         nd,
		filClient:    fc,
		api:          gwApi,
		cm:           cm,
		stagingMgr:   sbm,
		gwayHandler:  gateway.NewGatewayHandler(nd.Blockstore),
		cacher:       memo.NewCacher(),
		minerManager: mm,
		pinMgr:       pinMgr,
		log:          log,
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
	e2 := e.Group("/v2")

	// to upload contents you only need an upload key
	// to see info about contents you need a user-level key (see contents group)
	e2.POST("/contents", util.WithUser(s.handleAdd), s.AuthRequired(util.PermLevelUpload))
	// e2.GET("/contents", util.WithUser(s.handleListContent), s.AuthRequired(util.PermLevelUser))
	// e2.GET("/contents/:contentid", util.WithUser(s.handleGetContent), s.AuthRequired(util.PermLevelUser))
	// e2.GET("/contents/:cid/ensure-replication", s.handleEnsureReplication, s.AuthRequired(util.PermLevelUser))
	// e2.GET("/contents/:contentid/status", util.WithUser(s.handleContentStatus), s.AuthRequired(util.PermLevelUser))

}
