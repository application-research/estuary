package api

import (
	"github.com/application-research/estuary/config"
	contentmgr "github.com/application-research/estuary/content"
	"github.com/application-research/estuary/miner"
	"github.com/application-research/estuary/node"
	"github.com/application-research/estuary/pinner"
	"github.com/application-research/estuary/stagingbs"
	"github.com/application-research/estuary/util/gateway"
	"github.com/application-research/filclient"
	"github.com/filecoin-project/lotus/api"
	"github.com/labstack/echo/v4"
	explru "github.com/paskal/golang-lru/simplelru"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

type apiV2 struct {
	cfg          *config.Estuary
	DB           *gorm.DB
	tracer       trace.Tracer
	Node         *node.Node
	FilClient    *filclient.FilClient
	Api          api.Gateway
	CM           *contentmgr.ContentManager
	StagingMgr   *stagingbs.StagingBSMgr
	gwayHandler  *gateway.GatewayHandler
	cacher       *explru.ExpirableLRU
	minerManager miner.IMinerManager
	pinMgr       *pinner.EstuaryPinManager
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
	cacher *explru.ExpirableLRU,
	mm miner.IMinerManager,
	pinMgr *pinner.EstuaryPinManager,
	log *zap.SugaredLogger,
	trc trace.Tracer,
) *apiV2 {
	return &apiV2{
		cfg:          cfg,
		DB:           db,
		tracer:       trc,
		Node:         nd,
		FilClient:    fc,
		Api:          gwApi,
		CM:           cm,
		StagingMgr:   sbm,
		gwayHandler:  gateway.NewGatewayHandler(nd.Blockstore),
		cacher:       cacher,
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
	_ = e.Group("/v2")
}
