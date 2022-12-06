package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"github.com/multiformats/go-multiaddr"
	"github.com/urfave/cli/v2"
	"os"
	"path/filepath"
	"strings"
	"time"

	explru "github.com/paskal/golang-lru/simplelru"
	"golang.org/x/time/rate"

	"github.com/application-research/estuary/deal/transfer"
	"github.com/application-research/estuary/sanitycheck"
	"github.com/application-research/estuary/shuttle"
	"golang.org/x/crypto/bcrypt"

	"github.com/application-research/estuary/collections"
	"github.com/application-research/estuary/constants"
	contentmgr "github.com/application-research/estuary/content"
	contentqueue "github.com/application-research/estuary/content/queue"
	"github.com/application-research/estuary/miner"
	"github.com/application-research/estuary/model"
	"github.com/application-research/estuary/node/modules/peering"
	"go.opencensus.io/stats/view"

	"github.com/application-research/estuary/autoretrieve"
	"github.com/application-research/estuary/build"
	"github.com/application-research/estuary/config"
	"github.com/application-research/estuary/metrics"
	"github.com/application-research/estuary/node"
	"github.com/application-research/estuary/pinner"
	"github.com/application-research/estuary/stagingbs"
	"github.com/application-research/estuary/util"
	"github.com/application-research/filclient"
	"github.com/google/uuid"
	gsimpl "github.com/ipfs/go-graphsync/impl"
	logging "github.com/ipfs/go-log/v2"
	"github.com/libp2p/go-libp2p/core/protocol"
	routed "github.com/libp2p/go-libp2p/p2p/host/routed"
	"go.opentelemetry.io/otel"

	"golang.org/x/xerrors"

	"github.com/application-research/estuary/api"
	apiv1 "github.com/application-research/estuary/api/v1"
	apiv2 "github.com/application-research/estuary/api/v2"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/lotus/chain/types"
	lcli "github.com/filecoin-project/lotus/cli"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

var appVersion string
var log = logging.Logger("estuary").With("app_version", appVersion)

func before(cctx *cli.Context) error {
	level := util.LogLevel

	_ = logging.SetLogLevel("dt-impl", level)
	_ = logging.SetLogLevel("autoretrieve", level)
	_ = logging.SetLogLevel("estuary", level)
	_ = logging.SetLogLevel("paych", level)
	_ = logging.SetLogLevel("filclient", "warn") // filclient is too chatting for default loglevel (info), maybe sub-system loglevel should be supported
	_ = logging.SetLogLevel("dt_graphsync", level)
	_ = logging.SetLogLevel("dt-chanmon", level)
	_ = logging.SetLogLevel("markets", level)
	_ = logging.SetLogLevel("data_transfer_network", level)
	_ = logging.SetLogLevel("rpc", level)
	_ = logging.SetLogLevel("bs-wal", level)
	_ = logging.SetLogLevel("provider.batched", level)
	_ = logging.SetLogLevel("bs-migrate", level)
	_ = logging.SetLogLevel("rcmgr", level)
	_ = logging.SetLogLevel("est-node", level)

	return nil
}

func overrideSetOptions(flags []cli.Flag, cctx *cli.Context, cfg *config.Estuary) error {
	for _, flag := range flags {
		name := flag.Names()[0]
		if cctx.IsSet(name) {
			log.Debugf("estuary cli flag %s is set to %s", name, cctx.String(name))
		} else {
			continue
		}

		switch name {
		case "node-api-url":
			cfg.Node.ApiURL = cctx.String("node-api-url")
		case "datadir":
			cfg.DataDir = cctx.String("datadir")
		case "blockstore":
			cfg.Node.Blockstore = cctx.String("blockstore")
		case "no-blockstore-cache":
			cfg.Node.NoBlockstoreCache = cctx.Bool("no-blockstore-cache")
		case "write-log-truncate":
			cfg.Node.WriteLogTruncate = cctx.Bool("write-log-truncate")
		case "write-log-flush":
			cfg.Node.HardFlushWriteLog = cctx.Bool("write-log-flush")
		case "write-log":
			if wl := cctx.String("write-log"); wl != "" {
				if wl[0] == '/' {
					cfg.Node.WriteLogDir = wl
				} else {
					cfg.Node.WriteLogDir = filepath.Join(cctx.String("datadir"), wl)
				}
			}
		case "database":
			cfg.DatabaseConnString = cctx.String("database")
		case "apilisten":
			cfg.ApiListen = cctx.String("apilisten")
		case "announce":
			_, err := multiaddr.NewMultiaddr(cctx.String("announce"))
			if err != nil {
				return fmt.Errorf("failed to parse announce address %s: %w", cctx.String("announce"), err)
			}
			cfg.Node.AnnounceAddrs = []string{cctx.String("announce")}
		case "peering-peers":
			//	The peer is an array of multiaddress so we need to allow
			//	the user to specify ID and Addrs
			var peers []peering.PeeringPeer
			peeringPeersStr := cctx.String("peering-peers")

			err := json.Unmarshal([]byte(peeringPeersStr), &peers)
			if err != nil {
				return fmt.Errorf("failed to parse peering addresses %s: %w", cctx.String("peering-peers"), err)
			}
			cfg.Node.PeeringPeers = append(cfg.Node.PeeringPeers, peers...)

		case "lightstep-token":
			cfg.LightstepToken = cctx.String("lightstep-token")
		case "hostname":
			cfg.Hostname = cctx.String("hostname")
		case "replication":
			cfg.Replication = cctx.Int("replication")
		case "lowmem":
			cfg.LowMem = cctx.Bool("lowmem")
		case "disable-deals-storage":
			cfg.DisableFilecoinStorage = cctx.Bool("disable-deals-storage")
		case "disable-new-deals":
			cfg.Deal.IsDisabled = cctx.Bool("disable-new-deals")
		case "verified-deal":
			cfg.Deal.IsVerified = cctx.Bool("verified-deal")
		case "fail-deals-on-transfer-failure":
			cfg.Deal.FailOnTransferFailure = cctx.Bool("fail-deals-on-transfer-failure")
		case "disable-local-content-adding":
			cfg.Content.DisableLocalAdding = cctx.Bool("disable-local-content-adding")
		case "disable-content-adding":
			cfg.Content.DisableGlobalAdding = cctx.Bool("disable-content-adding")
		case "jaeger-tracing":
			cfg.Jaeger.EnableTracing = cctx.Bool("jaeger-tracing")
		case "jaeger-provider-url":
			cfg.Jaeger.ProviderUrl = cctx.String("jaeger-provider-url")
		case "jaeger-sampler-ratio":
			cfg.Jaeger.SamplerRatio = cctx.Float64("jaeger-sampler-ratio")
		case "logging":
			cfg.Logging.ApiEndpointLogging = cctx.Bool("logging")
		case "disable-auto-retrieve":
			cfg.DisableAutoRetrieve = cctx.Bool("disable-auto-retrieve")
		case "bitswap-max-work-per-peer":
			cfg.Node.Bitswap.MaxOutstandingBytesPerPeer = cctx.Int64("bitswap-max-work-per-peer")
		case "bitswap-target-message-size":
			cfg.Node.Bitswap.TargetMessageSize = cctx.Int("bitswap-target-message-size")
		case "rpc-incoming-queue-size":
			cfg.RpcEngine.Websocket.IncomingQueueSize = cctx.Int("rpc-incoming-queue-size")
		case "rpc-outgoing-queue-size":
			cfg.RpcEngine.Websocket.OutgoingQueueSize = cctx.Int("rpc-outgoing-queue-size")
		case "rpc-queue-handlers":
			cfg.RpcEngine.Websocket.QueueHandlers = cctx.Int("rpc-queue-handlers")
		case "queue-eng-driver":
			cfg.RpcEngine.Queue.Driver = cctx.String("queue-eng-driver")
		case "queue-eng-host":
			cfg.RpcEngine.Queue.Host = cctx.String("queue-eng-host")
		case "queue-eng-enabled":
			cfg.RpcEngine.Queue.Enabled = cctx.Bool("queue-eng-enabled")
		case "queue-eng-consumers":
			cfg.RpcEngine.Queue.Consumers = cctx.Int("queue-eng-consumers")
		case "staging-bucket":
			cfg.StagingBucket.Enabled = cctx.Bool("staging-bucket")
		case "indexer-url":
			cfg.Node.IndexerURL = cctx.String("indexer-url")
		case "indexer-advertisement-interval":
			value, err := time.ParseDuration(cctx.String("indexer-advertisement-interval"))
			if err != nil {
				return fmt.Errorf("failed to parse indexer advertisement interval: %v", err)
			}
			cfg.Node.IndexerAdvertisementInterval = value
		case "advertise-offline-autoretrieves":
			cfg.Node.AdvertiseOfflineAutoretrieves = cctx.Bool("advertise-offline-autoretrieves")
		case "deal-protocol-version":
			dprs := make(map[protocol.ID]bool, 0)
			for _, dprv := range cctx.StringSlice("deal-protocol-version") {
				p, ok := config.DealProtocolsVersionsMap[dprv]
				if !ok {
					return fmt.Errorf("%s: is not a valid deal protocol version", dprv)
				}
				dprs[p] = true
			}

			if len(dprs) > 0 {
				cfg.Deal.EnabledDealProtocolsVersions = dprs
			}

		case "max-price":
			maxPrice, err := types.ParseFIL(cctx.String("max-price"))
			if err != nil {
				return fmt.Errorf("failed to parse max-price %s: %w", cctx.String("max-price"), err)
			}
			cfg.Deal.MaxPrice = abi.TokenAmount(maxPrice)

		case "max-verified-price":
			maxVerifiedPrice, err := types.ParseFIL(cctx.String("max-verified-price"))
			if err != nil {
				return fmt.Errorf("failed to parse max-verified-price %s: %w", cctx.String("max-verified-price"), err)
			}
			cfg.Deal.MaxVerifiedPrice = abi.TokenAmount(maxVerifiedPrice)

		case "rate-limit":
			cfg.RateLimit = rate.Limit(cctx.Float64("rate-limit"))

		default:
		}
	}
	return cfg.SetRequiredOptions()
}

const TOKEN_LABEL_ADMIN = "admin"

func main() {
	//set global time to UTC
	utc, _ := time.LoadLocation("UTC")
	time.Local = utc

	cfg := config.NewEstuary(appVersion)

	app := cli.NewApp()
	app.Version = appVersion
	app.Usage = "Estuary server CLI"
	app.Before = before

	app.Flags = getAppFlags(cfg)

	app.Commands = []*cli.Command{
		{
			Name:  "setup",
			Usage: "Creates an initial auth token under new user \"admin\"",
			Flags: getSetupFlags(cfg),
			Action: func(cctx *cli.Context) error {
				if err := cfg.Load(cctx.String("config")); err != nil && err != config.ErrNotInitialized { // still want to report parsing errors
					return err
				}

				if err := overrideSetOptions(app.Flags, cctx, cfg); err != nil {
					return nil
				}

				username := strings.ToLower(cctx.String("username"))
				if username == "" {
					return errors.New("setup username cannot be empty")
				}

				ok := constants.IsAdminUsernameValid(username)
				if !ok {
					return errors.New("username must be alphanumeric and 1-32 characters")
				}

				password := cctx.String("password")
				if password == "" {
					return errors.New("setup password cannot be empty")
				}

				ok = constants.IsAdminPasswordValid(password)
				if !ok {
					return errors.New("password must be at least eight characters and contain at least one letter and one number")
				}
				return Setup(username, password, cfg)
			},
		}, {
			Name:  "configure",
			Usage: "Saves a configuration file to the location specified by the config parameter",
			Action: func(cctx *cli.Context) error {
				configFile := cctx.String("config")
				if err := cfg.Load(configFile); err != nil && err != config.ErrNotInitialized { // still want to report parsing errors
					return err
				}

				if err := overrideSetOptions(app.Flags, cctx, cfg); err != nil {
					return err
				}
				return cfg.Save(configFile)
			},
		}, {
			Name:  "shuttle-init",
			Usage: "Initializes a shuttle node, returns handle and authorization token",
			Action: func(cctx *cli.Context) error {
				configFile := cctx.String("config")
				if err := cfg.Load(configFile); err != nil && err != config.ErrNotInitialized { // still want to report parsing errors
					return err
				}

				db, err := setupDatabase(cfg.DatabaseConnString)
				if err != nil {
					return err
				}

				shuttle := &model.Shuttle{
					Handle: "SHUTTLE" + uuid.New().String() + "HANDLE",
					Token:  "SECRET" + uuid.New().String() + "SECRET",
					Open:   false,
				}

				if err := db.Create(shuttle).Error; err != nil {
					return err
				}

				log.Infof(`{"handle":"%s","token":"%s"}`, shuttle.Handle, shuttle.Token)
				return nil
			},
		},
	}
	app.Action = func(cctx *cli.Context) error {
		if err := cfg.Load(cctx.String("config")); err != nil && err != config.ErrNotInitialized { // For backward compatibility, don't error if no config file
			return err
		}

		if err := overrideSetOptions(app.Flags, cctx, cfg); err != nil {
			return err
		}
		return Run(cctx.Context, cfg)
	}
	if err := app.Run(os.Args); err != nil {
		log.Fatalf("could not run estuary app: %+v", err)
	}
}

func setupDatabase(dbConnStr string) (*gorm.DB, error) {
	db, err := util.SetupDatabase(dbConnStr)
	if err != nil {
		return nil, err
	}

	if err = migrateSchemas(db); err != nil {
		return nil, err
	}

	// 'manually' add unique composite index on collection fields because gorms syntax for it is tricky
	if err := db.Exec("create unique index if not exists collection_refs_paths on collection_refs (path,collection)").Error; err != nil {
		return nil, fmt.Errorf("failed to create collection paths index: %w", err)
	}

	var count int64
	if err := db.Model(&model.StorageMiner{}).Count(&count).Error; err != nil {
		return nil, err
	}

	if count == 0 {
		fmt.Println("adding default miner list to database...")
		for _, m := range build.DefaultMiners {
			db.Create(&model.StorageMiner{Address: util.DbAddr{Addr: m}})
		}
	}
	return db, nil
}

func migrateSchemas(db *gorm.DB) error {
	if err := db.AutoMigrate(
		&util.Content{},
		&util.Object{},
		&util.ObjRef{},
		&collections.Collection{},
		&collections.CollectionRef{},
		&model.ContentDeal{},
		&model.DfeRecord{},
		&model.PieceCommRecord{},
		&model.ProposalRecord{},
		&util.RetrievalFailureRecord{},
		&model.RetrievalSuccessRecord{},
		&model.MinerStorageAsk{},
		&model.StorageMiner{},
		&util.User{},
		&util.AuthToken{},
		&util.InviteCode{},
		&model.Shuttle{},
		&autoretrieve.Autoretrieve{},
		&model.SanityCheck{},
		&autoretrieve.PublishedBatch{},
		&model.StagingZone{},
		&model.StagingZoneTracker{},
		&model.ShuttleConnection{},
	); err != nil {
		return err
	}
	return nil
}

func Setup(username, password string, cfg *config.Estuary) error {
	db, err := setupDatabase(cfg.DatabaseConnString)
	if err != nil {
		return err
	}

	quietdb := db.Session(&gorm.Session{
		Logger: logger.Discard,
	})

	var exist *util.User
	if err := quietdb.First(&exist, "username = ?", username).Error; err != nil {
		if !xerrors.Is(err, gorm.ErrRecordNotFound) {
			return err
		}
		exist = nil
	}

	if exist != nil {
		return fmt.Errorf("a user already exist for that username:%s", username)
	}

	//	work with bcrypt on cli defined password.
	hashedPasswordBytes, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.MinCost)
	if err != nil {
		return fmt.Errorf("hashing admin password failed: %w", err)
	}

	newUser := &util.User{
		UUID:     uuid.New().String(),
		Username: username,
		Salt:     uuid.New().String(), // default salt.
		PassHash: string(hashedPasswordBytes),
		Perm:     100,
	}
	if err := db.Create(newUser).Error; err != nil {
		return fmt.Errorf("admin user creation failed: %w", err)
	}

	token := "EST" + uuid.New().String() + "ARY"
	authToken := &util.AuthToken{
		Token:     token,
		TokenHash: util.GetTokenHash(token),
		Label:     TOKEN_LABEL_ADMIN,
		User:      newUser.ID,
		Expiry:    time.Now().Add(constants.TokenExpiryDurationAdmin),
	}
	if err := db.Create(authToken).Error; err != nil {
		return fmt.Errorf("admin token creation failed: %w", err)
	}

	fmt.Printf("Auth Token: %v\n", authToken.Token)
	return nil
}

func Run(ctx context.Context, cfg *config.Estuary) error {

	if err := cfg.Validate(); err != nil {
		return err
	}

	db, err := setupDatabase(cfg.DatabaseConnString)
	if err != nil {
		return err
	}

	// stand up saninty check manager
	sanitycheckMgr := sanitycheck.NewManager(db, log)

	init := Initializer{&cfg.Node, db, nil}
	nd, err := node.Setup(ctx, &init, sanitycheckMgr.HandleMissingBlocks)
	if err != nil {
		return err
	}

	for _, a := range nd.Host.Addrs() {
		log.Infof("%s/p2p/%s\n", a, nd.Host.ID())
	}

	go func() {
		for _, ai := range node.BootstrapPeers {
			if err := nd.Host.Connect(ctx, ai); err != nil {
				log.Warnf("failed to connect to bootstrapper: %s", err)
				continue
			}
		}

		if err := nd.Dht.Bootstrap(ctx); err != nil {
			log.Warnf("dht bootstrapping failed: %s", err)
		}
	}()

	if err = view.Register(metrics.DefaultViews...); err != nil {
		log.Errorf("Cannot register the OpenCensus view: %s", err)
		return err
	}

	walletAddr, err := nd.Wallet.GetDefault()
	if err != nil {
		return err
	}

	// send a CLI context to lotus that contains only the node "api-url" flag set, so that other flags don't accidentally conflict with lotus cli flags
	// https://github.com/filecoin-project/lotus/blob/731da455d46cb88ee5de9a70920a2d29dec9365c/cli/util/api.go#L37
	flset := flag.NewFlagSet("lotus", flag.ExitOnError)
	flset.String("api-url", "", "node api url")
	err = flset.Set("api-url", cfg.Node.ApiURL)
	if err != nil {
		return err
	}

	ncctx := cli.NewContext(cli.NewApp(), flset, nil)
	gatewayApi, closer, err := lcli.GetGatewayAPI(ncctx)
	if err != nil {
		return err
	}
	defer closer()

	// setup tracing to jaeger if enabled
	if cfg.Jaeger.EnableTracing {
		tp, err := metrics.NewJaegerTraceProvider("estuary",
			cfg.Jaeger.ProviderUrl, cfg.Jaeger.SamplerRatio)
		if err != nil {
			return err
		}
		otel.SetTracerProvider(tp)
	}

	sbmgr, err := stagingbs.NewStagingBSMgr(cfg.StagingDataDir)
	if err != nil {
		return err
	}

	var opts []func(*filclient.Config)
	if cfg.LowMem {
		opts = append(opts, func(cfg *filclient.Config) {
			cfg.GraphsyncOpts = []gsimpl.Option{
				gsimpl.MaxInProgressIncomingRequests(100),
				gsimpl.MaxInProgressOutgoingRequests(100),
				gsimpl.MaxMemoryResponder(4 << 30),
				gsimpl.MaxMemoryPerPeerResponder(16 << 20),
				gsimpl.MaxInProgressIncomingRequestsPerPeer(10),
				gsimpl.MessageSendRetries(2),
				gsimpl.SendMessageTimeout(2 * time.Minute),
			}
		})
	}

	opts = append(opts, func(config *filclient.Config) {
		config.Lp2pDTConfig.Server.ThrottleLimit = cfg.Node.Libp2pThrottleLimit
	})

	rhost := routed.Wrap(nd.Host, nd.FilDht)
	fc, err := filclient.NewClient(rhost, gatewayApi, nd.Wallet, walletAddr, nd.Blockstore, nd.Datastore, cfg.DataDir, opts...)
	if err != nil {
		return err
	}

	cntQueueMgr := contentqueue.NewQueueManager(cfg.DisableFilecoinStorage, cfg.Content.MinSize)

	// stand up shuttle manager
	shuttleMgr, err := shuttle.NewManager(ctx, db, cfg, log, sanitycheckMgr, cntQueueMgr)
	if err != nil {
		return err
	}

	// stand up transfer manager
	transferMgr := transfer.NewManager(db, fc, log, shuttleMgr)
	if err := transferMgr.SubscribeEventListener(ctx); err != nil {
		return fmt.Errorf("subscribing to libp2p transfer manager: %w", err)
	}

	// stand up miner manager
	minerMgr := miner.NewMinerManager(db, fc, cfg, gatewayApi, log)

	// stand up content manager
	cm, err := contentmgr.NewContentManager(db, gatewayApi, fc, init.trackingBstore, nd, cfg, minerMgr, log, shuttleMgr, transferMgr, cntQueueMgr)
	if err != nil {
		return err
	}
	fc.SetPieceCommFunc(cm.GetPieceCommitment)

	// stand up pin manager
	pinmgr := pinner.NewEstuaryPinManager(cm.DoPinning, cm.UpdatePinStatus, &pinner.PinManagerOpts{
		MaxActivePerUser: 20,
		QueueDataDir:     cfg.DataDir,
	}, cm, shuttleMgr)
	go pinmgr.Run(50)
	go pinmgr.RunPinningRetryWorker(ctx, db, cfg) // pinning retry worker, re-attempt pinning contents, not yet pinned after a period of time

	go cm.Run(ctx) // deal making and deal reconciliation

	// Start autoretrieve if not disabled
	if !cfg.DisableAutoRetrieve {
		init.trackingBstore.SetCidReqFunc(cm.RefreshContentForCid)

		ap, err := autoretrieve.NewProvider(
			db,
			cfg.Node.IndexerAdvertisementInterval,
			cfg.Node.IndexerURL,
			cfg.Node.AdvertiseOfflineAutoretrieves,
		)
		if err != nil {
			return err
		}

		go func() {
			defer func() {
				if err := recover(); err != nil {
					log.Errorf("Autoretrieve provide loop panicked, cancelling until the executable is restarted: %v", err)
				}
			}()

			if err = ap.Run(context.Background()); err != nil {
				log.Errorf("Autoretrieve provide loop failed, cancelling until the executable is restarted: %v", err)
			}
		}()
		defer ap.Stop()
	}

	// resume all resumable legacy data transfer for local contents
	go func() {
		time.Sleep(time.Second * 10)
		if err := transferMgr.RestartAllTransfersForLocation(ctx, constants.ContentLocationLocal, make(chan struct{})); err != nil {
			log.Errorf("failed to restart transfers: %s", err)
		}
	}()

	cacher := explru.NewExpirableLRU(constants.CacheSize, nil, constants.CacheDuration, constants.CachePurgeEveryDuration)
	extendedCacher := explru.NewExpirableLRU(constants.ExtendedCacheSize, nil, constants.ExtendedCacheDuration, constants.ExtendedCachePurgeEveryDuration)

	// stand up api server
	apiTracer := otel.Tracer("api")
	apiV1 := apiv1.NewAPIV1(cfg, db, nd, fc, gatewayApi, sbmgr, cm, cacher, extendedCacher, minerMgr, pinmgr, log, apiTracer, shuttleMgr, transferMgr)
	apiV2 := apiv2.NewAPIV2(cfg, db, nd, fc, gatewayApi, sbmgr, cm, cacher, extendedCacher, minerMgr, pinmgr, log, apiTracer)

	apiEngine := api.NewEngine(cfg, apiTracer)
	apiEngine.RegisterAPI(apiV1)
	apiEngine.RegisterAPI(apiV2)

	return apiEngine.Start()
}
