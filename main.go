package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"golang.org/x/crypto/bcrypt"

	"github.com/application-research/estuary/collections"
	"github.com/application-research/estuary/constants"
	"github.com/application-research/estuary/node/modules/peering"
	"github.com/multiformats/go-multiaddr"

	"go.opencensus.io/stats/view"

	"github.com/application-research/estuary/autoretrieve"
	"github.com/application-research/estuary/build"
	"github.com/application-research/estuary/config"
	drpc "github.com/application-research/estuary/drpc"
	"github.com/application-research/estuary/metrics"
	"github.com/application-research/estuary/node"
	"github.com/application-research/estuary/pinner"
	"github.com/application-research/estuary/stagingbs"
	"github.com/application-research/estuary/util"
	"github.com/application-research/estuary/util/gateway"
	"github.com/application-research/filclient"
	"github.com/google/uuid"
	"github.com/ipfs/go-cid"
	gsimpl "github.com/ipfs/go-graphsync/impl"
	logging "github.com/ipfs/go-log/v2"
	"github.com/libp2p/go-libp2p/core/protocol"
	routed "github.com/libp2p/go-libp2p/p2p/host/routed"
	"github.com/mitchellh/go-homedir"
	"github.com/whyrusleeping/memo"
	"go.opentelemetry.io/otel"

	"go.opentelemetry.io/otel/trace"
	"golang.org/x/xerrors"

	datatransfer "github.com/filecoin-project/go-data-transfer"
	"github.com/filecoin-project/lotus/api"
	lcli "github.com/filecoin-project/lotus/cli"
	cli "github.com/urfave/cli/v2"

	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

var appVersion string
var log = logging.Logger("estuary").With("app_version", appVersion)

type storageMiner struct {
	gorm.Model
	Address         util.DbAddr `gorm:"unique"`
	Suspended       bool
	SuspendedReason string
	Name            string
	Version         string
	Location        string
	Owner           uint
}

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
			cfg.RPCMessage.IncomingQueueSize = cctx.Int("rpc-incoming-queue-size")
		case "rpc-outgoing-queue-size":
			cfg.RPCMessage.OutgoingQueueSize = cctx.Int("rpc-outgoing-queue-size")
		case "rpc-queue-handlers":
			cfg.RPCMessage.QueueHandlers = cctx.Int("rpc-queue-handlers")
		case "staging-bucket":
			cfg.StagingBucket.Enabled = cctx.Bool("staging-bucket")
		case "indexer-url":
			cfg.Node.IndexerURL = cctx.String("indexer-url")
		case "indexer-tick-interval":
			cfg.Node.IndexerTickInterval = cctx.Int("indexer-tick-interval")
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

	hDir, err := homedir.Dir()
	if err != nil {
		log.Fatalf("could not determine homedir for estuary app: %+v", err)
	}

	app := cli.NewApp()
	app.Version = appVersion

	cfg := config.NewEstuary(appVersion)

	app.Usage = "Estuary server CLI"

	app.Before = before

	app.Flags = []cli.Flag{
		util.FlagLogLevel,
		&cli.StringFlag{
			Name:  "repo",
			Value: "~/.lotus",
		},
		&cli.StringFlag{
			Name:    "node-api-url",
			Value:   cfg.Node.ApiURL,
			Usage:   "lotus api gateway url",
			EnvVars: []string{"FULLNODE_API_INFO"},
		},
		&cli.StringFlag{
			Name:  "config",
			Usage: "specify configuration file location",
			Value: filepath.Join(hDir, ".estuary"),
		},
		&cli.StringFlag{
			Name:    "database",
			Usage:   "specify connection string for estuary database",
			Value:   cfg.DatabaseConnString,
			EnvVars: []string{"ESTUARY_DATABASE"},
		},
		&cli.StringFlag{
			Name:    "apilisten",
			Usage:   "address for the api server to listen on",
			Value:   cfg.ApiListen,
			EnvVars: []string{"ESTUARY_API_LISTEN"},
		},
		&cli.StringFlag{
			Name:    "announce",
			Usage:   "announce address for the libp2p server to listen on",
			EnvVars: []string{"ESTUARY_ANNOUNCE"},
		},
		&cli.StringFlag{
			Name:  "peering-peers",
			Usage: "peering addresses for the libp2p server to listen on",
		},
		&cli.StringFlag{
			Name:    "datadir",
			Usage:   "directory to store data in",
			Value:   cfg.DataDir,
			EnvVars: []string{"ESTUARY_DATADIR"},
		},
		&cli.StringFlag{
			Name:   "write-log",
			Usage:  "enable write log blockstore in specified directory",
			Value:  cfg.Node.WriteLogDir,
			Hidden: true,
		},
		&cli.BoolFlag{
			Name:  "disable-deals-storage",
			Usage: "stops estuary from making new deals and updating existing deals, essentially runs as an ipfs node instead",
			Value: cfg.DisableFilecoinStorage,
		},
		&cli.BoolFlag{
			Name:  "logging",
			Usage: "enable api endpoint logging",
			Value: cfg.Logging.ApiEndpointLogging,
		},
		&cli.BoolFlag{
			Name:  "disable-auto-retrieve",
			Usage: "disables autoretrieve",
			Value: cfg.DisableAutoRetrieve,
		},
		&cli.StringFlag{
			Name:    "lightstep-token",
			Usage:   "specify lightstep access token for enabling trace exports",
			EnvVars: []string{"ESTUARY_LIGHTSTEP_TOKEN"},
			Value:   cfg.LightstepToken,
		},
		&cli.StringFlag{
			Name:  "hostname",
			Usage: "specify hostname this node will be reachable at",
			Value: cfg.Hostname,
		},
		&cli.BoolFlag{
			Name:  "fail-deals-on-transfer-failure",
			Usage: "consider deals failed when the transfer to the miner fails",
			Value: cfg.Deal.FailOnTransferFailure,
		},
		&cli.BoolFlag{
			Name:  "disable-new-deals",
			Usage: "prevents the worker from making any new deals, but existing deals will still be updated/checked",
			Value: cfg.Deal.IsDisabled,
		},
		&cli.BoolFlag{
			Name:  "disable-swagger-endpoint",
			Usage: "do not create the /swagger/* endpoints",
			Value: cfg.DisableSwaggerEndpoint,
		},
		&cli.BoolFlag{
			Name:  "verified-deal",
			Usage: "Defaults to makes deals as verified deal using datacap. Set to false to make deal as regular deal using real FIL(no datacap)",
			Value: cfg.Deal.IsVerified,
		},
		&cli.BoolFlag{
			Name:  "disable-content-adding",
			Usage: "disallow new content ingestion globally",
			Value: cfg.Content.DisableGlobalAdding,
		},
		&cli.BoolFlag{
			Name:  "disable-local-content-adding",
			Usage: "disallow new content ingestion on this node (shuttles are unaffected)",
			Value: cfg.Content.DisableLocalAdding,
		},
		&cli.StringFlag{
			Name:  "blockstore",
			Usage: "specify blockstore parameters",
			Value: cfg.Node.Blockstore,
		},
		&cli.BoolFlag{
			Name:  "write-log-truncate",
			Usage: "enables log truncating",
			Value: cfg.Node.WriteLogTruncate,
		},
		&cli.BoolFlag{
			Name:  "write-log-flush",
			Usage: "enable hard flushing blockstore",
			Value: cfg.Node.HardFlushWriteLog,
		},
		&cli.BoolFlag{
			Name:  "no-blockstore-cache",
			Usage: "disable blockstore caching",
			Value: cfg.Node.NoBlockstoreCache,
		},
		&cli.IntFlag{
			Name:  "replication",
			Usage: "sets replication factor",
			Value: cfg.Replication,
		},
		&cli.BoolFlag{
			Name:  "lowmem",
			Usage: "TEMP: turns down certain parameters to attempt to use less memory (will be replaced by a more specific flag later)",
			Value: cfg.LowMem,
		},
		&cli.BoolFlag{
			Name:  "jaeger-tracing",
			Usage: "enables jaeger tracing",
			Value: cfg.Jaeger.EnableTracing,
		},
		&cli.StringFlag{
			Name:  "jaeger-provider-url",
			Usage: "sets the jaeger provider url",
			Value: cfg.Jaeger.ProviderUrl,
		},
		&cli.Float64Flag{
			Name:  "jaeger-sampler-ratio",
			Usage: "If less than 1 probabilistic metrics will be used.",
			Value: cfg.Jaeger.SamplerRatio,
		},
		&cli.Int64Flag{
			Name:  "bitswap-max-work-per-peer",
			Usage: "sets the bitswap max work per peer",
			Value: cfg.Node.Bitswap.MaxOutstandingBytesPerPeer,
		},
		&cli.IntFlag{
			Name:  "bitswap-target-message-size",
			Usage: "sets the bitswap target message size",
			Value: cfg.Node.Bitswap.TargetMessageSize,
		},
		&cli.IntFlag{
			Name:  "rpc-incoming-queue-size",
			Usage: "sets incoming rpc message queue size",
			Value: cfg.RPCMessage.IncomingQueueSize,
		},
		&cli.IntFlag{
			Name:  "rpc-outgoing-queue-size",
			Usage: "sets outgoing rpc message queue size",
			Value: cfg.RPCMessage.OutgoingQueueSize,
		},
		&cli.IntFlag{
			Name:  "rpc-queue-handlers",
			Usage: "sets rpc message handler count",
			Value: cfg.RPCMessage.QueueHandlers,
		},
		&cli.BoolFlag{
			Name:  "staging-bucket",
			Usage: "enable staging bucket",
			Value: cfg.StagingBucket.Enabled,
		},
		&cli.StringSliceFlag{
			Name:  "deal-protocol-version",
			Usage: "sets the deal protocol version. defaults to v110 (go-fil-markets) and v120 (boost)",
		},
		&cli.StringFlag{
			Name:  "indexer-url",
			Usage: "sets the indexer advertisement url",
			Value: cfg.Node.IndexerURL,
		},
		&cli.IntFlag{
			Name:  "indexer-tick-interval",
			Usage: "sets the indexer advertisement interval in minutes",
			Value: cfg.Node.IndexerTickInterval,
		},
	}
	app.Commands = []*cli.Command{
		{
			Name:  "setup",
			Usage: "Creates an initial auth token under new user \"admin\"",
			Flags: []cli.Flag{
				&cli.StringFlag{
					Name:  "username",
					Usage: "specify setup username",
				},
				&cli.StringFlag{
					Name:  "password",
					Usage: "specify setup password",
				},
				&cli.StringFlag{
					Name:  "config",
					Usage: "specify configuration file location",
					Value: filepath.Join(hDir, ".estuary"),
				},
				&cli.StringFlag{
					Name:    "database",
					Usage:   "specify connection string for estuary database",
					Value:   cfg.DatabaseConnString,
					EnvVars: []string{"ESTUARY_DATABASE"},
				},
			},
			Action: func(cctx *cli.Context) error {
				if err := cfg.Load(cctx.String("config")); err != nil && err != config.ErrNotInitialized { // still want to report parsing errors
					return err
				}

				if err := overrideSetOptions(app.Flags, cctx, cfg); err != nil {
					return nil
				}

				username := cctx.String("username")
				if username == "" {
					return errors.New("setup username cannot be empty")
				}

				password := cctx.String("password")
				if password == "" {
					return errors.New("setup password cannot be empty")
				}

				db, err := setupDatabase(cfg.DatabaseConnString)
				if err != nil {
					return err
				}

				quietdb := db.Session(&gorm.Session{
					Logger: logger.Discard,
				})

				username = strings.ToLower(username)

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

				salt := uuid.New().String()

				//	work with bcrypt on cli defined password.
				var passwordBytes = []byte(password)
				hashedPasswordBytes, err := bcrypt.GenerateFromPassword(passwordBytes, bcrypt.MinCost)

				newUser := &util.User{
					UUID:     uuid.New().String(),
					Username: username,
					Salt:     salt, // default salt.
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
		},
	}
	app.Action = func(cctx *cli.Context) error {
		log.Infof("estuary version: %s", appVersion)

		if err := cfg.Load(cctx.String("config")); err != nil && err != config.ErrNotInitialized { // For backward compatibility, don't error if no config file
			return err
		}

		if err := overrideSetOptions(app.Flags, cctx, cfg); err != nil {
			return err
		}

		db, err := setupDatabase(cfg.DatabaseConnString)
		if err != nil {
			return err
		}

		init := Initializer{&cfg.Node, db, nil}
		nd, err := node.Setup(cctx.Context, &init)
		if err != nil {
			return err
		}

		if err = view.Register(metrics.DefaultViews...); err != nil {
			log.Fatalf("Cannot register the OpenCensus view: %v", err)
			return err
		}

		addr, err := nd.Wallet.GetDefault()
		if err != nil {
			return err
		}

		sbmgr, err := stagingbs.NewStagingBSMgr(cfg.StagingDataDir)
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
		api, closer, err := lcli.GetGatewayAPI(ncctx)
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

		s := &Server{
			DB:               db,
			Node:             nd,
			Api:              api,
			StagingMgr:       sbmgr,
			tracer:           otel.Tracer("api"),
			cacher:           memo.NewCacher(),
			gwayHandler:      gateway.NewGatewayHandler(nd.Blockstore),
			cfg:              cfg,
			trackingChannels: make(map[string]*util.ChanTrack),
		}

		// TODO: this is an ugly self referential hack... should fix
		pinmgr := pinner.NewPinManager(s.doPinning, s.PinStatusFunc, &pinner.PinManagerOpts{
			MaxActivePerUser: 20,
			QueueDataDir:     cfg.DataDir,
		})
		go pinmgr.Run(50)

		rhost := routed.Wrap(nd.Host, nd.FilDht)

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

		fc, err := filclient.NewClient(rhost, api, nd.Wallet, addr, nd.Blockstore, nd.Datastore, cfg.DataDir, opts...)
		if err != nil {
			return err
		}

		for _, a := range nd.Host.Addrs() {
			fmt.Printf("%s/p2p/%s\n", a, nd.Host.ID())
		}

		go func() {
			for _, ai := range node.BootstrapPeers {
				if err := nd.Host.Connect(cctx.Context, ai); err != nil {
					fmt.Println("failed to connect to bootstrapper: ", err)
					continue
				}
			}

			if err := nd.Dht.Bootstrap(cctx.Context); err != nil {
				fmt.Println("dht bootstrapping failed: ", err)
			}
		}()

		// Subscribe to data transfer events from Boost - we need this to get started and finished actual timestamps
		_, err = fc.Libp2pTransferMgr.Subscribe(func(dbid uint, fst filclient.ChannelState) {
			go func() {
				s.tcLk.Lock()
				trk, _ := s.trackingChannels[fst.ChannelID.String()]
				s.tcLk.Unlock()

				// if this state type is already announced, ignore it - rate limit events, only the most recent state is needed
				if trk != nil && trk.Last.Status == fst.Status {
					return
				}
				s.trackTransfer(&fst.ChannelID, dbid, &fst)

				switch fst.Status {
				case datatransfer.Requested:
					if err := s.CM.SetDataTransferStartedOrFinished(cctx.Context, dbid, fst.TransferID, &fst, true); err != nil {
						log.Errorf("failed to set data transfer started from event: %s", err)
					}
				case datatransfer.TransferFinished, datatransfer.Completed:
					if err := s.CM.SetDataTransferStartedOrFinished(cctx.Context, dbid, fst.TransferID, &fst, false); err != nil {
						log.Errorf("failed to set data transfer started from event: %s", err)
					}
				default:
					// for every other events
					trsFailed, msg := util.TransferFailed(&fst)
					if err = s.CM.handleRpcTransferStatus(context.TODO(), constants.ContentLocationLocal, &drpc.TransferStatus{
						Chanid:   fst.TransferID,
						DealDBID: dbid,
						State:    &fst,
						Failed:   trsFailed,
						Message:  fmt.Sprintf("status: %d(%s), message: %s", fst.Status, msg, fst.Message),
					}); err != nil {
						log.Errorf("failed to set data transfer update from event: %s", err)
					}
				}
			}()
		})
		if err != nil {
			return fmt.Errorf("subscribing to libp2p transfer manager: %w", err)
		}

		cm, err := NewContentManager(db, api, fc, init.trackingBstore, nd.NotifBlockstore, nd.Provider, pinmgr, nd, cfg)
		if err != nil {
			return err
		}
		s.CM = cm

		fc.SetPieceCommFunc(cm.getPieceCommitment)
		s.FilClient = fc

		if !cfg.DisableAutoRetrieve {
			init.trackingBstore.SetCidReqFunc(cm.RefreshContentForCid)
		}

		go cm.Run(cctx.Context)                                                 // deal making and deal reconciliation
		go cm.handleShuttleMessages(cctx.Context, cfg.RPCMessage.QueueHandlers) // register workers/handlers to process shuttle rpc messages from a channel(queue)

		// Start autoretrieve if not disabled
		if !cfg.DisableAutoRetrieve {
			s.Node.ArEngine, err = autoretrieve.NewAutoretrieveEngine(context.Background(), cfg, s.DB, s.Node.Host, s.Node.Datastore, s.FilClient.GetDtMgr())
			if err != nil {
				return err
			}

			go s.Node.ArEngine.Run()
			defer s.Node.ArEngine.Shutdown()
		}

		go func() {
			time.Sleep(time.Second * 10)

			if err := s.RestartAllTransfersForLocation(cctx.Context, constants.ContentLocationLocal); err != nil {
				log.Errorf("failed to restart transfers: %s", err)
			}
		}()

		return s.ServeAPI()
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
	if err := db.Model(&storageMiner{}).Count(&count).Error; err != nil {
		return nil, err
	}

	if count == 0 {
		fmt.Println("adding default miner list to database...")
		for _, m := range build.DefaultMiners {
			db.Create(&storageMiner{Address: util.DbAddr{Addr: m}})
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
		&contentDeal{},
		&dfeRecord{},
		&PieceCommRecord{},
		&proposalRecord{},
		&util.RetrievalFailureRecord{},
		&retrievalSuccessRecord{},
		&minerStorageAsk{},
		&storageMiner{},
		&util.User{},
		&util.AuthToken{},
		&util.InviteCode{},
		&Shuttle{},
		&autoretrieve.Autoretrieve{}); err != nil {
		return err
	}
	return nil
}

type Server struct {
	cfg        *config.Estuary
	tracer     trace.Tracer
	Node       *node.Node
	DB         *gorm.DB
	FilClient  *filclient.FilClient
	Api        api.Gateway
	CM         *ContentManager
	StagingMgr *stagingbs.StagingBSMgr

	gwayHandler *gateway.GatewayHandler

	cacher *memo.Cacher

	tcLk             sync.Mutex
	trackingChannels map[string]*util.ChanTrack
}

func (s *Server) GarbageCollect(ctx context.Context) error {
	// since we're reference counting all the content, garbage collection becomes easy
	// its even easier if we don't care that its 'perfect'

	// We can probably even just remove stuff when its references are removed from the database
	keych, err := s.Node.Blockstore.AllKeysChan(ctx)
	if err != nil {
		return err
	}

	for c := range keych {
		keep, err := s.trackingObject(c)
		if err != nil {
			return err
		}

		if !keep {
			// can batch these deletes and execute them at the datastore layer for more perfs
			if err := s.Node.Blockstore.DeleteBlock(ctx, c); err != nil {
				return err
			}
		}
	}

	return nil
}

func (s *Server) trackingObject(c cid.Cid) (bool, error) {
	var count int64
	if err := s.DB.Model(&util.Object{}).Where("cid = ?", c.Bytes()).Count(&count).Error; err != nil {
		if xerrors.Is(err, gorm.ErrRecordNotFound) {
			return false, nil
		}
		return false, err
	}

	return count > 0, nil
}

func (s *Server) RestartAllTransfersForLocation(ctx context.Context, loc string) error {
	var deals []contentDeal
	if err := s.DB.Model(contentDeal{}).
		Joins("left join contents on contents.id = content_deals.content").
		Where("not content_deals.failed and content_deals.deal_id = 0 and content_deals.dt_chan != '' and location = ?", loc).
		Scan(&deals).Error; err != nil {
		return err
	}

	go func() {
		for _, d := range deals {
			chid, err := d.ChannelID()
			if err != nil {
				// Only legacy (push) transfers need to be restarted by Estuary.
				// Newer (pull) transfers are restarted by the Storage Provider.
				// So if it's not a legacy channel ID, ignore it.
				continue
			}

			if err := s.CM.RestartTransfer(ctx, loc, chid, d); err != nil {
				log.Errorf("failed to restart transfer: %s", err)
				continue
			}
		}
	}()
	return nil
}

func (s *Server) trackTransfer(chanid *datatransfer.ChannelID, dealdbid uint, st *filclient.ChannelState) {
	s.tcLk.Lock()
	defer s.tcLk.Unlock()

	s.trackingChannels[chanid.String()] = &util.ChanTrack{
		Dbid: dealdbid,
		Last: st,
	}
}

// RestartTransfer tries to resume incomplete data transfers between client and storage providers.
// It supports only legacy deals (PushTransfer)
func (cm *ContentManager) RestartTransfer(ctx context.Context, loc string, chanid datatransfer.ChannelID, d contentDeal) error {
	maddr, err := d.MinerAddr()
	if err != nil {
		return err
	}

	var dealUUID *uuid.UUID
	if d.DealUUID != "" {
		parsed, err := uuid.Parse(d.DealUUID)
		if err != nil {
			return fmt.Errorf("parsing deal uuid %s: %w", d.DealUUID, err)
		}
		dealUUID = &parsed
	}

	_, isPushTransfer, err := cm.getProviderDealStatus(ctx, &d, maddr, dealUUID)
	if err != nil {
		return err
	}

	if !isPushTransfer {
		return nil
	}

	if loc == constants.ContentLocationLocal {
		// get the deal data transfer state pull deals
		st, err := cm.FilClient.TransferStatus(ctx, &chanid)
		if err != nil && err != filclient.ErrNoTransferFound {
			return err
		}

		if st == nil {
			return fmt.Errorf("no data transfer state was found")
		}

		cannotRestart := !util.CanRestartTransfer(st)
		if cannotRestart {
			trsFailed, msg := util.TransferFailed(st)
			if trsFailed {
				if err := cm.DB.Model(contentDeal{}).Where("id = ?", d.ID).UpdateColumns(map[string]interface{}{
					"failed":    true,
					"failed_at": time.Now(),
				}).Error; err != nil {
					return err
				}
				errMsg := fmt.Sprintf("status: %d(%s), message: %s", st.Status, msg, st.Message)
				return fmt.Errorf("deal in database is in progress, but data transfer is terminated: %s", errMsg)
			}
			return nil
		}
		return cm.FilClient.RestartTransfer(ctx, &chanid)
	}
	return cm.sendRestartTransferCmd(ctx, loc, chanid, d)
}

func (cm *ContentManager) sendRestartTransferCmd(ctx context.Context, loc string, chanid datatransfer.ChannelID, d contentDeal) error {
	return cm.sendShuttleCommand(ctx, loc, &drpc.Command{
		Op: drpc.CMD_RestartTransfer,
		Params: drpc.CmdParams{
			RestartTransfer: &drpc.RestartTransfer{
				ChanID:    chanid,
				DealDBID:  d.ID,
				ContentID: d.Content,
			},
		},
	})
}
