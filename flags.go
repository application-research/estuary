package main

import (
	"github.com/application-research/estuary/config"
	"github.com/application-research/estuary/util"
	"github.com/mitchellh/go-homedir"
	"github.com/urfave/cli/v2"
	"path/filepath"
)

func getAppFlags(cfg *config.Estuary) []cli.Flag {
	hDir, err := homedir.Dir()
	if err != nil {
		log.Fatalf("could not determine homedir for estuary app: %+v", err)
	}

	return []cli.Flag{
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
			Value: cfg.RpcEngine.Websocket.IncomingQueueSize,
		},
		&cli.IntFlag{
			Name:  "rpc-outgoing-queue-size",
			Usage: "sets outgoing rpc message queue size",
			Value: cfg.RpcEngine.Websocket.OutgoingQueueSize,
		},
		&cli.IntFlag{
			Name:  "rpc-queue-handlers",
			Usage: "sets rpc message handler count",
			Value: cfg.RpcEngine.Websocket.QueueHandlers,
		},
		&cli.BoolFlag{
			Name:  "queue-eng-enabled",
			Usage: "enable queue engine for rpc",
			Value: cfg.RpcEngine.Queue.Enabled,
		},
		&cli.IntFlag{
			Name:  "queue-eng-consumers",
			Usage: "sets number of consumers per topic",
			Value: cfg.RpcEngine.Queue.Consumers,
		},
		&cli.StringFlag{
			Name:  "queue-eng-driver",
			Usage: "sets the type of queue",
			Value: cfg.RpcEngine.Queue.Driver,
		},
		&cli.StringFlag{
			Name:  "queue-eng-host",
			Usage: "sets the host address for the queue",
			Value: cfg.RpcEngine.Queue.Host,
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
		&cli.StringFlag{
			Name:  "indexer-advertisement-interval",
			Usage: "sets the indexer advertisement interval using a Go time string (e.g. '1m30s')",
			Value: cfg.Node.IndexerAdvertisementInterval.String(),
		},
		&cli.BoolFlag{
			Name:  "advertise-offline-autoretrieves",
			Usage: "if set, registered autoretrieves will be advertised even if they are not currently online",
		},
		&cli.StringFlag{
			Name:  "max-price",
			Usage: "sets the max price for non-verified deals",
			Value: cfg.Deal.MaxPrice.String(),
		},
		&cli.StringFlag{
			Name:  "max-verified-price",
			Usage: "sets the max price for verified deals",
			Value: cfg.Deal.MaxVerifiedPrice.String(),
		},
	}
}

func getSetupFlags(cfg *config.Estuary) []cli.Flag {
	hDir, err := homedir.Dir()
	if err != nil {
		log.Fatalf("could not determine homedir for estuary app: %+v", err)
	}

	return []cli.Flag{
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
	}
}
