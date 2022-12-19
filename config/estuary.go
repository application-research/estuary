package config

import (
	"errors"
	"path/filepath"
	"time"

	"golang.org/x/time/rate"

	rcmgr "github.com/libp2p/go-libp2p/p2p/host/resource-manager"

	"github.com/application-research/estuary/build"
	"github.com/application-research/estuary/constants"
	"github.com/application-research/estuary/node/modules/peering"
	"github.com/application-research/filclient"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/libp2p/go-libp2p/core/protocol"
)

type Estuary struct {
	AppVersion             string        `json:"app_version"`
	DatabaseConnString     string        `json:"database_conn_string"`
	StagingDataDir         string        `json:"staging_data_dir"`
	ServerCacheDir         string        `json:"server_cache_dir"`
	DataDir                string        `json:"data_dir"`
	ApiListen              string        `json:"api_listen"`
	LightstepToken         string        `json:"lightstep_token"`
	Hostname               string        `json:"hostname"`
	DisableAutoRetrieve    bool          `json:"enable_autoretrieve"`
	LowMem                 bool          `json:"low_mem"`
	DisableFilecoinStorage bool          `json:"disable_filecoin_storage"`
	DisableSwaggerEndpoint bool          `json:"disable_swagger_endpoint"`
	Node                   Node          `json:"node"`
	Jaeger                 Jaeger        `json:"jaeger"`
	Deal                   Deal          `json:"deal"`
	Content                Content       `json:"content"`
	Logging                Logging       `json:"logging"`
	StagingBucket          StagingBucket `json:"staging_bucket"`
	Replication            int           `json:"replication"`
	RpcEngine              RpcEngine     `json:"rpc_engine"`
	Pinning                Pinning       `json:"pinning"`
	RateLimit              rate.Limit    `json:"rate_limit"`
}

func (cfg *Estuary) Load(filename string) error {
	return load(cfg, filename)
}

// save writes the config from `cfg` into `filename`.
func (cfg *Estuary) Save(filename string) error {
	return save(cfg, filename)
}

func (cfg *Estuary) SetRequiredOptions() error {
	//TODO validate required options values - check empty strings etc

	cfg.StagingDataDir = filepath.Join(cfg.DataDir, "stagingdata")
	cfg.ServerCacheDir = filepath.Join(cfg.DataDir, "cache")
	cfg.Node.WalletDir = filepath.Join(cfg.DataDir, "estuary-wallet")
	cfg.Node.DatastoreDir = filepath.Join(cfg.DataDir, "estuary-leveldb")
	cfg.Node.Libp2pKeyFile = filepath.Join(cfg.DataDir, "estuary-peer.key")

	if cfg.Node.Blockstore == "" {
		cfg.Node.Blockstore = filepath.Join(cfg.DataDir, "estuary-blocks")
	}
	return nil
}

func (cfg *Estuary) Validate() error {
	// TODO validate more options values - check empty strings etc
	if cfg.Node.ApiURL == "" {
		return errors.New("node api url cannot be empty")
	}
	return nil
}

func NewEstuary(appVersion string) *Estuary {
	return &Estuary{
		AppVersion:             appVersion,
		DataDir:                ".",
		DatabaseConnString:     build.DefaultDatabaseValue,
		ApiListen:              ":3004",
		LightstepToken:         "",
		Hostname:               "http://localhost:3004",
		Replication:            6,
		LowMem:                 false,
		DisableFilecoinStorage: false,
		DisableSwaggerEndpoint: false,
		DisableAutoRetrieve:    true,
		RateLimit:              rate.Limit(20),

		Deal: Deal{
			IsDisabled:            false,
			FailOnTransferFailure: false,
			IsVerified:            true,
			Duration:              abi.ChainEpoch(constants.DealDuration), // Making default deal duration be three weeks less than the maximum to ensure miners who start their deals early dont run into issues
			EnabledDealProtocolsVersions: map[protocol.ID]bool{
				filclient.DealProtocolv110: true,
				filclient.DealProtocolv120: true,
			},
			MaxVerifiedPrice: constants.VerifiedDealMaxPrice,
			MaxPrice:         constants.DealMaxPrice,
		},

		Content: Content{
			DisableLocalAdding:  false,
			DisableGlobalAdding: false,
			MaxSize:             constants.MaxDealContentSize,
			MinSize:             constants.MinDealContentSize,
		},

		StagingBucket: StagingBucket{
			Enabled:           true,
			AggregateInterval: time.Minute * 5, // aggregate staging buckets every 5 minutes
			CreationInterval:  time.Minute * 2, // aggregate staging buckets every 2 minutes
		},

		Pinning: Pinning{
			RetryWorker: RetryWorker{
				Interval:               1 * time.Hour, // check every 1hr
				BatchSelectionLimit:    1000,
				BatchSelectionDuration: time.Hour * 24 * 30 * 6, // select pins from 6 months ago only
			},
		},

		Jaeger: Jaeger{
			EnableTracing: false,
			ProviderUrl:   "http://localhost:14268/api/traces",
			SamplerRatio:  1,
		},

		Logging: Logging{
			ApiEndpointLogging: false,
		},

		Node: Node{
			AnnounceAddrs: []string{},
			ListenAddrs: []string{
				"/ip4/0.0.0.0/tcp/6744",
			},
			PeeringPeers:      peering.DefaultPeers,
			WriteLogDir:       "",
			HardFlushWriteLog: false,
			WriteLogTruncate:  false,
			NoBlockstoreCache: false,

			IndexerURL:                   constants.DefaultIndexerURL,
			IndexerAdvertisementInterval: time.Minute,

			ApiURL: "wss://api.chain.love",

			Bitswap: Bitswap{
				MaxOutstandingBytesPerPeer: 5 << 20,
				TargetMessageSize:          0,
			},

			NoLimiter: true,
			Limits: rcmgr.ScalingLimitConfig{
				SystemBaseLimit: rcmgr.BaseLimit{
					Memory:          10 << 30,
					StreamsInbound:  64 << 10,
					StreamsOutbound: 128 << 10,
					Streams:         256 << 10,
					ConnsInbound:    256,
					ConnsOutbound:   256,
					Conns:           1024,
					FD:              8192,
				},
				TransientBaseLimit: rcmgr.BaseLimit{
					Memory:          4096,
					StreamsInbound:  2 << 10,
					StreamsOutbound: 4 << 10,
					Streams:         4 << 10,
					ConnsInbound:    256,
					ConnsOutbound:   256,
					Conns:           512,
					FD:              1024,
				},
				// TODO: remove after https://github.com/libp2p/go-libp2p/pull/1878 is released
				ServicePeerBaseLimit: rcmgr.BaseLimit{
					StreamsInbound:  128,
					StreamsOutbound: 256,
					Streams:         256,
					Memory:          16 << 20,
				},
				ServicePeerLimitIncrease: rcmgr.BaseLimitIncrease{
					StreamsInbound:  4,
					StreamsOutbound: 8,
					Streams:         8,
					Memory:          4 << 20,
				},
			},
			ConnectionManager: ConnectionManager{
				LowWater:  2000,
				HighWater: 3000,
			},
			Libp2pThrottleLimit: 100,
		},
		RpcEngine: RpcEngine{
			Websocket: WebsocketEngine{
				IncomingQueueSize: 100000,
				OutgoingQueueSize: 100000,
				QueueHandlers:     30,
			},
			Queue: QueueEngine{
				Host:      "",
				Enabled:   false,
				Consumers: 5,
				Driver:    "nsq",
			},
		},
	}
}
