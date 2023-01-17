package config

import (
	"errors"
	"golang.org/x/time/rate"
	"path/filepath"

	rcmgr "github.com/libp2p/go-libp2p/p2p/host/resource-manager"

	"github.com/application-research/estuary/constants"
	"github.com/application-research/estuary/node/modules/peering"
)

const DefaultWebsocketAddr = "/ip4/0.0.0.0/tcp/6747/ws"

type EstuaryRemote struct {
	Api       string `json:"api"`
	Handle    string `json:"handle"`
	AuthToken string `json:"auth_token"`
}

type Shuttle struct {
	AppVersion         string        `json:"app_version"`
	DatabaseConnString string        `json:"database_conn_string"`
	StagingDataDir     string        `json:"staging_data_dir"`
	DataDir            string        `json:"data_dir"`
	ApiListen          string        `json:"api_listen"`
	Hostname           string        `json:"hostname"`
	Private            bool          `json:"private"`
	Dev                bool          `json:"dev"`
	NoReloadPinQueue   bool          `json:"no_reload_pin_queue"`
	RateLimit          rate.Limit    `json:"rate_limit"`
	Node               Node          `json:"node"`
	Jaeger             Jaeger        `json:"jaeger"`
	Content            Content       `json:"content"`
	Logging            Logging       `json:"logging"`
	EstuaryRemote      EstuaryRemote `json:"estuary_remote"`
	RpcEngine          RpcEngine     `json:"rpc_engine"`
}

func (cfg *Shuttle) Load(filename string) error {
	return load(cfg, filename)
}

// save writes the config from `cfg` into `filename`.
func (cfg *Shuttle) Save(filename string) error {
	return save(cfg, filename)
}

func (cfg *Shuttle) Validate() error {
	if cfg.EstuaryRemote.AuthToken == "" {
		return errors.New("no auth-token configured or specified on command line")
	}

	if cfg.EstuaryRemote.Handle == "" {
		return errors.New("no handle configured or specified on command line")
	}
	return nil
}

func (cfg *Shuttle) SetRequiredOptions() error {
	//TODO validate flags values - empty strings etc

	cfg.StagingDataDir = filepath.Join(cfg.DataDir, "staging")
	cfg.Node.WalletDir = filepath.Join(cfg.DataDir, "wallet")
	cfg.Node.DatastoreDir = filepath.Join(cfg.DataDir, "leveldb")
	cfg.Node.Libp2pKeyFile = filepath.Join(cfg.DataDir, "peer.key")

	if cfg.Node.Blockstore == "" {
		cfg.Node.Blockstore = filepath.Join(cfg.DataDir, "blocks")
	} else if cfg.Node.Blockstore[0] != '/' && cfg.Node.Blockstore[0] != ':' {
		cfg.Node.Blockstore = filepath.Join(cfg.DataDir, cfg.Node.Blockstore)
	}
	return nil
}

func NewShuttle(appVersion string) *Shuttle {
	return &Shuttle{
		AppVersion:         appVersion,
		DataDir:            ".",
		DatabaseConnString: "sqlite=estuary-shuttle.db",
		ApiListen:          ":3005",
		Hostname:           "",
		Private:            false,
		Dev:                false,
		NoReloadPinQueue:   false,
		RateLimit:          rate.Limit(200),

		Content: Content{
			DisableLocalAdding: false,
			MaxSize:            constants.MaxDealContentSize,
			MinSize:            constants.MinDealContentSize,
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
				"/ip4/0.0.0.0/tcp/6745",
				"/ip4/0.0.0.0/udp/6746/quic",
			},
			PeeringPeers:              peering.DefaultPeers,
			EnableWebsocketListenAddr: false,

			WriteLogDir:       "",
			HardFlushWriteLog: false,
			WriteLogTruncate:  false,
			NoBlockstoreCache: false,

			ApiURL: "wss://api.chain.love",

			Bitswap: Bitswap{
				MaxOutstandingBytesPerPeer: 5 << 20,
				TargetMessageSize:          16 << 10,
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

		EstuaryRemote: EstuaryRemote{
			Api:       "api.estuary.tech",
			Handle:    "",
			AuthToken: "",
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
