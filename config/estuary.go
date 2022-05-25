package config

import (
	"path/filepath"

	"github.com/application-research/estuary/build"
)

type Estuary struct {
	DatabaseConnString     string
	StagingDataDir         string
	ServerCacheDir         string
	DataDir                string
	ApiListen              string
	EnableAutoRetrieve     bool
	LightstepToken         string
	Hostname               string
	NodeConfig             Node
	JaegerConfig           Jaeger
	DealConfig             Deal
	ContentConfig          Content
	LowMem                 bool
	DisableFilecoinStorage bool
	Replication            int
	LoggingConfig          Logging
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
	cfg.NodeConfig.WalletDir = filepath.Join(cfg.DataDir, "estuary-wallet")
	cfg.NodeConfig.DatastoreDir = filepath.Join(cfg.DataDir, "estuary-leveldb")
	cfg.NodeConfig.Libp2pKeyFile = filepath.Join(cfg.DataDir, "estuary-peer.key")

	if cfg.NodeConfig.Blockstore == "" {
		cfg.NodeConfig.Blockstore = filepath.Join(cfg.DataDir, "estuary-blocks")
	}
	return nil
}

func NewEstuary() *Estuary {
	return &Estuary{
		DataDir:                ".",
		DatabaseConnString:     build.DefaultDatabaseValue,
		ApiListen:              ":3004",
		LightstepToken:         "",
		Hostname:               "http://localhost:3004",
		Replication:            6,
		LowMem:                 false,
		DisableFilecoinStorage: false,
		EnableAutoRetrieve:     false,

		DealConfig: Deal{
			Disable:               false,
			FailOnTransferFailure: false,
			Verified:              true,
		},

		ContentConfig: Content{
			DisableLocalAdding:  false,
			DisableGlobalAdding: false,
		},

		JaegerConfig: Jaeger{
			EnableTracing: false,
			ProviderUrl:   "http://localhost:14268/api/traces",
			SamplerRatio:  1,
		},

		LoggingConfig: Logging{
			ApiEndpointLogging: false,
		},

		NodeConfig: Node{
			AnnounceAddrs: []string{},
			ListenAddrs: []string{
				"/ip4/0.0.0.0/tcp/6744",
			},
			WriteLogDir:       "",
			HardFlushWriteLog: false,
			WriteLogTruncate:  false,
			NoBlockstoreCache: false,

			ApiURL: "wss://api.chain.love",

			BitswapConfig: BitswapConfig{
				MaxOutstandingBytesPerPeer: 5 << 20,
				TargetMessageSize:          0,
			},

			NoLimiter: true,
			LimitsConfig: Limits{
				SystemLimitConfig: SystemLimit{
					MinMemory:      1 << 30,
					MaxMemory:      10 << 30,
					MemoryFraction: .2,

					StreamsInbound:  64 << 10,
					StreamsOutbound: 128 << 10,
					Streams:         256 << 10,

					ConnsInbound:  256,
					ConnsOutbound: 256,
					Conns:         1024,

					FD: 8192,
				},
				TransientLimitConfig: TransientLimit{
					StreamsInbound:  2 << 10,
					StreamsOutbound: 4 << 10,
					Streams:         4 << 10,

					ConnsInbound:  256,
					ConnsOutbound: 256,
					Conns:         512,

					FD: 1024,
				},
			},
			ConnectionManagerConfig: ConnectionManager{
				LowWater:  2000,
				HighWater: 3000,
			},
		},
	}
}
