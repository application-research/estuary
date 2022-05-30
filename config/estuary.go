package config

import (
	"path/filepath"

	"github.com/application-research/estuary/build"
)

type Estuary struct {
	DatabaseConnString     string  `json:"database_conn_string"`
	StagingDataDir         string  `json:"staging_data_dir"`
	ServerCacheDir         string  `json:"server_cache_dir"`
	DataDir                string  `json:"data_dir"`
	ApiListen              string  `json:"api_listen"`
	EnableAutoRetrieve     bool    `json:"enable_autoretrieve"`
	LightstepToken         string  `json:"lightstep_token"`
	Hostname               string  `json:"hostname"`
	Node                   Node    `json:"node"`
	Jaeger                 Jaeger  `json:"jaeger"`
	Deal                   Deal    `json:"deal"`
	Content                Content `json:"content"`
	LowMem                 bool    `json:"low_mem"`
	DisableFilecoinStorage bool    `json:"disable_filecoin_storage"`
	Replication            int     `json:"replication"`
	Logging                Logging `json:"logging"`
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

		Deal: Deal{
			Disable:               false,
			FailOnTransferFailure: false,
			Verified:              true,
		},

		Content: Content{
			DisableLocalAdding:  false,
			DisableGlobalAdding: false,
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
			ListenAddrs: []string{
				"/ip4/0.0.0.0/tcp/6744",
			},
			WriteLogDir:       "",
			HardFlushWriteLog: false,
			WriteLogTruncate:  false,
			NoBlockstoreCache: false,

			Bitswap: Bitswap{
				MaxOutstandingBytesPerPeer: 5 << 20,
				TargetMessageSize:          0,
			},

			NoLimiter: true,
			Limits: Limits{
				SystemLimit: SystemLimit{
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
				TransientLimit: TransientLimit{
					StreamsInbound:  2 << 10,
					StreamsOutbound: 4 << 10,
					Streams:         4 << 10,

					ConnsInbound:  256,
					ConnsOutbound: 256,
					Conns:         512,

					FD: 1024,
				},
			},
			ConnectionManager: ConnectionManager{
				LowWater:  2000,
				HighWater: 3000,
			},
		},
	}
}
