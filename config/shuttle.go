package config

import (
	"errors"
	"path/filepath"
)

const DefaultWebsocketAddr = "/ip4/0.0.0.0/tcp/6747/ws"

type EstuaryRemoteConfig struct {
	Api       string
	Handle    string
	AuthToken string
}

type Shuttle struct {
	DatabaseConnString string
	StagingDataDir     string
	DataDir            string
	ApiListen          string
	AutoRetrieve       bool
	Hostname           string
	Private            bool
	Dev                bool
	NoReloadPinQueue   bool
	NodeConfig         Node
	JaegerConfig       Jaeger
	ContentConfig      Content
	LoggingConfig      Logging
	EstuaryConfig      EstuaryRemoteConfig
}

func (cfg *Shuttle) Load(filename string) error {
	return load(cfg, filename)
}

// save writes the config from `cfg` into `filename`.
func (cfg *Shuttle) Save(filename string) error {
	return save(cfg, filename)
}

func (cfg *Shuttle) Validate() error {
	if cfg.EstuaryConfig.AuthToken == "" {
		return errors.New("no auth-token configured or specified on command line")
	}

	if cfg.EstuaryConfig.Handle == "" {
		return errors.New("no handle configured or specified on command line")
	}
	return nil
}

func (cfg *Shuttle) SetRequiredOptions() error {
	//TODO validate flags values - empty strings etc

	cfg.StagingDataDir = filepath.Join(cfg.DataDir, "staging")
	cfg.NodeConfig.WalletDir = filepath.Join(cfg.DataDir, "wallet")
	cfg.NodeConfig.DatastoreDir = filepath.Join(cfg.DataDir, "leveldb")
	cfg.NodeConfig.Libp2pKeyFile = filepath.Join(cfg.DataDir, "peer.key")

	if cfg.NodeConfig.Blockstore == "" {
		cfg.NodeConfig.Blockstore = filepath.Join(cfg.DataDir, "blocks")
	} else if cfg.NodeConfig.Blockstore[0] != '/' && cfg.NodeConfig.Blockstore[0] != ':' {
		cfg.NodeConfig.Blockstore = filepath.Join(cfg.DataDir, cfg.NodeConfig.Blockstore)
	}
	return nil
}

func NewShuttle() *Shuttle {
	return &Shuttle{
		DataDir:            ".",
		DatabaseConnString: "sqlite=estuary-shuttle.db",
		ApiListen:          ":3005",
		Hostname:           "",
		Private:            false,
		Dev:                false,
		NoReloadPinQueue:   false,

		ContentConfig: Content{
			DisableLocalAdding: false,
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
				"/ip4/0.0.0.0/tcp/6745",
				"/ip4/0.0.0.0/udp/6746/quic",
			},
			EnableWebsocketListenAddr: false,

			WriteLogDir:       "",
			HardFlushWriteLog: false,
			WriteLogTruncate:  false,
			NoBlockstoreCache: false,

			BitswapConfig: BitswapConfig{
				MaxOutstandingBytesPerPeer: 5 << 20,
				TargetMessageSize:          16 << 10,
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

		EstuaryConfig: EstuaryRemoteConfig{
			Api:       "api.estuary.tech",
			Handle:    "",
			AuthToken: "",
		},
	}
}
