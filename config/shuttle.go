package config

import (
	"errors"
	"os"
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

// Sets the root of many paths
func (cfg *Shuttle) SetDataDir(ddir string) {
	cfg.StagingDataDir = updateRootDir(ddir, cfg.DataDir, cfg.StagingDataDir)
	cfg.NodeConfig.UpdateRoot(ddir, cfg.DataDir)
	cfg.DataDir = ddir
}

func NewShuttle() *Shuttle {

	pwd, _ := os.Getwd()

	listens := []string{
		"/ip4/0.0.0.0/tcp/6745",
		"/ip4/0.0.0.0/udp/6746/quic",
	}

	cfg := Shuttle{

		DataDir:            pwd,
		StagingDataDir:     filepath.Join(pwd, "staging"),
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
			ListenAddrs:       listens,
			BlockstoreDir:     filepath.Join(pwd, "blocks"),
			WriteLogDir:       "",
			HardFlushWriteLog: false,
			WriteLogTruncate:  false,
			NoBlockstoreCache: false,
			Libp2pKeyFile:     filepath.Join(pwd, "peer.key"),
			DatastoreDir:      filepath.Join(pwd, "leveldb"),
			WalletDir:         filepath.Join(pwd, "wallet"),
			AnnounceAddrs:     []string{},
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

	return &cfg
}
