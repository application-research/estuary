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
	Node               NodeConfig
	Jaeger             JaegerConfig
	Content            ContentConfig
	Logging            LoggingConfig
	Estuary            EstuaryRemoteConfig
}

func (cfg *Shuttle) Load(filename string) error {
	return load(cfg, filename)
}

// save writes the config from `cfg` into `filename`.
func (cfg *Shuttle) Save(filename string) error {
	return save(cfg, filename)
}

func (cfg *Shuttle) Validate() error {
	if cfg.Estuary.AuthToken == "" {
		return errors.New("no auth-token configured or specified on command line")
	}

	if cfg.Estuary.Handle == "" {
		return errors.New("no handle configured or specified on command line")
	}
	return nil
}

// Sets the root of many paths
func (cfg *Shuttle) SetDataDir(ddir string) {
	cfg.StagingDataDir = updateRootDir(ddir, cfg.DataDir, cfg.StagingDataDir)
	cfg.Node.UpdateRoot(ddir, cfg.DataDir)
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

		Content: ContentConfig{
			DisableLocalAdding: false,
		},

		Jaeger: JaegerConfig{
			EnableTracing: false,
			ProviderUrl:   "http://localhost:14268/api/traces",
			SamplerRatio:  1,
		},

		Logging: LoggingConfig{
			ApiEndpointLogging: false,
		},

		Node: NodeConfig{
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
		},

		Estuary: EstuaryRemoteConfig{
			Api:       "api.estuary.tech",
			Handle:    "",
			AuthToken: "",
		},
	}

	return &cfg
}
