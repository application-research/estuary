package config

import (
	"os"
	"path/filepath"

	"github.com/application-research/estuary/build"
)

type Estuary struct {
	Database       string
	StagingData    string
	DataDir        string
	ApiListen      string
	AutoRetrieve   bool
	LightstepToken string
	Hostname       string
	Node           Config
	Jaeger         JaegerConfig
	Deals          DealsConfig
	Content        ContentConfig
	LowMem         bool
	Replication    int
	Logging        LoggingConfig
}

func (cfg *Estuary) Load(filename string) error {
	return load(cfg, filename)
}

// save writes the config from `cfg` into `filename`.
func (cfg *Estuary) Save(filename string) error {
	return save(cfg, filename)
}

// Sets the root of many paths
func (cfg *Estuary) SetDataDir(ddir string) {
	cfg.StagingData = updateRootDir(ddir, cfg.DataDir, cfg.StagingData)
	cfg.Node.UpdateRoot(ddir, cfg.DataDir)
	cfg.DataDir = ddir
}

func NewEstuary() *Estuary {

	pwd, _ := os.Getwd()

	cfg := Estuary{
		DataDir:        pwd,
		StagingData:    filepath.Join(pwd, "stagingdata"),
		Database:       build.DefaultDatabaseValue,
		ApiListen:      ":3004",
		LightstepToken: "",
		Hostname:       "http://localhost:3004",
		Replication:    6,
		LowMem:         false,

		Deals: DealsConfig{
			NoStorageCron:         false,
			Disable:               false,
			FailOnTransferFailure: false,
		},

		Content: ContentConfig{
			Disable:       false,
			GlobalDisable: false,
		},

		Jaeger: JaegerConfig{
			JaegerTracing:      false,
			JaegerProviderUrl:  "http://localhost:14268/api/traces",
			JaegerSamplerRatio: 1,
		},

		Logging: LoggingConfig{
			ApiEndpointLogging: false,
		},

		Node: Config{
			ListenAddrs: []string{
				"/ip4/0.0.0.0/tcp/6744",
			},
			Blockstore:       filepath.Join(pwd, "estuary-blocks"),
			Libp2pKeyFile:    filepath.Join(pwd, "estuary-peer.key"),
			Datastore:        filepath.Join(pwd, "estuary-leveldb"),
			WalletDir:        filepath.Join(pwd, "estuary-wallet"),
			WriteLog:         "",
			WriteLogTruncate: false,
			NoLimiter:        true,
		},
	}

	return &cfg
}
