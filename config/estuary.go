package config

import (
	"os"
	"path/filepath"

	"github.com/application-research/estuary/build"
)

type Estuary struct {
	DatabaseConnString     string
	StagingDataDir         string
	DataDir                string
	ApiListen              string
	AutoRetrieve           bool
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

// Sets the root of many paths
func (cfg *Estuary) SetDataDir(ddir string) {
	cfg.StagingDataDir = updateRootDir(ddir, cfg.DataDir, cfg.StagingDataDir)
	cfg.NodeConfig.UpdateRoot(ddir, cfg.DataDir)
	cfg.DataDir = ddir
}

func NewEstuary() *Estuary {

	pwd, _ := os.Getwd()

	cfg := Estuary{
		DataDir:                pwd,
		StagingDataDir:         filepath.Join(pwd, "stagingdata"),
		DatabaseConnString:     build.DefaultDatabaseValue,
		ApiListen:              ":3004",
		LightstepToken:         "",
		Hostname:               "http://localhost:3004",
		Replication:            6,
		LowMem:                 false,
		DisableFilecoinStorage: false,

		DealConfig: Deal{
			Disable:               false,
			FailOnTransferFailure: false,
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
			ListenAddrs: []string{
				"/ip4/0.0.0.0/tcp/6744",
			},
			BlockstoreDir:    filepath.Join(pwd, "estuary-blocks"),
			Libp2pKeyFile:    filepath.Join(pwd, "estuary-peer.key"),
			DatastoreDir:     filepath.Join(pwd, "estuary-leveldb"),
			WalletDir:        filepath.Join(pwd, "estuary-wallet"),
			WriteLogDir:      "",
			WriteLogTruncate: false,
			NoLimiter:        true,
		},
	}

	return &cfg
}
