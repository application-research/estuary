package config

import (
	"path/filepath"
	"time"

	"github.com/application-research/estuary/node/modules/peering"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/application-research/filclient"
	"github.com/libp2p/go-libp2p-core/protocol"
	"github.com/application-research/estuary/build"
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
	EnableAutoRetrieve     bool          `json:"enable_autoretrieve"`
	LowMem                 bool          `json:"low_mem"`
	DisableFilecoinStorage bool          `json:"disable_filecoin_storage"`
	Node                   Node          `json:"node"`
	Jaeger                 Jaeger        `json:"jaeger"`
	Deal                   Deal          `json:"deal"`
	Content                Content       `json:"content"`
	Logging                Logging       `json:"logging"`
	FilClient              FilClient     `json:"fil_client"`
	StagingBucket          StagingBucket `json:"staging_bucket"`
	ShuttleMessageHandlers int           `json:"shuttle_message_Handlers"`
	Replication            int           `json:"replication"`
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
		EnableAutoRetrieve:     false,

		Deal: Deal{
			IsDisabled:            false,
			FailOnTransferFailure: false,
			IsVerified:            true,
			Duration:              abi.ChainEpoch(1555200 - (2880 * 21)), // Making default deal duration be three weeks less than the maximum to ensure miners who start their deals early dont run into issues
			EnabledDealProtocolsVersions: map[protocol.ID]bool{
				filclient.DealProtocolv110: true,
				filclient.DealProtocolv120: true,
			},
		},

		Content: Content{
			DisableLocalAdding:  false,
			DisableGlobalAdding: false,
		},

		StagingBucket: StagingBucket{
			Enabled:                 true,
			MaxLifeTime:             time.Hour * 8,
			MaxContentAge:           time.Hour * 24 * 7,
			MaxItems:                10000,
			MaxSize:                 int64((abi.PaddedPieceSize(16<<30).Unpadded() * 9) / 10),                // 14.29 Gib
			MinSize:                 int64(int64((abi.PaddedPieceSize(16<<30).Unpadded()*9)/10) - (1 << 30)), // 13.29 GiB
			KeepAlive:               time.Minute * 40,
			MinDealSize:             256 << 20,                                               //0.25 Gib
			IndividualDealThreshold: int64((abi.PaddedPieceSize(4<<30).Unpadded() * 9) / 10), // 90% of the unpadded data size for a 4GB piece, the 10% gap is to accommodate car file packing overhead, can probably do this better
			AggregateInterval:       time.Minute * 5,                                         // aggregate staging buckets every 5 minutes
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

			IndexerURL:          "https://cid.contact",
			IndexerTickInterval: 720,

			ApiURL: "wss://api.chain.love",

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
		ShuttleMessageHandlers: 30,
	}
}
