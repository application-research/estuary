package config

import (
	"github.com/application-research/estuary/node/modules/peering"
	rcmgr "github.com/libp2p/go-libp2p/p2p/host/resource-manager"
)

type Node struct {
	ListenAddrs               []string              `json:"listen_addrs"`
	AnnounceAddrs             []string              `json:"announce_addrs"`
	PeeringPeers              []peering.PeeringPeer `json:"peering_peers"`
	IndexerTickInterval       int                   `json:"indexer_tick_interval"`
	EnableWebsocketListenAddr bool                  `json:"enable_websocket_listen_addr"`
	HardFlushWriteLog         bool                  `json:"hard_flush_write_log"`
	WriteLogTruncate          bool                  `json:"write_log_truncate"`
	NoBlockstoreCache         bool                  `json:"no_blockstore_cache"`
	NoLimiter                 bool                  `json:"no_limiter"`
	IndexerURL                string                `json:"indexer_url"`
	Blockstore                string                `json:"blockstore"`
	WriteLogDir               string                `json:"write_log_dir"`
	Libp2pKeyFile             string                `json:"libp2p_key_file"`
	DatastoreDir              string                `json:"datastore_dir"`
	WalletDir                 string                `json:"wallet_dir"`
	ApiURL                    string                `json:"api_url"`
	Bitswap                   Bitswap               `json:"bitswap"`
	Limits                    Limits                `json:"limits"`
	ConnectionManager         ConnectionManager     `json:"connection_manager"`
}

func (cfg *Node) GetLimiter() *rcmgr.BasicLimiter {
	lim := rcmgr.NewDefaultLimiter()
	cfg.Limits.apply(lim)
	return lim
}
