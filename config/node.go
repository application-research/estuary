package config

import (
	"encoding/json"
	"github.com/application-research/estuary/node/modules/peering"
	rcmgr "github.com/libp2p/go-libp2p-resource-manager"
)

type Node struct {
	ListenAddrs               []string              `json:"listen_addrs"`
	AnnounceAddrs             []string              `json:"announce_addrs"`
	PeeringPeers              []peering.PeeringPeer `json:"peering_peers"`
	EnableWebsocketListenAddr bool                  `json:"enable_websocket_listen_addr"`

	Blockstore string `json:"blockstore"`

	WriteLogDir       string `json:"write_log_dir"`
	HardFlushWriteLog bool   `json:"hard_flush_write_log"`
	WriteLogTruncate  bool   `json:"write_log_truncate"`
	NoBlockstoreCache bool   `json:"no_blockstore_cache"`
	NoLimiter         bool   `json:"no_limiter"`

	Libp2pKeyFile string `json:"libp2p_key_file"`

	DatastoreDir string `json:"datastore_dir"`

	WalletDir string `json:"wallet_dir"`

	ApiURL            string            `json:"api_url"`
	Bitswap           Bitswap           `json:"bitswap"`
	Limits            Limits            `json:"limits"`
	ConnectionManager ConnectionManager `json:"connection_manager"`
}

func (cfg *Node) GetPeeringPeersStr() string {
	out, _ := json.Marshal(cfg.PeeringPeers)
	return string(out)
}

func (cfg *Node) GetLimiter() *rcmgr.BasicLimiter {
	lim := rcmgr.NewDefaultLimiter()
	cfg.Limits.apply(lim)
	return lim
}
