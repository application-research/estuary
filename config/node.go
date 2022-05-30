package config

import rcmgr "github.com/libp2p/go-libp2p-resource-manager"

type Node struct {
	ListenAddrs               []string `json:"listen_addrs"`
	AnnounceAddrs             []string `json:"announce_addrs"`
	EnableWebsocketListenAddr bool     `json:"enable_websocket_listen_addr"`

	Blockstore string `json:"blockstore"`

	WriteLogDir       string `json:"write_log_dir"`
	HardFlushWriteLog bool   `json:"hard_flush_write_log"`
	WriteLogTruncate  bool   `json:"write_log_truncate"`
	NoBlockstoreCache bool   `json:"no_blockstore_cache"`
	NoLimiter         bool   `json:"no_limiter"`

	Libp2pKeyFile string `json:"libp2p_key_file"`

	DatastoreDir string `json:"datastore_dir"`

	WalletDir string `json:"wallet_dir"`

	Bitswap           Bitswap           `json:"bitswap"`
	Limits            Limits            `json:"limits"`
	ConnectionManager ConnectionManager `json:"connection_manager"`
}

type Bitswap struct {
	MaxOutstandingBytesPerPeer int64 `json:"max_outstanding_bytes_per_peer"`
	TargetMessageSize          int   `json:"target_message_size"`
}

func (cfg *Node) GetLimiter() *rcmgr.BasicLimiter {
	lim := rcmgr.NewDefaultLimiter()
	cfg.Limits.apply(lim)
	return lim
}
