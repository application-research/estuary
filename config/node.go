package config

import rcmgr "github.com/libp2p/go-libp2p-resource-manager"

type Node struct {
	ListenAddrs               []string `json:"Swarm"`
	AnnounceAddrs             []string `json:"Announce"`
	EnableWebsocketListenAddr bool

	Blockstore string

	WriteLogDir       string
	HardFlushWriteLog bool
	WriteLogTruncate  bool
	NoBlockstoreCache bool
	NoLimiter         bool

	Libp2pKeyFile string

	DatastoreDir string

	WalletDir string

	BitswapConfig           BitswapConfig
	LimitsConfig            Limits
	ConnectionManagerConfig ConnectionManager

	ApiURL string
}

func (cfg *Node) GetLimiter() *rcmgr.BasicLimiter {
	lim := rcmgr.NewDefaultLimiter()
	cfg.LimitsConfig.apply(lim)
	return lim
}
