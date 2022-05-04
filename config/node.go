package config

import rcmgr "github.com/libp2p/go-libp2p-resource-manager"

type Node struct {
	ListenAddrs   []string `json:"Swarm"`
	AnnounceAddrs []string `json:"Announce"`

	BlockstoreDir string

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
}

type BitswapConfig struct {
	MaxOutstandingBytesPerPeer int64
	TargetMessageSize          int
}

func (cfg *Node) AddListener(newAddr string) {
	if !cfg.HasListener(newAddr) {
		cfg.ListenAddrs = append(cfg.ListenAddrs, newAddr)
	}
}

func (cfg *Node) HasListener(find string) bool {
	for _, addr := range cfg.ListenAddrs {
		if addr == find {
			return true
		}
	}
	return false
}

func (cfg *Node) GetLimiter() *rcmgr.BasicLimiter {
	lim := rcmgr.NewDefaultLimiter()
	cfg.LimitsConfig.apply(lim)
	return lim
}

// Sets the root of many paths
func (cfg *Node) UpdateRoot(newRoot string, oldRoot string) {
	cfg.BlockstoreDir = updateRootDir(newRoot, oldRoot, cfg.BlockstoreDir)
	cfg.Libp2pKeyFile = updateRootDir(newRoot, oldRoot, cfg.Libp2pKeyFile)
	cfg.DatastoreDir = updateRootDir(newRoot, oldRoot, cfg.DatastoreDir)
	cfg.WalletDir = updateRootDir(newRoot, oldRoot, cfg.WalletDir)
	cfg.WriteLogDir = updateRootDir(newRoot, oldRoot, cfg.WriteLogDir)
}
