package config

type NodeConfig struct {
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

	BitswapConfig BitswapConfig
}

type BitswapConfig struct {
	MaxOutstandingBytesPerPeer int64
	TargetMessageSize          int
}

func (cfg *NodeConfig) AddListener(newAddr string) {
	if !cfg.HasListener(newAddr) {
		cfg.ListenAddrs = append(cfg.ListenAddrs, newAddr)
	}
}

func (cfg *NodeConfig) HasListener(find string) bool {
	for _, addr := range cfg.ListenAddrs {
		if addr == find {
			return true
		}
	}
	return false
}

// Sets the root of many paths
func (cfg *NodeConfig) UpdateRoot(newRoot string, oldRoot string) {
	cfg.BlockstoreDir = updateRootDir(newRoot, oldRoot, cfg.BlockstoreDir)
	cfg.Libp2pKeyFile = updateRootDir(newRoot, oldRoot, cfg.Libp2pKeyFile)
	cfg.DatastoreDir = updateRootDir(newRoot, oldRoot, cfg.DatastoreDir)
	cfg.WalletDir = updateRootDir(newRoot, oldRoot, cfg.WalletDir)
	cfg.WriteLogDir = updateRootDir(newRoot, oldRoot, cfg.WriteLogDir)
}
