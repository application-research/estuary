package config

import (
	"context"

	"github.com/ipfs/go-cid"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
)

type Config struct {
	ListenAddrs   []string
	AnnounceAddrs []string

	Blockstore string

	WriteLog          string
	HardFlushWriteLog bool
	WriteLogTruncate  bool
	NoBlockstoreCache bool

	Libp2pKeyFile string

	Datastore string

	WalletDir string

	BitswapConfig BitswapConfig

	BlockstoreWrap func(blockstore.Blockstore) (blockstore.Blockstore, error)

	KeyProviderFunc func(context.Context) (<-chan cid.Cid, error)
}

type BitswapConfig struct {
	MaxOutstandingBytesPerPeer int64
	TargetMessageSize          int
}
