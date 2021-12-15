package node

import (
	"context"
	crand "crypto/rand"
	"fmt"
	"io/ioutil"
	"os"
	"strings"

	migratebs "github.com/application-research/estuary/util/migratebs"
	"github.com/application-research/filclient/keystore"
	autobatch "github.com/application-research/go-bs-autobatch"
	lmdb "github.com/filecoin-project/go-bs-lmdb"
	badgerbs "github.com/filecoin-project/lotus/blockstore/badger"
	"github.com/filecoin-project/lotus/chain/types"
	"github.com/filecoin-project/lotus/chain/wallet"
	"github.com/ipfs/go-bitswap"
	bsnet "github.com/ipfs/go-bitswap/network"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	nsds "github.com/ipfs/go-datastore/namespace"
	flatfs "github.com/ipfs/go-ds-flatfs"
	levelds "github.com/ipfs/go-ds-leveldb"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	"github.com/ipfs/go-ipfs-provider/batched"
	"github.com/ipfs/go-ipfs-provider/queue"
	logging "github.com/ipfs/go-log/v2"
	metri "github.com/ipfs/go-metrics-interface"
	mprome "github.com/ipfs/go-metrics-prometheus"
	"github.com/libp2p/go-libp2p"
	connmgr "github.com/libp2p/go-libp2p-connmgr"
	"github.com/libp2p/go-libp2p-core/crypto"
	"github.com/libp2p/go-libp2p-core/host"
	metrics "github.com/libp2p/go-libp2p-core/metrics"
	"github.com/libp2p/go-libp2p-core/peer"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	"github.com/libp2p/go-libp2p-kad-dht/fullrt"
	record "github.com/libp2p/go-libp2p-record"
	"github.com/multiformats/go-multiaddr"
	bsm "github.com/whyrusleeping/go-bs-measure"
	"golang.org/x/xerrors"
)

var log = logging.Logger("est-node")

var bootstrappers = []string{
	"/dnsaddr/bootstrap.libp2p.io/p2p/QmNnooDu7bfjPFoTZYxMNLWUQJyrVwtbZg5gBMjTezGAJN",
	"/dnsaddr/bootstrap.libp2p.io/p2p/QmQCU2EcMqAqQPR2i9bChDtGNJchTbq5TbXJJ16u19uLTa",
	"/dnsaddr/bootstrap.libp2p.io/p2p/QmbLHAnMoJPWSCR5Zhtx6BHJX9KiKNN6tpvbUcqanj75Nb",
	"/dnsaddr/bootstrap.libp2p.io/p2p/QmcZf59bWwK5XFi76CZX8cbJ4BhTzzA3gU1ZjYZcYW3dwt",
}

var BootstrapPeers []peer.AddrInfo

func init() {
	if err := mprome.Inject(); err != nil {
		panic(err)
	}

	for _, bsp := range bootstrappers {
		ma, err := multiaddr.NewMultiaddr(bsp)
		if err != nil {
			log.Errorf("failed to parse bootstrap address: ", err)
			continue
		}

		ai, err := peer.AddrInfoFromP2pAddr(ma)
		if err != nil {
			log.Errorf("failed to create address info: ", err)
			continue
		}

		BootstrapPeers = append(BootstrapPeers, *ai)
	}
}

type EstuaryBlockstore interface {
	blockstore.Blockstore
	DeleteMany(context.Context, []cid.Cid) error
}

type Node struct {
	Dht      *dht.IpfsDHT
	Provider *batched.BatchProvidingSystem
	FullRT   *fullrt.FullRT
	FilDht   *dht.IpfsDHT
	Host     host.Host

	// Set for gathering disk usage

	StorageDir string
	//Lmdb      *lmdb.Blockstore
	Datastore datastore.Batching

	Blockstore      blockstore.Blockstore
	Bitswap         *bitswap.Bitswap
	NotifBlockstore *NotifyBlockstore

	Wallet *wallet.LocalWallet

	Bwc *metrics.BandwidthCounter

	Config *Config
}

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

func Setup(ctx context.Context, cfg *Config) (*Node, error) {
	peerkey, err := loadOrInitPeerKey(cfg.Libp2pKeyFile)
	if err != nil {
		return nil, err
	}

	ds, err := levelds.NewDatastore(cfg.Datastore, nil)
	if err != nil {
		return nil, err
	}

	bwc := metrics.NewBandwidthCounter()

	cmgr, err := connmgr.NewConnManager(2000, 3000)
	if err != nil {
		return nil, err
	}
	opts := []libp2p.Option{
		libp2p.ListenAddrStrings(cfg.ListenAddrs...),
		libp2p.NATPortMap(),
		libp2p.ConnectionManager(cmgr),
		libp2p.Identity(peerkey),
		libp2p.BandwidthReporter(bwc),
		libp2p.DefaultTransports,
	}

	if len(cfg.AnnounceAddrs) > 0 {
		var addrs []multiaddr.Multiaddr
		for _, anna := range cfg.AnnounceAddrs {
			a, err := multiaddr.NewMultiaddr(anna)
			if err != nil {
				return nil, fmt.Errorf("failed to parse announce addr: %w", err)
			}
			addrs = append(addrs, a)
		}
		opts = append(opts, libp2p.AddrsFactory(func([]multiaddr.Multiaddr) []multiaddr.Multiaddr {
			return addrs
		}))
	}

	h, err := libp2p.New(opts...)
	if err != nil {
		return nil, err
	}

	dhtopts := fullrt.DHTOption(
		//dht.Validator(in.Validator),
		dht.Datastore(ds),
		dht.BootstrapPeers(BootstrapPeers...),
		dht.BucketSize(20),
	)

	frt, err := fullrt.NewFullRT(h, dht.DefaultPrefix, dhtopts)
	if err != nil {
		return nil, xerrors.Errorf("constructing fullrt: %w", err)
	}

	ipfsdht, err := dht.New(ctx, h, dht.Datastore(ds))
	if err != nil {
		return nil, xerrors.Errorf("constructing dht: %w", err)
	}

	filopts := []dht.Option{dht.Mode(dht.ModeAuto),
		dht.Datastore(nsds.Wrap(ds, datastore.NewKey("fildht"))),
		dht.Validator(record.NamespacedValidator{
			"pk": record.PublicKeyValidator{},
		}),
		dht.ProtocolPrefix("/fil/kad/testnetnet"),
		dht.QueryFilter(dht.PublicQueryFilter),
		dht.RoutingTableFilter(dht.PublicRoutingTableFilter),
		dht.DisableProviders(),
		dht.DisableValues()}
	fildht, err := dht.New(ctx, h, filopts...)
	if err != nil {
		return nil, err
	}

	mbs, stordir, err := loadBlockstore(cfg.Blockstore, cfg.WriteLog, cfg.HardFlushWriteLog, cfg.WriteLogTruncate, cfg.NoBlockstoreCache)
	if err != nil {
		return nil, err
	}

	var blkst blockstore.Blockstore = mbs
	if cfg.BlockstoreWrap != nil {
		wrapper, err := cfg.BlockstoreWrap(blkst)
		if err != nil {
			return nil, err
		}
		blkst = wrapper
	}

	bsnet := bsnet.NewFromIpfsHost(h, frt)

	peerwork := cfg.BitswapConfig.MaxOutstandingBytesPerPeer
	if peerwork == 0 {
		peerwork = 5 << 20
	}

	bsopts := []bitswap.Option{
		bitswap.EngineBlockstoreWorkerCount(600),
		bitswap.TaskWorkerCount(600),
		bitswap.MaxOutstandingBytesPerPeer(int(peerwork)),
	}

	if tms := cfg.BitswapConfig.TargetMessageSize; tms != 0 {
		bsopts = append(bsopts, bitswap.WithTargetMessageSize(tms))
	}

	bsctx := metri.CtxScope(ctx, "estuary.exch")
	bswap := bitswap.New(bsctx, bsnet, blkst, bsopts...)

	wallet, err := setupWallet(cfg.WalletDir)
	if err != nil {
		return nil, err
	}

	provq, err := queue.NewQueue(context.Background(), "provq", ds)
	if err != nil {
		return nil, err
	}

	prov, err := batched.New(frt, provq,
		batched.KeyProvider(cfg.KeyProviderFunc),
		batched.Datastore(ds),
	)
	if err != nil {
		return nil, xerrors.Errorf("setup batched provider: %w", err)
	}

	prov.Run() // TODO: call close at some point

	return &Node{
		Dht:        ipfsdht,
		FilDht:     fildht,
		FullRT:     frt,
		Provider:   prov,
		Host:       h,
		Blockstore: mbs,
		//Lmdb:       lmdbs,
		Datastore:  ds,
		Bitswap:    bswap.(*bitswap.Bitswap),
		Wallet:     wallet,
		Bwc:        bwc,
		Config:     cfg,
		StorageDir: stordir,
	}, nil
}

func parseBsCfg(bscfg string) (string, []string, string, error) {
	if bscfg[0] != ':' {
		return "", nil, "", fmt.Errorf("cfg must start with colon")
	}

	var inParen bool
	var parenStart int
	var parenEnd int
	var end int
	for i := 1; i < len(bscfg); i++ {
		if inParen {
			if bscfg[i] == ')' {
				inParen = false
				parenEnd = i
			}
			continue
		}

		if bscfg[i] == '(' {
			inParen = true
			parenStart = i
		}

		if bscfg[i] == ':' {
			end = i
			break
		}
	}

	if parenStart == 0 {
		return bscfg[1:end], nil, bscfg[end+1:], nil
	}

	t := bscfg[1:parenStart]
	params := strings.Split(bscfg[parenStart+1:parenEnd], ",")

	return t, params, bscfg[end+1:], nil
}

/* format:
:lmdb:/path/to/thing
*/
func constructBlockstore(bscfg string) (EstuaryBlockstore, string, error) {
	if !strings.HasPrefix(bscfg, ":") {
		lmdbs, err := lmdb.Open(&lmdb.Options{
			Path:   bscfg,
			NoSync: true,
		})
		if err != nil {
			return nil, "", err
		}
		return lmdbs, bscfg, nil
	}

	spec, params, path, err := parseBsCfg(bscfg)
	if err != nil {
		return nil, "", err
	}

	switch spec {
	case "lmdb":
		lmdbs, err := lmdb.Open(&lmdb.Options{
			Path:   path,
			NoSync: true,
		})
		if err != nil {
			return nil, path, err
		}
		return lmdbs, "", nil
	case "flatfs":
		if len(params) > 0 {
			return nil, "", fmt.Errorf("flatfs params not yet supported")
		}
		sf, err := flatfs.ParseShardFunc("/repo/flatfs/shard/v1/next-to-last/3")
		if err != nil {
			return nil, "", err
		}

		ds, err := flatfs.CreateOrOpen(path, sf, false)
		if err != nil {
			return nil, "", err
		}

		return &deleteManyWrap{blockstore.NewBlockstoreNoPrefix(ds)}, path, nil
	case "migrate":
		if len(params) != 2 {
			return nil, "", fmt.Errorf("migrate blockstore requires two params (%d given)", len(params))
		}

		from, _, err := constructBlockstore(params[0])
		if err != nil {
			return nil, "", fmt.Errorf("failed to construct source blockstore for migration: %w", err)
		}

		to, destPath, err := constructBlockstore(params[1])
		if err != nil {
			return nil, "", fmt.Errorf("failed to construct dest blockstore for migration: %w", err)
		}

		mgbs, err := migratebs.NewBlockstore(from, to, true)
		if err != nil {
			return nil, "", err
		}

		return mgbs, destPath, nil
	default:
		return nil, "", fmt.Errorf("unrecognized blockstore spec: %q", spec)
	}
}

func loadBlockstore(bscfg string, wal string, flush, walTruncate, nocache bool) (blockstore.Blockstore, string, error) {
	bstore, dir, err := constructBlockstore(bscfg)
	if err != nil {
		return nil, "", err
	}

	if wal != "" {
		opts := badgerbs.DefaultOptions(wal)
		opts.Truncate = walTruncate

		writelog, err := badgerbs.Open(opts)
		if err != nil {
			return nil, "", err
		}

		ab, err := autobatch.NewBlockstore(bstore, writelog, 200, 200, flush)
		if err != nil {
			return nil, "", err
		}

		if flush {
			if err := ab.Flush(context.Background()); err != nil {
				return nil, "", err
			}
		}

		if walTruncate {
			return nil, "", fmt.Errorf("truncation and full flush complete, halting execution")
		}

		bstore = ab
	}

	ctx := metri.CtxScope(context.TODO(), "estuary.bstore")

	bstore = bsm.New("estuary.blks.base", bstore)

	if !nocache {
		cbstore, err := blockstore.CachedBlockstore(ctx, bstore, blockstore.CacheOpts{
			//HasBloomFilterSize:   512 << 20,
			//HasBloomFilterHashes: 7,
			HasARCCacheSize: 8 << 20,
		})
		if err != nil {
			return nil, "", err
		}
		bstore = &deleteManyWrap{cbstore}
	}

	notifbs := NewNotifBs(bstore)
	mbs := bsm.New("estuary.repo", notifbs)

	var blkst blockstore.Blockstore = mbs

	return blkst, dir, nil
}

func loadOrInitPeerKey(kf string) (crypto.PrivKey, error) {
	data, err := ioutil.ReadFile(kf)
	if err != nil {
		if !os.IsNotExist(err) {
			return nil, err
		}

		k, _, err := crypto.GenerateEd25519Key(crand.Reader)
		if err != nil {
			return nil, err
		}

		data, err := crypto.MarshalPrivateKey(k)
		if err != nil {
			return nil, err
		}

		if err := ioutil.WriteFile(kf, data, 0600); err != nil {
			return nil, err
		}

		return k, nil
	}
	return crypto.UnmarshalPrivateKey(data)
}

func setupWallet(dir string) (*wallet.LocalWallet, error) {
	kstore, err := keystore.OpenOrInitKeystore(dir)
	if err != nil {
		return nil, err
	}

	wallet, err := wallet.NewWallet(kstore)
	if err != nil {
		return nil, err
	}

	addrs, err := wallet.WalletList(context.TODO())
	if err != nil {
		return nil, err
	}

	if len(addrs) == 0 {
		_, err := wallet.WalletNew(context.TODO(), types.KTSecp256k1)
		if err != nil {
			return nil, err
		}
	}

	defaddr, err := wallet.GetDefault()
	if err != nil {
		return nil, err
	}

	fmt.Println("Wallet address is: ", defaddr)

	return wallet, nil
}

type deleteManyWrap struct {
	blockstore.Blockstore
}

func (dmw *deleteManyWrap) DeleteMany(ctx context.Context, cids []cid.Cid) error {
	for _, c := range cids {
		if err := dmw.Blockstore.DeleteBlock(ctx, c); err != nil {
			return err
		}
	}

	return nil
}
