package node

import (
	"context"
	crand "crypto/rand"
	"fmt"
	"io/ioutil"
	"os"
	"time"

	autobatch "github.com/application-research/go-bs-autobatch"
	lmdb "github.com/filecoin-project/go-bs-lmdb"
	badgerbs "github.com/filecoin-project/lotus/blockstore/badger"
	"github.com/filecoin-project/lotus/chain/types"
	"github.com/filecoin-project/lotus/chain/wallet"
	"github.com/ipfs/go-bitswap"
	bsnet "github.com/ipfs/go-bitswap/network"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	levelds "github.com/ipfs/go-ds-leveldb"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	"github.com/ipfs/go-ipfs-provider/batched"
	"github.com/ipfs/go-ipfs-provider/queue"
	"github.com/libp2p/go-libp2p"
	connmgr "github.com/libp2p/go-libp2p-connmgr"
	"github.com/libp2p/go-libp2p-core/host"
	metrics "github.com/libp2p/go-libp2p-core/metrics"
	crypto "github.com/libp2p/go-libp2p-crypto"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	"github.com/libp2p/go-libp2p-kad-dht/fullrt"
	"github.com/whyrusleeping/estuary/keystore"
	bsm "github.com/whyrusleeping/go-bs-measure"
)

type EstuaryBlockstore interface {
	blockstore.Blockstore
	DeleteMany([]cid.Cid) error
}

type Node struct {
	Dht      *dht.IpfsDHT
	Provider *batched.BatchProvidingSystem
	FullRT   *fullrt.FullRT
	Host     host.Host

	Lmdb      *lmdb.Blockstore
	Datastore datastore.Batching

	Blockstore      blockstore.Blockstore
	Bitswap         *bitswap.Bitswap
	NotifBlockstore *NotifyBlockstore

	Wallet *wallet.LocalWallet

	Bwc *metrics.BandwidthCounter

	Config *Config
}

type Config struct {
	ListenAddrs []string

	Blockstore string

	WriteLog string

	Libp2pKeyFile string

	Datastore string

	WalletDir string

	BlockstoreWrap func(blockstore.Blockstore) (blockstore.Blockstore, error)

	KeyProviderFunc func(context.Context) (<-chan cid.Cid, error)
}

func Setup(ctx context.Context, cfg *Config) (*Node, error) {
	peerkey, err := loadOrInitPeerKey(cfg.Libp2pKeyFile)
	if err != nil {
		return nil, err
	}

	bwc := metrics.NewBandwidthCounter()

	h, err := libp2p.New(ctx,
		libp2p.ListenAddrStrings(cfg.ListenAddrs...),
		libp2p.NATPortMap(),
		libp2p.ConnectionManager(connmgr.NewConnManager(2000, 3000, time.Minute)),
		libp2p.Identity(peerkey),
		libp2p.BandwidthReporter(bwc),
	)
	if err != nil {
		return nil, err
	}

	frt, err := fullrt.NewFullRT(h, dht.DefaultPrefix)
	if err != nil {
		return nil, err
	}

	dht, err := dht.New(ctx, h)
	if err != nil {
		return nil, err
	}

	lmdbs, err := lmdb.Open(&lmdb.Options{
		Path:   cfg.Blockstore,
		NoSync: true,
	})
	if err != nil {
		return nil, err
	}

	var bstore EstuaryBlockstore = lmdbs

	if cfg.WriteLog != "" {
		writelog, err := badgerbs.Open(badgerbs.DefaultOptions(cfg.WriteLog))
		if err != nil {
			return nil, err
		}

		ab, err := autobatch.NewBlockstore(bstore, writelog, 200, 200)
		if err != nil {
			return nil, err
		}

		bstore = ab
	}

	notifbs := NewNotifBs(bstore)
	mbs := bsm.New("estuary", notifbs)

	ds, err := levelds.NewDatastore(cfg.Datastore, nil)
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
	bswap := bitswap.New(ctx, bsnet, blkst)

	wallet, err := setupWallet(cfg.WalletDir)
	if err != nil {
		return nil, err
	}

	provq, err := queue.NewQueue(context.Background(), "provq", ds)
	if err != nil {
		return nil, err
	}

	kprov := batched.KeyProvider(cfg.KeyProviderFunc)

	prov, err := batched.New(frt, provq, kprov)
	if err != nil {
		return nil, err
	}

	return &Node{
		Dht:        dht,
		FullRT:     frt,
		Provider:   prov,
		Host:       h,
		Blockstore: mbs,
		Lmdb:       lmdbs,
		Datastore:  ds,
		Bitswap:    bswap.(*bitswap.Bitswap),
		Wallet:     wallet,
		Bwc:        bwc,
		Config:     cfg,
	}, nil
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
