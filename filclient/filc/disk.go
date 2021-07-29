package main

import (
	"context"
	crand "crypto/rand"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"

	lmdb "github.com/filecoin-project/go-bs-lmdb"
	"github.com/filecoin-project/lotus/chain/actors/builtin/market"
	"github.com/filecoin-project/lotus/chain/types"
	"github.com/filecoin-project/lotus/chain/wallet"
	lcli "github.com/filecoin-project/lotus/cli"
	"github.com/ipfs/go-bitswap"
	bsnet "github.com/ipfs/go-bitswap/network"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	levelds "github.com/ipfs/go-ds-leveldb"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p-core/host"
	metrics "github.com/libp2p/go-libp2p-core/metrics"
	crypto "github.com/libp2p/go-libp2p-crypto"
	cli "github.com/urfave/cli/v2"
	"github.com/whyrusleeping/estuary/filclient"
	"github.com/whyrusleeping/estuary/keystore"
)

type dealData struct {
	Proposal *market.ClientDealProposal
}

func dealsPath(baseDir string) string {
	return filepath.Join(baseDir, "deals")
}

func keyPath(baseDir string) string {
	return filepath.Join(baseDir, "libp2p.key")
}

func blockstorePath(baseDir string) string {
	return filepath.Join(baseDir, "blockstore")
}

func datastorePath(baseDir string) string {
	return filepath.Join(baseDir, "datastore")
}

func walletPath(baseDir string) string {
	return filepath.Join(baseDir, "wallet")
}

func saveDealProposal(dataDir string, propcid cid.Cid, proposal *market.ClientDealProposal) error {
	dealsPath := dealsPath(dataDir)

	if err := os.MkdirAll(dealsPath, 0755); err != nil {
		return err
	}

	data := &dealData{
		Proposal: proposal,
	}

	fi, err := os.Create(filepath.Join(dealsPath, propcid.String()))
	if err != nil {
		return err
	}
	defer fi.Close()

	if err := json.NewEncoder(fi).Encode(data); err != nil {
		return err
	}

	return nil
}

func listDeals(dataDir string) ([]cid.Cid, error) {
	elems, err := ioutil.ReadDir(dealsPath(dataDir))
	if err != nil {
		return nil, err
	}

	var out []cid.Cid
	for _, e := range elems {
		fmt.Println(e.Name())
		c, err := cid.Decode(e.Name())
		if err == nil {
			out = append(out, c)
		}
	}
	return out, nil
}

func clientFromNode(cctx *cli.Context, nd *Node, dir string) (*filclient.FilClient, func(), error) {
	api, closer, err := lcli.GetGatewayAPI(cctx)
	if err != nil {
		return nil, nil, err
	}

	addr, err := nd.Wallet.GetDefault()
	if err != nil {
		return nil, nil, err
	}

	fc, err := filclient.NewClient(nd.Host, api, nd.Wallet, addr, nd.Blockstore, nd.Datastore, dir)
	if err != nil {
		return nil, nil, err
	}

	fc.RetrievalProgressLogging = true

	return fc, closer, nil
}

func getClient(cctx *cli.Context, dir string) (*filclient.FilClient, func(), error) {
	nd, err := setup(context.Background(), dir)
	if err != nil {
		return nil, nil, err
	}

	return clientFromNode(cctx, nd, dir)
}

type Node struct {
	Host host.Host

	Datastore datastore.Batching

	Blockstore blockstore.Blockstore
	Bitswap    *bitswap.Bitswap

	Wallet *wallet.LocalWallet
}

func setup(ctx context.Context, cfgdir string) (*Node, error) {
	peerkey, err := loadOrInitPeerKey(keyPath(cfgdir))
	if err != nil {
		return nil, err
	}

	bwc := metrics.NewBandwidthCounter()

	h, err := libp2p.New(ctx,
		//libp2p.ConnectionManager(connmgr.NewConnManager(500, 800, time.Minute)),
		libp2p.Identity(peerkey),
		libp2p.BandwidthReporter(bwc),
	)
	if err != nil {
		return nil, err
	}

	bstore, err := lmdb.Open(&lmdb.Options{
		Path:   blockstorePath(cfgdir),
		NoSync: true,
	})
	if err != nil {
		return nil, err
	}

	ds, err := levelds.NewDatastore(datastorePath(cfgdir), nil)
	if err != nil {
		return nil, err
	}

	bsnet := bsnet.NewFromIpfsHost(h, nil)
	bswap := bitswap.New(ctx, bsnet, bstore)

	wallet, err := setupWallet(walletPath(cfgdir))
	if err != nil {
		return nil, err
	}

	return &Node{
		Host:       h,
		Blockstore: bstore,
		Datastore:  ds,
		Bitswap:    bswap.(*bitswap.Bitswap),
		Wallet:     wallet,
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
		_, err := wallet.WalletNew(context.TODO(), types.KTBLS)
		if err != nil {
			return nil, err
		}
	}

	return wallet, nil
}
