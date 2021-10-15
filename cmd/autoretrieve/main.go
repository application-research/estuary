package main

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/application-research/filclient"
	"github.com/application-research/filclient/keystore"
	"github.com/application-research/filclient/retrievehelper"
	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-fil-markets/retrievalmarket"
	"github.com/filecoin-project/go-state-types/big"
	"github.com/filecoin-project/lotus/api"
	"github.com/filecoin-project/lotus/chain/wallet"
	lcli "github.com/filecoin-project/lotus/cli"
	bsmsg "github.com/ipfs/go-bitswap/message"
	bitswap_message_pb "github.com/ipfs/go-bitswap/message/pb"
	bsnet "github.com/ipfs/go-bitswap/network"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	flatfs "github.com/ipfs/go-ds-flatfs"
	leveldb "github.com/ipfs/go-ds-leveldb"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p-core/crypto"
	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/peer"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	"github.com/libp2p/go-libp2p-kad-dht/fullrt"
	"github.com/multiformats/go-multiaddr"
	"github.com/urfave/cli/v2"
	"golang.org/x/xerrors"
)

func main() {
	app := cli.NewApp()

	app.Flags = []cli.Flag{
		&cli.StringFlag{
			Name:    "datadir",
			Value:   "./estuary-ar",
			EnvVars: []string{"ESTUARY_AR_DATADIR"},
		},
		&cli.StringFlag{
			Name:  "endpoint",
			Value: "https://api.estuary.tech/retrieval-candidates",
		},
	}

	app.Action = func(cctx *cli.Context) error {
		ddir := cctx.String("datadir")

		api, closer, err := lcli.GetGatewayAPI(cctx)
		if err != nil {
			return err
		}
		defer closer()

		nd, err := newAutoRetrieveNode(cctx.Context, ddir, api, []multiaddr.Multiaddr{multiaddr.StringCast("/ip4/0.0.0.0/tcp/6746")})
		if err != nil {
			return err
		}

		fmt.Printf("p2p address: %v\n", nd.host.Addrs())
		fmt.Printf("p2p id: %v\n", nd.host.ID())

		<-cctx.Context.Done()

		return nil
	}

	err := app.Run(os.Args)
	if err != nil {
		log.Fatalln(err)
	}
}

type autoRetrieveNode struct {
	datastore  datastore.Batching
	blockstore blockstore.Blockstore
	wallet     *wallet.LocalWallet // If nil, only free retrievals will be attempted
	fc         *filclient.FilClient
	host       host.Host
}

const datastoreSubdir = "datastore"
const blockstoreSubdir = "blockstore"
const walletSubdir = "wallet"

func newAutoRetrieveNode(ctx context.Context, dataDir string, api api.Gateway, listenAddrs []multiaddr.Multiaddr) (autoRetrieveNode, error) {
	var node autoRetrieveNode

	// Datastore
	{
		datastore, err := leveldb.NewDatastore(filepath.Join(dataDir, datastoreSubdir), nil)
		if err != nil {
			return autoRetrieveNode{}, err
		}

		node.datastore = datastore
	}

	// Blockstore
	{
		parseShardFunc, err := flatfs.ParseShardFunc("/repo/flatfs/shard/v1/next-to-last/3")
		if err != nil {
			return autoRetrieveNode{}, err
		}

		blockstoreDatastore, err := flatfs.CreateOrOpen(filepath.Join(dataDir, blockstoreSubdir), parseShardFunc, false)
		if err != nil {
			return autoRetrieveNode{}, err
		}

		node.blockstore = blockstore.NewBlockstoreNoPrefix(blockstoreDatastore)
	}

	// Host
	{
		var peerkey crypto.PrivKey
		keyPath := filepath.Join(dataDir, "peerkey")
		keyFile, err := os.ReadFile(keyPath)
		if err != nil {
			fmt.Printf("Generating new peer key\n")

			if !os.IsNotExist(err) {
				return autoRetrieveNode{}, err
			}

			key, _, err := crypto.GenerateEd25519Key(rand.Reader)
			if err != nil {
				return autoRetrieveNode{}, err
			}
			peerkey = key

			data, err := crypto.MarshalPrivateKey(key)
			if err != nil {
				return autoRetrieveNode{}, err
			}

			if err := os.WriteFile(keyPath, data, 0600); err != nil {
				return autoRetrieveNode{}, err
			}
		} else {
			key, err := crypto.UnmarshalPrivateKey(keyFile)
			if err != nil {
				return autoRetrieveNode{}, err
			}

			peerkey = key
		}

		if peerkey == nil {
			panic("sanity check: peer key is uninitialized")
		}

		host, err := libp2p.New(ctx, libp2p.ListenAddrs(listenAddrs...), libp2p.Identity(peerkey))
		if err != nil {
			return autoRetrieveNode{}, err
		}

		node.host = host
	}

	// Wallet Address
	{
		keystore, err := keystore.OpenOrInitKeystore(filepath.Join(dataDir, walletSubdir))
		if err != nil {
			return autoRetrieveNode{}, err
		}

		wallet, err := wallet.NewWallet(keystore)
		if err != nil {
			return autoRetrieveNode{}, err
		}

		node.wallet = wallet
	}

	// FilClient
	{
		addr, err := node.wallet.GetDefault()
		if err != nil {
			fmt.Printf("Could not load any default wallet address, only free retrievals will be attempted (%v)\n", err)
			addr = address.Undef
		} else {
			fmt.Printf("Using default wallet address %s", addr)
		}

		fc, err := filclient.NewClient(node.host, api, node.wallet, addr, node.blockstore, node.datastore, dataDir)
		if err != nil {
			return autoRetrieveNode{}, err
		}

		node.fc = fc
	}

	// Bitswap
	{
		fullRT, err := fullrt.NewFullRT(node.host, dht.DefaultPrefix, fullrt.DHTOption(
			dht.Datastore(node.datastore),
			dht.BucketSize(20),
			dht.BootstrapPeers(dht.GetDefaultBootstrapPeerAddrInfos()...),
		))
		if err != nil {
			return autoRetrieveNode{}, err
		}

		bsnet := bsnet.NewFromIpfsHost(node.host, fullRT)
		receiver := &bsnetReceiver{
			simplifiedLog: true,
			bsnet:         bsnet,
			fc:            node.fc,
			blockstore:    node.blockstore,
		}
		bsnet.SetDelegate(receiver)
	}

	return node, nil
}

type bsnetReceiver struct {
	simplifiedLog bool
	blockstore    blockstore.Blockstore
	fc            *filclient.FilClient
	bsnet         bsnet.BitSwapNetwork
}

func (r *bsnetReceiver) ReceiveMessage(ctx context.Context, sender peer.ID, incoming bsmsg.BitSwapMessage) {

	resMsg := bsmsg.New(false)

	for _, entry := range incoming.Wantlist() {
		if entry.WantType == bitswap_message_pb.Message_Wantlist_Have {
			candidates, err := GetRetrievalCandidates("https://api.estuary.tech/retrieval-candidates", entry.Cid)
			if err != nil {
				continue
			}

			if len(candidates) > 0 {
				err := r.retrieveFromBestCandidate(ctx, candidates)
				if err != nil {
					if r.simplifiedLog {
						fmt.Printf("x")
					}
					resMsg.AddDontHave(entry.Cid)
					continue
				}

				if r.simplifiedLog {
					fmt.Printf(".")
				}

				resMsg.AddHave(entry.Cid)
			} else {
				resMsg.AddDontHave(entry.Cid)
			}
		} else if entry.WantType == bitswap_message_pb.Message_Wantlist_Block {
			block, err := r.blockstore.Get(entry.Cid)
			if err != nil {
				resMsg.AddDontHave(entry.Cid)
				continue
			}
			resMsg.AddBlock(block)

			if r.simplifiedLog {
				fmt.Printf("[B]")
			}
		}
	}

	r.bsnet.SendMessage(ctx, sender, resMsg)

	if r.simplifiedLog {
		fmt.Printf(">")
	}
}

func (r *bsnetReceiver) ReceiveError(error) {
	if r.simplifiedLog {
		fmt.Printf("e")
	}
}

func (r *bsnetReceiver) PeerConnected(peer.ID) {
	if r.simplifiedLog {
		fmt.Printf("+")
	}
}

func (r *bsnetReceiver) PeerDisconnected(peer.ID) {
	if r.simplifiedLog {
		fmt.Printf("-")
	}
}

type RetrievalCandidate struct {
	Miner   address.Address
	RootCid cid.Cid
	DealID  uint
}

func GetRetrievalCandidates(endpoint string, c cid.Cid) ([]RetrievalCandidate, error) {

	endpointURL, err := url.Parse(endpoint)
	if err != nil {
		return nil, xerrors.Errorf("endpoint %s is not a valid url", endpoint)
	}
	endpointURL.Path = path.Join(endpointURL.Path, c.String())

	resp, err := http.Get(endpointURL.String())
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("http request to endpoint %s got status %v", endpointURL, resp.StatusCode)
	}

	var res []RetrievalCandidate

	if err := json.NewDecoder(resp.Body).Decode(&res); err != nil {
		return nil, xerrors.Errorf("could not unmarshal http response for cid %s", c)
	}

	return res, nil
}

// Select the most preferable miner to retrieve from and execute the retrieval
func (r *bsnetReceiver) retrieveFromBestCandidate(ctx context.Context, candidates []RetrievalCandidate) error {
	type CandidateQuery struct {
		Candidate RetrievalCandidate
		Response  *retrievalmarket.QueryResponse
	}
	checked := 0
	var queries []CandidateQuery
	var queriesLk sync.Mutex

	var wg sync.WaitGroup
	wg.Add(len(candidates))

	for _, candidate := range candidates {
		if r.simplifiedLog {
			fmt.Printf("?")
		} else {
			fmt.Printf("%v/%v\n", checked, len(candidates))
		}

		go func(candidate RetrievalCandidate) {
			defer wg.Done()

			query, err := r.fc.RetrievalQuery(ctx, candidate.Miner, candidate.RootCid)
			if err != nil {
				if r.simplifiedLog {
					fmt.Printf("[?e]")
				} else {
					fmt.Printf("Retrieval query for miner %s failed: %v\n", candidate.Miner, err)
				}
				return
			}

			if r.simplifiedLog {
				fmt.Printf("[?d]")
			}

			queriesLk.Lock()
			queries = append(queries, CandidateQuery{Candidate: candidate, Response: query})
			checked++
			queriesLk.Unlock()
		}(candidate)
	}

	wg.Wait()

	if len(queries) == 0 {
		return xerrors.Errorf("retrieval failed: queries failed for all miners")
	}

	sort.Slice(queries, func(i, j int) bool {
		a := queries[i].Response
		b := queries[i].Response

		// Always prefer unsealed to sealed, no matter what
		if a.UnsealPrice.IsZero() && !b.UnsealPrice.IsZero() {
			return true
		}

		// Select lower price, or continue if equal
		aTotalPrice := totalCost(a)
		bTotalPrice := totalCost(b)
		if !aTotalPrice.Equals(bTotalPrice) {
			return aTotalPrice.LessThan(bTotalPrice)
		}

		// Select smaller size, or continue if equal
		if a.Size != b.Size {
			return a.Size < b.Size
		}

		return false
	})

	// Now attempt retrievals in serial from first to last, until one works.
	// stats will get set if a retrieval succeeds - if no retrievals work, it
	// will still be nil after the loop finishes
	var stats *filclient.RetrievalStats
	for _, query := range queries {
		proposal, err := retrievehelper.RetrievalProposalForAsk(query.Response, query.Candidate.RootCid, nil)
		if err != nil {
			continue
		}

		if r.simplifiedLog {
			fmt.Print("!")
		}

		retrieveCtx, retrieveCancel := context.WithCancel(ctx)
		const timeout time.Duration = time.Second * 5
		var lastBytesReceived uint64 = 0
		lastBytesReceivedTime := time.Now()
		stats, err = r.fc.RetrieveContentWithProgressCallback(retrieveCtx, query.Candidate.Miner, proposal, func(bytesReceived uint64) {
			if lastBytesReceived != bytesReceived {
				lastBytesReceivedTime = time.Now()
				lastBytesReceived = bytesReceived
			}

			if time.Since(lastBytesReceivedTime) > timeout {
				retrieveCancel()
				return
			}
		})
		if err != nil {
			if r.simplifiedLog {
				fmt.Print("[!e]")
			} else {
				fmt.Printf("Failed to retrieve content with candidate miner %s: %v\n", query.Candidate.Miner, err)
			}
			continue
		}

		if r.simplifiedLog {
			fmt.Print("[!d]")
		}

		break
	}

	if stats == nil {
		return xerrors.New("retrieval failed: all miners failed to respond")
	}

	return nil
}

func totalCost(qres *retrievalmarket.QueryResponse) big.Int {
	return big.Add(big.Mul(qres.MinPricePerByte, big.NewIntUnsigned(qres.Size)), qres.UnsealPrice)
}
