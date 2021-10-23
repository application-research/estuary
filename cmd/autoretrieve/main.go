package main

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
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
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	flatfs "github.com/ipfs/go-ds-flatfs"
	leveldb "github.com/ipfs/go-ds-leveldb"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	logging "github.com/ipfs/go-log/v2"
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

var logger = logging.Logger("estuary-ar")

func main() {
	logging.SetLogLevel("estuary-ar", "DEBUG")

	app := cli.NewApp()

	app.Flags = []cli.Flag{
		&cli.StringFlag{
			Name:    "datadir",
			Value:   "./estuary-ar",
			EnvVars: []string{"ESTUARY_AR_DATADIR"},
		},
		&cli.IntFlag{
			Name:  "timeout",
			Value: 60,
			Usage: "Time in seconds to wait on a hanging retrieval before moving on",
		},
		&cli.StringFlag{
			Name:  "endpoint",
			Value: "https://api.estuary.tech/retrieval-candidates",
		},
	}

	app.Action = func(cctx *cli.Context) error {
		dataDir := cctx.String("datadir")

		api, closer, err := lcli.GetGatewayAPI(cctx)
		if err != nil {
			return err
		}
		defer closer()

		nd, err := newAutoRetrieveNode(cctx.Context, Config{
			retrievalTimeout: time.Second * time.Duration(cctx.Int("timeout")),
			dataDir:          dataDir,
			listenAddrs:      []multiaddr.Multiaddr{multiaddr.StringCast("/ip4/0.0.0.0/tcp/6746")},
		}, api)
		if err != nil {
			return err
		}

		logger.Infof("P2P Address: %v", nd.host.Addrs())
		logger.Infof("P2P ID: %v", nd.host.ID())

		<-cctx.Context.Done()

		return nil
	}

	err := app.Run(os.Args)
	if err != nil {
		log.Fatalln(err)
	}
}

type Config struct {
	retrievalTimeout time.Duration
	dataDir          string
	listenAddrs      []multiaddr.Multiaddr
}

type autoRetrieveNode struct {
	datastore  datastore.Batching
	blockstore senderBlockstore
	wallet     *wallet.LocalWallet // If nil, only free retrievals will be attempted
	fc         *filclient.FilClient
	host       host.Host
	bsnet      bsnet.BitSwapNetwork
}

const datastoreSubdir = "datastore"
const blockstoreSubdir = "blockstore"
const walletSubdir = "wallet"

func newAutoRetrieveNode(ctx context.Context, config Config, api api.Gateway) (autoRetrieveNode, error) {
	var node autoRetrieveNode

	// Datastore
	{
		datastore, err := leveldb.NewDatastore(filepath.Join(config.dataDir, datastoreSubdir), nil)
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

		blockstoreDatastore, err := flatfs.CreateOrOpen(filepath.Join(config.dataDir, blockstoreSubdir), parseShardFunc, false)
		if err != nil {
			return autoRetrieveNode{}, err
		}

		node.blockstore = senderBlockstore{
			Blockstore:  blockstore.NewBlockstoreNoPrefix(blockstoreDatastore),
			bsnet:       nil,
			waitListsLk: new(sync.Mutex),
			waitLists:   make(map[cid.Cid][]waitListEntry),
		}
	}

	// Host
	{
		var peerkey crypto.PrivKey
		keyPath := filepath.Join(config.dataDir, "peerkey")
		keyFile, err := os.ReadFile(keyPath)
		if err != nil {
			logger.Infof("Generating new peer key...")

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

		host, err := libp2p.New(ctx, libp2p.ListenAddrs(config.listenAddrs...), libp2p.Identity(peerkey))
		if err != nil {
			return autoRetrieveNode{}, err
		}

		node.host = host
	}

	// Wallet Address
	{
		keystore, err := keystore.OpenOrInitKeystore(filepath.Join(config.dataDir, walletSubdir))
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
			logger.Warnf("Could not load any default wallet address, only free retrievals will be attempted (%v)", err)
			addr = address.Undef
		} else {
			logger.Infof("Using default wallet address %s", addr)
		}

		fc, err := filclient.NewClient(node.host, api, node.wallet, addr, node.blockstore, node.datastore, config.dataDir)
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
			bsnet:                bsnet,
			fc:                   node.fc,
			blockstore:           node.blockstore,
			config:               config,
			retrievalsInProgress: make(map[cid.Cid]bool),
		}
		bsnet.SetDelegate(receiver)

		node.bsnet = bsnet
		node.blockstore.bsnet = node.bsnet
	}

	return node, nil
}

type senderBlockstore struct {
	blockstore.Blockstore
	bsnet       bsnet.BitSwapNetwork
	waitListsLk *sync.Mutex
	waitLists   map[cid.Cid][]waitListEntry
}

func (sbs senderBlockstore) Put(block blocks.Block) error {
	sbs.waitListsLk.Lock()
	waitList := sbs.waitLists[block.Cid()]
	delete(sbs.waitLists, block.Cid())
	sbs.waitListsLk.Unlock()

	for _, entry := range waitList {
		msg := bsmsg.New(false)
		msg.AddBlock(block)
		sbs.bsnet.SendMessage(context.Background(), entry.peerID, msg)
	}

	return sbs.Blockstore.Put(block)
}

func (sbs senderBlockstore) PutMany(blocks []blocks.Block) error {
	// TODO: batch SendMessage as well
	for _, block := range blocks {
		sbs.waitListsLk.Lock()
		waitList := sbs.waitLists[block.Cid()]
		delete(sbs.waitLists, block.Cid())
		sbs.waitListsLk.Unlock()

		for _, entry := range waitList {
			msg := bsmsg.New(false)
			msg.AddBlock(block)
			sbs.bsnet.SendMessage(context.Background(), entry.peerID, msg)
		}
	}

	return sbs.Blockstore.PutMany(blocks)
}

type waitListEntry struct {
	peerID peer.ID
}

type bsnetReceiver struct {
	blockstore             senderBlockstore
	fc                     *filclient.FilClient
	bsnet                  bsnet.BitSwapNetwork
	config                 Config
	retrievalsInProgressLk sync.Mutex
	retrievalsInProgress   map[cid.Cid]bool
}

func (r *bsnetReceiver) ReceiveMessage(ctx context.Context, sender peer.ID, incoming bsmsg.BitSwapMessage) {

	resMsg := bsmsg.New(false)

	for _, entry := range incoming.Wantlist() {

		// Check if the block is in the blockstore
		block, err := r.blockstore.Get(entry.Cid)
		if err != nil {
			// If it wasn't, then check for retrieval candidates
			candidates, err := GetRetrievalCandidates("https://api.estuary.tech/retrieval-candidates", entry.Cid)

			// If error, then don't have
			if err != nil {
				resMsg.AddDontHave(entry.Cid)
				continue
			}

			// If none found, then don't have
			if len(candidates) == 0 {
				resMsg.AddDontHave(entry.Cid)
				continue
			}

			// Otherwise, at least one was successfully found, so check if WANT_HAVE or WANT_BLOCK
			if entry.WantType == bitswap_message_pb.Message_Wantlist_Have {
				// If just WANT_HAVE, send HAVE, but don't do the actual retrieval yet
				resMsg.AddHave(entry.Cid)
			} else if entry.WantType == bitswap_message_pb.Message_Wantlist_Block {
				// If WANT_BLOCK, then first add peer to block wait list...
				r.blockstore.waitListsLk.Lock()
				r.blockstore.waitLists[entry.Cid] = append(r.blockstore.waitLists[entry.Cid], waitListEntry{
					peerID: sender,
				})
				r.blockstore.waitListsLk.Unlock()

				// ...and then check if there's already a retrieval running that contains this CID in its tree
				retrievalInProgress := false
				for _, candidate := range candidates {
					r.retrievalsInProgressLk.Lock()
					if _, ok := r.retrievalsInProgress[candidate.RootCid]; ok {
						retrievalInProgress = true
					}
					r.retrievalsInProgressLk.Unlock()

					// Check if we can break after we unlock
					if retrievalInProgress {
						break
					}
				}

				// If we didn't find any retrieval in progress already...
				if !retrievalInProgress {

					// ...start it on this goroutine
					if err := r.retrieveFromBestCandidate(ctx, candidates); err != nil {
						logger.Errorf("Could not retrieve %s: %v", entry.Cid, err)
					} else {
						logger.Infof("Successfully retrieved %v", entry.Cid)
					}
				}
			}
			continue
		}

		if entry.WantType == bitswap_message_pb.Message_Wantlist_Have {
			resMsg.AddHave(entry.Cid)
		} else if entry.WantType == bitswap_message_pb.Message_Wantlist_Block {
			resMsg.AddBlock(block)
		}
	}

	// haveCount := len(resMsg.Haves())
	// blockCount := len(resMsg.Blocks())
	// dontHaveCount := len(resMsg.DontHaves())
	// fmt.Printf("Finished bitswap message to %v (%v HAVE, %v BLOCK, %v DONTHAVE)\n", sender, haveCount, blockCount, dontHaveCount)

	r.bsnet.SendMessage(ctx, sender, resMsg)
}

func (r *bsnetReceiver) ReceiveError(err error) {
	//fmt.Printf("Bitswap receive error: %v\n", err)
}

func (r *bsnetReceiver) PeerConnected(id peer.ID) {
	//fmt.Printf("Connected to peer %v\n", id)
}

func (r *bsnetReceiver) PeerDisconnected(id peer.ID) {
	//fmt.Printf("Disconnected from peer %v\n", id)
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

type CandidateQuery struct {
	Candidate RetrievalCandidate
	Response  *retrievalmarket.QueryResponse
}

// Select the most preferable miner to retrieve from and execute the retrieval
func (r *bsnetReceiver) retrieveFromBestCandidate(ctx context.Context, candidates []RetrievalCandidate) error {
	checked := 0
	var queries []CandidateQuery
	var queriesLk sync.Mutex

	var wg sync.WaitGroup
	wg.Add(len(candidates))

	for _, candidate := range candidates {
		go func(candidate RetrievalCandidate) {
			defer wg.Done()

			query, err := r.fc.RetrievalQuery(ctx, candidate.Miner, candidate.RootCid)
			if err != nil {
				logger.Errorf(
					"Failed to query retrieval %v/%v from miner %s for %s: %v",
					checked+1,
					len(candidates),
					candidate.Miner,
					candidate.RootCid,
					err,
				)
				return
			}

			logger.Infof(
				"Retrieval query %v/%v succeeded from miner %s for %s",
				checked+1,
				len(candidates),
				candidate.Miner,
				candidate.RootCid,
			)

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
	for i, query := range queries {

		r.retrievalsInProgressLk.Lock()
		logger.Infof(
			"Attempting retrieval %v/%v from miner %s for %s (%v retrievals in progress)",
			i+1,
			len(queries),
			query.Candidate.Miner,
			query.Candidate.RootCid,
			len(r.retrievalsInProgress),
		)
		r.retrievalsInProgressLk.Unlock()

		var err error
		stats, err = r.retrieve(ctx, query)
		if err != nil {

			logger.Errorf(
				"Failed to retrieve %v/%v from miner %s for %s: %v",
				i+1,
				len(queries),
				query.Candidate.Miner,
				query.Candidate.RootCid,
				err,
			)

			if errors.Is(err, ErrRetrievalAlreadyRunning) {
				return err
			}

			continue
		}

		logger.Infof(
			"Retrieval %v/%v succeeded from miner %s for %s",
			i+1,
			len(queries),
			query.Candidate.Miner,
			query.Candidate.RootCid,
		)

		break
	}

	if stats == nil {
		return xerrors.New("all retrievals failed")
	}

	return nil
}

var ErrRetrievalAlreadyRunning = xerrors.New("retrieval already running")

func (r *bsnetReceiver) retrieve(ctx context.Context, query CandidateQuery) (*filclient.RetrievalStats, error) {
	r.retrievalsInProgressLk.Lock()

	// If we identify a retrieval already running for this a potential root
	// CID at this point, fail out immediately, this retrieval will be
	// pointless
	if _, ok := r.retrievalsInProgress[query.Candidate.RootCid]; ok {
		r.retrievalsInProgressLk.Unlock()
		return nil, fmt.Errorf("%w: root cid %s", ErrRetrievalAlreadyRunning, query.Candidate.RootCid)
	}

	// Mark the root CID's retrieval as in progress
	r.retrievalsInProgress[query.Candidate.RootCid] = true

	r.retrievalsInProgressLk.Unlock()

	defer func() {
		r.retrievalsInProgressLk.Lock()
		delete(r.retrievalsInProgress, query.Candidate.RootCid)
		r.retrievalsInProgressLk.Unlock()
	}()

	proposal, err := retrievehelper.RetrievalProposalForAsk(query.Response, query.Candidate.RootCid, nil)
	if err != nil {
		return nil, err
	}

	retrieveCtx, retrieveCancel := context.WithCancel(ctx)
	var lastBytesReceived uint64 = 0
	lastBytesReceivedTime := time.Now()
	return r.fc.RetrieveContentWithProgressCallback(retrieveCtx, query.Candidate.Miner, proposal, func(bytesReceived uint64) {
		if lastBytesReceived != bytesReceived {
			lastBytesReceivedTime = time.Now()
			lastBytesReceived = bytesReceived
		}

		if time.Since(lastBytesReceivedTime) > r.config.retrievalTimeout {
			retrieveCancel()
			return
		}
	})
}

func totalCost(qres *retrievalmarket.QueryResponse) big.Int {
	return big.Add(big.Mul(qres.MinPricePerByte, big.NewIntUnsigned(qres.Size)), qres.UnsealPrice)
}
