package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/application-research/estuary/node"
	"github.com/application-research/filclient"
	"github.com/application-research/filclient/retrievehelper"
	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-fil-markets/retrievalmarket"
	lcli "github.com/filecoin-project/lotus/cli"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	"github.com/urfave/cli/v2"
)

func main() {
	app := cli.NewApp()

	app.Flags = []cli.Flag{
		&cli.StringFlag{
			Name:    "datadir",
			Value:   ".",
			EnvVars: []string{"ESTUARY_AR_DATADIR"},
		},
		&cli.StringFlag{
			Name:  "endpoint",
			Value: "https://api.estuary.tech/retrieval-candidates",
		},
	}

	app.Action = func(cctx *cli.Context) error {
		ddir := cctx.String("datadir")

		var arbs *autoRetrieveBlockstore
		cfg := node.Config{
			ListenAddrs: []string{
				"/ip4/0.0.0.0/tcp/6746",
			},
			Blockstore:       filepath.Join(ddir, "estuary-ar-blocks"),
			Libp2pKeyFile:    filepath.Join(ddir, "estuary-ar-peer-key"),
			Datastore:        filepath.Join(ddir, "estuary-ar-leveldb"),
			WalletDir:        filepath.Join(ddir, "estuary-ar-wallet"),
			WriteLogTruncate: cctx.Bool("estuary-ar-write-log-truncate"),

			BlockstoreWrap: func(bs blockstore.Blockstore) (blockstore.Blockstore, error) {
				arbs = &autoRetrieveBlockstore{Blockstore: bs, endpoint: cctx.String("endpoint")}
				var test blockstore.Blockstore = arbs
				test.Get(cid.Undef)
				return arbs, nil
			},
		}

		nd, err := node.Setup(cctx.Context, &cfg)
		if err != nil {
			return err
		}

		arbs.notifBlockstore = nd.NotifBlockstore

		// Set the blockstore filclient instance
		api, closer, err := lcli.GetGatewayAPI(cctx)
		if err != nil {
			return err
		}
		defer closer()

		addr, err := nd.Wallet.GetDefault()
		if err != nil {
			return err
		}

		fc, err := filclient.NewClient(nd.Host, api, nd.Wallet, addr, nd.Blockstore, nd.Datastore, ddir)
		if err != nil {
			return err
		}

		arbs.fc = fc

		fmt.Println("p2p address:", nd.Host.Addrs())
		fmt.Println("p2p id:", nd.Host.ID())

		<-cctx.Context.Done()

		return nil
	}

	err := app.Run(os.Args)
	if err != nil {
		log.Fatalln(err)
	}
}

type retrievalCandidate struct {
	Miner   address.Address
	RootCid cid.Cid
	DealID  uint
}

type autoRetrieveBlockstore struct {
	blockstore.Blockstore
	endpoint        string
	notifBlockstore *node.NotifyBlockstore
	fc              *filclient.FilClient
}

var total int = 0
var totalSecond int = 0
var mu sync.Mutex
var last time.Time = time.Now()
var cidCounts map[cid.Cid]int = make(map[cid.Cid]int)
var max int = 0
var s int = 0

var cacheLk sync.Mutex
var cache map[cid.Cid][]retrievalCandidate = make(map[cid.Cid][]retrievalCandidate)

func (bs *autoRetrieveBlockstore) GetSize(c cid.Cid) (int, error) {

	// mu.Lock()
	// defer mu.Unlock()

	// now := time.Now()
	// elapsed := now.Sub(last)
	// if elapsed.Seconds() >= 1.0 {
	// 	s++
	// 	last = now
	// 	fmt.Printf("[%vs elapsed] %v GetSize() calls this second. Of total %v cids, %v have been unique (highest duplicate count: %v)\n", s, totalSecond, total, len(cidCounts), max)
	// 	totalSecond = 0
	// }

	// total++
	// totalSecond++
	// count, ok := cidCounts[c]
	// if !ok {
	// 	count = 0
	// }
	// count++
	// if count > max {
	// 	max = count
	// }
	// cidCounts[c] = count

	cacheLk.Lock()
	res, ok := cache[c]
	cacheLk.Unlock()
	if ok {
		return 1000, nil
	}

	resp, err := http.Get(bs.endpoint + "/" + c.String())
	if err != nil {
		return 0, err
	}
	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("http request failed")
	}

	if err := json.NewDecoder(resp.Body).Decode(&res); err != nil {
		return 0, fmt.Errorf("could not unmarshal http response for %s, may have been null", c)
	}

	cacheLk.Lock()
	cache[c] = res
	cacheLk.Unlock()

	return 1000, nil
}

func (bs *autoRetrieveBlockstore) Get(c cid.Cid) (blocks.Block, error) {
	fmt.Println("received auto retrieve request for cid", c)

	// Try to get this cid from the local blockstore
	block, bsErr := bs.Blockstore.Get(c)

	// If that failed...
	if bsErr != nil {
		// ...maybe it wasn't present
		if errors.Is(bsErr, blockstore.ErrNotFound) {
			// In which case, we hit the api endpoint and ask which miners have this information
			candidates, err := bs.getRetrievalCandidates(context.Background(), c)
			if err != nil {
				return nil, err
			}

			fmt.Println("starting retrieval for cid", c)

			if err := bs.retrieveFromCandidates(context.Background(), candidates); err != nil {
				return nil, err
			}

			// Wait for the requested cid to show up in the blockstore
			blockCh := bs.notifBlockstore.WaitFor(c)

			<-blockCh

			fmt.Println("finished retrieval for cid", c)

			return block, nil
		}
	}

	// If there was no error getting the block locally, just return that
	return block, nil
}

func (bs *autoRetrieveBlockstore) getRetrievalCandidates(ctx context.Context, c cid.Cid) ([]retrievalCandidate, error) {
	cacheLk.Lock()
	candidates := cache[c]
	cacheLk.Unlock()

	return candidates, nil
}

// Select the most preferable miner to retrieve from and execute the retrieval
func (bs *autoRetrieveBlockstore) retrieveFromCandidates(ctx context.Context, candidates []retrievalCandidate) error {

	type result struct {
		proposal *retrievalmarket.DealProposal
		maddr    address.Address
	}

	var resultsLk sync.Mutex
	var results []result

	// Run retrieval queries for each miner candidate in parallel, and collect
	// them into results
	var wg sync.WaitGroup
	wg.Add(len(candidates))
	for _, candidate := range candidates {

		// Copy loop var for goroutine
		candidate := candidate

		go func() {
			ask, err := bs.fc.RetrievalQuery(ctx, candidate.Miner, candidate.RootCid)

			if err != nil {
				fmt.Printf("retrieval query for miner %s failed: %v\n", candidate.Miner, err)
			} else {
				proposal, err := retrievehelper.RetrievalProposalForAsk(ask, candidate.RootCid, nil)
				if err != nil {
					fmt.Printf("failed to create retrieval proposal for ask: %v\n", err)
				}
				resultsLk.Lock()
				results = append(results, result{
					proposal,
					candidate.Miner,
				})
				resultsLk.Unlock()
			}

			wg.Done()
		}()
	}
	wg.Wait()

	// If none of the retrieval queries succeeded, we can exit early
	if len(results) == 0 {
		return fmt.Errorf("all retrieval queries failed")
	}

	// Sort the results from lowest to highest proposal price
	// TODO: lots more we do here to rank results!
	sort.Slice(results, func(i, j int) bool {
		return results[i].proposal.PricePerByte.LessThan(results[j].proposal.PricePerByte)
	})

	for _, res := range results {
		_, err := bs.fc.RetrieveContent(ctx, res.maddr, res.proposal)
		if err != nil {
			fmt.Printf("retrieval failed: %v", err)
			continue
		}

		return nil
	}

	return fmt.Errorf("could not retrieve content from any of the candidate miners")
}
