package main

import (
	"context"
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

	"github.com/application-research/estuary/node"
	"github.com/application-research/filclient"
	"github.com/application-research/filclient/retrievehelper"
	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-fil-markets/retrievalmarket"
	"github.com/filecoin-project/go-state-types/big"
	lcli "github.com/filecoin-project/lotus/cli"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	"github.com/urfave/cli/v2"
	"golang.org/x/xerrors"
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
	defer resp.Body.Close()
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
	// Try to get this cid from the local blockstore
	block, bsErr := bs.Blockstore.Get(c)

	// If that failed...
	if bsErr != nil {
		// ...maybe it wasn't present
		if errors.Is(bsErr, blockstore.ErrNotFound) {
			// In which case, we hit the api endpoint and ask which miners have this information
			candidates, err := GetRetrievalCandidates("https://api.estuary.tech/retrieval-candidates", c)
			if err != nil {
				return nil, err
			}

			if err := bs.retrieveFromBestCandidate(context.Background(), candidates); err != nil {
				fmt.Printf("x")
				return nil, err
			}

			// Wait for the requested cid to show up in the blockstore
			blockCh := bs.notifBlockstore.WaitFor(c)

			<-blockCh

			fmt.Printf("\n%s\n", c)

			return block, nil
		}
	}

	// If there was no error getting the block locally, just return that
	return block, nil
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

type RetrievalCandidate struct {
	Miner   address.Address
	RootCid cid.Cid
	DealID  uint
}

// Select the most preferable miner to retrieve from and execute the retrieval
func (bs *autoRetrieveBlockstore) retrieveFromBestCandidate(ctx context.Context, candidates []RetrievalCandidate) error {
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

		// Copy into loop, cursed go
		candidate := candidate

		go func() {
			defer wg.Done()

			query, err := bs.fc.RetrievalQuery(ctx, candidate.Miner, candidate.RootCid)
			if err != nil {
				fmt.Printf("Retrieval query for miner %s failed: %v", candidate.Miner, err)
				return
			}

			queriesLk.Lock()
			queries = append(queries, CandidateQuery{Candidate: candidate, Response: query})
			checked++
			fmt.Printf("%v/%v\r", checked, len(candidates))
			queriesLk.Unlock()
		}()
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

		stats, err = bs.fc.RetrieveContent(ctx, query.Candidate.Miner, proposal)
		if err != nil {
			fmt.Printf("Failed to retrieve content with candidate miner %s: %v\n", query.Candidate.Miner, err)
			continue
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
