package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"sync"

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
	}

	app.Action = func(cctx *cli.Context) error {
		ddir := cctx.String("datadir")

		var arbs *autoRetrieveBlockstore
		cfg := node.Config{
			ListenAddrs: []string{
				"/ip4/0.0.0.0/tcp/6744",
			},
			Blockstore:       filepath.Join(ddir, "estuary-ar-blocks"),
			Libp2pKeyFile:    filepath.Join(ddir, "estuary-ar-peer-key"),
			Datastore:        filepath.Join(ddir, "estuary-ar-leveldb"),
			WalletDir:        filepath.Join(ddir, "estuary-ar-wallet"),
			WriteLogTruncate: cctx.Bool("estuary-ar-write-log-truncate"),

			BlockstoreWrap: func(bs blockstore.Blockstore) (blockstore.Blockstore, error) {
				arbs = &autoRetrieveBlockstore{Blockstore: bs}
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

		return nil
	}

	err := app.Run(os.Args)
	if err != nil {
		log.Fatalln(err)
	}
}

type retrievalCandidate struct {
	maddr   address.Address
	rootCid cid.Cid
}

type autoRetrieveBlockstore struct {
	blockstore.Blockstore
	notifBlockstore *node.NotifyBlockstore
	fc              *filclient.FilClient
}

func (bs autoRetrieveBlockstore) Get(c cid.Cid) (blocks.Block, error) {
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

			if err := bs.retrieveFromCandidates(context.Background(), candidates); err != nil {
				return nil, err
			}

			// Wait for the requested cid to show up in the blockstore
			blockCh := bs.notifBlockstore.WaitFor(c)

			<-blockCh

			return block, nil
		}
	}

	// If there was no error getting the block locally, just return that
	return block, nil
}

func (bs *autoRetrieveBlockstore) getRetrievalCandidates(ctx context.Context, c cid.Cid) ([]retrievalCandidate, error) {
	panic("todo")
}

// Select the most preferable miner to retrieve from and execute the retrieval
func (bs *autoRetrieveBlockstore) retrieveFromCandidates(ctx context.Context, candidates []retrievalCandidate) error {

	// This does not currently need a mutex because each goroutine gets its own
	// dedicated index to write to, but be aware!
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
		go func() {
			ask, err := bs.fc.RetrievalQuery(ctx, candidate.maddr, candidate.rootCid)

			if err != nil {
				fmt.Printf("retrieval query for miner %s failed: %v\n", candidate.maddr, err)
			} else {
				proposal, err := retrievehelper.RetrievalProposalForAsk(ask, candidate.rootCid, nil)
				if err != nil {
					fmt.Printf("failed to create retrieval proposal for ask: %v\n", err)
				}
				resultsLk.Lock()
				results = append(results, result{
					proposal,
					candidate.maddr,
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
