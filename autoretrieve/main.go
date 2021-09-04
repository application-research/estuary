package autoretrieve

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"path"
	"sort"
	"sync"

	"github.com/application-research/estuary/dbmgr"
	"github.com/application-research/filclient"
	"github.com/application-research/filclient/keystore"
	"github.com/application-research/filclient/retrievehelper"
	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-fil-markets/retrievalmarket"
	"github.com/filecoin-project/lotus/chain/wallet"
	lcli "github.com/filecoin-project/lotus/cli"
	"github.com/ipfs/go-bitswap"
	bsnet "github.com/ipfs/go-bitswap/network"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	flatfs "github.com/ipfs/go-ds-flatfs"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	"github.com/libp2p/go-libp2p"
	"github.com/urfave/cli/v2"
)

func main() {
	app := cli.NewApp()

	app.Flags = []cli.Flag{
		&cli.StringFlag{
			Name:    "database",
			Value:   "sqlite=../estuary.db", // Uses same database from base Estuary dir
			EnvVars: []string{"ESTUARY_AR_DATABASE"},
		},
	}

	app.Action = func(cctx *cli.Context) error {
		// Connect to database

		dbval := cctx.String("database")
		db, err := dbmgr.NewDBMgr(dbval)
		if err != nil {
			return err
		}

		// Initialize libp2p host

		h, err := libp2p.New(cctx.Context)
		if err != nil {
			return err
		}
		defer h.Close()

		// Open blockstore

		ds, err := flatfs.Open("blocks", false)
		if err != nil {
			return err
		}
		defer ds.Close()

		// Set up client

		api, closer, err := lcli.GetGatewayAPI(cctx)
		if err != nil {
			return err
		}
		defer closer()

		ddir := "."

		walletDir := path.Join(ddir, "wallet")
		ks, err := keystore.OpenOrInitKeystore(walletDir)
		if err != nil {
			return err
		}

		wallet, err := wallet.NewWallet(ks)
		if err != nil {
			return err
		}

		walletAddr, err := wallet.GetDefault()
		if err != nil {
			return err
		}

		fc, err := filclient.NewClient(h, api, wallet, walletAddr, blockstore.NewBlockstore(ds), ds, ddir)
		if err != nil {
			return err
		}

		// Set up auto retrieve blockstore interface

		bs := AutoRetrieveBlockstore{
			blockstore.NewBlockstore(ds),
			db,
			fc,
		}

		// Start bitswap

		bsnet := bsnet.NewFromIpfsHost(h, nil)
		bitswap.New(cctx.Context, bsnet, bs)

		return nil
	}

	err := app.Run(os.Args)
	if err != nil {
		log.Fatalln(err)
	}
}

type AutoRetrieveBlockstore struct {
	blockstore.Blockstore
	db *dbmgr.DBMgr
	fc *filclient.FilClient
}

func (bs AutoRetrieveBlockstore) Get(c cid.Cid) (blocks.Block, error) {
	// Try to get this cid from the local blockstore
	block, bsErr := bs.Blockstore.Get(c)

	// If that failed...
	if bsErr != nil {
		// ...maybe it wasn't present
		if errors.Is(bsErr, blockstore.ErrNotFound) {
			// In which case, we check if the cid is tracked in the database
			object, err := bs.db.Objects().WithCid(c).GetSingle()
			if err != nil {
				if errors.Is(err, dbmgr.ErrNotFound) {
					return nil, fmt.Errorf("cid %s not tracked: %w", c, bsErr)
				}
				return nil, err
			}

			// And if it is (and it's not in the blockstore, since we're
			// here), then we can retrieve it and add it to the blockstore!
			bs.retrieve(object)
		}
	}

	return block, nil
}

func (bs AutoRetrieveBlockstore) Put(block blocks.Block) error {

	bs.Blockstore.Put(block)
}

func (bs *AutoRetrieveBlockstore) retrieve(object dbmgr.Object) (blocks.Block, error) {
	objRefs, err := bs.db.ObjRefs().WithObjectID(object.ID).Get()
	if err != nil {
		return nil, err
	}

	objRefCount := len(objRefs)

	// Determine which content to fetch
	var contentToRetrieve dbmgr.ContentID
	if objRefCount == 0 {
		// If no object refs in the database, there's no way to know what to
		// fetch!
		return nil, fmt.Errorf("no reference objects for object with cid %s", object.Cid)
	} else if objRefCount == 1 {
		// If only one object ref, obviously we'll use that one
		contentToRetrieve = objRefs[0].Content
	} else {
		// If there are multiple object refs, we check and see if one of them is
		// registered as a root content in the database by checking if any have
		// the requested cid. Root contents are preferable because as of
		// implementation (2 Sep 2021), most miners aren't able to respond to
		// partial content requests.

		contents, err := bs.db.Contents().WithCid(object.Cid.CID).Get()
		if err != nil {
			return nil, err
		}

		// Check if we found any contents from the database with the requested
		// cid, and if so...
		if len(contents) > 0 {
			// ...it's a root content, so we should get that one
			contentToRetrieve = dbmgr.ContentID(contents[0].ID)
		} else {
			// Otherwise, no root contents were found, so we have no choice but
			// to just try to retrieve an arbitrary partial content
			contentToRetrieve = objRefs[0].Content
		}
	}

	// Identify the miners that have the requested content

	deals, err := bs.db.Deals().WithContentID(contentToRetrieve).WithFailed(false).Get()
	if err != nil {
		return nil, err
	}

	minerAddrs := make([]address.Address, len(deals))
	for i, deal := range deals {
		minerAddrs[i], err = address.NewFromString(deal.Miner)
		if err != nil {
			return nil, err
		}
	}

	return blocks, nil
}

// Select the most preferable miner to retrieve from and execute the retrieval
func (bs *AutoRetrieveBlockstore) retrieveFromCandidates(ctx context.Context, c cid.Cid, minerAddrs []address.Address) error {
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
	wg.Add(len(minerAddrs))
	for _, maddr := range minerAddrs {
		go func() {
			ask, err := bs.fc.RetrievalQuery(ctx, maddr, c)

			if err != nil {
				fmt.Printf("retrieval query for miner %s failed: %v\n", maddr, err)
			} else {
				proposal, err := retrievehelper.RetrievalProposalForAsk(ask, c, nil)
				if err != nil {
					fmt.Printf("failed to create retrieval proposal for ask: %v\n", err)
				}
				resultsLk.Lock()
				results = append(results, result{
					proposal,
					maddr,
				})
				resultsLk.Unlock()
			}

			wg.Done()
		}()
	}
	wg.Wait()

	// If none of the retrieval queries succeeded, we can exit early
	if len(results) == 0 {
		return fmt.Errorf("all retrieval queries failed for cid %s", c)
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
