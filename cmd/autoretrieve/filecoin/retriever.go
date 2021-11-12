package filecoin

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"path"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/application-research/estuary/cmd/autoretrieve/blocks"
	"github.com/application-research/filclient"
	"github.com/application-research/filclient/keystore"
	"github.com/application-research/filclient/retrievehelper"
	"github.com/dustin/go-humanize"
	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-fil-markets/retrievalmarket"
	"github.com/filecoin-project/go-state-types/big"
	"github.com/filecoin-project/lotus/api"
	"github.com/filecoin-project/lotus/chain/types"
	"github.com/filecoin-project/lotus/chain/wallet"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	"github.com/ipfs/go-log/v2"
	"github.com/libp2p/go-libp2p-core/host"
	"golang.org/x/xerrors"
)

var logger = log.Logger("autoretrieve")

const walletSubdir = "wallet"

var (
	ErrNoCandidates            = errors.New("no candidates")
	ErrRetrievalAlreadyRunning = errors.New("retrieval already running")
)

type RetrieverConfig struct {
	DataDir          string
	Endpoint         string
	MinerBlacklist   map[address.Address]bool
	RetrievalTimeout time.Duration
}

type Retriever struct {
	config              RetrieverConfig
	filClient           *filclient.FilClient
	runningRetrievals   map[cid.Cid]bool
	runningRetrievalsLk sync.Mutex
}

type retrievalCandidate struct {
	Miner   address.Address
	RootCid cid.Cid
	DealID  uint
}

type candidateQuery struct {
	candidate retrievalCandidate
	response  *retrievalmarket.QueryResponse
}

func NewRetriever(config RetrieverConfig, host host.Host, api api.Gateway, datastore datastore.Batching, blockManager *blocks.Manager) (*Retriever, error) {

	keystore, err := keystore.OpenOrInitKeystore(filepath.Join(config.DataDir, walletSubdir))
	if err != nil {
		return nil, err
	}

	wallet, err := wallet.NewWallet(keystore)
	if err != nil {
		return nil, err
	}

	walletAddr, err := wallet.GetDefault()
	if err != nil {
		logger.Warnf("Could not load any default wallet address, only free retrievals will be attempted (%v)", err)
		walletAddr = address.Undef
	} else {
		logger.Infof("Using default wallet address %s", walletAddr)
	}

	filClient, err := filclient.NewClient(host, api, wallet, walletAddr, blockManager, datastore, config.DataDir)
	if err != nil {
		return nil, err
	}

	retriever := &Retriever{
		config:            config,
		filClient:         filClient,
		runningRetrievals: make(map[cid.Cid]bool),
	}

	return retriever, nil
}

// Request will tell the retriever to start trying to retrieve a certain CID. If
// there are no candidates available, this function will immediately return with
// an error. If a candidate is found, retrieval will begin in the background and
// nil will be returned.
//
// Retriever itself does not provide any mechanism for determining when a block
// becomes available - that is up to the caller.
func (retriever *Retriever) Request(cid cid.Cid) error {

	// TODO: before looking up candidates from the endpoint, we could cache
	// candidates and use that cached info. We only really have to look up an
	// up-to-date candidate list from the endpoint if we need to begin a new
	// retrieval.
	candidates, err := retriever.lookupCandidates(cid)
	if err != nil {
		return fmt.Errorf("could not get retrieval candidates for %s: %w", cid, err)
	}

	if len(candidates) == 0 {
		return fmt.Errorf("no retrieval candidates were found for %s", cid)
	}

	// If we got to this point, one or more candidates have been found and we
	// are good to go ahead with the retrieval
	go retriever.retrieveFromBestCandidate(context.Background(), candidates)

	return nil
}

// Takes an unsorted list of candidates, orders them, and attempts retrievals in serial until one succeeds.
func (retriever *Retriever) retrieveFromBestCandidate(ctx context.Context, candidates []retrievalCandidate) error {
	queries := retriever.queryCandidates(ctx, candidates)

	sort.Slice(queries, func(i, j int) bool {
		a := queries[i].response
		b := queries[i].response

		// Always prefer unsealed to sealed, no matter what
		if a.UnsealPrice.IsZero() && !b.UnsealPrice.IsZero() {
			return true
		}

		// Select lower price, or continue if equal
		aTotalCost := totalCost(a)
		bTotalCost := totalCost(b)
		if !aTotalCost.Equals(bTotalCost) {
			return aTotalCost.LessThan(bTotalCost)
		}

		// Select smaller size, or continue if equal
		if a.Size != b.Size {
			return a.Size < b.Size
		}

		return false
	})

	// stats will be nil after the loop if none of the retrievals successfully
	// complete
	var stats *filclient.RetrievalStats
	for i, query := range queries {
		if retriever.isRetrievalRunning(query.candidate.RootCid) {
			break
		}

		logger.Infof(
			"Attempting retrieval %v/%v from miner %s for %s",
			i+1,
			len(queries),
			query.candidate.Miner,
			query.candidate.RootCid,
		)

		stats_, err := retriever.retrieve(ctx, query)
		if err != nil {
			logger.Errorf(
				"Failed to retrieve %v/%v from miner %s for %s: %v",
				i+1,
				len(queries),
				query.candidate.Miner,
				query.candidate.RootCid,
				err,
			)

			if errors.Is(err, ErrRetrievalAlreadyRunning) {
				logger.Warnf("Tried to start a second retrieval for the same CID %s", query.candidate.RootCid)
				return err
			}

			continue
		}

		stats = stats_

		logger.Infof(
			"Successfully retrieved candidate %v/%v from miner %s for %s\n"+
				"\tDuration: %s\n"+
				"\tSize: %s\n"+
				"\tAverage Speed: %s/s\n"+
				"\tTotal Payment: %s",
			i+1,
			len(queries),
			query.candidate.Miner,
			query.candidate.RootCid,
			stats.Duration,
			humanize.IBytes(stats.Size),
			humanize.IBytes(stats.AverageSpeed),
			types.FIL(stats.TotalPayment),
		)

		break
	}

	if stats == nil {
		return fmt.Errorf("all retrievals failed")
	}

	return nil
}

func (retriever *Retriever) retrieve(ctx context.Context, query candidateQuery) (*filclient.RetrievalStats, error) {
	if err := retriever.registerRunningRetrieval(query.candidate.RootCid); err != nil {
		return nil, fmt.Errorf("could not register retrieval from miner %s for %s: %v", query.candidate.RootCid, query.candidate.Miner, err)
	}
	defer retriever.unregisterRunningRetrieval(query.candidate.RootCid)

	proposal, err := retrievehelper.RetrievalProposalForAsk(query.response, query.candidate.RootCid, nil)
	if err != nil {
		return nil, fmt.Errorf("could not create retrieval proposal: %w", err)
	}

	startTime := time.Now()

	retrieveCtx, retrieveCancel := context.WithCancel(ctx)
	var lastBytesReceived uint64 = 0
	var doneLk sync.Mutex
	done := false
	lastBytesReceivedTimer := time.AfterFunc(retriever.config.RetrievalTimeout, func() {
		doneLk.Lock()
		done = true
		doneLk.Unlock()

		retrieveCancel()
		logger.Errorf("Retrieval timed out after not receiving data for %s (started %s ago, stopped at %s)", retriever.config.RetrievalTimeout, time.Since(startTime), humanize.IBytes(lastBytesReceived))
	})
	stats, err := retriever.filClient.RetrieveContentWithProgressCallback(retrieveCtx, query.candidate.Miner, proposal, func(bytesReceived uint64) {
		doneLk.Lock()
		if !done {
			if lastBytesReceived != bytesReceived {
				if !lastBytesReceivedTimer.Stop() {
					<-lastBytesReceivedTimer.C
				}
				lastBytesReceivedTimer.Reset(retriever.config.RetrievalTimeout)
				lastBytesReceived = bytesReceived
			}
		}
		doneLk.Unlock()
	})

	lastBytesReceivedTimer.Stop()
	doneLk.Lock()
	done = true
	doneLk.Unlock()

	if err != nil {
		return nil, err
	}

	return stats, nil
}

func (retriever *Retriever) isRetrievalRunning(cid cid.Cid) bool {
	retriever.runningRetrievalsLk.Lock()
	defer retriever.runningRetrievalsLk.Unlock()

	return retriever.runningRetrievals[cid]
}

// Will register a retrieval as running, or ErrRetrievalAlreadyRunning if the
// CID is already registered.
func (retriever *Retriever) registerRunningRetrieval(cid cid.Cid) error {
	retriever.runningRetrievalsLk.Lock()
	defer retriever.runningRetrievalsLk.Unlock()

	if running := retriever.runningRetrievals[cid]; running {
		return ErrRetrievalAlreadyRunning
	}
	retriever.runningRetrievals[cid] = true

	return nil
}

// Unregisters a running retrieval. No-op if no retrieval is running.
func (retriever *Retriever) unregisterRunningRetrieval(cid cid.Cid) {
	retriever.runningRetrievalsLk.Lock()
	defer retriever.runningRetrievalsLk.Unlock()

	delete(retriever.runningRetrievals, cid)
}

// Returns a list of miners known to have the requested block, with blacklisted
// miners filtered out.
func (retriever *Retriever) lookupCandidates(cid cid.Cid) ([]retrievalCandidate, error) {
	// Create URL with CID
	endpointURL, err := url.Parse(retriever.config.Endpoint)
	if err != nil {
		return nil, fmt.Errorf("endpoint %s is not a valid url", retriever.config.Endpoint)
	}
	endpointURL.Path = path.Join(endpointURL.Path, cid.String())

	// Request candidates from endpoint
	resp, err := http.Get(endpointURL.String())
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("http request to endpoint %s got status %v", endpointURL, resp.StatusCode)
	}

	// Read candidate list from response body
	var unfiltered []retrievalCandidate
	if err := json.NewDecoder(resp.Body).Decode(&unfiltered); err != nil {
		return nil, xerrors.Errorf("could not unmarshal http response for cid %s", cid)
	}

	// Remove blacklisted miners
	var res []retrievalCandidate
	for _, candidate := range unfiltered {
		if !retriever.config.MinerBlacklist[candidate.Miner] {
			res = append(res, candidate)
		}
	}

	return res, nil
}

func (retriever *Retriever) queryCandidates(ctx context.Context, candidates []retrievalCandidate) []candidateQuery {
	var queries []candidateQuery
	var queriesLk sync.Mutex

	var wg sync.WaitGroup
	wg.Add(len(candidates))

	for i, candidate := range candidates {
		go func(i int, candidate retrievalCandidate) {
			defer wg.Done()

			query, err := retriever.filClient.RetrievalQuery(ctx, candidate.Miner, candidate.RootCid)
			if err != nil {
				// queriesLk.Lock()
				// logger.Errorf(
				// 	"Failed to query retrieval %v/%v from miner %s for %s: %v",
				// 	i,
				// 	len(candidates),
				// 	candidate.Miner,
				// 	candidate.RootCid,
				// 	err,
				// )
				// queriesLk.Unlock()
				return
			}

			queriesLk.Lock()

			queries = append(queries, candidateQuery{candidate: candidate, response: query})

			// logger.Infof(
			// 	"Retrieval query %v/%v succeeded from miner %s for %s",
			// 	i,
			// 	len(candidates),
			// 	candidate.Miner,
			// 	candidate.RootCid,
			// )

			queriesLk.Unlock()
		}(i, candidate)
	}

	wg.Wait()

	return queries
}

func totalCost(qres *retrievalmarket.QueryResponse) big.Int {
	return big.Add(big.Mul(qres.MinPricePerByte, big.NewIntUnsigned(qres.Size)), qres.UnsealPrice)
}
