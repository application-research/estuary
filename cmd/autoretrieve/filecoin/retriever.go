package filecoin

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"path"
	"sort"
	"sync"
	"time"

	"github.com/application-research/estuary/cmd/autoretrieve/blocks"
	"github.com/application-research/estuary/cmd/autoretrieve/metrics"
	"github.com/application-research/filclient"
	"github.com/application-research/filclient/retrievehelper"
	"github.com/dustin/go-humanize"
	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-fil-markets/retrievalmarket"
	"github.com/filecoin-project/go-state-types/big"
	"github.com/filecoin-project/lotus/api"
	"github.com/filecoin-project/lotus/chain/types"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	"github.com/libp2p/go-libp2p-core/host"
)

var (
	ErrNoCandidates                = errors.New("no candidates")
	ErrRetrievalAlreadyRunning     = errors.New("retrieval already running")
	ErrHitRetrievalLimit           = errors.New("hit retrieval limit")
	ErrInvalidEndpointURL          = errors.New("invalid endpoint URL")
	ErrEndpointRequestFailed       = errors.New("endpoint request failed")
	ErrEndpointBodyInvalid         = errors.New("endpoint body invalid")
	ErrProposalCreationFailed      = errors.New("proposal creation failed")
	ErrRetrievalRegistrationFailed = errors.New("retrieval registration failed")
	ErrRetrievalFailed             = errors.New("retrieval failed")
	ErrAllRetrievalsFailed         = errors.New("all retrievals failed")
)

// All config values should be safe to leave uninitialized
type RetrieverConfig struct {
	MinerBlacklist         map[address.Address]bool
	MinerWhitelist         map[address.Address]bool
	PerMinerRetrievalLimit uint
	RetrievalTimeout       time.Duration // Set to zero to disable
	Metrics                metrics.Metrics
}

type Retriever struct {
	config    RetrieverConfig
	endpoint  string
	filClient *filclient.FilClient

	runningRetrievals        map[cid.Cid]bool
	activeRetrievalsPerMiner map[address.Address]uint
	runningRetrievalsLk      sync.Mutex
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

// Possible errors: ErrInitKeystoreFailed, ErrInitWalletFailed,
// ErrInitFilClientFailed
func NewRetriever(
	config RetrieverConfig,
	filClient *filclient.FilClient,
	endpoint string,
	host host.Host,
	api api.Gateway,
	datastore datastore.Batching,
	blockManager *blocks.Manager,
) (*Retriever, error) {

	if config.Metrics == nil {
		config.Metrics = &metrics.Noop{}
	}

	retriever := &Retriever{
		config:                   config,
		endpoint:                 endpoint,
		filClient:                filClient,
		runningRetrievals:        make(map[cid.Cid]bool),
		activeRetrievalsPerMiner: make(map[address.Address]uint),
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
//
// Possible errors: ErrInvalidEndpointURL, ErrEndpointRequestFailed,
// ErrEndpointBodyInvalid, ErrNoCandidates
func (retriever *Retriever) Request(cid cid.Cid) error {

	requestInfo := metrics.RequestInfo{
		RequestCid: cid,
	}

	// TODO: before looking up candidates from the endpoint, we could cache
	// candidates and use that cached info. We only really have to look up an
	// up-to-date candidate list from the endpoint if we need to begin a new
	// retrieval.
	candidates, err := retriever.lookupCandidates(cid)
	retriever.config.Metrics.RecordGetCandidatesResult(requestInfo, metrics.GetCandidatesResult{
		Err: err,
	})
	if err != nil {
		return fmt.Errorf("could not get retrieval candidates for %s: %w", cid, err)
	}

	// If we got to this point, one or more candidates have been found and we
	// are good to go ahead with the retrieval
	go retriever.retrieveFromBestCandidate(context.Background(), cid, candidates)

	return nil
}

// Takes an unsorted list of candidates, orders them, and attempts retrievals in
// serial until one succeeds.
//
// Possible errors: ErrAllRetrievalsFailed
func (retriever *Retriever) retrieveFromBestCandidate(ctx context.Context, cid cid.Cid, candidates []retrievalCandidate) error {
	queries := retriever.queryCandidates(ctx, cid, candidates)

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
	for _, query := range queries {
		candidateInfo := metrics.CandidateInfo{
			RequestInfo: metrics.RequestInfo{RequestCid: cid},
			RootCid:     query.candidate.RootCid,
			Miner:       query.candidate.Miner,
		}
		if err := retriever.tryRegisterRunningRetrieval(query.candidate.RootCid, query.candidate.Miner); err != nil {
			// TODO: send some info to metrics about this

			if errors.Is(err, ErrRetrievalAlreadyRunning) {
				break
			}

			continue
		}
		retriever.config.Metrics.RecordRetrieval(candidateInfo)
		stats_, err := retriever.retrieve(ctx, query)
		if err != nil {
			// TODO: this should not have to be separate
			retriever.config.Metrics.RecordRetrievalResult(candidateInfo, metrics.RetrievalResult{
				Duration:      0,
				BytesReceived: 0,
				TotalPayment:  types.FIL(big.Zero()),
				Err:           err,
			})
		} else {
			retriever.config.Metrics.RecordRetrievalResult(candidateInfo, metrics.RetrievalResult{
				Duration:      stats_.Duration,
				BytesReceived: stats_.Size,
				TotalPayment:  types.FIL(stats_.TotalPayment),
				Err:           err,
			})

		}

		retriever.unregisterRunningRetrieval(query.candidate.RootCid, query.candidate.Miner)

		if err != nil {
			continue
		}

		stats = stats_

		break
	}

	if stats == nil {
		return ErrAllRetrievalsFailed
	}

	return nil
}

// Possible errors: ErrRetrievalRegistrationFailed, ErrProposalCreationFailed,
// ErrRetrievalFailed
func (retriever *Retriever) retrieve(ctx context.Context, query candidateQuery) (*filclient.RetrievalStats, error) {
	proposal, err := retrievehelper.RetrievalProposalForAsk(query.response, query.candidate.RootCid, nil)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrProposalCreationFailed, err)
	}

	startTime := time.Now()

	retrieveCtx, retrieveCancel := context.WithCancel(ctx)
	defer retrieveCancel()
	var lastBytesReceived uint64 = 0
	var doneLk sync.Mutex
	done := false
	timedOut := false
	var lastBytesReceivedTimer *time.Timer

	// Start the timeout tracker only if retrieval timeout isn't 0
	if retriever.config.RetrievalTimeout != 0 {
		lastBytesReceivedTimer = time.AfterFunc(retriever.config.RetrievalTimeout, func() {
			doneLk.Lock()
			done = true
			doneLk.Unlock()

			retrieveCancel()
			timedOut = true
		})
	}
	stats, err := retriever.filClient.RetrieveContentWithProgressCallback(retrieveCtx, query.candidate.Miner, proposal, func(bytesReceived uint64) {
		if lastBytesReceivedTimer != nil {
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
		}
	})
	if timedOut {
		err = fmt.Errorf(
			"timed out after not receiving data for %s (started %s ago, stopped at %s)",
			retriever.config.RetrievalTimeout,
			time.Since(startTime),
			humanize.IBytes(lastBytesReceived),
		)
	}

	lastBytesReceivedTimer.Stop()
	doneLk.Lock()
	done = true
	doneLk.Unlock()

	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrRetrievalFailed, err)
	}

	return stats, nil
}

// Possible errors: ErrRetrievalAlreadyRunning, ErrHitRetrievalLimit
func (retriever *Retriever) tryRegisterRunningRetrieval(cid cid.Cid, miner address.Address) error {
	retriever.runningRetrievalsLk.Lock()
	defer retriever.runningRetrievalsLk.Unlock()

	// If limit is enabled (non-zero) and we have already hit it, we can't
	// allow this retrieval to start
	noLimit := retriever.config.PerMinerRetrievalLimit == 0
	atLimit := retriever.activeRetrievalsPerMiner[miner] >= retriever.config.PerMinerRetrievalLimit
	if !noLimit && atLimit {
		return ErrHitRetrievalLimit
	}

	if retriever.runningRetrievals[cid] {
		return ErrRetrievalAlreadyRunning
	}

	retriever.runningRetrievals[cid] = true
	retriever.activeRetrievalsPerMiner[miner] = retriever.activeRetrievalsPerMiner[miner] + 1

	return nil
}

// Unregisters a running retrieval. No-op if no retrieval is running.
func (retriever *Retriever) unregisterRunningRetrieval(cid cid.Cid, miner address.Address) {
	retriever.runningRetrievalsLk.Lock()
	defer retriever.runningRetrievalsLk.Unlock()

	delete(retriever.runningRetrievals, cid)
	retriever.activeRetrievalsPerMiner[miner] = retriever.activeRetrievalsPerMiner[miner] - 1
	if retriever.activeRetrievalsPerMiner[miner] == 0 {
		delete(retriever.activeRetrievalsPerMiner, miner)
	}
}

// Returns a list of miners known to have the requested block, with blacklisted
// miners filtered out.
//
// Possible errors - ErrInvalidEndpointURL, ErrEndpointRequestFailed, ErrEndpointBodyInvalid,
// ErrNoCandidates
func (retriever *Retriever) lookupCandidates(cid cid.Cid) ([]retrievalCandidate, error) {
	// Create URL with CID
	endpointURL, err := url.Parse(retriever.endpoint)
	if err != nil {
		return nil, fmt.Errorf("%w: '%s'", ErrInvalidEndpointURL, retriever.endpoint)
	}
	endpointURL.Path = path.Join(endpointURL.Path, cid.String())

	// Request candidates from endpoint
	resp, err := http.Get(endpointURL.String())
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrEndpointRequestFailed, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("%w: %s sent status %v", ErrEndpointRequestFailed, endpointURL, resp.StatusCode)
	}

	// Read candidate list from response body
	var unfiltered []retrievalCandidate
	if err := json.NewDecoder(resp.Body).Decode(&unfiltered); err != nil {
		return nil, ErrEndpointBodyInvalid
	}

	// Remove blacklisted miners, or non-whitelisted miners
	var res []retrievalCandidate
	for _, candidate := range unfiltered {
		// Skip blacklist
		if retriever.config.MinerBlacklist[candidate.Miner] {
			continue
		}

		// Skip non-whitelist IF the whitelist isn't empty
		if len(retriever.config.MinerWhitelist) > 0 && !retriever.config.MinerWhitelist[candidate.Miner] {
			continue
		}

		res = append(res, candidate)
	}

	return res, nil
}

func (retriever *Retriever) queryCandidates(ctx context.Context, cid cid.Cid, candidates []retrievalCandidate) []candidateQuery {
	var queries []candidateQuery
	var queriesLk sync.Mutex

	var wg sync.WaitGroup
	wg.Add(len(candidates))

	for i, candidate := range candidates {
		go func(i int, candidate retrievalCandidate) {
			defer wg.Done()

			candidateInfo := metrics.CandidateInfo{
				RequestInfo: metrics.RequestInfo{RequestCid: cid},
				RootCid:     candidate.RootCid,
				Miner:       candidate.Miner,
			}

			retriever.config.Metrics.RecordQuery(candidateInfo)
			query, err := retriever.filClient.RetrievalQuery(ctx, candidate.Miner, candidate.RootCid)
			retriever.config.Metrics.RecordQueryResult(candidateInfo, metrics.QueryResult{
				Err: err,
			})
			if err != nil {
				return
			}

			queriesLk.Lock()
			queries = append(queries, candidateQuery{candidate: candidate, response: query})
			queriesLk.Unlock()
		}(i, candidate)
	}

	wg.Wait()

	return queries
}

func totalCost(qres *retrievalmarket.QueryResponse) big.Int {
	return big.Add(big.Mul(qres.MinPricePerByte, big.NewIntUnsigned(qres.Size)), qres.UnsealPrice)
}
