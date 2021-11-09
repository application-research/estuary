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
	"sync"
	"time"

	"github.com/application-research/estuary/cmd/autoretrieve/blocks"
	"github.com/application-research/filclient"
	"github.com/application-research/filclient/keystore"
	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-data-transfer/channelmonitor"
	"github.com/filecoin-project/go-fil-markets/retrievalmarket"
	"github.com/filecoin-project/lotus/api"
	"github.com/filecoin-project/lotus/chain/wallet"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	gsimpl "github.com/ipfs/go-graphsync/impl"
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
	DataDir        string
	Endpoint       string
	MinerBlacklist map[address.Address]bool
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

	const maxTraversalLinks = 32 * (1 << 20)
	filClient, err := filclient.NewClientWithConfig(&filclient.Config{
		DataDir:    config.DataDir,
		Api:        api,
		Wallet:     wallet,
		Addr:       walletAddr,
		Blockstore: blockManager,
		Datastore:  datastore,
		Host:       host,
		GraphsyncOpts: []gsimpl.Option{
			gsimpl.MaxInProgressIncomingRequests(200),
			gsimpl.MaxInProgressOutgoingRequests(200),
			gsimpl.MaxMemoryResponder(8 << 30),
			gsimpl.MaxMemoryPerPeerResponder(32 << 20),
			gsimpl.MaxInProgressIncomingRequestsPerPeer(20),
			gsimpl.MessageSendRetries(2),
			gsimpl.SendMessageTimeout(2 * time.Minute),
			gsimpl.MaxLinksPerIncomingRequests(maxTraversalLinks),
			gsimpl.MaxLinksPerOutgoingRequests(maxTraversalLinks),
		},
		ChannelMonitorConfig: channelmonitor.Config{

			AcceptTimeout:          time.Hour * 24,
			RestartDebounce:        time.Second * 10,
			RestartBackoff:         time.Second * 20,
			MaxConsecutiveRestarts: 15,
			//RestartAckTimeout:      time.Second * 30,
			CompleteTimeout: time.Minute * 40,

			// Called when a restart completes successfully
			//OnRestartComplete func(id datatransfer.ChannelID)
		},
	})
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
func (retriever *Retriever) retrieveFromBestCandidate(ctx context.Context, candidates []retrievalCandidate) {
	queries := retriever.queryCandidates(ctx, candidates)

	for _, query := range queries {
		if err := retriever.registerRunningRetrieval(query.candidate.RootCid); err != nil {
			logger.Errorf("")
		}

		retriever.unregisterRunningRetrieval(query.candidate.RootCid)
	}
}

// Will register a retrieval as running, or ErrRetrievalAlreadyRunning if the
// CID is already registered.
func (retriever *Retriever) registerRunningRetrieval(cid cid.Cid) error {
	retriever.runningRetrievalsLk.Lock()
	defer retriever.runningRetrievalsLk.Unlock()

	if _, ok := retriever.runningRetrievals[cid]; ok {
		return fmt.Errorf("could not register running retrieval: %w", ErrRetrievalAlreadyRunning)
	}
	retriever.runningRetrievals[cid] = true

	return nil
}

// Unregisters a running retrieval. No-op if no retrieval is running.
func (retriever *Retriever) unregisterRunningRetrieval(cid cid.Cid) {
	retriever.runningRetrievalsLk.Lock()
	delete(retriever.runningRetrievals, cid)
	retriever.runningRetrievalsLk.Unlock()
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
	checked := 0
	var queries []candidateQuery
	var queriesLk sync.Mutex

	var wg sync.WaitGroup
	wg.Add(len(candidates))

	for _, candidate := range candidates {
		go func(candidate retrievalCandidate) {
			defer wg.Done()

			query, err := retriever.filClient.RetrievalQuery(ctx, candidate.Miner, candidate.RootCid)
			if err != nil {
				queriesLk.Lock()
				checked++
				logger.Errorf(
					"Failed to query retrieval %v/%v from miner %s for %s: %v",
					checked,
					len(candidates),
					candidate.Miner,
					candidate.RootCid,
					err,
				)
				queriesLk.Unlock()
				return
			}

			queriesLk.Lock()

			queries = append(queries, candidateQuery{candidate: candidate, response: query})
			checked++

			logger.Infof(
				"Retrieval query %v/%v succeeded from miner %s for %s",
				checked,
				len(candidates),
				candidate.Miner,
				candidate.RootCid,
			)

			queriesLk.Unlock()
		}(candidate)
	}

	wg.Wait()

	return queries
}
