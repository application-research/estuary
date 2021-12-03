package metrics

import (
	"time"

	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/lotus/chain/types"
	"github.com/ipfs/go-cid"
)

type WalletInfo struct {
	Err  error
	Addr address.Address
}

type RequestInfo struct {
	RequestCid cid.Cid
}

// Can be used to identify a specific candidate within a request
type CandidateInfo struct {
	RequestInfo

	// The root CID that will be retrieved from the miner
	RootCid cid.Cid

	// Miner to retrieve from
	Miner address.Address
}

type GetCandidatesResult struct {
	Err   error
	Count uint
}

type QueryResult struct {
	// Fill out whole struct even with non-nil error
	Err error
}

type RetrievalResult struct {
	Duration      time.Duration
	BytesReceived uint64
	TotalPayment  types.FIL

	// Fill out whole struct even with non-nil error
	Err error
}

type RequestResult struct {
	Duration     time.Duration
	TotalPayment types.FIL

	// Fill out whole struct even with non-nil error
	Err error
}

type Metrics interface {
	// Called whenever the wallet is set
	RecordWallet(WalletInfo)

	// Called once, after getting candidates
	RecordGetCandidatesResult(RequestInfo, GetCandidatesResult)

	// Called before each query
	RecordQuery(CandidateInfo)

	// Called after each query is finished
	RecordQueryResult(CandidateInfo, QueryResult)

	// Called before each retrieval attempt
	RecordRetrieval(CandidateInfo)

	// Called after each retrieval attempt
	RecordRetrievalResult(CandidateInfo, RetrievalResult)
}

type Noop struct{}

func (metrics *Noop) RecordWallet(WalletInfo)                                    {}
func (metrics *Noop) RecordGetCandidatesResult(RequestInfo, GetCandidatesResult) {}
func (metrics *Noop) RecordQuery(CandidateInfo)                                  {}
func (metrics *Noop) RecordQueryResult(CandidateInfo, QueryResult)               {}
func (metrics *Noop) RecordRetrieval(CandidateInfo)                              {}
func (metrics *Noop) RecordRetrievalResult(CandidateInfo, RetrievalResult)       {}
