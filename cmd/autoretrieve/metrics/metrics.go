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

type QueryResult struct {
	Duration time.Duration

	// Fill out whole struct even with non-nil error
	Err error
}

type RetrievalResult struct {
	Duration      time.Duration
	BytesReceived uint
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
	RecordWallet(WalletInfo)

	// Called once, at the beginning of a request
	RecordRequest(RequestInfo)

	// Called once, after getting candidates
	RecordGetCandidatesResult(count uint)

	// Called before each query
	RecordQuery(CandidateInfo)

	// Called after each query is finished
	RecordQueryResult(CandidateInfo, QueryResult)

	// Called before each retrieval attempt
	RecordRetrieval(CandidateInfo)

	// Called after each retrieval attempt
	RecordRetrievalResult(CandidateInfo, RetrievalResult)

	// Called once, at the end of a request
	RecordRequestResult(RequestInfo, RequestResult)
}
