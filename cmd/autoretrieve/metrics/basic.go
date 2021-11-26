package metrics

import (
	"fmt"

	"github.com/dustin/go-humanize"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-log/v2"
)

type Basic struct {
	logger log.EventLogger
}

func NewBasic(logger log.EventLogger) *Basic {
	return &Basic{
		logger: logger,
	}
}

func (metrics *Basic) RecordWallet(info WalletInfo) {
	if info.Err != nil {
		metrics.logger.Warnf("Could not load any default wallet address, only free retrievals will be attempted: %v", info.Err)
	} else {
		metrics.logger.Infof("Using default wallet address %s", info.Addr)
	}
}

func (metrics *Basic) RecordGetCandidatesResult(info RequestInfo, result GetCandidatesResult) {
	if result.Err != nil {
		metrics.logger.Errorf("Could not get candidates: %v", result.Err)
	}
}

func (metrics *Basic) RecordQuery(info CandidateInfo) {

}

func (metrics *Basic) RecordQueryResult(info CandidateInfo, result QueryResult) {
	if result.Err != nil {
		metrics.logger.Errorf(
			"Failed to query miner %s for %s (root %s): %v",
			info.Miner,
			shortCid(info.RequestCid),
			shortCid(info.RootCid),
			result.Err,
		)
	}
}

func (metrics *Basic) RecordRetrieval(info CandidateInfo) {
	metrics.logger.Infof(
		"Attempting retrieval from miner %s for %s (root %s)",
		info.Miner,
		shortCid(info.RequestCid),
		shortCid(info.RootCid),
	)
}

func (metrics *Basic) RecordRetrievalResult(info CandidateInfo, result RetrievalResult) {
	if result.Err != nil {
		metrics.logger.Errorf(
			"Failed to retrieve from miner %s for %s (root %s): %v",
			info.Miner,
			shortCid(info.RequestCid),
			shortCid(info.RootCid),
			result.Err,
		)
	} else {
		metrics.logger.Infof(
			"Successfully retrieved from miner %s for %s (root %s)\n"+
				"\tDuration: %s\n"+
				"\tBytes Received: %s\n"+
				"\tTotal Payment: %s",
			info.Miner,
			shortCid(info.RequestCid),
			shortCid(info.RootCid),
			result.Duration,
			humanize.IBytes(result.BytesReceived),
			result.TotalPayment,
		)
	}
}

func shortCid(cid cid.Cid) string {
	str := cid.String()
	return fmt.Sprintf("...%s", str[len(str)-10:])
}
