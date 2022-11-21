package contentmgr

import (
	"context"

	"github.com/application-research/estuary/model"
	"github.com/application-research/estuary/util"
	"github.com/application-research/filclient"
	"github.com/application-research/filclient/retrievehelper"
	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-fil-markets/retrievalmarket"
	"github.com/ipfs/go-cid"
	"github.com/labstack/gommon/log"
)

func (cm *ContentManager) RecordRetrievalFailure(rfr *util.RetrievalFailureRecord) error {
	return cm.DB.Create(rfr).Error
}

func (cm *ContentManager) TryRetrieve(ctx context.Context, maddr address.Address, c cid.Cid, ask *retrievalmarket.QueryResponse) error {
	proposal, err := retrievehelper.RetrievalProposalForAsk(ask, c, nil)
	if err != nil {
		return err
	}

	stats, err := cm.FilClient.RetrieveContent(ctx, maddr, proposal)
	if err != nil {
		return err
	}

	cm.RecordRetrievalSuccess(c, maddr, stats)
	return nil
}

func (cm *ContentManager) RecordRetrievalSuccess(cc cid.Cid, m address.Address, rstats *filclient.RetrievalStats) {
	if err := cm.DB.Create(&model.RetrievalSuccessRecord{
		Cid:          util.DbCID{CID: cc},
		Miner:        m.String(),
		Peer:         rstats.Peer.String(),
		Size:         rstats.Size,
		DurationMs:   rstats.Duration.Milliseconds(),
		AverageSpeed: rstats.AverageSpeed,
		TotalPayment: rstats.TotalPayment.String(),
		NumPayments:  rstats.NumPayments,
		AskPrice:     rstats.AskPrice.String(),
	}).Error; err != nil {
		log.Errorf("failed to write retrieval success record: %s", err)
	}
}
