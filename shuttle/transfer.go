package shuttle

import (
	"context"

	rpcevent "github.com/application-research/estuary/shuttle/rpc/event"

	"github.com/application-research/estuary/model"
	"github.com/application-research/filclient"
	datatransfer "github.com/filecoin-project/go-data-transfer"
	"github.com/ipfs/go-cid"
)

func (m *manager) StartTransfer(ctx context.Context, loc string, cd *model.ContentDeal, datacid cid.Cid) error {
	miner, err := cd.MinerAddr()
	if err != nil {
		return err
	}

	return m.sendRPCMessage(ctx, loc, &rpcevent.Command{
		Op: rpcevent.CMD_StartTransfer,
		Params: rpcevent.CmdParams{
			StartTransfer: &rpcevent.StartTransfer{
				DealDBID:  cd.ID,
				ContentID: cd.Content,
				Miner:     miner,
				PropCid:   cd.PropCid.CID,
				DataCid:   datacid,
			},
		},
	})
}

func (m *manager) RequestTransferStatus(ctx context.Context, loc string, dealid uint, chid string) error {
	return m.sendRPCMessage(ctx, loc, &rpcevent.Command{
		Op: rpcevent.CMD_ReqTxStatus,
		Params: rpcevent.CmdParams{
			ReqTxStatus: &rpcevent.ReqTxStatus{
				DealDBID: dealid,
				ChanID:   chid,
			},
		},
	})
}

func (m *manager) RestartTransfer(ctx context.Context, loc string, chanid datatransfer.ChannelID, d model.ContentDeal) error {
	return m.sendRPCMessage(ctx, loc, &rpcevent.Command{
		Op: rpcevent.CMD_RestartTransfer,
		Params: rpcevent.CmdParams{
			RestartTransfer: &rpcevent.RestartTransfer{
				ChanID:    chanid,
				DealDBID:  d.ID,
				ContentID: d.Content,
			},
		},
	})
}

func (m *manager) GetTransferStatus(ctx context.Context, contLoc string, d *model.ContentDeal) (*filclient.ChannelState, error) {
	st, err := m.rpcMgr.GetTransferStatus(d.ID)
	if err != nil {
		return nil, err
	}

	if st != nil {
		return st, err
	}
	return nil, m.RequestTransferStatus(ctx, contLoc, d.ID, d.DTChan)
}
