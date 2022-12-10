package shuttle

import (
	"context"
	"fmt"
	"time"

	"github.com/application-research/estuary/constants"
	dealstatus "github.com/application-research/estuary/deal/status"
	"github.com/application-research/estuary/drpc"
	"github.com/application-research/estuary/model"
	"github.com/application-research/filclient"
	datatransfer "github.com/filecoin-project/go-data-transfer"
	"github.com/ipfs/go-cid"
)

type transferStatusRecord struct {
	State    *filclient.ChannelState
	Shuttle  string
	Received time.Time
}

func (m *manager) StartTransfer(ctx context.Context, loc string, cd *model.ContentDeal, datacid cid.Cid) error {
	miner, err := cd.MinerAddr()
	if err != nil {
		return err
	}

	return m.sendRPCMessage(ctx, loc, &drpc.Command{
		Op: drpc.CMD_StartTransfer,
		Params: drpc.CmdParams{
			StartTransfer: &drpc.StartTransfer{
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
	return m.sendRPCMessage(ctx, loc, &drpc.Command{
		Op: drpc.CMD_ReqTxStatus,
		Params: drpc.CmdParams{
			ReqTxStatus: &drpc.ReqTxStatus{
				DealDBID: dealid,
				ChanID:   chid,
			},
		},
	})
}

func (m *manager) RestartTransfer(ctx context.Context, loc string, chanid datatransfer.ChannelID, d model.ContentDeal) error {
	return m.sendRPCMessage(ctx, loc, &drpc.Command{
		Op: drpc.CMD_RestartTransfer,
		Params: drpc.CmdParams{
			RestartTransfer: &drpc.RestartTransfer{
				ChanID:    chanid,
				DealDBID:  d.ID,
				ContentID: d.Content,
			},
		},
	})
}

func (m *manager) GetTransferStatus(ctx context.Context, contLoc string, d *model.ContentDeal) (*filclient.ChannelState, error) {
	val, ok := m.transferStatuses.Get(d.ID)
	if !ok {
		if err := m.RequestTransferStatus(ctx, contLoc, d.ID, d.DTChan); err != nil {
			return nil, err
		}
		return nil, nil
	}

	tsr, ok := val.(*transferStatusRecord)
	if !ok {
		return nil, fmt.Errorf("invalid type placed in remote transfer status cache: %T", val)
	}
	return tsr.State, nil
}

func (m *manager) handleRpcTransferStarted(ctx context.Context, handle string, param *drpc.TransferStartedOrFinished) error {
	if err := m.transferStatusUpdater.SetDataTransferStartedOrFinished(ctx, param.DealDBID, param.Chanid, param.State, true); err != nil {
		return err
	}
	m.log.Debugw("Started data transfer on shuttle", "chanid", param.Chanid, "shuttle", handle)
	return nil
}

func (m *manager) handleRpcTransferFinished(ctx context.Context, handle string, param *drpc.TransferStartedOrFinished) error {
	if err := m.transferStatusUpdater.SetDataTransferStartedOrFinished(ctx, param.DealDBID, param.Chanid, param.State, false); err != nil {
		return err
	}
	m.log.Debugw("Finished data transfer on shuttle", "chanid", param.Chanid, "shuttle", handle)
	return nil
}

func (m *manager) HandleRpcTransferStatus(ctx context.Context, handle string, param *drpc.TransferStatus) error {
	if param.DealDBID == 0 {
		return fmt.Errorf("received transfer status update with no identifier")
	}

	var cd model.ContentDeal
	if err := m.db.First(&cd, "id = ?", param.DealDBID).Error; err != nil {
		return err
	}

	if cd.DTChan == "" {
		if err := m.db.Model(model.ContentDeal{}).Where("id = ?", param.DealDBID).UpdateColumns(map[string]interface{}{
			"dt_chan": param.Chanid,
		}).Error; err != nil {
			return err
		}
	}

	if param.Failed {
		miner, err := cd.MinerAddr()
		if err != nil {
			return err
		}

		if oerr := m.dealStatusUpdater.RecordDealFailure(&dealstatus.DealFailureError{
			Miner:               miner,
			Phase:               "data-transfer-remote",
			Message:             fmt.Sprintf("failure from shuttle %s: %s", handle, param.Message),
			Content:             cd.Content,
			UserID:              cd.UserID,
			MinerVersion:        cd.MinerVersion,
			DealProtocolVersion: cd.DealProtocolVersion,
			DealUUID:            cd.DealUUID,
		}); oerr != nil {
			return oerr
		}

		if err := m.db.Model(model.ContentDeal{}).Where("id = ?", cd.ID).UpdateColumns(map[string]interface{}{
			"failed":    true,
			"failed_at": time.Now(),
		}).Error; err != nil {
			return err
		}

		sts := datatransfer.Failed
		if param.State != nil {
			sts = param.State.Status
		}

		param.State = &filclient.ChannelState{
			Status:  sts,
			Message: fmt.Sprintf("failure from shuttle %s: %s", handle, param.Message),
		}
	}

	// update transfer state for only shuttles
	if handle != constants.ContentLocationLocal {
		m.updateTransferStatus(ctx, handle, cd.ID, param.State)
	}
	return nil
}

func (m *manager) updateTransferStatus(ctx context.Context, loc string, dealdbid uint, st *filclient.ChannelState) {
	m.transferStatuses.Add(dealdbid, &transferStatusRecord{
		State:    st,
		Shuttle:  loc,
		Received: time.Now(),
	})
}
