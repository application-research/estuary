package transfer

import (
	"context"
	"fmt"
	"time"

	"github.com/application-research/estuary/constants"
	dealstatus "github.com/application-research/estuary/deal/status"
	"github.com/application-research/estuary/model"
	"github.com/application-research/estuary/util"
	"github.com/application-research/filclient"
	datatransfer "github.com/filecoin-project/go-data-transfer"
	"golang.org/x/xerrors"
)

func (m *manager) SubscribeEventListener(ctx context.Context) error {
	// Subscribe to data transfer events from Boost - we need this to get started and finished actual timestamps
	_, err := m.fc.Libp2pTransferMgr.Subscribe(func(dbid uint, fst filclient.ChannelState) {
		go func() {
			m.tcLk.Lock()
			trk, _ := m.trackingChannels[fst.ChannelID.String()]
			m.tcLk.Unlock()

			// if this state type is already announced, ignore it - rate limit events, only the most recent state is needed
			if trk != nil && trk.Last.Status == fst.Status {
				return
			}
			m.trackTransfer(&fst.ChannelID, dbid, &fst)

			switch fst.Status {
			case datatransfer.Requested:
				if err := m.SetDataTransferStartedOrFinished(ctx, dbid, fst.TransferID, &fst, true); err != nil {
					m.log.Errorf("failed to set data transfer started from event: %s", err)
				}
			case datatransfer.TransferFinished, datatransfer.Completed:
				if err := m.SetDataTransferStartedOrFinished(ctx, dbid, fst.TransferID, &fst, false); err != nil {
					m.log.Errorf("failed to set data transfer started from event: %s", err)
				}
			default:
				// for every other events
				trsFailed, msg := util.TransferFailed(&fst)
				if err := m.UpdateDataTransferStatus(ctx, dbid, fst.TransferID, &fst, trsFailed, msg); err != nil {
					m.log.Errorf("failed to set data transfer update from event: %s", err)
				}
			}
		}()
	})
	return err
}

func (m *manager) trackTransfer(chanid *datatransfer.ChannelID, dealdbid uint, st *filclient.ChannelState) {
	m.tcLk.Lock()
	defer m.tcLk.Unlock()
	m.trackingChannels[chanid.String()] = &util.ChanTrack{
		Dbid: dealdbid,
		Last: st,
	}
}

func (m *manager) SetDataTransferStartedOrFinished(ctx context.Context, dealDBID uint, chanIDOrTransferID string, st *filclient.ChannelState, isStarted bool) error {
	if st == nil {
		return nil
	}

	var deal model.ContentDeal
	if err := m.db.First(&deal, "id = ?", dealDBID).Error; err != nil {
		return err
	}

	var cont util.Content
	if err := m.db.First(&cont, "id = ?", deal.Content).Error; err != nil {
		return err
	}

	updates := map[string]interface{}{
		"dt_chan": chanIDOrTransferID,
	}

	switch isStarted {
	case true:
		updates["transfer_started"] = time.Now() // boost transfers does not support stages, so we can't get actual timestamps
		if s := st.Stages.GetStage("Requested"); s != nil {
			updates["transfer_started"] = s.CreatedTime.Time()
		}
	default:
		updates["transfer_finished"] = time.Now() // boost transfers does not support stages, so we can't get actual timestamps
		if s := st.Stages.GetStage("TransferFinished"); s != nil {
			updates["transfer_finished"] = s.CreatedTime.Time()
		}
	}

	if err := m.db.Model(model.ContentDeal{}).Where("id = ?", dealDBID).UpdateColumns(updates).Error; err != nil {
		return xerrors.Errorf("failed to update deal with channel ID: %w", err)
	}
	return nil
}

func (m *manager) UpdateDataTransferStatus(ctx context.Context, dealDBID uint, chanIDOrTransferID string, st *filclient.ChannelState, isFailed bool, msg string) error {
	if dealDBID == 0 {
		return fmt.Errorf("received transfer status update with no identifier")
	}

	var cd model.ContentDeal
	if err := m.db.First(&cd, "id = ?", dealDBID).Error; err != nil {
		return err
	}

	if cd.DTChan == "" {
		if err := m.db.Model(model.ContentDeal{}).Where("id = ?", dealDBID).UpdateColumns(map[string]interface{}{
			"dt_chan": chanIDOrTransferID,
		}).Error; err != nil {
			return err
		}
	}

	if isFailed {
		miner, err := cd.MinerAddr()
		if err != nil {
			return err
		}

		errMsg := fmt.Sprintf("status: %d(%s), message: %s", st.Status, msg, st.Message)
		if oerr := m.dealStatusUpdater.RecordDealFailure(&dealstatus.DealFailureError{
			Miner:               miner,
			Phase:               "data-transfer-remote",
			Message:             fmt.Sprintf("failure from shuttle %s: %s", constants.ContentLocationLocal, errMsg),
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
	}
	return nil
}
