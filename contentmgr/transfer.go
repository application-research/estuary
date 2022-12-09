package contentmgr

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/application-research/estuary/constants"
	"github.com/application-research/estuary/drpc"
	"github.com/application-research/estuary/model"
	"github.com/application-research/estuary/util"
	"github.com/application-research/filclient"
	datatransfer "github.com/filecoin-project/go-data-transfer"
	"github.com/google/uuid"
	"github.com/ipfs/go-cid"
)

func (cm *ContentManager) RestartAllTransfersForLocation(ctx context.Context, loc string) error {
	var deals []model.ContentDeal
	if err := cm.db.Model(model.ContentDeal{}).
		Joins("left join contents on contents.id = content_deals.content").
		Where("not content_deals.failed and content_deals.deal_id = 0 and content_deals.dt_chan != '' and location = ?", loc).
		Scan(&deals).Error; err != nil {
		return err
	}

	go func() {
		for _, d := range deals {
			chid, err := d.ChannelID()
			if err != nil {
				// Only legacy (push) transfers need to be restarted by Estuary.
				// Newer (pull) transfers are restarted by the Storage Provider.
				// So if it's not a legacy channel ID, ignore it.
				continue
			}

			if err := cm.RestartTransfer(ctx, loc, chid, d); err != nil {
				cm.log.Errorf("failed to restart transfer: %s", err)
				continue
			}
		}
	}()
	return nil
}

// RestartTransfer tries to resume incomplete data transfers between client and storage providers.
// It supports only legacy deals (PushTransfer)
func (cm *ContentManager) RestartTransfer(ctx context.Context, loc string, chanid datatransfer.ChannelID, d model.ContentDeal) error {
	maddr, err := d.MinerAddr()
	if err != nil {
		return err
	}

	var dealUUID *uuid.UUID
	if d.DealUUID != "" {
		parsed, err := uuid.Parse(d.DealUUID)
		if err != nil {
			return fmt.Errorf("parsing deal uuid %s: %w", d.DealUUID, err)
		}
		dealUUID = &parsed
	}

	_, isPushTransfer, err := cm.GetProviderDealStatus(ctx, &d, maddr, dealUUID)
	if err != nil {
		return err
	}

	if !isPushTransfer {
		return nil
	}

	if loc == constants.ContentLocationLocal {
		// get the deal data transfer state pull deals
		st, err := cm.filClient.TransferStatus(ctx, &chanid)
		if err != nil && err != filclient.ErrNoTransferFound {
			return err
		}

		if st == nil {
			return fmt.Errorf("no data transfer state was found")
		}

		cannotRestart := !util.CanRestartTransfer(st)
		if cannotRestart {
			trsFailed, msg := util.TransferFailed(st)
			if trsFailed {
				if err := cm.db.Model(model.ContentDeal{}).Where("id = ?", d.ID).UpdateColumns(map[string]interface{}{
					"failed":    true,
					"failed_at": time.Now(),
				}).Error; err != nil {
					return err
				}
				errMsg := fmt.Sprintf("status: %d(%s), message: %s", st.Status, msg, st.Message)
				return fmt.Errorf("deal in database is in progress, but data transfer is terminated: %s", errMsg)
			}
			return nil
		}
		return cm.filClient.RestartTransfer(ctx, &chanid)
	}
	return cm.sendRestartTransferCmd(ctx, loc, chanid, d)
}

func (cm *ContentManager) sendRestartTransferCmd(ctx context.Context, loc string, chanid datatransfer.ChannelID, d model.ContentDeal) error {
	return cm.SendShuttleCommand(ctx, loc, &drpc.Command{
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

type transferStatusRecord struct {
	State    *filclient.ChannelState
	Shuttle  string
	Received time.Time
}

func (cm *ContentManager) GetTransferStatus(ctx context.Context, d *model.ContentDeal, contCID cid.Cid, contLoc string) (*filclient.ChannelState, error) {
	ctx, span := cm.tracer.Start(ctx, "getTransferStatus")
	defer span.End()

	if d.DTChan == "" {
		return nil, nil
	}

	if contLoc == constants.ContentLocationLocal {
		chanst, err := cm.transferStatusByID(ctx, d.DTChan)
		if err != nil {
			return nil, err
		}
		return chanst, nil
	}

	val, ok := cm.remoteTransferStatus.Get(d.ID)
	if !ok {
		if err := cm.sendRequestTransferStatusCmd(ctx, contLoc, d.ID, d.DTChan); err != nil {
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

func (cm *ContentManager) sendRequestTransferStatusCmd(ctx context.Context, loc string, dealid uint, chid string) error {
	return cm.SendShuttleCommand(ctx, loc, &drpc.Command{
		Op: drpc.CMD_ReqTxStatus,
		Params: drpc.CmdParams{
			ReqTxStatus: &drpc.ReqTxStatus{
				DealDBID: dealid,
				ChanID:   chid,
			},
		},
	})
}

// get the data transfer state by transfer ID (compatible with both deal protocol v1 and v2)
func (cm *ContentManager) transferStatusByID(ctx context.Context, id string) (*filclient.ChannelState, error) {
	chanst, err := cm.filClient.TransferStatusByID(ctx, id)
	if err != nil && err != filclient.ErrNoTransferFound && !strings.Contains(err.Error(), "No channel for channel ID") && !strings.Contains(err.Error(), "datastore: key not found") {
		return nil, err
	}
	return chanst, nil
}

func (cm *ContentManager) updateTransferStatus(ctx context.Context, loc string, dealdbid uint, st *filclient.ChannelState) {
	cm.remoteTransferStatus.Add(dealdbid, &transferStatusRecord{
		State:    st,
		Shuttle:  loc,
		Received: time.Now(),
	})
}
