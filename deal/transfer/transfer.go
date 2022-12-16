package transfer

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/application-research/estuary/constants"
	dealstatus "github.com/application-research/estuary/deal/status"
	"github.com/application-research/estuary/model"
	"github.com/application-research/estuary/shuttle"
	"github.com/application-research/estuary/util"
	"github.com/application-research/filclient"
	"github.com/filecoin-project/go-address"
	datatransfer "github.com/filecoin-project/go-data-transfer"
	"github.com/filecoin-project/go-fil-markets/storagemarket"
	"github.com/google/uuid"
	"github.com/ipfs/go-cid"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
	"golang.org/x/xerrors"
	"gorm.io/gorm"
)

type IManager interface {
	RestartAllTransfersForLocation(ctx context.Context, loc string, done chan struct{}) error
	RestartTransfer(ctx context.Context, loc string, chanid datatransfer.ChannelID, d model.ContentDeal) error
	GetProviderDealStatus(ctx context.Context, d *model.ContentDeal, maddr address.Address, dealUUID *uuid.UUID) (*storagemarket.ProviderDealState, bool, error)
	GetTransferStatus(ctx context.Context, d *model.ContentDeal, contCID cid.Cid, contLoc string) (*filclient.ChannelState, error)
	StartDataTransfer(ctx context.Context, cd *model.ContentDeal) error
	SetDataTransferStartedOrFinished(ctx context.Context, dealDBID uint, chanIDOrTransferID string, st *filclient.ChannelState, isStarted bool) error
	UpdateDataTransferStatus(ctx context.Context, dealDBID uint, chanIDOrTransferID string, st *filclient.ChannelState, isFailed bool, msg string) error
	SubscribeEventListener(ctx context.Context) error
}

type manager struct {
	db                *gorm.DB
	log               *zap.SugaredLogger
	fc                *filclient.FilClient
	tracer            trace.Tracer
	shuttleMgr        shuttle.IManager
	dealStatusUpdater dealstatus.IUpdater
	tcLk              sync.Mutex
	trackingChannels  map[string]*util.ChanTrack
}

func NewManager(db *gorm.DB, fc *filclient.FilClient, log *zap.SugaredLogger, shuttleMgr shuttle.IManager) IManager {
	return &manager{
		db:                db,
		log:               log,
		fc:                fc,
		tracer:            otel.Tracer("replicator"),
		shuttleMgr:        shuttleMgr,
		dealStatusUpdater: dealstatus.NewUpdater(db, log),
		trackingChannels:  make(map[string]*util.ChanTrack),
	}
}

func (m *manager) RestartAllTransfersForLocation(ctx context.Context, loc string, done chan struct{}) error {
	var deals []model.ContentDeal
	if err := m.db.Model(model.ContentDeal{}).
		Joins("left join contents on contents.id = content_deals.content").
		Where("not content_deals.failed and content_deals.deal_id = 0 and content_deals.dt_chan != '' and location = ?", loc).
		Scan(&deals).Error; err != nil {
		return err
	}

	go func() {
		for _, d := range deals {
			select {
			case <-done:
				return
			default:

			}

			chid, err := d.ChannelID()
			if err != nil {
				// Only legacy (push) transfers need to be restarted by Estuary.
				// Newer (pull) transfers are restarted by the Storage Provider.
				// So if it's not a legacy channel ID, ignore it.
				continue
			}

			if err := m.RestartTransfer(ctx, loc, chid, d); err != nil {
				m.log.Errorf("failed to restart transfer: %s", err)
				continue
			}
		}
	}()
	return nil
}

// RestartTransfer tries to resume incomplete data transfers between client and storage providers.
// It supports only legacy deals (PushTransfer)
func (m *manager) RestartTransfer(ctx context.Context, loc string, chanid datatransfer.ChannelID, d model.ContentDeal) error {
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

	_, isPushTransfer, err := m.GetProviderDealStatus(ctx, &d, maddr, dealUUID)
	if err != nil {
		return err
	}

	if !isPushTransfer {
		return nil
	}

	if loc == constants.ContentLocationLocal {
		// get the deal data transfer state pull deals
		st, err := m.fc.TransferStatus(ctx, &chanid)
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
				if err := m.db.Model(model.ContentDeal{}).Where("id = ?", d.ID).UpdateColumns(map[string]interface{}{
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
		return m.fc.RestartTransfer(ctx, &chanid)
	}
	return m.shuttleMgr.RestartTransfer(ctx, loc, chanid, d)
}

// TODO - use from deal package when ready
// first check deal protocol version 2, then check version 1
func (m *manager) GetProviderDealStatus(ctx context.Context, d *model.ContentDeal, maddr address.Address, dealUUID *uuid.UUID) (*storagemarket.ProviderDealState, bool, error) {
	isPushTransfer := false
	providerDealState, err := m.fc.DealStatus(ctx, maddr, d.PropCid.CID, dealUUID)
	if err != nil && providerDealState == nil {
		isPushTransfer = true
		providerDealState, err = m.fc.DealStatus(ctx, maddr, d.PropCid.CID, nil)
	}
	return providerDealState, isPushTransfer, err
}

func (m *manager) GetTransferStatus(ctx context.Context, d *model.ContentDeal, contCID cid.Cid, contLoc string) (*filclient.ChannelState, error) {
	ctx, span := m.tracer.Start(ctx, "getTransferStatus")
	defer span.End()

	if d.DTChan == "" {
		return nil, nil
	}

	if contLoc == constants.ContentLocationLocal {
		chanst, err := m.transferStatusByID(ctx, d.DTChan)
		if err != nil {
			return nil, err
		}
		return chanst, nil
	}
	return m.shuttleMgr.GetTransferStatus(ctx, contLoc, d)
}

// get the data transfer state by transfer ID (compatible with both deal protocol v1 and v2)
func (m *manager) transferStatusByID(ctx context.Context, id string) (*filclient.ChannelState, error) {
	chanst, err := m.fc.TransferStatusByID(ctx, id)
	if err != nil && err != filclient.ErrNoTransferFound && !strings.Contains(err.Error(), "No channel for channel ID") && !strings.Contains(err.Error(), "datastore: key not found") {
		return nil, err
	}
	return chanst, nil
}

func (m *manager) StartDataTransfer(ctx context.Context, cd *model.ContentDeal) error {
	var cont util.Content
	if err := m.db.First(&cont, "id = ?", cd.Content).Error; err != nil {
		return err
	}

	if cont.Location != constants.ContentLocationLocal {
		return m.shuttleMgr.StartTransfer(ctx, cont.Location, cd, cont.Cid.CID)
	}

	miner, err := cd.MinerAddr()
	if err != nil {
		return err
	}

	chanid, err := m.fc.StartDataTransfer(ctx, miner, cd.PropCid.CID, cont.Cid.CID)
	if err != nil {
		if oerr := m.dealStatusUpdater.RecordDealFailure(&dealstatus.DealFailureError{
			Miner:               miner,
			Phase:               "start-data-transfer",
			Message:             err.Error(),
			Content:             cont.ID,
			UserID:              cont.UserID,
			DealProtocolVersion: cd.DealProtocolVersion,
			MinerVersion:        cd.MinerVersion,
		}); oerr != nil {
			return oerr
		}
		return nil
	}

	cd.DTChan = chanid.String()
	if err := m.db.Model(model.ContentDeal{}).Where("id = ?", cd.ID).UpdateColumns(map[string]interface{}{
		"dt_chan": chanid.String(),
	}).Error; err != nil {
		return xerrors.Errorf("failed to update deal with channel ID: %w", err)
	}
	m.log.Debugw("Started data transfer", "chanid", chanid)
	return nil
}
