package deal

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/application-research/estuary/constants"
	dealstatus "github.com/application-research/estuary/deal/status"
	"github.com/application-research/estuary/model"
	"github.com/application-research/estuary/util"
	datatransfer "github.com/filecoin-project/go-data-transfer"
	"github.com/filecoin-project/go-fil-markets/storagemarket"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/lotus/chain/types"
	"github.com/google/uuid"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/xerrors"
	"gorm.io/gorm"
)

func (m *manager) checkContentDeals(ctx context.Context, contID uint64) (int, error) {
	ctx, span := m.tracer.Start(ctx, "checkContentDeals", trace.WithAttributes(
		attribute.Int("content", int(contID)),
	))
	defer span.End()

	m.log.Debugf("trying to check deal for content: %d", contID)

	content, err := m.contMgr.GetContent(contID)
	if err != nil {
		return 0, err
	}

	// get content deals, if any
	var deals []model.ContentDeal
	if err := m.db.Find(&deals, "content = ? AND NOT failed", contID).Error; err != nil {
		if !xerrors.Is(err, gorm.ErrRecordNotFound) {
			return 0, err
		}
	}

	replicationFactor := m.cfg.Replication
	if content.Replication > 0 {
		replicationFactor = content.Replication
	}

	// for new contents, there will be no deals
	if len(deals) == 0 {
		return replicationFactor, nil
	}

	// check on each of the existing deals, see if any needs fixing
	var countLk sync.Mutex
	var numSealed, numPublished, numProgress int
	var wg sync.WaitGroup

	errs := make([]error, len(deals))
	for i, d := range deals {
		dl := d
		wg.Add(1)
		go func(i int) {
			d := deals[i]
			defer wg.Done()

			status, err := m.checkDeal(ctx, &dl, content)
			if err != nil {
				var dfe *dealstatus.DealFailureError
				if xerrors.As(err, &dfe) {
					return
				} else {
					errs[i] = err
					return
				}
			}

			countLk.Lock()
			defer countLk.Unlock()
			switch status {
			case DEAL_CHECK_UNKNOWN, DEAL_NEARLY_EXPIRED, DEAL_CHECK_SLASHED:
				if err := m.repairDeal(&d); err != nil {
					errs[i] = xerrors.Errorf("repairing deal failed: %w", err)
					return
				}
			case DEAL_CHECK_SECTOR_ON_CHAIN:
				numSealed++
			case DEAL_CHECK_DEALID_ON_CHAIN:
				numPublished++
			case DEAL_CHECK_PROGRESS:
				numProgress++
			default:
				m.log.Errorf("unrecognized deal check status: %d", status)
			}
		}(i)
	}
	wg.Wait()

	// return the last error found, log the rest
	var retErr error
	for _, err := range errs {
		if err != nil {
			if retErr != nil {
				m.log.Errorf("check deal failure: %s", err)
			}
			retErr = err
		}
	}
	if retErr != nil {
		return 0, fmt.Errorf("deal check errored: %w", retErr)
	}

	if content.Location != constants.ContentLocationLocal {
		// after reconciling content deals,
		// check If this is a shuttle content and that the shuttle is online and can start data transfer
		isOnline, err := m.shuttleMgr.IsOnline(content.Location)
		if err != nil || !isOnline {
			m.log.Warnf("content shuttle: %s, is not online", content.Location)
			return 0, err
		}
	}

	// check if content has enough good deals after reconcialiation,
	// if not enough good deals, go make more
	goodDeals := numSealed + numPublished + numProgress
	dealsToBeMade := replicationFactor - goodDeals
	if dealsToBeMade <= 0 {
		// no new deal is needed
		return 0, nil
	}

	// if content is offloaded, do not proceed - since it needs the blocks for data transfer
	if content.Offloaded {
		go func() {
			if err := m.contMgr.RefreshContent(context.Background(), content.ID); err != nil {
				m.log.Errorf("failed to retrieve content in need of repair %d: %s", content.ID, err)
			}
		}()
		return 0, fmt.Errorf("cont: %d offloaded for deal making", content.ID)
	}
	return dealsToBeMade, nil
}

func (m *manager) checkDeal(ctx context.Context, d *model.ContentDeal, content *util.Content) (int, error) {
	ctx, cancel := context.WithTimeout(ctx, time.Minute*5) // NB: if we ever hit this, its bad. but we at least need *some* timeout there
	defer cancel()

	ctx, span := m.tracer.Start(ctx, "checkDeal", trace.WithAttributes(
		attribute.Int("deal", int(d.ID)),
	))
	defer span.End()
	m.log.Debugw("checking deal", "miner", d.Miner, "content", d.Content, "dbid", d.ID)

	maddr, err := d.MinerAddr()
	if err != nil {
		return DEAL_CHECK_UNKNOWN, err
	}

	// get the deal data transfer state
	chanst, err := m.transferMgr.GetTransferStatus(ctx, d, content.Cid.CID, content.Location)
	if err != nil {
		return DEAL_CHECK_UNKNOWN, err
	}

	// if data transfer state is available,
	// try to set the actual timestamp of transfer states - start, finished, failed, cancelled etc
	// NB: boost does not support stages
	if chanst != nil {
		updates := make(map[string]interface{})
		if chanst.Status == datatransfer.TransferFinished || chanst.Status == datatransfer.Completed {
			if d.TransferStarted.IsZero() && chanst.Status == datatransfer.Completed {
				updates["transfer_started"] = time.Now() // boost transfers does not support stages, so we can't get actual timestamps
				if s := chanst.Stages.GetStage("Requested"); s != nil {
					updates["transfer_started"] = s.CreatedTime.Time()
				}
			}

			if d.TransferFinished.IsZero() {
				updates["transfer_finished"] = time.Now() // boost transfers does not support stages, so we can't get actual timestamps
				if s := chanst.Stages.GetStage("TransferFinished"); s != nil {
					updates["transfer_finished"] = s.CreatedTime.Time()
				}
			}
		}

		// if transfer is Failed or Cancelled
		trsFailed, msgStage := util.TransferFailed(chanst)
		if d.FailedAt.IsZero() && trsFailed {
			updates["failed"] = true
			updates["failed_at"] = time.Now() // boost transfers does not support stages, so we can't get actual timestamps
			if s := chanst.Stages.GetStage(msgStage); s != nil {
				updates["failed_at"] = s.CreatedTime.Time()
			}
		}

		if len(updates) > 0 {
			if err := m.db.Model(model.ContentDeal{}).Where("id = ?", d.ID).UpdateColumns(updates).Error; err != nil {
				return DEAL_CHECK_UNKNOWN, err
			}
		}
	}

	// get chain head - needed to check expired deals below
	head, err := m.api.ChainHead(ctx)
	if err != nil {
		return DEAL_CHECK_UNKNOWN, fmt.Errorf("failed to check chain head: %w", err)
	}

	// if the deal is on chain, then check is it still healthy (slashed, expired etc) and it's sealed(active) state
	if d.DealID != 0 {
		ok, deal, err := m.fc.CheckChainDeal(ctx, abi.DealID(d.DealID))
		if err != nil {
			return DEAL_CHECK_UNKNOWN, fmt.Errorf("failed to check chain deal: %w", err)
		}
		if !ok {
			return DEAL_CHECK_UNKNOWN, nil
		}

		// check slashed health
		if deal.State.SlashEpoch > 0 {
			if err := m.db.Model(model.ContentDeal{}).Where("id = ?", d.ID).UpdateColumn("slashed", true).Error; err != nil {
				return DEAL_CHECK_UNKNOWN, err
			}

			if err := m.dealStatusUpdater.RecordDealFailure(&dealstatus.DealFailureError{
				Miner:               maddr,
				Phase:               "check-chain-deal",
				Message:             fmt.Sprintf("deal %d was slashed at epoch %d", d.DealID, deal.State.SlashEpoch),
				Content:             d.Content,
				UserID:              d.UserID,
				DealProtocolVersion: d.DealProtocolVersion,
				MinerVersion:        d.MinerVersion,
			}); err != nil {
				return DEAL_CHECK_UNKNOWN, err
			}
			return DEAL_CHECK_SLASHED, nil
		}

		// check expiration health
		if deal.Proposal.EndEpoch-head.Height() < constants.MinSafeDealLifetime {
			return DEAL_NEARLY_EXPIRED, nil
		}

		// checked sealed/active health - TODO set actual sealed time
		if deal.State.SectorStartEpoch > 0 {
			if d.SealedAt.IsZero() {
				if err := m.db.Model(model.ContentDeal{}).Where("id = ?", d.ID).UpdateColumn("sealed_at", time.Now()).Error; err != nil {
					return DEAL_CHECK_UNKNOWN, err
				}
			}
			return DEAL_CHECK_SECTOR_ON_CHAIN, nil
		}
		return DEAL_CHECK_DEALID_ON_CHAIN, nil
	}

	// case where deal isnt yet on chain, then check deal state with miner/provider
	m.log.Debugw("checking deal status with miner", "miner", maddr, "propcid", d.PropCid.CID, "dealUUID", d.DealUUID)
	subctx, cancel := context.WithTimeout(ctx, time.Second*10)
	defer cancel()

	var dealUUID *uuid.UUID
	if d.DealUUID != "" {
		parsed, err := uuid.Parse(d.DealUUID)
		if err != nil {
			return DEAL_CHECK_UNKNOWN, fmt.Errorf("parsing deal uuid %s: %w", d.DealUUID, err)
		}
		dealUUID = &parsed
	}

	// pull deal state from miner/provider
	provds, isPushTransfer, err := m.GetProviderDealStatus(subctx, d, maddr, dealUUID)
	if err != nil {
		// if we cant get deal status from a miner and the data hasnt landed on chain what do we do?
		expired, err := m.dealHasExpired(ctx, d, head)
		if err != nil {
			return DEAL_CHECK_UNKNOWN, xerrors.Errorf("failed to check if deal was expired: %w", err)
		}

		if expired {
			// deal expired, miner didnt start it in time
			if err := m.dealStatusUpdater.RecordDealFailure(&dealstatus.DealFailureError{
				Miner:               maddr,
				Phase:               "check-status",
				Message:             "was unable to check deal status with miner and now deal has expired",
				Content:             d.Content,
				UserID:              d.UserID,
				DealProtocolVersion: d.DealProtocolVersion,
				MinerVersion:        d.MinerVersion,
			}); err != nil {
				return DEAL_CHECK_UNKNOWN, err
			}
			return DEAL_CHECK_UNKNOWN, nil
		}
		// dont fail it out until they run out of time, they might just be offline momentarily
		return DEAL_CHECK_PROGRESS, nil
	}

	// since not on chain and not on miner, give up and create a new deal
	if provds == nil {
		return DEAL_CHECK_UNKNOWN, fmt.Errorf("failed to lookup provider deal state")
	}

	if provds.Proposal == nil {
		if time.Since(d.CreatedAt) > time.Hour*24*14 {
			if err := m.dealStatusUpdater.RecordDealFailure(&dealstatus.DealFailureError{
				Miner:               maddr,
				Phase:               "check-status",
				Message:             "miner returned nil response proposal and deal expired",
				Content:             d.Content,
				UserID:              d.UserID,
				DealProtocolVersion: d.DealProtocolVersion,
				MinerVersion:        d.MinerVersion,
			}); err != nil {
				return DEAL_CHECK_UNKNOWN, err
			}
		}
		return DEAL_CHECK_UNKNOWN, nil
	}

	if provds.State == storagemarket.StorageDealError {
		m.log.Warnf("deal state for deal %d from miner %s is error: %s", d.ID, maddr.String(), provds.Message)
	}

	if provds.DealID != 0 {
		deal, err := m.api.StateMarketStorageDeal(ctx, provds.DealID, types.EmptyTSK)
		if err != nil || deal == nil {
			return DEAL_CHECK_UNKNOWN, fmt.Errorf("failed to lookup deal on chain: %w", err)
		}

		pCID, _, _, err := m.commpMgr.GetPieceCommitment(ctx, content.Cid.CID, m.blockstore)
		if err != nil {
			return DEAL_CHECK_UNKNOWN, xerrors.Errorf("failed to look up piece commitment for content: %w", err)
		}

		if deal.Proposal.Provider != maddr || deal.Proposal.PieceCID != pCID {
			m.log.Warnf("proposal in deal ID miner sent back did not match our expectations")
			return DEAL_CHECK_UNKNOWN, nil
		}

		m.log.Debugf("Confirmed deal ID, updating in database: %d %d %d", d.Content, d.ID, provds.DealID)
		if err := m.updateDealID(d, int64(provds.DealID)); err != nil {
			return DEAL_CHECK_UNKNOWN, err
		}
		return DEAL_CHECK_DEALID_ON_CHAIN, nil
	}

	if provds.PublishCid != nil {
		m.log.Debugw("checking publish CID", "content", d.Content, "miner", d.Miner, "propcid", d.PropCid.CID, "publishCid", *provds.PublishCid)
		id, err := m.getDealID(ctx, *provds.PublishCid, d)
		if err != nil {
			m.log.Debugf("failed to find message on chain: %s", *provds.PublishCid)
			if provds.Proposal.StartEpoch < head.Height() {
				// deal expired, miner didn`t start it in time
				if err := m.dealStatusUpdater.RecordDealFailure(&dealstatus.DealFailureError{
					Miner:               maddr,
					Phase:               "check-status",
					Message:             "deal did not make it on chain in time (but has publish deal cid set)",
					Content:             d.Content,
					UserID:              d.UserID,
					DealProtocolVersion: d.DealProtocolVersion,
					MinerVersion:        d.MinerVersion,
				}); err != nil {
					return DEAL_CHECK_UNKNOWN, err
				}
				return DEAL_CHECK_UNKNOWN, nil
			}
			return DEAL_CHECK_PROGRESS, nil
		}

		m.log.Debugf("Found deal ID, updating in database: %d %d %d", d.Content, d.ID, id)
		if err := m.updateDealID(d, int64(id)); err != nil {
			return DEAL_CHECK_UNKNOWN, err
		}
		return DEAL_CHECK_DEALID_ON_CHAIN, nil
	}

	// proposal expired, miner didnt start it in time
	if provds.Proposal.StartEpoch < head.Height() {
		if err := m.dealStatusUpdater.RecordDealFailure(&dealstatus.DealFailureError{
			Miner:               maddr,
			Phase:               "check-status",
			Message:             "deal did not make it on chain in time",
			Content:             d.Content,
			UserID:              d.UserID,
			DealProtocolVersion: d.DealProtocolVersion,
			MinerVersion:        d.MinerVersion,
		}); err != nil {
			return DEAL_CHECK_UNKNOWN, err
		}
		return DEAL_CHECK_UNKNOWN, nil
	}

	// Weird case where we somehow dont have the data transfer started for this deal.
	// if data transfer has not started and miner still has time, start the data transfer
	if d.DTChan == "" && time.Since(d.CreatedAt) < time.Hour {
		if isPushTransfer {
			m.log.Warnf("creating new data transfer for local deal that is missing it: %d", d.ID)
			if err := m.transferMgr.StartDataTransfer(ctx, d); err != nil {
				m.log.Errorw("failed to start new data transfer for weird state deal", "deal", d.ID, "miner", d.Miner, "err", err)
				// If this fails out, just fail the deal and start from
				// scratch. This is already a weird state.
				return DEAL_CHECK_UNKNOWN, nil
			}
		}
		return DEAL_CHECK_PROGRESS, nil
	}

	if chanst != nil {
		if trsFailed, _ := util.TransferFailed(chanst); trsFailed {
			if err := m.dealStatusUpdater.RecordDealFailure(&dealstatus.DealFailureError{
				Miner:               maddr,
				Phase:               "data-transfer",
				Message:             fmt.Sprintf("data transfer issue: %s", chanst.Message),
				Content:             content.ID,
				UserID:              d.UserID,
				DealProtocolVersion: d.DealProtocolVersion,
				MinerVersion:        d.MinerVersion,
			}); err != nil {
				return DEAL_CHECK_UNKNOWN, err
			}

			if m.cfg.Deal.FailOnTransferFailure {
				return DEAL_CHECK_UNKNOWN, nil
			}
		}
	}
	return DEAL_CHECK_PROGRESS, nil
}

func (m *manager) repairDeal(d *model.ContentDeal) error {
	if d.DealID != 0 {
		m.log.Debugw("miner faulted on deal", "deal", d.DealID, "content", d.Content, "miner", d.Miner)
		maddr, err := d.MinerAddr()
		if err != nil {
			m.log.Errorf("failed to get miner address from deal (%s): %w", d.Miner, err)
		}

		if err := m.dealStatusUpdater.RecordDealFailure(&dealstatus.DealFailureError{
			Miner:               maddr,
			Phase:               "fault",
			Message:             fmt.Sprintf("miner faulted on deal: %d", d.DealID),
			Content:             d.Content,
			UserID:              d.UserID,
			DealProtocolVersion: d.DealProtocolVersion,
			MinerVersion:        d.MinerVersion,
		}); err != nil {
			return err
		}
	}

	m.log.Debugw("repair deal", "propcid", d.PropCid.CID, "miner", d.Miner, "content", d.Content)
	if err := m.db.Model(model.ContentDeal{}).Where("id = ?", d.ID).UpdateColumns(map[string]interface{}{
		"failed":    true,
		"failed_at": time.Now(),
	}).Error; err != nil {
		return err
	}
	return nil
}
