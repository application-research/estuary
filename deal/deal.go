package deal

import (
	"bytes"
	"context"
	"fmt"
	"sync"
	"time"

	dealqueuemgr "github.com/application-research/estuary/deal/queue"
	"github.com/filecoin-project/go-state-types/big"
	marketv9 "github.com/filecoin-project/go-state-types/builtin/v9/market"

	"go.uber.org/zap"

	"github.com/application-research/estuary/config"
	"github.com/application-research/estuary/constants"
	"github.com/application-research/estuary/content/commp"
	dealstatus "github.com/application-research/estuary/deal/status"
	"github.com/application-research/estuary/deal/transfer"
	"github.com/application-research/estuary/miner"
	"github.com/application-research/estuary/model"
	"github.com/application-research/estuary/node"
	"github.com/application-research/estuary/shuttle"
	"github.com/application-research/estuary/util"
	"github.com/application-research/filclient"
	"github.com/filecoin-project/boost/transport/httptransport"
	"github.com/filecoin-project/go-address"
	cborutil "github.com/filecoin-project/go-cbor-util"
	datatransfer "github.com/filecoin-project/go-data-transfer"
	"github.com/filecoin-project/go-fil-markets/storagemarket"
	"github.com/filecoin-project/go-fil-markets/storagemarket/network"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/lotus/api"
	"github.com/filecoin-project/lotus/chain/types"
	"github.com/filecoin-project/specs-actors/v6/actors/builtin/market"
	"github.com/google/uuid"
	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multiaddr"
	"github.com/pkg/errors"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/xerrors"
	"gorm.io/gorm"
)

const (
	DEAL_CHECK_UNKNOWN = iota
	DEAL_CHECK_PROGRESS
	DEAL_CHECK_DEALID_ON_CHAIN
	DEAL_CHECK_SECTOR_ON_CHAIN
	DEAL_NEARLY_EXPIRED
	DEAL_CHECK_SLASHED
)

type IManager interface {
	GetProviderDealStatus(ctx context.Context, d *model.ContentDeal, maddr address.Address, dealUUID *uuid.UUID) (*storagemarket.ProviderDealState, bool, error)
	CheckContentReadyForDealMaking(ctx context.Context, content util.Content) error
	MakeDealWithMiner(ctx context.Context, content util.Content, miner address.Address) (*model.ContentDeal, error)
	DealMakingDisabled() bool
	SetDealMakingEnabled(enable bool)
}

type deal struct {
	minerAddr      address.Address
	isPushTransfer bool
	contentDeal    *model.ContentDeal
}

type manager struct {
	db                   *gorm.DB
	api                  api.Gateway
	fc                   *filclient.FilClient
	node                 *node.Node
	cfg                  *config.Estuary
	tracer               trace.Tracer
	blockstore           node.EstuaryBlockstore
	dealDisabledLk       sync.Mutex
	isDealMakingDisabled bool
	shuttleMgr           shuttle.IManager
	minerManager         miner.IMinerManager
	log                  *zap.SugaredLogger
	transferMgr          transfer.IManager
	commpMgr             commp.IManager
	dealStatusUpdater    dealstatus.IUpdater
	dealQueueMgr         dealqueuemgr.IManager
}

func NewManager(
	ctx context.Context,
	db *gorm.DB,
	api api.Gateway,
	fc *filclient.FilClient,
	tbs *util.TrackingBlockstore,
	nd *node.Node,
	cfg *config.Estuary,
	minerManager miner.IMinerManager,
	log *zap.SugaredLogger,
	shuttleMgr shuttle.IManager,
	transferMgr transfer.IManager,
	commpMgr commp.IManager,
) IManager {
	m := &manager{
		cfg:                  cfg,
		db:                   db,
		api:                  api,
		fc:                   fc,
		blockstore:           tbs.Under().(node.EstuaryBlockstore),
		node:                 nd,
		shuttleMgr:           shuttleMgr,
		isDealMakingDisabled: cfg.Deal.IsDisabled,
		tracer:               otel.Tracer("replicator"),
		minerManager:         minerManager,
		log:                  log,
		transferMgr:          transferMgr,
		commpMgr:             commpMgr,
		dealStatusUpdater:    dealstatus.NewUpdater(db, log),
		dealQueueMgr:         dealqueuemgr.NewManager(db, cfg, log),
	}

	m.runWorkers(ctx)
	return m
}

func (m *manager) runWorkers(ctx context.Context) {
	m.log.Infof("deal workers")

	go m.runDealBackFillWorker(ctx)

	go m.runDealCheckWorker(ctx)

	go m.runDealWorker(ctx)

	m.log.Infof("spun up deal workers")
}

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

func (m *manager) checkDeal(ctx context.Context, d *model.ContentDeal, content util.Content) (int, error) {
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

func (m *manager) updateDealID(d *model.ContentDeal, id int64) error {
	if err := m.db.Model(model.ContentDeal{}).Where("id = ?", d.ID).Updates(map[string]interface{}{
		"deal_id":     id,
		"on_chain_at": time.Now(),
	}).Error; err != nil {
		return err
	}
	return nil
}

func (m *manager) dealHasExpired(ctx context.Context, d *model.ContentDeal, head *types.TipSet) (bool, error) {
	prop, err := m.getProposalRecord(d.PropCid.CID)
	if err != nil {
		m.log.Warnf("failed to get proposal record for deal %d: %s", d.ID, err)

		if time.Since(d.CreatedAt) > time.Hour*24*14 {
			return true, nil
		}
		return false, err
	}

	if prop.Proposal.StartEpoch < head.Height() {
		return true, nil
	}
	return false, nil
}

var ErrNotOnChainYet = fmt.Errorf("message not found on chain")

func (cm *manager) getDealID(ctx context.Context, pubcid cid.Cid, d *model.ContentDeal) (abi.DealID, error) {
	mlookup, err := cm.api.StateSearchMsg(ctx, types.EmptyTSK, pubcid, 1000, false)
	if err != nil {
		return 0, xerrors.Errorf("could not find published deal on chain: %w", err)
	}

	if mlookup == nil {
		return 0, ErrNotOnChainYet
	}

	if mlookup.Message != pubcid {
		// TODO: can probably deal with this by checking the message contents?
		return 0, xerrors.Errorf("publish deal message was replaced on chain")
	}

	msg, err := cm.api.ChainGetMessage(ctx, mlookup.Message)
	if err != nil {
		return 0, err
	}

	var params market.PublishStorageDealsParams
	if err := params.UnmarshalCBOR(bytes.NewReader(msg.Params)); err != nil {
		return 0, err
	}

	dealix := -1
	for i, pd := range params.Deals {
		pd := pd
		nd, err := cborutil.AsIpld(&pd)
		if err != nil {
			return 0, xerrors.Errorf("failed to compute deal proposal ipld node: %w", err)
		}

		if nd.Cid() == d.PropCid.CID {
			dealix = i
			break
		}
	}

	if dealix == -1 {
		return 0, fmt.Errorf("our deal was not in this publish message")
	}

	if mlookup.Receipt.ExitCode != 0 {
		return 0, xerrors.Errorf("miners deal publish failed (exit: %d)", mlookup.Receipt.ExitCode)
	}

	var retval market.PublishStorageDealsReturn
	if err := retval.UnmarshalCBOR(bytes.NewReader(mlookup.Receipt.Return)); err != nil {
		return 0, xerrors.Errorf("publish deal return was improperly formatted: %w", err)
	}

	if len(retval.IDs) != len(params.Deals) {
		return 0, fmt.Errorf("return value from publish deals did not match length of params")
	}
	return retval.IDs[dealix], nil
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

func (m *manager) CheckContentReadyForDealMaking(ctx context.Context, content util.Content) error {
	if !content.Active || content.Pinning {
		return fmt.Errorf("cannot make deals for content that is not pinned")
	}

	if content.Offloaded {
		return fmt.Errorf("cannot make more deals for offloaded content, must retrieve first")
	}

	if content.Size < m.cfg.Content.MinSize {
		return fmt.Errorf("content %d below individual deal size threshold. (size: %d, threshold: %d)", content.ID, content.Size, m.cfg.Content.MinSize)
	}

	if content.Size > m.cfg.Content.MaxSize {
		return fmt.Errorf("content %d above individual deal size threshold. (size: %d, threshold: %d)", content.ID, content.Size, m.cfg.Content.MaxSize)
	}

	if m.DealMakingDisabled() {
		return fmt.Errorf("deal making is disabled for now")
	}

	// check content is not corrupted
	var isCorrupted *model.SanityCheck
	if err := m.db.First(&isCorrupted, "content_id = ?", content.ID).Error; err != nil {
		if !xerrors.Is(err, gorm.ErrRecordNotFound) {
			return err
		}
		isCorrupted = nil
	}
	if isCorrupted != nil {
		return fmt.Errorf("cnt: %d ignored due to missing blocks", content.ID)
	}

	// if it's a shuttle content and the shuttle is not online, do not proceed
	isOnline, err := m.shuttleMgr.IsOnline(content.Location)
	if err != nil {
		return err
	}

	if content.Location != constants.ContentLocationLocal && !isOnline {
		return fmt.Errorf("content shuttle: %s, is not online", content.Location)
	}

	// only verified deals need datacap checks
	if m.cfg.Deal.IsVerified {
		bl, err := m.fc.Balance(ctx)
		if err != nil {
			return errors.Wrap(err, "could not retrieve dataCap from client balance")
		}

		if bl == nil || bl.VerifiedClientBalance == nil {
			return errors.New("verifed deals requires datacap, please see https://verify.glif.io or use the --verified-deal=false for non-verified deals")
		}

		if bl.VerifiedClientBalance.LessThan(big.NewIntUnsigned(uint64(abi.UnpaddedPieceSize(content.Size).Padded()))) {
			// how do we notify admin to top up datacap?
			return errors.Wrapf(err, "will not make deal, client address dataCap:%d GiB is lower than content size:%d GiB", big.Div(*bl.VerifiedClientBalance, big.NewIntUnsigned(uint64(1073741824))), abi.UnpaddedPieceSize(content.Size).Padded()/1073741824)
		}
	}
	return nil
}

func (m *manager) makeDealsForContent(ctx context.Context, content util.Content, dealsToBeMade int, existingContDeals []model.ContentDeal) error {
	ctx, span := m.tracer.Start(ctx, "makeDealsForContent", trace.WithAttributes(
		attribute.Int64("content", int64(content.ID)),
		attribute.Int("count", dealsToBeMade),
	))
	defer span.End()

	if err := m.CheckContentReadyForDealMaking(ctx, content); err != nil {
		return errors.Wrapf(err, "content %d not ready for dealmaking", content.ID)
	}

	_, _, pieceSize, err := m.commpMgr.GetPieceCommitment(ctx, content.Cid.CID, m.blockstore)
	if err != nil {
		return xerrors.Errorf("failed to compute piece commitment while making deals %d: %w", content.ID, err)
	}

	// to make sure content replicas are distributed, make new deals with miners that currently don't store this content
	excludedMiners := make(map[address.Address]bool)
	for _, d := range existingContDeals {
		if d.Failed {
			// TODO: this is an interesting choice, because it gives miners more chances to try again if they fail.
			// I think that as we get a more diverse set of stable miners, we can *not* do this.
			continue
		}
		maddr, err := d.MinerAddr()
		if err != nil {
			return err
		}
		excludedMiners[maddr] = true
	}

	miners, err := m.minerManager.PickMiners(ctx, dealsToBeMade*2, pieceSize.Padded(), excludedMiners, true)
	if err != nil {
		return err
	}

	var readyDeals []deal
	for _, mn := range miners {
		cd, err := m.MakeDealWithMiner(ctx, content, mn.Address)
		if err != nil {
			return err
		}
		isPushTransfer := cd.DealProtocolVersion == filclient.DealProtocolv110
		readyDeals = append(readyDeals, deal{minerAddr: mn.Address, isPushTransfer: isPushTransfer, contentDeal: cd})
		if len(readyDeals) >= dealsToBeMade {
			break
		}
	}

	// Now start up some data transfers!
	// note: its okay if we dont start all the data transfers, we can just do it next time around
	for _, rDeal := range readyDeals {
		// If the data transfer is a pull transfer, we don't need to explicitly
		// start the transfer (the Storage Provider will start pulling data as
		// soon as it accepts the proposal)
		if !rDeal.isPushTransfer {
			continue
		}

		// start data transfer async
		go func(d deal) {
			if err := m.transferMgr.StartDataTransfer(ctx, d.contentDeal); err != nil {
				m.log.Errorw("failed to start data transfer", "err", err, "miner", d.minerAddr)
			}
		}(rDeal)
	}
	return nil
}

func (cm *manager) sendProposalV120(ctx context.Context, contentLoc string, netprop network.Proposal, propCid cid.Cid, dealUUID uuid.UUID, dbid uint) (func() error, bool, error) {
	// In deal protocol v120 the transfer will be initiated by the
	// storage provider (a pull transfer) so we need to prepare for
	// the data request

	// Create an auth token to be used in the request
	authToken, err := httptransport.GenerateAuthToken()
	if err != nil {
		return nil, false, xerrors.Errorf("generating auth token for deal: %w", err)
	}

	rootCid := netprop.Piece.Root
	size := netprop.Piece.RawBlockSize
	var announceAddr multiaddr.Multiaddr
	if contentLoc == constants.ContentLocationLocal {
		if len(cm.node.Config.AnnounceAddrs) == 0 {
			return nil, false, xerrors.Errorf("cannot serve deal data: no announce address configured for estuary node")
		}

		addrstr := cm.node.Config.AnnounceAddrs[0] + "/p2p/" + cm.node.Host.ID().String()
		announceAddr, err = multiaddr.NewMultiaddr(addrstr)
		if err != nil {
			return nil, false, xerrors.Errorf("cannot parse announce address '%s': %w", addrstr, err)
		}

		// Add an auth token for the data to the auth DB
		err := cm.fc.Libp2pTransferMgr.PrepareForDataRequest(ctx, dbid, authToken, propCid, rootCid, size)
		if err != nil {
			return nil, false, xerrors.Errorf("preparing for data request: %w", err)
		}
	} else {
		// first check if shuttle is online

		isOnline, err := cm.shuttleMgr.IsOnline(contentLoc)
		if err != nil {
			return nil, false, err
		}

		if !isOnline {
			return nil, false, xerrors.Errorf("shuttle is not online: %s", contentLoc)
		}

		addrInfo, err := cm.shuttleMgr.AddrInfo(contentLoc)
		if err != nil {
			return nil, false, err
		}
		// TODO: This is the address that the shuttle reports to the Estuary
		// primary node, but is it ok if it's also the address reported
		// as where to download files publically? If it's a public IP does
		// that mean that messages from Estuary primary node would go through
		// public internet to get to shuttle?
		if addrInfo == nil || len(addrInfo.Addrs) == 0 {
			return nil, false, xerrors.Errorf("no address found for shuttle: %s", contentLoc)
		}
		addrstr := addrInfo.Addrs[0].String() + "/p2p/" + addrInfo.ID.String()
		announceAddr, err = multiaddr.NewMultiaddr(addrstr)
		if err != nil {
			return nil, false, xerrors.Errorf("cannot parse announce address '%s': %w", addrstr, err)
		}

		// If the content is not on the primary estuary node (it's on a shuttle)
		// The Storage Provider will pull the data from the shuttle,
		// so add an auth token for the data to the shuttle's auth DB
		err = cm.shuttleMgr.PrepareForDataRequest(ctx, contentLoc, dbid, authToken, propCid, rootCid, size)
		if err != nil {
			return nil, false, xerrors.Errorf("sending prepare for data request command to shuttle: %w", err)
		}
	}

	cleanup := func() error {
		if contentLoc == constants.ContentLocationLocal {
			return cm.fc.Libp2pTransferMgr.CleanupPreparedRequest(ctx, dbid, authToken)
		}
		return cm.shuttleMgr.CleanupPreparedRequest(ctx, contentLoc, dbid, authToken)
	}

	// Send the deal proposal to the storage provider
	propPhase, err := cm.fc.SendProposalV120(ctx, dbid, netprop, dealUUID, announceAddr, authToken)
	return cleanup, propPhase, err
}

func (m *manager) MakeDealWithMiner(ctx context.Context, content util.Content, miner address.Address) (*model.ContentDeal, error) {
	ctx, span := m.tracer.Start(ctx, "makeDealWithMiner", trace.WithAttributes(
		attribute.Int64("content", int64(content.ID)),
		attribute.Stringer("miner", miner),
	))
	defer span.End()

	proto, err := m.minerManager.GetDealProtocolForMiner(ctx, miner)
	if err != nil {
		return nil, m.dealStatusUpdater.RecordDealFailure(&dealstatus.DealFailureError{
			Miner:   miner,
			Phase:   "deal-protocol-version",
			Message: err.Error(),
			Content: content.ID,
			UserID:  content.UserID,
		})
	}

	ask, err := m.minerManager.GetAsk(ctx, miner, 0)
	if err != nil {
		var clientErr *filclient.Error
		if !(xerrors.As(err, &clientErr) && clientErr.Code == filclient.ErrLotusError) {
			if err := m.dealStatusUpdater.RecordDealFailure(&dealstatus.DealFailureError{
				Miner:               miner,
				Phase:               "query-ask",
				Message:             err.Error(),
				Content:             content.ID,
				UserID:              content.UserID,
				DealProtocolVersion: proto,
			}); err != nil {
				return nil, xerrors.Errorf("failed to record deal failure: %w", err)
			}
		}
		return nil, xerrors.Errorf("failed to get ask for miner %s: %w", miner, err)
	}

	price := ask.GetPrice(m.cfg.Deal.IsVerified)
	if ask.PriceIsTooHigh(m.cfg) {
		return nil, fmt.Errorf("miners price is too high: %s %s", miner, price)
	}

	prop, err := m.fc.MakeDeal(ctx, miner, content.Cid.CID, price, ask.MinPieceSize, m.cfg.Deal.Duration, m.cfg.Deal.IsVerified)
	if err != nil {
		return nil, xerrors.Errorf("failed to construct a deal proposal: %w", err)
	}

	dp, err := m.putProposalRecord(prop.DealProposal)
	if err != nil {
		return nil, err
	}

	propnd, err := cborutil.AsIpld(prop.DealProposal)
	if err != nil {
		return nil, xerrors.Errorf("failed to compute deal proposal ipld node: %w", err)
	}

	dealUUID := uuid.New()
	deal := &model.ContentDeal{
		Content:             content.ID,
		PropCid:             util.DbCID{CID: propnd.Cid()},
		DealUUID:            dealUUID.String(),
		Miner:               miner.String(),
		Verified:            m.cfg.Deal.IsVerified,
		UserID:              content.UserID,
		DealProtocolVersion: proto,
		MinerVersion:        ask.MinerVersion,
	}

	if err := m.db.Create(deal).Error; err != nil {
		return nil, xerrors.Errorf("failed to create database entry for deal: %w", err)
	}

	// Send the deal proposal to the storage provider
	var cleanupDealPrep func() error
	var propPhase bool
	isPushTransfer := proto == filclient.DealProtocolv110

	switch proto {
	case filclient.DealProtocolv110:
		propPhase, err = m.fc.SendProposalV110(ctx, *prop, propnd.Cid())
	case filclient.DealProtocolv120:
		cleanupDealPrep, propPhase, err = m.sendProposalV120(ctx, content.Location, *prop, propnd.Cid(), dealUUID, deal.ID)
	default:
		err = fmt.Errorf("unrecognized deal protocol %s", proto)
	}

	if err != nil {
		// Clean up the database entry
		if err := m.db.Delete(&model.ContentDeal{}, deal).Error; err != nil {
			return nil, fmt.Errorf("failed to delete content deal from db: %w", err)
		}

		// Clean up the proposal database entry
		if err := m.db.Delete(&model.ProposalRecord{}, dp).Error; err != nil {
			return nil, fmt.Errorf("failed to delete deal proposal from db: %w", err)
		}

		// Clean up the preparation for deal request
		if cleanupDealPrep != nil {
			if err := cleanupDealPrep(); err != nil {
				m.log.Errorw("cleaning up deal prepared request", "error", err)
			}
		}

		// Record a deal failure
		phase := "send-proposal"
		if propPhase {
			phase = "propose"
		}
		if err := m.dealStatusUpdater.RecordDealFailure(&dealstatus.DealFailureError{
			Miner:               miner,
			Phase:               phase,
			Message:             err.Error(),
			Content:             content.ID,
			UserID:              content.UserID,
			DealProtocolVersion: proto,
			MinerVersion:        ask.MinerVersion,
		}); err != nil {
			return nil, fmt.Errorf("failed to record deal failure: %w", err)
		}
		return nil, err
	}

	// If the data transfer is a pull transfer, we don't need to explicitly
	// start the transfer (the Storage Provider will start pulling data as
	// soon as it accepts the proposal)
	if !isPushTransfer {
		return deal, nil
	}

	// It's a push transfer, so start the data transfer
	if err := m.transferMgr.StartDataTransfer(ctx, deal); err != nil {
		return nil, fmt.Errorf("failed to start data transfer: %w", err)
	}
	return deal, nil
}

func (m *manager) putProposalRecord(dealprop *marketv9.ClientDealProposal) (*model.ProposalRecord, error) {
	nd, err := cborutil.AsIpld(dealprop)
	if err != nil {
		return nil, err
	}

	dp := &model.ProposalRecord{
		PropCid: util.DbCID{CID: nd.Cid()},
		Data:    nd.RawData(),
	}

	if err := m.db.Create(dp).Error; err != nil {
		return nil, err
	}
	return dp, nil
}

func (m *manager) getProposalRecord(propCid cid.Cid) (*market.ClientDealProposal, error) {
	var proprec model.ProposalRecord
	if err := m.db.First(&proprec, "prop_cid = ?", propCid.Bytes()).Error; err != nil {
		return nil, err
	}

	var prop market.ClientDealProposal
	if err := prop.UnmarshalCBOR(bytes.NewReader(proprec.Data)); err != nil {
		return nil, err
	}
	return &prop, nil
}

func (m *manager) DealMakingDisabled() bool {
	m.dealDisabledLk.Lock()
	defer m.dealDisabledLk.Unlock()
	return m.isDealMakingDisabled
}

func (m *manager) SetDealMakingEnabled(enable bool) {
	m.dealDisabledLk.Lock()
	defer m.dealDisabledLk.Unlock()
	m.isDealMakingDisabled = !enable
}
