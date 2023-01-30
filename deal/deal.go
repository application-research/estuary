package deal

import (
	"bytes"
	"context"
	"fmt"
	"sync"
	"time"

	content "github.com/application-research/estuary/content"
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
	CheckContentReadyForDealMaking(ctx context.Context, content *util.Content) error
	MakeDealWithMiner(ctx context.Context, content *util.Content, miner address.Address) (*model.ContentDeal, error)
	DealMakingDisabled() bool
	SetDealMakingEnabled(enable bool)
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
	contMgr              content.IManager
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
	contMgr content.IManager,
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
		dealQueueMgr:         dealqueuemgr.NewManager(cfg, log),
		contMgr:              contMgr,
	}

	m.runWorkers(ctx)
	return m
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

func (m *manager) CheckContentReadyForDealMaking(ctx context.Context, content *util.Content) error {
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

	if content.Location != constants.ContentLocationLocal {
		// if it's a shuttle content and the shuttle is not online, do not proceed
		isOnline, err := m.shuttleMgr.IsOnline(content.Location)
		if err != nil {
			return err
		}
		if !isOnline {
			return fmt.Errorf("content shuttle: %s, is not online", content.Location)
		}
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

func (m *manager) makeDealsForContent(ctx context.Context, contID uint64, dealsToBeMade int) ([]*model.ContentDeal, error) {
	ctx, span := m.tracer.Start(ctx, "makeDealsForContent", trace.WithAttributes(
		attribute.Int64("content", int64(contID)),
		attribute.Int("count", dealsToBeMade),
	))
	defer span.End()

	content, err := m.contMgr.GetContent(contID)
	if err != nil {
		return nil, err
	}

	if err := m.CheckContentReadyForDealMaking(ctx, content); err != nil {
		return nil, errors.Wrapf(err, "content %d not ready for dealmaking", content.ID)
	}

	_, _, pieceSize, err := m.commpMgr.GetPieceCommitment(ctx, content.Cid.CID, m.blockstore)
	if err != nil {
		return nil, xerrors.Errorf("failed to compute piece commitment while making deals %d: %w", content.ID, err)
	}

	// get content deals to filter miners - TODO move this to miners package
	var existingContDeals []model.ContentDeal
	if err := m.db.Find(&existingContDeals, "content = ? AND NOT failed", content.ID).Error; err != nil {
		if !xerrors.Is(err, gorm.ErrRecordNotFound) {
			return nil, err
		}
	}

	// to make sure content replicas are distributed, make new deals with miners that currently don't store this content
	excludedMiners := make(map[address.Address]bool)
	for _, d := range existingContDeals {
		maddr, err := d.MinerAddr()
		if err != nil {
			return nil, err
		}
		excludedMiners[maddr] = true
	}

	minerCount := 10000 // pick enough miners so we can try to make the most deal
	miners, err := m.minerManager.PickMiners(ctx, minerCount, pieceSize.Padded(), excludedMiners, true)
	if err != nil {
		return nil, err
	}

	dealsMade := make([]*model.ContentDeal, 0)
	for i := 0; i < dealsToBeMade; i++ {
		for _, mn := range miners {
			if excludedMiners[mn.Address] {
				continue
			}

			cd, err := m.MakeDealWithMiner(ctx, content, mn.Address)
			if err != nil {
				m.log.Warnf("failed to make deal for cont: %d, with miner: %s - %s", contID, mn.Address, err)
				continue
			}

			if err := m.dealQueueMgr.MadeOneDeal(contID, m.db); err != nil {
				return nil, err
			}

			dealsMade = append(dealsMade, cd)
			excludedMiners[mn.Address] = true
		}
	}
	return dealsMade, nil
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

func (m *manager) MakeDealWithMiner(ctx context.Context, content *util.Content, miner address.Address) (*model.ContentDeal, error) {
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

	prop, err := m.fc.MakeDeal(ctx, miner, content.Cid.CID, price, ask.MinPieceSize, m.cfg.Deal.Duration, m.cfg.Deal.IsVerified, m.cfg.Deal.RemoveUnsealed)
	if err != nil {
		return nil, xerrors.Errorf("failed to construct a deal proposal: %w", err)
	}

	propnd, err := cborutil.AsIpld(prop.DealProposal)
	if err != nil {
		return nil, xerrors.Errorf("failed to compute deal proposal ipld node: %w", err)
	}

	dp, err := m.putProposalRecord(prop.DealProposal)
	if err != nil {
		return nil, err
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
		if err := m.db.Unscoped().Delete(&model.ContentDeal{}, deal).Error; err != nil {
			return nil, fmt.Errorf("failed to delete content deal from db: %w", err)
		}

		// Clean up the proposal database entry
		if err := m.db.Unscoped().Delete(&model.ProposalRecord{}, dp).Error; err != nil {
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
