package contentmgr

import (
	"bytes"
	"context"
	"fmt"
	"sync"
	"time"

	marketv9 "github.com/filecoin-project/go-state-types/builtin/v9/market"

	"github.com/application-research/estuary/constants"
	"github.com/application-research/estuary/model"
	"github.com/application-research/estuary/util"
	dagsplit "github.com/application-research/estuary/util/dagsplit"
	"github.com/application-research/filclient"
	"github.com/filecoin-project/boost/transport/httptransport"
	"github.com/filecoin-project/go-address"
	cborutil "github.com/filecoin-project/go-cbor-util"
	datatransfer "github.com/filecoin-project/go-data-transfer"
	"github.com/filecoin-project/go-fil-markets/storagemarket"
	"github.com/filecoin-project/go-fil-markets/storagemarket/network"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/big"
	"github.com/filecoin-project/lotus/chain/types"
	"github.com/filecoin-project/specs-actors/v6/actors/builtin/market"
	"github.com/google/uuid"
	"github.com/ipfs/go-blockservice"
	"github.com/ipfs/go-cid"
	cbor "github.com/ipfs/go-ipld-cbor"
	ipld "github.com/ipfs/go-ipld-format"
	"github.com/ipfs/go-merkledag"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	"github.com/multiformats/go-multiaddr"
	"github.com/pkg/errors"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/xerrors"
	"gorm.io/gorm"
)

type deal struct {
	minerAddr      address.Address
	isPushTransfer bool
	contentDeal    *model.ContentDeal
}

func (cm *ContentManager) runDealWorker(ctx context.Context) {
	// run the deal reconciliation and deal making worker
	for {
		select {
		case c := <-cm.queueMgr.NextContent():
			cm.log.Debugf("checking content: %d", c)

			var content util.Content
			if err := cm.db.First(&content, "id = ?", c).Error; err != nil {
				cm.log.Errorf("finding content %d in database: %s", c, err)
				continue
			}

			err := cm.ensureStorage(context.TODO(), content, func(dur time.Duration) {
				cm.queueMgr.Add(content.ID, content.Size, dur)
			})
			if err != nil {
				cm.log.Errorf("failed to ensure replication of content %d: %s", content.ID, err)
				cm.queueMgr.Add(content.ID, content.Size, time.Minute*5)
			}
		}
	}
}

func (cm *ContentManager) Run(ctx context.Context) {
	cm.setUpStaging(ctx)
	// if FilecoinStorage is enabled, check content deals or make content deals
	if !cm.cfg.DisableFilecoinStorage {
		go func() {
			// rebuild toCheck queue
			if err := cm.rebuildToCheckQueue(); err != nil {
				cm.log.Errorf("failed to recheck existing content: %s", err)
			}

			// run the deal reconciliation and deal making worker
			cm.runDealWorker(ctx)
		}()
	}
}

func (cm *ContentManager) ensureStorage(ctx context.Context, content util.Content, done func(time.Duration)) error {
	ctx, span := cm.tracer.Start(ctx, "ensureStorage", trace.WithAttributes(
		attribute.Int("content", int(content.ID)),
	))
	defer span.End()

	// if the content is not active or is in pinning state, do not proceed
	if !content.Active || content.Pinning {
		return nil
	}

	// If this content is aggregated inside another piece of content, nothing to do here, that content will be processed
	if content.AggregatedIn > 0 {
		return nil
	}

	// If this is the 'root' of a dag split, we dont need to process it, as the splits will be processed instead
	if content.DagSplit && content.SplitFrom == 0 {
		return nil
	}

	// if content is offloaded, do not proceed - since it needs the blocks for commp and data transfer
	if content.Offloaded {
		cm.log.Warnf("cont: %d offloaded for deal making", content.ID)
		go func() {
			if err := cm.RefreshContent(context.Background(), content.ID); err != nil {
				cm.log.Errorf("failed to retrieve content in need of repair %d: %s", content.ID, err)
			}
			done(time.Second * 30)
		}()
		return nil
	}

	var isCorrupt *model.SanityCheck
	if err := cm.db.First(&isCorrupt, "content_id = ?", content.ID).Error; err != nil {
		if !xerrors.Is(err, gorm.ErrRecordNotFound) {
			return err
		}
		isCorrupt = nil
	}
	if isCorrupt != nil {
		cm.log.Warnf("cnt: %d ignored due to missing blocks", content.ID)
		return nil
	}

	// it's too big, need to split it up into chunks, no need to requeue dagsplit root content
	if content.Size > cm.cfg.Content.MaxSize {
		return cm.splitContent(ctx, content, cm.cfg.Content.MaxSize)
	}

	// get content deals, if any
	var deals []model.ContentDeal
	if err := cm.db.Find(&deals, "content = ? AND NOT failed", content.ID).Error; err != nil {
		if !xerrors.Is(err, gorm.ErrRecordNotFound) {
			return err
		}
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

			status, err := cm.checkDeal(ctx, &dl, content)
			if err != nil {
				var dfe *DealFailureError
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
				if err := cm.repairDeal(&d); err != nil {
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
				cm.log.Errorf("unrecognized deal check status: %d", status)
			}
		}(i)
	}
	wg.Wait()

	// return the last error found, log the rest
	var retErr error
	for _, err := range errs {
		if err != nil {
			if retErr != nil {
				cm.log.Errorf("check deal failure: %s", err)
			}
			retErr = err
		}
	}
	if retErr != nil {
		return fmt.Errorf("deal check errored: %w", retErr)
	}

	// after reconciling content deals,
	// check If this is a shuttle content and that the shuttle is online and can start data transfer
	isOnline, err := cm.shuttleMgr.IsOnline(content.Location)
	if err != nil {
		done(time.Minute * 15)
		return err
	}

	if content.Location != constants.ContentLocationLocal && !isOnline {
		cm.log.Warnf("content shuttle: %s, is not online", content.Location)
		done(time.Minute * 15)
		return nil
	}

	replicationFactor := cm.cfg.Replication
	if content.Replication > 0 {
		replicationFactor = content.Replication
	}

	// check if content has enough good deals after reconcialiation,
	// if not enough good deals, go make more
	goodDeals := numSealed + numPublished + numProgress
	dealsToBeMade := replicationFactor - goodDeals
	if dealsToBeMade <= 0 {
		if numSealed >= replicationFactor {
			done(time.Hour * 24)
		} else if numSealed+numPublished >= replicationFactor {
			done(time.Hour)
		} else {
			done(time.Minute * 10)
		}
		return nil
	}

	cm.log.Infof("getting commp for cont: %d", content.ID)
	pc, err := cm.lookupPieceCommRecord(content.Cid.CID)
	if err != nil {
		return err
	}

	if pc == nil {
		// pre-compute piece commitment in a goroutine and dont block the checker loop while doing so
		go func() {
			_, _, _, err := cm.GetPieceCommitment(context.Background(), content.Cid.CID, cm.blockstore)
			if err != nil {
				if err == ErrWaitForRemoteCompute {
					cm.log.Warnf("waiting for shuttle: %s to finish commp for cont: %d", content.Location, content.ID)
				} else {
					cm.log.Errorf("failed to compute piece commitment for content %d: %s", content.ID, err)
				}
				done(time.Minute * 5)
			} else {
				done(time.Second * 10)
			}
		}()
		return nil
	}

	if cm.DealMakingDisabled() {
		cm.log.Warnf("deal making is disabled for now")
		done(time.Minute * 60)
		return nil
	}

	// only verified deals need datacap checks
	if cm.cfg.Deal.IsVerified {
		bl, err := cm.filClient.Balance(ctx)
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

	go func() {
		// make some more deals!
		cm.log.Infow("making more deals for content", "content", content.ID, "curDealCount", len(deals), "newDeals", dealsToBeMade)
		if err := cm.makeDealsForContent(ctx, content, dealsToBeMade, deals); err != nil {
			cm.log.Errorf("failed to make more deals: %s", err)
		}
		done(time.Minute * 10)
	}()
	return nil
}

func (cm *ContentManager) splitContent(ctx context.Context, cont util.Content, size int64) error {
	ctx, span := cm.tracer.Start(ctx, "splitContent")
	defer span.End()

	var u util.User
	if err := cm.db.First(&u, "id = ?", cont.UserID).Error; err != nil {
		return fmt.Errorf("failed to load contents user from db: %w", err)
	}

	if !u.FlagSplitContent() {
		return fmt.Errorf("user does not have content splitting enabled")
	}

	cm.log.Debugf("splitting content %d (size: %d)", cont.ID, size)

	if cont.Location == constants.ContentLocationLocal {
		go func() {
			if err := cm.splitContentLocal(ctx, cont, size); err != nil {
				cm.log.Errorw("failed to split local content", "cont", cont.ID, "size", size, "err", err)
			}
		}()
		return nil
	} else {
		return cm.shuttleMgr.SplitContent(ctx, cont.Location, cont.ID, size)
	}
}

func (cm *ContentManager) GetContent(id uint) (*util.Content, error) {
	var content util.Content
	if err := cm.db.First(&content, "id = ?", id).Error; err != nil {
		return nil, err
	}
	return &content, nil
}

const (
	DEAL_CHECK_UNKNOWN = iota
	DEAL_CHECK_PROGRESS
	DEAL_CHECK_DEALID_ON_CHAIN
	DEAL_CHECK_SECTOR_ON_CHAIN
	DEAL_NEARLY_EXPIRED
	DEAL_CHECK_SLASHED
)

// first check deal protocol version 2, then check version 1
func (cm *ContentManager) GetProviderDealStatus(ctx context.Context, d *model.ContentDeal, maddr address.Address, dealUUID *uuid.UUID) (*storagemarket.ProviderDealState, bool, error) {
	isPushTransfer := false
	providerDealState, err := cm.filClient.DealStatus(ctx, maddr, d.PropCid.CID, dealUUID)
	if err != nil && providerDealState == nil {
		isPushTransfer = true
		providerDealState, err = cm.filClient.DealStatus(ctx, maddr, d.PropCid.CID, nil)
	}
	return providerDealState, isPushTransfer, err
}

func (cm *ContentManager) checkDeal(ctx context.Context, d *model.ContentDeal, content util.Content) (int, error) {
	ctx, cancel := context.WithTimeout(ctx, time.Minute*5) // NB: if we ever hit this, its bad. but we at least need *some* timeout there
	defer cancel()

	ctx, span := cm.tracer.Start(ctx, "checkDeal", trace.WithAttributes(
		attribute.Int("deal", int(d.ID)),
	))
	defer span.End()
	cm.log.Debugw("checking deal", "miner", d.Miner, "content", d.Content, "dbid", d.ID)

	maddr, err := d.MinerAddr()
	if err != nil {
		return DEAL_CHECK_UNKNOWN, err
	}

	// get the deal data transfer state
	chanst, err := cm.transferMgr.GetTransferStatus(ctx, d, content.Cid.CID, content.Location)
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
			if err := cm.db.Model(model.ContentDeal{}).Where("id = ?", d.ID).UpdateColumns(updates).Error; err != nil {
				return DEAL_CHECK_UNKNOWN, err
			}
		}
	}

	// get chain head - needed to check expired deals below
	head, err := cm.api.ChainHead(ctx)
	if err != nil {
		return DEAL_CHECK_UNKNOWN, fmt.Errorf("failed to check chain head: %w", err)
	}

	// if the deal is on chain, then check is it still healthy (slashed, expired etc) and it's sealed(active) state
	if d.DealID != 0 {
		ok, deal, err := cm.filClient.CheckChainDeal(ctx, abi.DealID(d.DealID))
		if err != nil {
			return DEAL_CHECK_UNKNOWN, fmt.Errorf("failed to check chain deal: %w", err)
		}
		if !ok {
			return DEAL_CHECK_UNKNOWN, nil
		}

		// check slashed health
		if deal.State.SlashEpoch > 0 {
			if err := cm.db.Model(model.ContentDeal{}).Where("id = ?", d.ID).UpdateColumn("slashed", true).Error; err != nil {
				return DEAL_CHECK_UNKNOWN, err
			}

			if err := cm.recordDealFailure(&DealFailureError{
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
				if err := cm.db.Model(model.ContentDeal{}).Where("id = ?", d.ID).UpdateColumn("sealed_at", time.Now()).Error; err != nil {
					return DEAL_CHECK_UNKNOWN, err
				}
			}
			return DEAL_CHECK_SECTOR_ON_CHAIN, nil
		}
		return DEAL_CHECK_DEALID_ON_CHAIN, nil
	}

	// case where deal isnt yet on chain, then check deal state with miner/provider
	cm.log.Debugw("checking deal status with miner", "miner", maddr, "propcid", d.PropCid.CID, "dealUUID", d.DealUUID)
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
	provds, isPushTransfer, err := cm.GetProviderDealStatus(subctx, d, maddr, dealUUID)
	if err != nil {
		// if we cant get deal status from a miner and the data hasnt landed on chain what do we do?
		expired, err := cm.dealHasExpired(ctx, d, head)
		if err != nil {
			return DEAL_CHECK_UNKNOWN, xerrors.Errorf("failed to check if deal was expired: %w", err)
		}

		if expired {
			// deal expired, miner didnt start it in time
			if err := cm.recordDealFailure(&DealFailureError{
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
			if err := cm.recordDealFailure(&DealFailureError{
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
		cm.log.Warnf("deal state for deal %d from miner %s is error: %s", d.ID, maddr.String(), provds.Message)
	}

	if provds.DealID != 0 {
		deal, err := cm.api.StateMarketStorageDeal(ctx, provds.DealID, types.EmptyTSK)
		if err != nil || deal == nil {
			return DEAL_CHECK_UNKNOWN, fmt.Errorf("failed to lookup deal on chain: %w", err)
		}

		pcr, err := cm.lookupPieceCommRecord(content.Cid.CID)
		if err != nil || pcr == nil {
			return DEAL_CHECK_UNKNOWN, xerrors.Errorf("failed to look up piece commitment for content: %w", err)
		}

		if deal.Proposal.Provider != maddr || deal.Proposal.PieceCID != pcr.Piece.CID {
			cm.log.Warnf("proposal in deal ID miner sent back did not match our expectations")
			return DEAL_CHECK_UNKNOWN, nil
		}

		cm.log.Debugf("Confirmed deal ID, updating in database: %d %d %d", d.Content, d.ID, provds.DealID)
		if err := cm.updateDealID(d, int64(provds.DealID)); err != nil {
			return DEAL_CHECK_UNKNOWN, err
		}
		return DEAL_CHECK_DEALID_ON_CHAIN, nil
	}

	if provds.PublishCid != nil {
		cm.log.Debugw("checking publish CID", "content", d.Content, "miner", d.Miner, "propcid", d.PropCid.CID, "publishCid", *provds.PublishCid)
		id, err := cm.getDealID(ctx, *provds.PublishCid, d)
		if err != nil {
			cm.log.Debugf("failed to find message on chain: %s", *provds.PublishCid)
			if provds.Proposal.StartEpoch < head.Height() {
				// deal expired, miner didn`t start it in time
				if err := cm.recordDealFailure(&DealFailureError{
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

		cm.log.Debugf("Found deal ID, updating in database: %d %d %d", d.Content, d.ID, id)
		if err := cm.updateDealID(d, int64(id)); err != nil {
			return DEAL_CHECK_UNKNOWN, err
		}
		return DEAL_CHECK_DEALID_ON_CHAIN, nil
	}

	// proposal expired, miner didnt start it in time
	if provds.Proposal.StartEpoch < head.Height() {
		if err := cm.recordDealFailure(&DealFailureError{
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
			cm.log.Warnf("creating new data transfer for local deal that is missing it: %d", d.ID)
			if err := cm.transferMgr.StartDataTransfer(ctx, d); err != nil {
				cm.log.Errorw("failed to start new data transfer for weird state deal", "deal", d.ID, "miner", d.Miner, "err", err)
				// If this fails out, just fail the deal and start from
				// scratch. This is already a weird state.
				return DEAL_CHECK_UNKNOWN, nil
			}
		}
		return DEAL_CHECK_PROGRESS, nil
	}

	if chanst != nil {
		if trsFailed, _ := util.TransferFailed(chanst); trsFailed {
			if err := cm.recordDealFailure(&DealFailureError{
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

			if cm.cfg.Deal.FailOnTransferFailure {
				return DEAL_CHECK_UNKNOWN, nil
			}
		}
	}
	return DEAL_CHECK_PROGRESS, nil
}

func (cm *ContentManager) updateDealID(d *model.ContentDeal, id int64) error {
	if err := cm.db.Model(model.ContentDeal{}).Where("id = ?", d.ID).Updates(map[string]interface{}{
		"deal_id":     id,
		"on_chain_at": time.Now(),
	}).Error; err != nil {
		return err
	}
	return nil
}

func (cm *ContentManager) dealHasExpired(ctx context.Context, d *model.ContentDeal, head *types.TipSet) (bool, error) {
	prop, err := cm.getProposalRecord(d.PropCid.CID)
	if err != nil {
		cm.log.Warnf("failed to get proposal record for deal %d: %s", d.ID, err)

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

func (cm *ContentManager) getDealID(ctx context.Context, pubcid cid.Cid, d *model.ContentDeal) (abi.DealID, error) {
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

func (cm *ContentManager) repairDeal(d *model.ContentDeal) error {
	if d.DealID != 0 {
		cm.log.Debugw("miner faulted on deal", "deal", d.DealID, "content", d.Content, "miner", d.Miner)
		maddr, err := d.MinerAddr()
		if err != nil {
			cm.log.Errorf("failed to get miner address from deal (%s): %w", d.Miner, err)
		}

		if err := cm.recordDealFailure(&DealFailureError{
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

	cm.log.Debugw("repair deal", "propcid", d.PropCid.CID, "miner", d.Miner, "content", d.Content)
	if err := cm.db.Model(model.ContentDeal{}).Where("id = ?", d.ID).UpdateColumns(map[string]interface{}{
		"failed":    true,
		"failed_at": time.Now(),
	}).Error; err != nil {
		return err
	}
	return nil
}

func (cm *ContentManager) CheckContentReadyForDealMaking(ctx context.Context, content util.Content) error {
	if !content.Active || content.Pinning {
		return fmt.Errorf("cannot make deals for content that is not pinned")
	}

	if content.Offloaded {
		return fmt.Errorf("cannot make more deals for offloaded content, must retrieve first")
	}

	if content.Size < cm.cfg.Content.MinSize {
		return fmt.Errorf("content %d below individual deal size threshold. (size: %d, threshold: %d)", content.ID, content.Size, cm.cfg.Content.MinSize)
	}

	// if it's a shuttle content and the shuttle is not online, do not proceed
	isOnline, err := cm.shuttleMgr.IsOnline(content.Location)
	if err != nil {
		return err
	}

	if content.Location != constants.ContentLocationLocal && !isOnline {
		return fmt.Errorf("content shuttle: %s, is not online", content.Location)
	}

	return nil
}

func (cm *ContentManager) makeDealsForContent(ctx context.Context, content util.Content, dealsToBeMade int, existingContDeals []model.ContentDeal) error {
	ctx, span := cm.tracer.Start(ctx, "makeDealsForContent", trace.WithAttributes(
		attribute.Int64("content", int64(content.ID)),
		attribute.Int("count", dealsToBeMade),
	))
	defer span.End()

	if err := cm.CheckContentReadyForDealMaking(ctx, content); err != nil {
		return errors.Wrapf(err, "content %d not ready for dealmaking", content.ID)
	}

	_, _, pieceSize, err := cm.GetPieceCommitment(ctx, content.Cid.CID, cm.blockstore)
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

	miners, err := cm.minerManager.PickMiners(ctx, dealsToBeMade*2, pieceSize.Padded(), excludedMiners, true)
	if err != nil {
		return err
	}

	var readyDeals []deal
	for _, m := range miners {
		cd, err := cm.MakeDealWithMiner(ctx, content, m.Address)
		if err != nil {
			return err
		}
		isPushTransfer := cd.DealProtocolVersion == filclient.DealProtocolv110
		readyDeals = append(readyDeals, deal{minerAddr: m.Address, isPushTransfer: isPushTransfer, contentDeal: cd})
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
			if err := cm.transferMgr.StartDataTransfer(ctx, d.contentDeal); err != nil {
				cm.log.Errorw("failed to start data transfer", "err", err, "miner", d.minerAddr)
			}
		}(rDeal)
	}
	return nil
}

func (cm *ContentManager) sendProposalV120(ctx context.Context, contentLoc string, netprop network.Proposal, propCid cid.Cid, dealUUID uuid.UUID, dbid uint) (func() error, bool, error) {
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
		err := cm.filClient.Libp2pTransferMgr.PrepareForDataRequest(ctx, dbid, authToken, propCid, rootCid, size)
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
			return cm.filClient.Libp2pTransferMgr.CleanupPreparedRequest(ctx, dbid, authToken)
		}
		return cm.shuttleMgr.CleanupPreparedRequest(ctx, contentLoc, dbid, authToken)
	}

	// Send the deal proposal to the storage provider
	propPhase, err := cm.filClient.SendProposalV120(ctx, dbid, netprop, dealUUID, announceAddr, authToken)
	return cleanup, propPhase, err
}

func (cm *ContentManager) MakeDealWithMiner(ctx context.Context, content util.Content, miner address.Address) (*model.ContentDeal, error) {
	ctx, span := cm.tracer.Start(ctx, "makeDealWithMiner", trace.WithAttributes(
		attribute.Int64("content", int64(content.ID)),
		attribute.Stringer("miner", miner),
	))
	defer span.End()

	proto, err := cm.minerManager.GetDealProtocolForMiner(ctx, miner)
	if err != nil {
		return nil, cm.recordDealFailure(&DealFailureError{
			Miner:   miner,
			Phase:   "deal-protocol-version",
			Message: err.Error(),
			Content: content.ID,
			UserID:  content.UserID,
		})
	}

	ask, err := cm.minerManager.GetAsk(ctx, miner, 0)
	if err != nil {
		var clientErr *filclient.Error
		if !(xerrors.As(err, &clientErr) && clientErr.Code == filclient.ErrLotusError) {
			if err := cm.recordDealFailure(&DealFailureError{
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

	price := ask.GetPrice(cm.cfg.Deal.IsVerified)
	if ask.PriceIsTooHigh(cm.cfg) {
		return nil, fmt.Errorf("miners price is too high: %s %s", miner, price)
	}

	prop, err := cm.filClient.MakeDeal(ctx, miner, content.Cid.CID, price, ask.MinPieceSize, cm.cfg.Deal.Duration, cm.cfg.Deal.IsVerified)
	if err != nil {
		return nil, xerrors.Errorf("failed to construct a deal proposal: %w", err)
	}

	dp, err := cm.putProposalRecord(prop.DealProposal)
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
		Verified:            cm.cfg.Deal.IsVerified,
		UserID:              content.UserID,
		DealProtocolVersion: proto,
		MinerVersion:        ask.MinerVersion,
	}

	if err := cm.db.Create(deal).Error; err != nil {
		return nil, xerrors.Errorf("failed to create database entry for deal: %w", err)
	}

	// Send the deal proposal to the storage provider
	var cleanupDealPrep func() error
	var propPhase bool
	isPushTransfer := proto == filclient.DealProtocolv110

	switch proto {
	case filclient.DealProtocolv110:
		propPhase, err = cm.filClient.SendProposalV110(ctx, *prop, propnd.Cid())
	case filclient.DealProtocolv120:
		cleanupDealPrep, propPhase, err = cm.sendProposalV120(ctx, content.Location, *prop, propnd.Cid(), dealUUID, deal.ID)
	default:
		err = fmt.Errorf("unrecognized deal protocol %s", proto)
	}

	if err != nil {
		// Clean up the database entry
		if err := cm.db.Delete(&model.ContentDeal{}, deal).Error; err != nil {
			return nil, fmt.Errorf("failed to delete content deal from db: %w", err)
		}

		// Clean up the proposal database entry
		if err := cm.db.Delete(&model.ProposalRecord{}, dp).Error; err != nil {
			return nil, fmt.Errorf("failed to delete deal proposal from db: %w", err)
		}

		// Clean up the preparation for deal request
		if cleanupDealPrep != nil {
			if err := cleanupDealPrep(); err != nil {
				cm.log.Errorw("cleaning up deal prepared request", "error", err)
			}
		}

		// Record a deal failure
		phase := "send-proposal"
		if propPhase {
			phase = "propose"
		}
		if err := cm.recordDealFailure(&DealFailureError{
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
	if err := cm.transferMgr.StartDataTransfer(ctx, deal); err != nil {
		return nil, fmt.Errorf("failed to start data transfer: %w", err)
	}
	return deal, nil
}

func (cm *ContentManager) putProposalRecord(dealprop *marketv9.ClientDealProposal) (*model.ProposalRecord, error) {
	nd, err := cborutil.AsIpld(dealprop)
	if err != nil {
		return nil, err
	}

	dp := &model.ProposalRecord{
		PropCid: util.DbCID{CID: nd.Cid()},
		Data:    nd.RawData(),
	}

	if err := cm.db.Create(dp).Error; err != nil {
		return nil, err
	}
	return dp, nil
}

func (cm *ContentManager) getProposalRecord(propCid cid.Cid) (*market.ClientDealProposal, error) {
	var proprec model.ProposalRecord
	if err := cm.db.First(&proprec, "prop_cid = ?", propCid.Bytes()).Error; err != nil {
		return nil, err
	}

	var prop market.ClientDealProposal
	if err := prop.UnmarshalCBOR(bytes.NewReader(proprec.Data)); err != nil {
		return nil, err
	}

	return &prop, nil
}

func (cm *ContentManager) recordDealFailure(dfe *DealFailureError) error {
	cm.log.Debugw("deal failure error", "miner", dfe.Miner, "uuid", dfe.DealUUID, "phase", dfe.Phase, "msg", dfe.Message, "content", dfe.Content)
	rec := dfe.Record()
	return cm.db.Create(rec).Error
}

type DealFailureError struct {
	Miner               address.Address
	DealUUID            string
	Phase               string
	Message             string
	Content             uint
	UserID              uint
	MinerAddress        string
	DealProtocolVersion protocol.ID
	MinerVersion        string
}

func (dfe *DealFailureError) Record() *model.DfeRecord {
	return &model.DfeRecord{
		Miner:               dfe.Miner.String(),
		DealUUID:            dfe.DealUUID,
		Phase:               dfe.Phase,
		Message:             dfe.Message,
		Content:             dfe.Content,
		UserID:              dfe.UserID,
		MinerVersion:        dfe.MinerVersion,
		DealProtocolVersion: dfe.DealProtocolVersion,
	}
}

func (dfe *DealFailureError) Error() string {
	return fmt.Sprintf("deal %s with miner %s failed in phase %s: %s", dfe.DealUUID, dfe.Message, dfe.Phase, dfe.Message)
}

// addObjectsToDatabase creates entries on the estuary database for CIDs related to an already pinned CID (`root`)
// These entries are saved on the `objects` table, while metadata about the `root` CID is mostly kept on the `contents` table
// The link between the `objects` and `contents` tables is the `obj_refs` table
func (cm *ContentManager) addObjectsToDatabase(ctx context.Context, contID uint, objects []*util.Object, loc string) (int64, error) {
	_, span := cm.tracer.Start(ctx, "addObjectsToDatabase")
	defer span.End()

	if err := cm.db.CreateInBatches(objects, 300).Error; err != nil {
		return 0, xerrors.Errorf("failed to create objects in db: %w", err)
	}

	refs := make([]util.ObjRef, 0, len(objects))
	var totalSize int64
	for _, o := range objects {
		refs = append(refs, util.ObjRef{
			Content: contID,
			Object:  o.ID,
		})
		totalSize += int64(o.Size)
	}

	span.SetAttributes(
		attribute.Int64("totalSize", totalSize),
		attribute.Int("numObjects", len(objects)),
	)

	if err := cm.db.CreateInBatches(refs, 500).Error; err != nil {
		return 0, xerrors.Errorf("failed to create refs: %w", err)
	}

	if err := cm.db.Model(util.Content{}).Where("id = ?", contID).UpdateColumns(map[string]interface{}{
		"active":   true,
		"size":     totalSize,
		"pinning":  false,
		"location": loc,
	}).Error; err != nil {
		return 0, xerrors.Errorf("failed to update content in database: %w", err)
	}
	return totalSize, nil
}

func (cm *ContentManager) migrateContentsToLocalNode(ctx context.Context, toMove []util.Content) error {
	for _, c := range toMove {
		if err := cm.migrateContentToLocalNode(ctx, c); err != nil {
			return err
		}
	}
	return nil
}

func (cm *ContentManager) migrateContentToLocalNode(ctx context.Context, cont util.Content) error {
	done, err := cm.safeFetchData(ctx, cont.Cid.CID)
	if err != nil {
		return fmt.Errorf("failed to fetch data: %w", err)
	}

	defer done()

	if err := cm.db.Model(util.ObjRef{}).Where("id = ?", cont.ID).UpdateColumns(map[string]interface{}{
		"offloaded": 0,
	}).Error; err != nil {
		return err
	}

	if err := cm.db.Model(util.Content{}).Where("id = ?", cont.ID).UpdateColumns(map[string]interface{}{
		"offloaded": false,
		"location":  constants.ContentLocationLocal,
	}).Error; err != nil {
		return err
	}

	// TODO: send unpin command to where the content was migrated from
	return nil
}

func (cm *ContentManager) safeFetchData(ctx context.Context, c cid.Cid) (func(), error) {
	// TODO: this method should mark each object it fetches as 'needed' before pulling the data so that
	// any concurrent deletion tasks can avoid deleting our data as we fetch it

	bserv := blockservice.New(cm.blockstore, cm.node.Bitswap)
	dserv := merkledag.NewDAGService(bserv)

	deref := func() {
		cm.log.Warnf("TODO: implement safe fetch data protections")
	}

	cset := cid.NewSet()
	err := merkledag.Walk(ctx, func(ctx context.Context, c cid.Cid) ([]*ipld.Link, error) {
		node, err := dserv.Get(ctx, c)
		if err != nil {
			return nil, err
		}

		if c.Type() == cid.Raw {
			return nil, nil
		}

		return util.FilterUnwalkableLinks(node.Links()), nil
	}, c, cset.Visit, merkledag.Concurrent())
	if err != nil {
		return deref, err
	}

	return deref, nil
}

func (cm *ContentManager) addrInfoForContentLocation(handle string) (*peer.AddrInfo, error) {
	if handle == constants.ContentLocationLocal {
		return &peer.AddrInfo{
			ID:    cm.node.Host.ID(),
			Addrs: cm.node.Host.Addrs(),
		}, nil
	}
	return cm.shuttleMgr.AddrInfo(handle)
}

func (cm *ContentManager) DealMakingDisabled() bool {
	cm.dealDisabledLk.Lock()
	defer cm.dealDisabledLk.Unlock()
	return cm.isDealMakingDisabled
}

func (cm *ContentManager) SetDealMakingEnabled(enable bool) {
	cm.dealDisabledLk.Lock()
	defer cm.dealDisabledLk.Unlock()
	cm.isDealMakingDisabled = !enable
}

func (cm *ContentManager) splitContentLocal(ctx context.Context, cont util.Content, size int64) error {
	dserv := merkledag.NewDAGService(blockservice.New(cm.node.Blockstore, nil))
	b := dagsplit.NewBuilder(dserv, uint64(size), 0)
	if err := b.Pack(ctx, cont.Cid.CID); err != nil {
		return err
	}

	cst := cbor.NewCborStore(cm.node.Blockstore)

	var boxCids []cid.Cid
	for _, box := range b.Boxes() {
		cc, err := cst.Put(ctx, box)
		if err != nil {
			return err
		}
		boxCids = append(boxCids, cc)
	}

	for i, c := range boxCids {
		content := &util.Content{
			Cid:         util.DbCID{CID: c},
			Name:        fmt.Sprintf("%s-%d", cont.Name, i),
			Active:      false, // will be active after it's blocks are saved
			Pinning:     true,
			UserID:      cont.UserID,
			Replication: cont.Replication,
			Location:    constants.ContentLocationLocal,
			DagSplit:    true,
			SplitFrom:   cont.ID,
		}

		if err := cm.db.Create(content).Error; err != nil {
			return xerrors.Errorf("failed to track new content in database: %w", err)
		}

		cntSize, err := cm.AddDatabaseTrackingToContent(ctx, content.ID, dserv, c, func(int64) {})
		if err != nil {
			return err
		}

		// queue splited contents
		cm.log.Debugw("queuing splited content child", "parent_contID", cont.ID, "child_contID", content.ID)
		cm.ToCheck(content.ID, cntSize)
	}

	if err := cm.db.Model(util.Content{}).Where("id = ?", cont.ID).UpdateColumns(map[string]interface{}{
		"dag_split": true,
		"size":      0,
		"active":    false,
		"pinning":   false,
	}).Error; err != nil {
		return err
	}
	return cm.db.Where("content = ?", cont.ID).Delete(&util.ObjRef{}).Error
}

var noDataTimeout = time.Minute * 10

func (cm *ContentManager) AddDatabaseTrackingToContent(ctx context.Context, cont uint, dserv ipld.NodeGetter, root cid.Cid, cb func(int64)) (int64, error) {
	ctx, span := cm.tracer.Start(ctx, "computeObjRefsUpdate")
	defer span.End()

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	gotData := make(chan struct{}, 1)
	go func() {
		nodata := time.NewTimer(noDataTimeout)
		defer nodata.Stop()

		for {
			select {
			case <-nodata.C:
				cancel()
			case <-gotData:
				nodata.Reset(noDataTimeout)
			case <-ctx.Done():
				return
			}
		}
	}()

	var objlk sync.Mutex
	var objects []*util.Object
	cset := cid.NewSet()

	defer func() {
		cm.inflightCidsLk.Lock()
		_ = cset.ForEach(func(c cid.Cid) error {
			v, ok := cm.inflightCids[c]
			if !ok || v <= 0 {
				cm.log.Errorf("cid should be inflight but isn't: %s", c)
			}

			cm.inflightCids[c]--
			if cm.inflightCids[c] == 0 {
				delete(cm.inflightCids, c)
			}
			return nil
		})
		cm.inflightCidsLk.Unlock()
	}()

	err := merkledag.Walk(ctx, func(ctx context.Context, c cid.Cid) ([]*ipld.Link, error) {
		// cset.Visit gets called first, so if we reach here we should immediately track the CID
		cm.inflightCidsLk.Lock()
		cm.inflightCids[c]++
		cm.inflightCidsLk.Unlock()

		node, err := dserv.Get(ctx, c)
		if err != nil {
			return nil, err
		}

		cb(int64(len(node.RawData())))

		select {
		case gotData <- struct{}{}:
		case <-ctx.Done():
		}

		objlk.Lock()
		objects = append(objects, &util.Object{
			Cid:  util.DbCID{CID: c},
			Size: len(node.RawData()),
		})
		objlk.Unlock()

		if c.Type() == cid.Raw {
			return nil, nil
		}

		return util.FilterUnwalkableLinks(node.Links()), nil
	}, root, cset.Visit, merkledag.Concurrent())

	if err != nil {
		return 0, err
	}
	return cm.addObjectsToDatabase(ctx, cont, objects, constants.ContentLocationLocal)
}

func (cm *ContentManager) AddDatabaseTracking(ctx context.Context, u *util.User, dserv ipld.NodeGetter, root cid.Cid, filename string, replication int) (*util.Content, error) {
	ctx, span := cm.tracer.Start(ctx, "computeObjRefs")
	defer span.End()

	content := &util.Content{
		Cid:         util.DbCID{CID: root},
		Name:        filename,
		Active:      false,
		Pinning:     true,
		UserID:      u.ID,
		Replication: replication,
		Location:    constants.ContentLocationLocal,
	}

	if err := cm.db.Create(content).Error; err != nil {
		return nil, xerrors.Errorf("failed to track new content in database: %w", err)
	}

	cntSize, err := cm.AddDatabaseTrackingToContent(ctx, content.ID, dserv, root, func(int64) {})
	if err != nil {
		return nil, err
	}
	content.Size = cntSize
	return content, nil
}
