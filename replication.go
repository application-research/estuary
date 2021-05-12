package main

import (
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/filecoin-project/go-address"
	cborutil "github.com/filecoin-project/go-cbor-util"
	datatransfer "github.com/filecoin-project/go-data-transfer"
	"github.com/filecoin-project/go-fil-markets/storagemarket"
	"github.com/filecoin-project/go-fil-markets/storagemarket/network"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/big"
	"github.com/filecoin-project/lotus/api"
	"github.com/filecoin-project/lotus/chain/types"
	"github.com/filecoin-project/specs-actors/actors/builtin/market"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	"github.com/libp2p/go-libp2p-core/peer"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	"github.com/whyrusleeping/estuary/filclient"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/xerrors"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

const defaultReplication = 6

type ContentManager struct {
	DB        *gorm.DB
	Api       api.Gateway
	FilClient *filclient.FilClient
	Dht       *dht.IpfsDHT

	tracer trace.Tracer

	Blockstore blockstore.Blockstore
	Tracker    *TrackingBlockstore

	ToCheck chan uint

	retrLk               sync.Mutex
	retrievalsInProgress map[uint]*retrievalProgress

	contentLk sync.RWMutex

	// Some fields for miner reputation management
	minerLk      sync.Mutex
	sortedMiners []address.Address
	lastComputed time.Time
}

func NewContentManager(db *gorm.DB, api api.Gateway, fc *filclient.FilClient, tbs *TrackingBlockstore, dht *dht.IpfsDHT) *ContentManager {
	return &ContentManager{
		Dht:                  dht,
		DB:                   db,
		Api:                  api,
		FilClient:            fc,
		Blockstore:           tbs.Under(),
		Tracker:              tbs,
		ToCheck:              make(chan uint, 10),
		retrievalsInProgress: make(map[uint]*retrievalProgress),
	}
}

func (cm *ContentManager) ContentWatcher() {
	if err := cm.startup(); err != nil {
		log.Errorf("failed to recheck existing content: %s", err)
	}

	timer := time.NewTimer(time.Minute * 5)

	for {
		select {
		case c := <-cm.ToCheck:
			var content Content
			if err := cm.DB.First(&content, "id = ?", c).Error; err != nil {
				log.Errorf("finding content %d in database: %s", c, err)
				continue
			}

			if err := cm.ensureStorage(context.TODO(), content); err != nil {
				log.Errorf("failed to ensure replication of content %d: %s", content.ID, err)
				continue
			}
		case <-timer.C:
			if err := cm.queueAllContent(); err != nil {
				log.Errorf("rechecking content: %s", err)
				continue
			}
			timer.Reset(time.Minute * 5)
		}
	}
}

func (cm *ContentManager) startup() error {
	go cm.RunReprovider()

	return cm.queueAllContent()
}

func (cm *ContentManager) queueAllContent() error {
	var allcontent []Content
	if err := cm.DB.Find(&allcontent, "active").Error; err != nil {
		return xerrors.Errorf("finding all content in database: %w", err)
	}

	go func() {
		for _, c := range allcontent {
			cm.ToCheck <- c.ID
		}
	}()

	return nil
}

type estimateResponse struct {
	Total *abi.TokenAmount
	Asks  []*minerStorageAsk
}

func (cm *ContentManager) estimatePrice(ctx context.Context, repl int, size abi.PaddedPieceSize, duration abi.ChainEpoch, verified bool) (*estimateResponse, error) {
	ctx, span := cm.tracer.Start(ctx, "estimatePrice", trace.WithAttributes(
		attribute.Int("replication", repl),
	))
	defer span.End()

	miners, err := cm.pickMiners(ctx, repl, size, nil)
	if err != nil {
		return nil, err
	}
	if len(miners) == 0 {
		return nil, fmt.Errorf("failed to find any miners for estimating deal price")
	}

	var asks []*minerStorageAsk
	total := abi.NewTokenAmount(0)
	for _, m := range miners {
		ask, err := cm.getAsk(ctx, m, time.Minute*30)
		if err != nil {
			return nil, err
		}

		asks = append(asks, ask)

		var price *abi.TokenAmount
		if verified {
			p, err := ask.GetVerifiedPrice()
			if err != nil {
				return nil, err
			}
			price = p
		} else {
			p, err := ask.GetPrice()
			if err != nil {
				return nil, err
			}
			price = p
		}

		dealSize := size
		if dealSize < ask.MinPieceSize {
			dealSize = ask.MinPieceSize
		}

		cost, err := filclient.ComputePrice(*price, dealSize, duration)
		if err != nil {
			return nil, err
		}

		total = types.BigAdd(total, *cost)
	}

	return &estimateResponse{
		Total: &total,
		Asks:  asks,
	}, nil
}

type minerStorageAsk struct {
	gorm.Model    `json:"-"`
	Miner         string              `gorm:"unique" json:"miner"`
	Price         string              `json:"price"`
	VerifiedPrice string              `json:"verifiedPrice"`
	MinPieceSize  abi.PaddedPieceSize `json:"minPieceSize"`
	MaxPieceSize  abi.PaddedPieceSize `json:"maxPieceSize"`
}

func (msa *minerStorageAsk) GetPrice() (*types.BigInt, error) {
	v, err := types.BigFromString(msa.Price)
	if err != nil {
		return nil, err
	}

	return &v, nil
}

func (msa *minerStorageAsk) GetVerifiedPrice() (*types.BigInt, error) {
	v, err := types.BigFromString(msa.VerifiedPrice)
	if err != nil {
		return nil, err
	}

	return &v, nil
}

func (cm *ContentManager) pickMinerDist(n int) (int, int) {
	if n < 3 {
		return n, 0
	}

	if n < 5 {
		return 3, n - 2
	}

	return n - (n / 2), n / 2

}
func (cm *ContentManager) pickMiners(ctx context.Context, n int, size abi.PaddedPieceSize, exclude map[address.Address]bool) ([]address.Address, error) {
	ctx, span := cm.tracer.Start(ctx, "pickMiners", trace.WithAttributes(
		attribute.Int("count", n),
	))
	defer span.End()
	if exclude == nil {
		exclude = make(map[address.Address]bool)
	}

	// some portion of the miners will be 'first N of our best miners' and the rest will be randomly chosen from our list
	// over time, our miner list will be all fairly high quality so this should just serve to shake things up a bit and
	// give miners more of a chance to prove themselves
	_, nrand := cm.pickMinerDist(n)

	randminers, err := cm.randomMinerList()
	if err != nil {
		return nil, err
	}

	var out []address.Address
	for _, m := range randminers {
		if len(out) >= nrand {
			break
		}

		if exclude[m] {
			continue
		}

		exclude[m] = true

		ask, err := cm.getAsk(ctx, m, time.Minute*30)
		if err != nil {
			log.Errorf("getting ask from %s failed: %s", m, err)
			continue
		}

		if cm.sizeIsCloseEnough(size, ask.MinPieceSize) {
			out = append(out, m)
		}
	}

	sortedminers, err := cm.sortedMinerList()
	if err != nil {
		return nil, err
	}

	for _, m := range sortedminers {
		if len(out) >= n {
			break
		}

		if exclude[m] {
			continue
		}

		ask, err := cm.getAsk(ctx, m, time.Minute*30)
		if err != nil {
			log.Errorf("getting ask from %s failed: %s", m, err)
			continue
		}

		if cm.sizeIsCloseEnough(size, ask.MinPieceSize) {
			out = append(out, m)
		}
	}

	return out, nil
}

func (cm *ContentManager) randomMinerList() ([]address.Address, error) {
	var dbminers []storageMiner
	if err := cm.DB.Order("random()").Find(&dbminers, "not suspended").Error; err != nil {
		return nil, err
	}

	out := make([]address.Address, 0, len(dbminers))
	for _, dbm := range dbminers {
		out = append(out, dbm.Address.Addr)
	}

	return out, nil
}

func (cm *ContentManager) getAsk(ctx context.Context, m address.Address, maxCacheAge time.Duration) (*minerStorageAsk, error) {
	ctx, span := cm.tracer.Start(ctx, "getAsk", trace.WithAttributes(
		attribute.Stringer("miner", m),
	))
	defer span.End()

	var msa minerStorageAsk
	if err := cm.DB.First(&msa, "miner = ?", m.String()).Error; err != nil {
		if !xerrors.Is(err, gorm.ErrRecordNotFound) {
			return nil, err
		}
	}

	if time.Since(msa.UpdatedAt) < maxCacheAge {
		return &msa, nil
	}

	netask, err := cm.FilClient.GetAsk(ctx, m)
	if err != nil {
		var apierr *filclient.ApiError
		if !xerrors.As(err, &apierr) {
			cm.recordDealFailure(&DealFailureError{
				Miner:   m,
				Phase:   "query-ask",
				Message: err.Error(),
			})
		}
		span.RecordError(err)
		return nil, err
	}

	nmsa := toDBAsk(netask)

	if err := cm.DB.Clauses(clause.OnConflict{
		Columns: []clause.Column{
			{Name: "miner"},
		},
		DoUpdates: clause.AssignmentColumns([]string{"price", "verified_price", "min_piece_size"}),
	}).Create(nmsa).Error; err != nil {
		span.RecordError(err)
		return nil, err
	}

	return nmsa, nil
}

func (cm *ContentManager) sizeIsCloseEnough(fsize, limit abi.PaddedPieceSize) bool {
	if fsize > limit {
		return true
	}

	/*
		if fsize*64 > limit {
			return true
		}
	*/

	return false
}

func toDBAsk(netask *network.AskResponse) *minerStorageAsk {
	return &minerStorageAsk{
		Miner:         netask.Ask.Ask.Miner.String(),
		Price:         netask.Ask.Ask.Price.String(),
		VerifiedPrice: netask.Ask.Ask.VerifiedPrice.String(),
		MinPieceSize:  netask.Ask.Ask.MinPieceSize,
		MaxPieceSize:  netask.Ask.Ask.MaxPieceSize,
	}
}

type contentDeal struct {
	gorm.Model
	Content  uint      `json:"content"`
	PropCid  dbCID     `json:"propCid"`
	Miner    string    `json:"miner"`
	DealID   int64     `json:"dealId"`
	Failed   bool      `json:"failed"`
	Verified bool      `json:"verified"`
	FailedAt time.Time `json:"failedAt,omitempty"`
	DTChan   string    `json:"dtChan"`
}

func (cd contentDeal) MinerAddr() (address.Address, error) {
	return address.NewFromString(cd.Miner)
}

var ErrNoChannelID = fmt.Errorf("no data transfer channel id in deal")

func (cd contentDeal) ChannelID() (datatransfer.ChannelID, error) {
	if cd.DTChan == "" {
		return datatransfer.ChannelID{}, ErrNoChannelID
	}

	parts := strings.Split(cd.DTChan, "-")
	if len(parts) != 3 {
		return datatransfer.ChannelID{}, fmt.Errorf("incorrectly formatted data transfer channel ID in contentDeal record")
	}

	initiator, err := peer.Decode(parts[0])
	if err != nil {
		return datatransfer.ChannelID{}, err
	}

	responder, err := peer.Decode(parts[1])
	if err != nil {
		return datatransfer.ChannelID{}, err
	}

	id, err := strconv.ParseUint(parts[2], 10, 64)
	if err != nil {
		return datatransfer.ChannelID{}, err
	}

	return datatransfer.ChannelID{
		Initiator: initiator,
		Responder: responder,
		ID:        datatransfer.TransferID(id),
	}, nil
}

func (cm *ContentManager) ensureStorage(ctx context.Context, content Content) error {
	ctx, span := cm.tracer.Start(ctx, "ensureStorage", trace.WithAttributes(
		attribute.Int("content", int(content.ID)),
	))
	defer span.End()

	verified := true

	// check if content has enough deals made for it
	// if not enough deals, go make more
	// check all existing deals, ensure they are still active
	// if not active, repair!

	var deals []contentDeal
	if err := cm.DB.Find(&deals, "content = ? AND NOT failed", content.ID).Error; err != nil {
		if !xerrors.Is(err, gorm.ErrRecordNotFound) {
			return err
		}
	}

	replicationFactor := defaultReplication
	if content.Replication > 0 {
		replicationFactor = content.Replication
	}

	minersAlready := make(map[address.Address]bool)
	for _, d := range deals {
		if d.Failed {
			// TODO: this is an interesting choice, because it gives miners more chances to try again if they fail.
			// I think that as we get a more diverse set of stable miners, we can *not* do this.
			continue
		}
		maddr, err := d.MinerAddr()
		if err != nil {
			return err
		}
		minersAlready[maddr] = true
	}

	if len(deals) < replicationFactor {
		// make some more deals!
		log.Infow("making more deals for content", "content", content.ID, "curDealCount", len(deals), "newDeals", replicationFactor-len(deals))
		if err := cm.makeDealsForContent(ctx, content, replicationFactor-len(deals), minersAlready, verified); err != nil {
			return err
		}
	}

	// check on each of the existing deals, see if they need fixing
	for _, d := range deals {
		ok, err := cm.checkDeal(ctx, &d)
		if err != nil {
			var dfe *DealFailureError
			if xerrors.As(err, &dfe) {
				cm.recordDealFailure(dfe)
				continue
			} else {
				return err
			}
		}

		if !ok {
			if err := cm.repairDeal(&d); err != nil {
				return xerrors.Errorf("repairing deal failed: %w", err)
			}
		}
	}

	return nil
}

func (cm *ContentManager) getContent(id uint) (*Content, error) {
	var content Content
	if err := cm.DB.First(&content, "id = ?", id).Error; err != nil {
		return nil, err
	}
	return &content, nil
}

func (cm *ContentManager) checkDeal(ctx context.Context, d *contentDeal) (bool, error) {
	ctx, span := cm.tracer.Start(ctx, "checkDeal", trace.WithAttributes(
		attribute.Int("deal", int(d.ID)),
	))
	defer span.End()
	log.Infow("checking deal", "miner", d.Miner, "content", d.Content, "dbid", d.ID)

	maddr, err := d.MinerAddr()
	if err != nil {
		return false, err
	}

	if d.DealID != 0 {
		return cm.FilClient.CheckChainDeal(ctx, abi.DealID(d.DealID))
	}

	// case where deal isnt yet on chain...

	provds, err := cm.FilClient.DealStatus(ctx, maddr, d.PropCid.CID)
	if err != nil {
		// if we cant get deal status from a miner and the data hasnt landed on
		// chain what do we do?
		return false, xerrors.Errorf("checking deal status through client failed: %w", err)
	}

	content, err := cm.getContent(d.Content)
	if err != nil {
		return false, err
	}

	head, err := cm.Api.ChainHead(ctx)
	if err != nil {
		return false, err
	}

	if provds.PublishCid != nil {
		log.Infow("checking publish CID", "content", d.Content, "miner", d.Miner, "propcid", d.PropCid.CID, "publishCid", *provds.PublishCid)
		id, err := cm.getDealID(ctx, *provds.PublishCid, d)
		if err != nil {
			if xerrors.Is(err, ErrNotOnChainYet) {
				// if they sent us a dealID, lets check it and verify
				if provds.DealID != 0 {
					deal, err := cm.Api.StateMarketStorageDeal(ctx, provds.DealID, types.EmptyTSK)
					if err == nil {
						pcr, err := cm.lookupPieceCommRecord(content.Cid.CID)
						if err != nil {
							return false, xerrors.Errorf("failed to look up piece commitment for content: %w", err)
						}

						if deal.Proposal.Provider != maddr || deal.Proposal.PieceCID != pcr.Piece.CID {
							log.Errorf("proposal in deal ID miner sent back did not match our expectations")
							return false, fmt.Errorf("deal checking issue")
						}

						log.Infof("Confirmed deal ID, updating in database: %d %d %d", d.Content, d.ID, id)
						d.DealID = int64(provds.DealID)
						if err := cm.DB.Save(&d).Error; err != nil {
							return false, xerrors.Errorf("failed to update database entry: %w", err)
						}

						return true, nil
					}
				}

				log.Infof("publish message not landed on chain yet: %s", *provds.PublishCid)
				if provds.Proposal.StartEpoch < head.Height() {
					// deal expired, miner didnt start it in time
					cm.recordDealFailure(&DealFailureError{
						Miner:   maddr,
						Phase:   "check-status",
						Message: "deal did not make it on chain in time (but has publish deal cid set)",
						Content: d.Content,
					})
					return false, nil
				}
				return true, nil
			}
			return false, xerrors.Errorf("failed to check deal id: %w", err)
		}

		log.Infof("Found deal ID, updating in database: %d %d %d", d.Content, d.ID, id)
		d.DealID = int64(id)
		if err := cm.DB.Save(&d).Error; err != nil {
			return false, xerrors.Errorf("failed to update database entry: %w", err)
		}
		return true, nil
	}

	if provds.Proposal.StartEpoch < head.Height() {
		// deal expired, miner didnt start it in time
		cm.recordDealFailure(&DealFailureError{
			Miner:   maddr,
			Phase:   "check-status",
			Message: "deal did not make it on chain in time",
			Content: d.Content,
		})
		return false, nil
	}
	// miner still has time...

	status, err := cm.GetTransferStatus(ctx, d, content.Cid.CID)
	if err != nil {
		return false, err
	}

	switch status.Status {
	case datatransfer.Failed:
		cm.recordDealFailure(&DealFailureError{
			Miner:   maddr,
			Phase:   "data-transfer",
			Message: fmt.Sprintf("transfer failed: %s", status.Message),
			Content: content.ID,
		})
	case datatransfer.Cancelled:
		cm.recordDealFailure(&DealFailureError{
			Miner:   maddr,
			Phase:   "data-transfer",
			Message: fmt.Sprintf("transfer cancelled: %s", status.Message),
			Content: content.ID,
		})
	case datatransfer.Requested:
		//fmt.Println("transfer is requested, hasnt started yet")
		// probably okay
	case datatransfer.TransferFinished, datatransfer.Finalizing, datatransfer.Completing, datatransfer.Completed:
		// these are all okay
		//fmt.Println("transfer is finished-ish!", status.Status)
	case datatransfer.Ongoing:
		//fmt.Println("transfer status is ongoing!")
		if err := cm.FilClient.CheckOngoingTransfer(ctx, maddr, status); err != nil {
			cm.recordDealFailure(&DealFailureError{
				Miner:   maddr,
				Phase:   "data-transfer",
				Message: fmt.Sprintf("error while checking transfer: %s", err),
				Content: content.ID,
			})
			return false, nil // TODO: returning false here feels excessive
		}
		// expected, this is fine
	default:
		fmt.Printf("Unexpected data transfer state: %d (msg = %s)\n", status.Status, status.Message)
	}

	return true, nil
}

func (cm *ContentManager) GetTransferStatus(ctx context.Context, d *contentDeal, ccid cid.Cid) (*filclient.ChannelState, error) {
	ctx, span := cm.tracer.Start(ctx, "getTransferStatus")
	defer span.End()

	miner, err := d.MinerAddr()
	if err != nil {
		return nil, err
	}

	chanid, err := d.ChannelID()
	switch err {
	case nil:
		chanst, err := cm.FilClient.TransferStatus(ctx, &chanid)
		if err != nil {
			return nil, err
		}
		return chanst, nil
	case ErrNoChannelID:
		chanst, err := cm.FilClient.TransferStatusForContent(ctx, ccid, miner)
		if err != nil && err != filclient.ErrNoTransferFound {
			return nil, err
		}

		if chanst != nil {
			d.DTChan = chanst.ChannelID.String()

			if err := cm.DB.Save(&d).Error; err != nil {
				return nil, err
			}
		}

		return chanst, nil
	default:
		return nil, err
	}
}

var ErrNotOnChainYet = fmt.Errorf("message not found on chain")

func (cm *ContentManager) getDealID(ctx context.Context, pubcid cid.Cid, d *contentDeal) (abi.DealID, error) {
	mlookup, err := cm.Api.StateSearchMsg(ctx, types.EmptyTSK, pubcid, 1000, false)
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

	msg, err := cm.Api.ChainGetMessage(ctx, mlookup.Message)
	if err != nil {
		return 0, err
	}

	var params market.PublishStorageDealsParams
	if err := params.UnmarshalCBOR(bytes.NewReader(msg.Params)); err != nil {
		return 0, err
	}

	dealix := -1
	for i, pd := range params.Deals {
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

func (cm *ContentManager) repairDeal(d *contentDeal) error {
	if d.DealID != 0 {
		log.Infow("miner faulted on deal", "deal", d.DealID, "content", d.Content, "miner", d.Miner)
		maddr, err := d.MinerAddr()
		if err != nil {
			log.Errorf("failed to get miner address from deal (%s): %w", d.Miner, err)
		}

		cm.recordDealFailure(&DealFailureError{
			Miner:   maddr,
			Phase:   "fault",
			Message: "miner faulted on deal",
			Content: d.Content,
		})
	}
	log.Infof("repair deal: ", d.PropCid.CID, d.Miner, d.Content)
	d.Failed = true
	d.FailedAt = time.Now()
	if err := cm.DB.Save(d).Error; err != nil {
		return err
	}

	return nil
}

var priceMax abi.TokenAmount

func init() {
	max, err := types.ParseFIL("0.00000003")
	if err != nil {
		panic(err)
	}
	priceMax = abi.TokenAmount(max)
}

func (cm *ContentManager) priceIsTooHigh(price abi.TokenAmount) bool {
	if types.BigCmp(price, priceMax) > 0 {
		return true
	}

	return false
}

type proposalRecord struct {
	PropCid dbCID
	Data    []byte
}

func (cm *ContentManager) makeDealsForContent(ctx context.Context, content Content, count int, exclude map[address.Address]bool, verified bool) error {
	ctx, span := cm.tracer.Start(ctx, "makeDealsForContent", trace.WithAttributes(
		attribute.Int64("content", int64(content.ID)),
		attribute.Int("count", count),
	))
	defer span.End()

	sealType := abi.RegisteredSealProof_StackedDrg32GiBV1_1 // pull from miner...
	_, size, err := cm.getPieceCommitment(ctx, sealType, content.Cid.CID, cm.Blockstore)
	if err != nil {
		return err
	}

	minerpool, err := cm.pickMiners(ctx, count*2, size.Padded(), exclude)
	if err != nil {
		return err
	}

	var asks []*network.AskResponse
	var ms []address.Address
	var successes int
	for _, m := range minerpool {
		ask, err := cm.FilClient.GetAsk(ctx, m)
		if err != nil {
			var apierr *filclient.ApiError
			if !xerrors.As(err, &apierr) {
				cm.recordDealFailure(&DealFailureError{
					Miner:   m,
					Phase:   "query-ask",
					Message: err.Error(),
					Content: content.ID,
				})
			}
			log.Warnf("failed to get ask for miner %s: %s\n", m, err)
			continue
		}

		if cm.priceIsTooHigh(ask.Ask.Ask.Price) {
			log.Infow("miners price is too high", "miner", m, "price", ask.Ask.Ask.Price)
			cm.recordDealFailure(&DealFailureError{
				Miner:   m,
				Phase:   "miner-search",
				Message: fmt.Sprintf("miners price is too high: %s", types.FIL(ask.Ask.Ask.Price)),
				Content: content.ID,
			})
			continue
		}

		ms = append(ms, m)
		asks = append(asks, ask)
		successes++
		if len(ms) >= count {
			break
		}
	}

	proposals := make([]*network.Proposal, len(ms))
	for i, m := range ms {
		if asks[i] == nil {
			continue
		}

		prop, err := cm.FilClient.MakeDeal(ctx, m, content.Cid.CID, asks[i].Ask.Ask.Price, asks[i].Ask.Ask.MinPieceSize, 1000000, verified)
		if err != nil {
			return xerrors.Errorf("failed to construct a deal proposal: %w", err)
		}

		proposals[i] = prop

		nd, err := cborutil.AsIpld(prop)
		if err != nil {
			return err
		}
		//fmt.Println("proposal cid: ", nd.Cid())

		if err := cm.DB.Create(&proposalRecord{
			PropCid: dbCID{nd.Cid()},
			Data:    nd.RawData(),
		}).Error; err != nil {
			return err
		}
	}

	deals := make([]*contentDeal, len(ms))
	responses := make([]*network.SignedResponse, len(ms))
	for i, p := range proposals {
		if p == nil {
			continue
		}

		dealresp, err := cm.FilClient.SendProposal(ctx, p)
		if err != nil {
			cm.recordDealFailure(&DealFailureError{
				Miner:   ms[i],
				Phase:   "send-proposal",
				Message: err.Error(),
				Content: content.ID,
			})
			continue
		}

		// TODO: verify signature!
		switch dealresp.Response.State {
		case storagemarket.StorageDealError:
			if err := cm.recordDealFailure(&DealFailureError{
				Miner:   ms[i],
				Phase:   "propose",
				Message: dealresp.Response.Message,
				Content: content.ID,
			}); err != nil {
				return err
			}
		case storagemarket.StorageDealProposalRejected:
			if err := cm.recordDealFailure(&DealFailureError{
				Miner:   ms[i],
				Phase:   "propose",
				Message: dealresp.Response.Message,
				Content: content.ID,
			}); err != nil {
				return err
			}
		default:
			if err := cm.recordDealFailure(&DealFailureError{
				Miner:   ms[i],
				Phase:   "propose",
				Message: fmt.Sprintf("unrecognized response state %d: %s", dealresp.Response.State, dealresp.Response.Message),
				Content: content.ID,
			}); err != nil {
				return err
			}
		case storagemarket.StorageDealWaitingForData, storagemarket.StorageDealProposalAccepted:
			responses[i] = dealresp

			cd := &contentDeal{
				Content:  content.ID,
				PropCid:  dbCID{dealresp.Response.Proposal},
				Miner:    ms[i].String(),
				Verified: verified,
			}

			if err := cm.DB.Create(cd).Error; err != nil {
				return xerrors.Errorf("failed to create database entry for deal: %w", err)
			}

			deals[i] = cd
		}
	}

	// Now start up some data transfers!
	// note: its okay if we dont start all the data transfers, we can just do it next time around
	for i, resp := range responses {
		if resp == nil {
			continue
		}

		cd := deals[i]
		if cd == nil {
			log.Warnf("have no contentDeal for response we are about to start transfer on")
			continue
		}

		chanid, err := cm.FilClient.StartDataTransfer(ctx, ms[i], resp.Response.Proposal, content.Cid.CID)
		if err != nil {
			if oerr := cm.recordDealFailure(&DealFailureError{
				Miner:   ms[i],
				Phase:   "start-data-transfer",
				Message: err.Error(),
				Content: content.ID,
			}); oerr != nil {
				return oerr
			}
			continue
		}

		cd.DTChan = chanid.String()

		if err := cm.DB.Save(cd).Error; err != nil {
			return xerrors.Errorf("failed to update deal with channel ID: %w", err)
		}

		log.Infow("Started data transfer", "chanid", chanid)
	}

	return nil
}

func (cm *ContentManager) recordDealFailure(dfe *DealFailureError) error {
	log.Infow("deal failure error", "miner", dfe.Miner, "phase", dfe.Phase, "msg", dfe.Message, "content", dfe.Content)
	return cm.DB.Create(dfe.Record()).Error
}

type DealFailureError struct {
	Miner   address.Address
	Phase   string
	Message string
	Content uint
}

type dfeRecord struct {
	gorm.Model
	Miner   string `json:"miner"`
	Phase   string `json:"phase"`
	Message string `json:"message"`
	Content uint   `json:"content"`
}

func (dfe *DealFailureError) Record() *dfeRecord {
	return &dfeRecord{
		Miner:   dfe.Miner.String(),
		Phase:   dfe.Phase,
		Message: dfe.Message,
		Content: dfe.Content,
	}
}

func (dfe *DealFailureError) Error() string {
	return fmt.Sprintf("deal with miner %s failed in phase %s: %s", dfe.Message, dfe.Phase, dfe.Message)
}

func averageAskPrice(asks []*network.AskResponse) types.FIL {
	total := abi.NewTokenAmount(0)
	for _, a := range asks {
		total = types.BigAdd(total, a.Ask.Ask.Price)
	}

	return types.FIL(big.Div(total, big.NewInt(int64(len(asks)))))
}

type PieceCommRecord struct {
	Data  dbCID `gorm:"unique"`
	Piece dbCID
	Size  abi.UnpaddedPieceSize
}

func (cm *ContentManager) lookupPieceCommRecord(data cid.Cid) (*PieceCommRecord, error) {
	var pcr PieceCommRecord
	err := cm.DB.First(&pcr, "data = ?", data.Bytes()).Error
	if err == nil {
		if !pcr.Piece.CID.Defined() {
			return nil, fmt.Errorf("got an undefined thing back from database")
		}
		return &pcr, nil
	}

	if !xerrors.Is(err, gorm.ErrRecordNotFound) {
		return nil, err
	}

	return nil, nil
}

func (cm *ContentManager) getPieceCommitment(ctx context.Context, rt abi.RegisteredSealProof, data cid.Cid, bs blockstore.Blockstore) (cid.Cid, abi.UnpaddedPieceSize, error) {
	_, span := cm.tracer.Start(ctx, "getPieceComm")
	defer span.End()

	pcr, err := cm.lookupPieceCommRecord(data)
	if err != nil {
		return cid.Undef, 0, err
	}
	if pcr != nil {
		return pcr.Piece.CID, pcr.Size, nil
	}

	pc, size, err := filclient.GeneratePieceCommitment(rt, data, bs)
	if err != nil {
		return cid.Undef, 0, xerrors.Errorf("failed to generate piece commitment: %w", err)
	}

	opcr := PieceCommRecord{
		Data:  dbCID{data},
		Piece: dbCID{pc},
		Size:  size,
	}

	if err := cm.DB.Clauses(clause.OnConflict{DoNothing: true}).Create(&opcr).Error; err != nil {
		return cid.Undef, 0, err
	}

	return pc, size, nil
}

func (cm *ContentManager) RefreshContentForCid(c cid.Cid) (blocks.Block, error) {
	var obj Object
	if err := cm.DB.First(&obj, "cid = ?", c.Bytes()).Error; err != nil {
		return nil, xerrors.Errorf("failed to get object from db: ", err)
	}

	var refs []ObjRef
	if err := cm.DB.Find(&refs, "object = ?", obj.ID).Error; err != nil {
		return nil, err
	}

	var contentToFetch uint
	switch len(refs) {
	case 0:
		return nil, xerrors.Errorf("have no object references for object %d in database")
	case 1:
		// easy case, fetch this thing.
		contentToFetch = refs[0].Content
	default:
		// have more than one reference for the same object. Need to pick one to retrieve

		// if one of the referenced contents has the requested cid as its root, then we should probably fetch that one

		var contents []Content
		if err := cm.DB.Find(&contents, "cid = ?", c.Bytes()).Error; err != nil {
			return nil, err
		}

		if len(contents) == 0 {
			// okay, this isnt anythings root cid. Just pick one I guess?
			contentToFetch = refs[0].Content
		} else {
			// good, this is a root cid, lets fetch that one.
			contentToFetch = contents[0].ID
		}
	}

	ctx := context.TODO()
	if err := cm.retrieveContent(ctx, contentToFetch); err != nil {
		return nil, xerrors.Errorf("failed to retrieve content to serve %d: %w", contentToFetch, err)
	}

	out, err := cm.Blockstore.Get(c)
	if err != nil {
		return nil, xerrors.Errorf("failed to get desired block even after retrieval: %w", err)
	}

	return out, nil
}

func (cm *ContentManager) RefreshContent(ctx context.Context, cont uint) error {
	if err := cm.DB.Model(&Content{}).Where("id = ?", cont).Update("offloaded", false).Error; err != nil {
		return err
	}

	if err := cm.DB.Model(&ObjRef{}).Where("content = ?", cont).Update("offloaded", 0).Error; err != nil {
		return err
	}

	return cm.retrieveContent(ctx, cont)
}

type retrievalProgress struct {
	wait   chan struct{}
	endErr error
}

func (cm *ContentManager) retrieveContent(ctx context.Context, contentToFetch uint) error {
	ctx, span := cm.tracer.Start(ctx, "retrieveContent", trace.WithAttributes(
		attribute.Int("content", int(contentToFetch)),
	))
	defer span.End()

	cm.retrLk.Lock()
	prog, ok := cm.retrievalsInProgress[contentToFetch]
	if !ok {
		prog = &retrievalProgress{
			wait: make(chan struct{}),
		}
		cm.retrievalsInProgress[contentToFetch] = prog
	}
	cm.retrLk.Unlock()

	if ok {
		select {
		case <-prog.wait:
			return prog.endErr
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	defer func() {
		cm.retrLk.Lock()
		delete(cm.retrievalsInProgress, contentToFetch)
		cm.retrLk.Unlock()

		close(prog.wait)
	}()

	if err := cm.runRetrieval(ctx, contentToFetch); err != nil {
		prog.endErr = err
		return err
	}

	return nil
}

func (cm *ContentManager) runRetrieval(ctx context.Context, contentToFetch uint) error {
	ctx, span := cm.tracer.Start(ctx, "runRetrieval")
	defer span.End()

	var content Content
	if err := cm.DB.First(&content, contentToFetch).Error; err != nil {
		return err
	}

	var deals []contentDeal
	if err := cm.DB.Find(&deals, "content = ? and not failed", contentToFetch).Error; err != nil {
		return err
	}

	if len(deals) == 0 {
		return xerrors.Errorf("no active deals for content %d we are trying to retrieve", contentToFetch)
	}

	// TODO: probably need some better way to pick miners to retrieve from...
	perm := rand.Perm(len(deals))
	for _, i := range perm {
		deal := deals[i]

		maddr, err := deal.MinerAddr()
		if err != nil {
			log.Errorf("deal %d had bad miner address: %s", deal.ID, err)
			continue
		}

		log.Infow("attempting retrieval deal", "content", contentToFetch, "miner", maddr)

		ask, err := cm.FilClient.RetrievalQuery(ctx, maddr, content.Cid.CID)
		if err != nil {
			span.RecordError(err)

			log.Errorw("failed to query retrieval", "miner", maddr, "content", content.Cid.CID, "err", err)
			cm.recordRetrievalFailure(&retrievalFailureRecord{
				Miner:   maddr.String(),
				Phase:   "query",
				Message: err.Error(),
			})
			continue
		}
		log.Infow("got retrieval ask", "content", content, "miner", maddr, "ask", ask)

		if err := cm.tryRetrieve(ctx, maddr, content.Cid.CID, ask); err != nil {
			span.RecordError(err)
			log.Errorw("failed to retrieve content", "miner", maddr, "content", content.Cid.CID, "err", err)
			cm.recordRetrievalFailure(&retrievalFailureRecord{
				Miner:   maddr.String(),
				Phase:   "query",
				Message: err.Error(),
			})
			continue
		}

		// success
		return nil
	}

	return fmt.Errorf("failed to retrieve with any miner we have deals with")
}
