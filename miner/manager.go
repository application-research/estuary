package miner

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/application-research/estuary/config"
	"github.com/application-research/estuary/constants"
	"github.com/application-research/estuary/model"
	"github.com/application-research/estuary/util"
	"github.com/application-research/filclient"
	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/lotus/api"
	"github.com/filecoin-project/lotus/chain/types"
	"github.com/labstack/gommon/log"
	"github.com/libp2p/go-libp2p/core/protocol"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"gorm.io/gorm"
)

type IMinerManager interface {
	EstimatePrice(ctx context.Context, repl int, pieceSize abi.PaddedPieceSize, duration abi.ChainEpoch, verified bool) (*estimateResponse, error)
	PickMiners(ctx context.Context, n int, pieceSize abi.PaddedPieceSize, exclude map[address.Address]bool, filterByPrice bool) ([]miner, error)
	GetDealProtocolForMiner(ctx context.Context, miner address.Address) (protocol.ID, error)
	ComputeSortedMinerList() ([]*minerDealStats, error)
	SortedMinerList() ([]address.Address, []*minerDealStats, error)
	GetAsk(ctx context.Context, m address.Address, maxCacheAge time.Duration) (*model.MinerStorageAsk, error)
	SetMinerInfo(m address.Address, params MinerSetInfoParams, u *util.User) error
	GetMsgForMinerClaim(miner address.Address, uid uint) []byte
	ClaimMiner(ctx context.Context, params ClaimMinerBody, u *util.User) error
	SuspendMiner(m address.Address, params SuspendMinerBody, u *util.User) error
	UnSuspendMiner(m address.Address, u *util.User) error
}

type MinerManager struct {
	// Some fields for miner reputation management
	minerLk      sync.Mutex
	sortedMiners []address.Address
	rawData      []*minerDealStats
	lastComputed time.Time
	db           *gorm.DB
	filClient    *filclient.FilClient
	cfg          *config.Estuary
	tracer       trace.Tracer
	api          api.Gateway
}

func NewMinerManager(db *gorm.DB, fc *filclient.FilClient, cfg *config.Estuary, api api.Gateway) IMinerManager {
	return &MinerManager{
		db:        db,
		filClient: fc,
		cfg:       cfg,
		tracer:    otel.Tracer("miner_manager"),
		api:       api,
	}
}

type estimateResponse struct {
	Total *abi.TokenAmount
	Asks  []*model.MinerStorageAsk
}

func (mm *MinerManager) EstimatePrice(ctx context.Context, repl int, pieceSize abi.PaddedPieceSize, duration abi.ChainEpoch, verified bool) (*estimateResponse, error) {
	ctx, span := mm.tracer.Start(ctx, "estimatePrice", trace.WithAttributes(
		attribute.Int("replication", repl),
	))
	defer span.End()

	miners, err := mm.PickMiners(ctx, repl, pieceSize, nil, false)
	if err != nil {
		return nil, err
	}

	if len(miners) == 0 {
		return nil, fmt.Errorf("failed to find any miners for estimating deal price")
	}

	asks := make([]*model.MinerStorageAsk, 0)
	total := abi.NewTokenAmount(0)
	for _, m := range miners {
		dealSize := pieceSize
		if dealSize < m.Ask.MinPieceSize {
			dealSize = m.Ask.MinPieceSize
		}

		price := m.Ask.GetPrice(verified)
		cost, err := filclient.ComputePrice(price, dealSize, duration)
		if err != nil {
			return nil, err
		}

		asks = append(asks, m.Ask)
		total = types.BigAdd(total, *cost)
	}

	return &estimateResponse{
		Total: &total,
		Asks:  asks,
	}, nil
}

func pickMinerDist(n int) (int, int) {
	if n < 3 {
		return n, 0
	}

	if n < 7 {
		return 2, n - 2
	}
	return n - (n / 2), n / 2
}

func (mm *MinerManager) PickMiners(ctx context.Context, n int, pieceSize abi.PaddedPieceSize, exclude map[address.Address]bool, filterByPrice bool) ([]miner, error) {
	ctx, span := mm.tracer.Start(ctx, "pickMiners", trace.WithAttributes(
		attribute.Int("count", n),
	))
	defer span.End()
	if exclude == nil {
		exclude = make(map[address.Address]bool)
	}

	// some portion of the miners will be 'first N of our best miners' and the rest will be randomly chosen from our list
	// over time, our miner list will be all fairly high quality so this should just serve to shake things up a bit and
	// give miners more of a chance to prove themselves
	_, nrand := pickMinerDist(n)

	out, err := mm.randomMinerListForDeal(ctx, nrand, pieceSize, exclude, filterByPrice)
	if err != nil {
		return nil, err
	}
	return mm.sortedMinersForDeal(ctx, out, n, pieceSize, exclude, filterByPrice)
}

// add a check to make sure miners selected is still active in db
func (mm *MinerManager) sortedMinersForDeal(ctx context.Context, out []miner, n int, pieceSize abi.PaddedPieceSize, exclude map[address.Address]bool, filterByPrice bool) ([]miner, error) {
	sortedMiners, _, err := mm.SortedMinerList()
	if err != nil {
		return nil, err
	}

	if len(sortedMiners) == 0 {
		return out, nil
	}

	if len(sortedMiners) > constants.TopMinerSel {
		sortedMiners = sortedMiners[:constants.TopMinerSel]
	}

	rand.Shuffle(len(sortedMiners), func(i, j int) {
		sortedMiners[i], sortedMiners[j] = sortedMiners[j], sortedMiners[i]
	})

	for _, m := range sortedMiners {
		if len(out) >= n {
			break
		}

		if exclude[m] {
			continue
		}

		proto, err := mm.GetDealProtocolForMiner(ctx, m)
		if err != nil {
			log.Warnf("getting deal protocol for %s failed: %s", m, err)
			continue
		}

		ask, err := mm.GetAsk(ctx, m, time.Minute*30)
		if err != nil {
			log.Warnf("getting ask from %s failed: %s", m, err)
			continue
		}

		if filterByPrice && ask.PriceIsTooHigh(mm.cfg) {
			continue
		}

		if ask.SizeIsCloseEnough(pieceSize) {
			out = append(out, miner{Address: m, DealProtocolVersion: proto, Ask: ask})
			exclude[m] = true
		}
	}
	return out, nil
}
