package main

import (
	"context"
	"math/rand"
	"time"

	"github.com/application-research/estuary/constants"
	"github.com/application-research/estuary/util"
	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-fil-markets/storagemarket/network"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/big"
	"github.com/filecoin-project/lotus/chain/types"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type storageMiner struct {
	gorm.Model
	Address         util.DbAddr `gorm:"unique"`
	Suspended       bool
	SuspendedReason string
	Name            string
	Version         string
	Location        string
	Owner           uint
}

type minerManager struct {
	lastGoodMiners map[string]bool
}

type minerStorageAsk struct {
	gorm.Model          `json:"-"`
	Miner               string              `gorm:"unique" json:"miner"`
	Price               string              `json:"price"`
	VerifiedPrice       string              `json:"verifiedPrice"`
	PriceBigInt         big.Int             `gorm:"-" json:"-"`
	VerifiedPriceBigInt big.Int             `gorm:"-" json:"-"`
	MinPieceSize        abi.PaddedPieceSize `json:"minPieceSize"`
	MaxPieceSize        abi.PaddedPieceSize `json:"maxPieceSize"`
	MinerVersion        string              `json:"miner_version"`
}

func (msa *minerStorageAsk) GetPrice(verified bool) types.BigInt {
	if verified {
		return msa.VerifiedPriceBigInt
	}
	return msa.PriceBigInt
}

func (cm *ContentManager) pickMinerDist(n int) (int, int) {
	if n < 3 {
		return n, 0
	}

	if n < 7 {
		return 2, n - 2
	}
	return n - (n / 2), n / 2
}

func (cm *ContentManager) pickMiners(ctx context.Context, n int, pieceSize abi.PaddedPieceSize, exclude map[address.Address]bool, filterByPrice bool) ([]miner, error) {
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

	out, err := cm.randomMinerListForDeal(ctx, nrand, pieceSize, exclude, filterByPrice)
	if err != nil {
		return nil, err
	}
	return cm.sortedMinersForDeal(ctx, out, n, pieceSize, exclude, filterByPrice)
}

// TODO - this is currently not used, if we choose to use it,
// add a check to make sure miners selected is still active in db
func (cm *ContentManager) sortedMinersForDeal(ctx context.Context, out []miner, n int, pieceSize abi.PaddedPieceSize, exclude map[address.Address]bool, filterByPrice bool) ([]miner, error) {
	sortedMiners, _, err := cm.sortedMinerList()
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

		proto, err := cm.FilClient.DealProtocolForMiner(ctx, m)
		if err != nil {
			log.Warnf("getting deal protocol for %s failed: %s", m, err)
			continue
		}

		_, ok := cm.EnabledDealProtocolsVersions[proto]
		if !ok {
			continue
		}

		ask, err := cm.getAsk(ctx, m, time.Minute*30)
		if err != nil {
			log.Errorf("getting ask from %s failed: %s", m, err)
			continue
		}

		if filterByPrice {
			price := ask.GetPrice(cm.cfg.Deal.IsVerified)
			if cm.priceIsTooHigh(price) {
				continue
			}
		}

		if cm.sizeIsCloseEnough(pieceSize, ask.MinPieceSize, ask.MaxPieceSize) {
			out = append(out, miner{address: m, dealProtocolVersion: proto, ask: ask})
			exclude[m] = true
		}
	}
	return out, nil
}

func (cm *ContentManager) randomMinerListForDeal(ctx context.Context, n int, pieceSize abi.PaddedPieceSize, exclude map[address.Address]bool, filterByPrice bool) ([]miner, error) {
	var dbminers []storageMiner
	if err := cm.DB.Find(&dbminers, "not suspended").Error; err != nil {
		return nil, err
	}

	out := make([]miner, 0)
	if len(dbminers) == 0 {
		return out, nil
	}

	rand.Shuffle(len(dbminers), func(i, j int) {
		dbminers[i], dbminers[j] = dbminers[j], dbminers[i]
	})

	for _, dbm := range dbminers {
		if len(out) >= n {
			break
		}

		if exclude[dbm.Address.Addr] {
			continue
		}

		proto, err := cm.FilClient.DealProtocolForMiner(ctx, dbm.Address.Addr)
		if err != nil {
			log.Warnf("getting deal protocol for %s failed: %s", dbm.Address.Addr, err)
			continue
		}

		_, ok := cm.EnabledDealProtocolsVersions[proto]
		if !ok {
			continue
		}

		ask, err := cm.getAsk(ctx, dbm.Address.Addr, time.Minute*30)
		if err != nil {
			log.Errorf("getting ask from %s failed: %s", dbm.Address.Addr, err)
			continue
		}

		if filterByPrice {
			price := ask.GetPrice(cm.cfg.Deal.IsVerified)
			if cm.priceIsTooHigh(price) {
				continue
			}
		}

		if cm.sizeIsCloseEnough(pieceSize, ask.MinPieceSize, ask.MaxPieceSize) {
			out = append(out, miner{address: dbm.Address.Addr, dealProtocolVersion: proto, ask: ask})
			exclude[dbm.Address.Addr] = true
		}
	}
	return out, nil
}

func (cm *ContentManager) getAsk(ctx context.Context, m address.Address, maxCacheAge time.Duration) (*minerStorageAsk, error) {
	ctx, span := cm.tracer.Start(ctx, "getAsk", trace.WithAttributes(
		attribute.Stringer("miner", m),
	))
	defer span.End()

	var asks []minerStorageAsk
	if err := cm.DB.Find(&asks, "miner = ?", m.String()).Error; err != nil {
		return nil, err
	}

	minerVersion, err := cm.updateMinerVersion(ctx, m)
	if err != nil {
		log.Warnf("failed to update miner version: %s", err)
	}

	if len(asks) > 0 && time.Since(asks[0].UpdatedAt) < maxCacheAge {
		ask := asks[0]
		priceBigInt, err := types.BigFromString(ask.Price)
		if err != nil {
			return nil, err
		}
		ask.PriceBigInt = priceBigInt

		verifiedPriceBigInt, err := types.BigFromString(ask.VerifiedPrice)
		if err != nil {
			return nil, err
		}
		ask.VerifiedPriceBigInt = verifiedPriceBigInt

		if ask.MinerVersion == "" {
			ask.MinerVersion = minerVersion
		}
		return &ask, nil
	}

	netask, err := cm.FilClient.GetAsk(ctx, m)
	if err != nil {
		span.RecordError(err)
		return nil, err
	}

	nmsa := toDBAsk(netask)
	nmsa.UpdatedAt = time.Now()
	nmsa.MinerVersion = minerVersion

	if err := cm.DB.Clauses(clause.OnConflict{
		Columns: []clause.Column{
			{Name: "miner"},
		},
		DoUpdates: clause.AssignmentColumns([]string{"price", "verified_price", "min_piece_size", "updated_at", "miner_version"}),
	}).Create(nmsa).Error; err != nil {
		span.RecordError(err)
		return nil, err
	}
	return nmsa, nil
}

func (cm *ContentManager) updateMinerVersion(ctx context.Context, m address.Address) (string, error) {
	vers, err := cm.FilClient.GetMinerVersion(ctx, m)
	if err != nil {
		return "", err
	}

	if vers != "" {
		if err := cm.DB.Model(storageMiner{}).Where("address = ?", m.String()).Update("version", vers).Error; err != nil {
			return "", err
		}
	}
	return vers, nil
}

func toDBAsk(netask *network.AskResponse) *minerStorageAsk {
	return &minerStorageAsk{
		Miner:               netask.Ask.Ask.Miner.String(),
		Price:               netask.Ask.Ask.Price.String(),
		VerifiedPrice:       netask.Ask.Ask.VerifiedPrice.String(),
		MinPieceSize:        netask.Ask.Ask.MinPieceSize,
		MaxPieceSize:        netask.Ask.Ask.MaxPieceSize,
		PriceBigInt:         netask.Ask.Ask.Price,
		VerifiedPriceBigInt: netask.Ask.Ask.VerifiedPrice,
	}
}

func minerHasGoodRep(m storageMiner) bool {
	return true
}

func pickMiner(ctx context.Context, pieceSize abi.PaddedPieceSize, exclude map[address.Address]bool, filterByPrice bool) (*miner, error) {
	// while until a miner is found, if no active miner found, return error
	return &miner{}, nil
}

func (mm minerManager) pickNextMiner() *storageMiner {
	return nil
}
