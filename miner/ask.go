package miner

import (
	"context"
	"time"

	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-fil-markets/storagemarket/network"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/big"
	"github.com/filecoin-project/lotus/chain/types"
	"github.com/labstack/gommon/log"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

var priceMax abi.TokenAmount

func init() {
	max, err := types.ParseFIL("0.00000003")
	if err != nil {
		panic(err)
	}
	priceMax = abi.TokenAmount(max)
}

type MinerStorageAsk struct {
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

func (msa *MinerStorageAsk) PriceIsTooHigh(isVerifiedDeal bool) bool {
	price := msa.GetPrice(isVerifiedDeal)
	if isVerifiedDeal {
		return types.BigCmp(price, abi.NewTokenAmount(0)) > 0
	}
	return types.BigCmp(price, priceMax) > 0
}

func (msa *MinerStorageAsk) GetPrice(isVerifiedDeal bool) types.BigInt {
	if isVerifiedDeal {
		return msa.VerifiedPriceBigInt
	}
	return msa.PriceBigInt
}

func (msa *MinerStorageAsk) SizeIsCloseEnough(pieceSize abi.PaddedPieceSize) bool {
	if pieceSize > msa.MinPieceSize && pieceSize < msa.MaxPieceSize {
		return true
	}
	return false
}

func (mgr *MinerManager) GetAsk(ctx context.Context, m address.Address, maxCacheAge time.Duration) (*MinerStorageAsk, error) {
	ctx, span := mgr.tracer.Start(ctx, "getAsk", trace.WithAttributes(
		attribute.Stringer("miner", m),
	))
	defer span.End()

	var asks []MinerStorageAsk
	if err := mgr.DB.Find(&asks, "miner = ?", m.String()).Error; err != nil {
		return nil, err
	}

	minerVersion, err := mgr.updateMinerVersion(ctx, m)
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

	netask, err := mgr.FilClient.GetAsk(ctx, m)
	if err != nil {
		span.RecordError(err)
		return nil, err
	}

	nmsa := ToDBAsk(netask)
	nmsa.UpdatedAt = time.Now()
	nmsa.MinerVersion = minerVersion

	if err := mgr.DB.Clauses(clause.OnConflict{
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

func ToDBAsk(netask *network.AskResponse) *MinerStorageAsk {
	return &MinerStorageAsk{
		Miner:               netask.Ask.Ask.Miner.String(),
		Price:               netask.Ask.Ask.Price.String(),
		VerifiedPrice:       netask.Ask.Ask.VerifiedPrice.String(),
		MinPieceSize:        netask.Ask.Ask.MinPieceSize,
		MaxPieceSize:        netask.Ask.Ask.MaxPieceSize,
		PriceBigInt:         netask.Ask.Ask.Price,
		VerifiedPriceBigInt: netask.Ask.Ask.VerifiedPrice,
	}
}
