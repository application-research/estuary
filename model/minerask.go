package model

import (
	"github.com/application-research/estuary/config"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/big"
	"github.com/filecoin-project/lotus/chain/types"
	"gorm.io/gorm"
)

func (msa *MinerStorageAsk) PriceIsTooHigh(cfg *config.Estuary) bool {
	price := msa.GetPrice(cfg.Deal.IsVerified)
	if cfg.Deal.IsVerified {
		return types.BigCmp(price, cfg.Deal.MaxVerifiedPrice) > 0
	}
	return types.BigCmp(price, cfg.Deal.MaxPrice) > 0
}

func (msa *MinerStorageAsk) GetPrice(isVerifiedDeal bool) types.BigInt {
	if isVerifiedDeal {
		return msa.VerifiedPriceBigInt
	}
	return msa.PriceBigInt
}

func (msa *MinerStorageAsk) SizeIsCloseEnough(pieceSize abi.PaddedPieceSize) bool {
	if pieceSize >= msa.MinPieceSize && pieceSize <= msa.MaxPieceSize {
		return true
	}
	return false
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
