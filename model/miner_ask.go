package model

import (
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/big"
	"github.com/filecoin-project/lotus/chain/types"
	"gorm.io/gorm"
)

var priceMax abi.TokenAmount

func init() {
	max, err := types.ParseFIL("0.00000003")
	if err != nil {
		panic(err)
	}
	priceMax = abi.TokenAmount(max)
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
