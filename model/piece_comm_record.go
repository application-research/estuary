package model

import (
	"github.com/application-research/estuary/util"
	"github.com/filecoin-project/go-state-types/abi"
)

type PieceCommRecord struct {
	Data    util.DbCID `gorm:"unique"`
	Piece   util.DbCID
	CarSize uint64
	Size    abi.UnpaddedPieceSize
}
