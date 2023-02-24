package model

import (
	"time"

	"github.com/application-research/estuary/util"
)

type RetrievalSuccessRecord struct {
	ID           uint       `gorm:"primarykey" json:"-"`
	CreatedAt    time.Time  `json:"createdAt"`
	Cid          util.DbCID `json:"cid"`
	Miner        string     `json:"miner"`
	Peer         string     `json:"peer"`
	Size         uint64     `json:"size"`
	DurationMs   int64      `json:"durationMs"`
	AverageSpeed uint64     `json:"averageSpeed"`
	TotalPayment string     `json:"totalPayment"`
	NumPayments  int        `json:"numPayments"`
	AskPrice     string     `json:"askPrice"`
}
