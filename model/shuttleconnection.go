package model

import (
	"time"

	"github.com/application-research/estuary/util"
	"gorm.io/gorm"
)

type ShuttleConnection struct {
	gorm.Model
	UpdatedAt             time.Time
	Handle                string `gorm:"unique;index"`
	Hostname              string
	AddrInfo              util.DbAddrInfo
	Address               util.DbAddr
	Private               bool
	ContentAddingDisabled bool
	SpaceLow              bool
	BlockstoreSize        uint64
	BlockstoreFree        uint64
	PinCount              int64
	PinQueueLength        int64
	QueueEngEnabled       bool
}
