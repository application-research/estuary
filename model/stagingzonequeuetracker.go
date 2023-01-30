package model

import (
	"gorm.io/gorm"
)

type StagingZoneTracker struct {
	gorm.Model
	LastContID   uint64 `gorm:"index;not null" json:"-"`
	StopAt       uint64 `gorm:"" json:"-"`
	BackfillDone bool
}
