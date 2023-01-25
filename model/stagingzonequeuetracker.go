package model

import (
	"gorm.io/gorm"
)

type StagingZoneQueueTracker struct {
	gorm.Model
	LastContID uint64 `gorm:"unique;not null" json:"-"`
	StopAt     uint64 `gorm:"not null" json:"-"`
}
