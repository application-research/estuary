package model

import (
	"gorm.io/gorm"
)

type StagingZoneTracker struct {
	gorm.Model
	LastContID uint `gorm:"index;not null" json:"-"`
}
