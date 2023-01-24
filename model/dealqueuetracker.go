package model

import (
	"gorm.io/gorm"
)

type DealQueueTracker struct {
	gorm.Model
	LastContID uint `gorm:"index;not null" json:"-"`
}
