package model

import (
	"gorm.io/gorm"
)

type SplitQueueTracker struct {
	gorm.Model
	LastContID uint `gorm:"index;not null" json:"-"`
}
