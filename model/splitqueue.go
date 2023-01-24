package model

import (
	"time"

	"gorm.io/gorm"
)

type SplitQueue struct {
	gorm.Model
	ID uint64 `gorm:"index:id_size_status;index:id_status" json:"id"`

	UserID  uint   `gorm:"index:user_size_status;index;not null" json:"user"`
	ContID  uint64 `gorm:"index;not null" json:"contentID"`
	Enabled bool

	Done          bool
	Failing       bool
	Attempted     uint
	NextAttemptAt time.Time
}
