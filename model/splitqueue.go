package model

import (
	"time"

	"gorm.io/gorm"
)

type SplitQueue struct {
	gorm.Model
	ID            uint64    `gorm:"primarykey" json:"-"`
	UserID        uint64    `gorm:"index:splits_user_id_cont_id;not null;index" json:"-"`
	ContID        uint64    `gorm:"index:splits_user_id_cont_id;not null;index;unique" json:"-"`
	Failing       bool      `gorm:"not null" json:"-"`
	Attempted     uint      `gorm:"index:attempted_next_attempt_at;index;not null" json:"-"`
	NextAttemptAt time.Time `gorm:"index:attempted_next_attempt_at;index;not null" json:"-"`
}
