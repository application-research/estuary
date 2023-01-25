package model

import (
	"time"

	"gorm.io/gorm"
)

type SplitQueue struct {
	gorm.Model
	ID            uint64    `gorm:"primarykey" json:"-"`
	UserID        uint64    `gorm:"index:user_id_cont_id;index;not null" json:"-"`
	ContID        uint64    `gorm:"index:user_id_cont_id;unique;not null;index" json:"-"`
	Failing       bool      `gorm:"not null" json:"-"`
	Attempted     uint      `gorm:"index:attempted_next_attempt_at;index;not null" json:"-"`
	NextAttemptAt time.Time `gorm:"index:attempted_next_attempt_at;index;not null" json:"-"`
}
