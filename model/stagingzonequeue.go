package model

import (
	"time"

	"gorm.io/gorm"
)

type StagingZoneQueue struct {
	gorm.Model
	ID            uint64    `gorm:"primarykey" json:"-"`
	UserID        uint64    `gorm:"index:cont_id_user_id;index;not null" json:"-"`
	ContID        uint64    `gorm:"index:cont_id_user_id;unique;index;not null" json:"-"`
	Failing       bool      `gorm:"not null" json:"-"`
	NextAttemptAt time.Time `gorm:"index;not null" json:"-"`
	IsBackFilled  bool      `gorm:"not null" json:"-"`
}
