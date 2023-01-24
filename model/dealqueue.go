package model

import (
	"time"

	"github.com/application-research/estuary/util"
	"gorm.io/gorm"
)

type DealQueue struct {
	gorm.Model
	ID uint64 `gorm:"index:id_size_status;index:id_status" json:"id"`

	CreatedAt time.Time `gorm:"index;not null" json:"createdAt"`

	UserID  uint   `gorm:"index:user_size_status;index;not null" json:"user"`
	ContID  uint64 `gorm:"index;not null" json:"contentID"`
	ContCID util.DbCID

	CommpDone          bool
	CommpFailing       bool
	CommpAttempted     uint
	CommpNextAttemptAt time.Time

	Refreshed              bool
	RefreshedNextAttemptAt time.Time

	Dealed                  bool
	DealsChecked            bool
	DealsCount              uint
	DealsExpectedCount      uint
	DealsCheckNextAttemptAt time.Time
}
