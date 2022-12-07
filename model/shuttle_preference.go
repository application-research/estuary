package model

import "gorm.io/gorm"

type ShuttlePreference struct {
	gorm.Model
	StorageMinerID uint `gorm:"index:idx_pref"`
	ShuttleID      uint
	Priority       uint `gorm:"index:idx_pref"`
}
