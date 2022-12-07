package model

import "gorm.io/gorm"

type ShuttlePreference struct {
	gorm.Model
	StorageMinerID uint
	ShuttleID      uint
	Priority       uint
}
