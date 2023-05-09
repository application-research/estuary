package util

import (
	"gorm.io/gorm"
)

type UsersStorageCapacity struct {
	gorm.Model

	UserId    uint
	Size      int64
	SoftLimit int64 `gorm:"default:1319413953331"` // Hardlimit*.8
	HardLimit int64 `gorm:"default:1649267441664"` // 1.5TB
}

func (usc *UsersStorageCapacity) GetUserStorageCapacity(user *User, db *gorm.DB) error {
	if err := db.First(&usc, "user_id = ?", user.ID).Error; err != nil {
		usc.Size = 0
		usc.UserId = user.ID
		db.Create(&usc)
	}
	// check if cache is outdated and resync?
	return nil
}

func (usc *UsersStorageCapacity) IncreaseAndValidateThreshold(add int64) bool {
	usc.Size += add
	return usc.Size <= usc.HardLimit
}
