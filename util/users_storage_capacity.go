package util

import (
	"gorm.io/gorm"
)

type UsersStorageCapacity struct {
	gorm.Model

	UserId    uint
	Size      int64
	SoftLimit int64
	HardLimit int64
}

func (usc *UsersStorageCapacity) GetUserStorageCapacity(user *User, db *gorm.DB) error {
	if err := db.Find(&usc, "user_id = ?", user.ID).Error; err != nil {
		usc.Size = 0
		usc.UserId = user.ID
		db.Save(&usc)
	}
	return nil
}

func (usc *UsersStorageCapacity) Save(db *gorm.DB) {
	db.Save(&usc)
}

func (usc *UsersStorageCapacity) IncreaseAndValidateThreshold(add int64) bool {
	usc.Size += add
	return usc.Size <= usc.HardLimit
}
