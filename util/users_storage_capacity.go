package util

import (
	"gorm.io/gorm"
	"time"
)

type UsersStorageCapacity struct {
	gorm.Model

	UserId     uint      `json:"user_id"`
	Size       int64     `json:"size" gorm:"default:0"`
	SoftLimit  int64     `json:"soft_limit" gorm:"default:1319413953331"` // Hardlimit*.8
	HardLimit  int64     `json:"hard_limit" gorm:"default:1649267441664"` // 1.5TB
	LastSyncAt time.Time `json:"last_sync_at"`
}

type Utilization struct {
	TotalSize int64
}

// CutOverUtilizationDate All content uploaded pass this date will count toward your user storage capacity
const CutOverUtilizationDate = "2023-05-12 00:00:00"

func (usc *UsersStorageCapacity) GetUserStorageCapacity(user *User, db *gorm.DB) error {
	if err := db.First(&usc, "user_id = ?", user.ID).Error; err != nil {
		usc.UserId = user.ID
		db.Create(&usc)
	}
	if isSyncNeeded(usc.LastSyncAt) {
		var usage Utilization
		if err := db.Raw(`SELECT (SELECT SUM(size) FROM contents where user_id = ? AND created_at >= ? AND NOT aggregate AND active AND deleted_at IS NULL) as total_size`, user.ID, CutOverUtilizationDate).
			Scan(&usage).Error; err != nil {
			return err
		}
		usc.Size = usage.TotalSize
		usc.LastSyncAt = time.Now()
		db.Save(&usc)
	}
	return nil
}

func (usc *UsersStorageCapacity) IncreaseAndValidateThreshold(add int64) bool {
	usc.Size += add
	return usc.Size <= usc.HardLimit
}

const SyncRefreshInHours = 24

func isSyncNeeded(t time.Time) bool {
	now := time.Now().UTC()
	duration := now.Sub(t.UTC())
	refresh := SyncRefreshInHours * time.Hour
	return duration >= refresh
}
