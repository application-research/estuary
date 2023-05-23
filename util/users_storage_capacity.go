package util

import (
	"gorm.io/gorm"
	"time"
)

type UsersStorageCapacity struct {
	gorm.Model

	UserId     uint      `json:"user_id"`
	Size       int64     `json:"size" gorm:"default:0"`
	SoftLimit  int64     `json:"soft_limit_bytes" gorm:"default:1319413953331"` // Hardlimit*.8
	HardLimit  int64     `json:"hard_limit_bytes" gorm:"default:1649267441664"` // 1.5TB
	LastSyncAt time.Time `json:"last_sync_at"`
}

type Utilization struct {
	TotalSize int64
}

// CutOverUtilizationDate All content uploaded pass this date will count toward your user storage capacity
const CutOverUtilizationDate = "2023-05-26 00:00:00"
const SyncRefreshInHours = 24

func (usc *UsersStorageCapacity) ValidateThreshold() bool {
	return usc.Size <= usc.HardLimit
}

func (usc *UsersStorageCapacity) IsSyncNeeded() bool {
	now := time.Now().UTC()
	duration := now.Sub(usc.LastSyncAt.UTC())
	refresh := SyncRefreshInHours * time.Hour
	return duration >= refresh
}
