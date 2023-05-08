package util

import (
	"time"

	"gorm.io/gorm"
)

type User struct {
	gorm.Model
	UUID     string `gorm:"unique"`
	Username string `gorm:"unique"`
	Salt     string
	PassHash string
	DID      string

	UserEmail string

	Address   DbAddr
	AuthToken AuthToken `gorm:"-"`
	Perm      int
	Flags     int

	StorageDisabled bool
}

func (u *User) FlagSplitContent() bool {
	return u.Flags&8 != 0
}

type AuthToken struct {
	gorm.Model
	Token      string `gorm:"unique;->"` // read only to prevent storing new tokens but not break existing tokens
	TokenHash  string `gorm:"unique"`
	Label      string
	User       uint
	UploadOnly bool
	Expiry     time.Time
	IsSession  bool
}

type InviteCode struct {
	gorm.Model
	Code      string `gorm:"unique"`
	CreatedBy uint
	ClaimedBy uint
}

// Max Storage per User 1.5 TB
const MaxUserStorageThreshold = int64(1_500_000_000_000)

func IsUserReachedStorageThreshold(userId uint, db *gorm.DB) (bool, error) {
	var sum int64
	err := db.Model(&Content{}).
		Where("user_id = ?", userId).
		Pluck("SUM(size)", &sum).
		Error

	if err != nil {
		return false, err
	}
	return sum >= MaxUserStorageThreshold, nil
}
