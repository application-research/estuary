package util

import (
	"time"

	"gorm.io/gorm"
)

type User struct {
	gorm.Model
	UUID     string `gorm:"unique"`
	Username string `gorm:"unique"`
	PassHash string
	DID      string

	UserEmail string

	Address   DbAddr
	AuthToken AuthToken
	Perm      int
	Flags     int

	StorageDisabled bool
}

func (u *User) FlagSplitContent() bool {
	return u.Flags&8 != 0
}

type AuthToken struct {
	gorm.Model
	Token      string `gorm:"unique"`
	User       uint
	UploadOnly bool
	Expiry     time.Time
}

type InviteCode struct {
	gorm.Model
	Code      string `gorm:"unique"`
	CreatedBy uint
	ClaimedBy uint
}
