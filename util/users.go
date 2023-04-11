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
	AuthAddress     string
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
}

type InviteCode struct {
	gorm.Model
	Code      string `gorm:"unique"`
	CreatedBy uint
	ClaimedBy uint
}
