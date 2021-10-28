package main

import (
	"time"

	"github.com/application-research/estuary/util"
	"gorm.io/gorm"
)

type User struct {
	gorm.Model
	UUID     string `gorm:"unique"`
	Username string `gorm:"unique"`
	PassHash string
	DID      string

	UserEmail string

	Address   util.DbAddr
	authToken AuthToken
	Perm      int
	Flags     int

	StorageDisabled bool
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
