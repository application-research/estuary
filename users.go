package main

import (
	"time"

	"gorm.io/gorm"
)

type User struct {
	gorm.Model
	UUID     string `gorm:"unique"`
	Username string `gorm:"unique"`
	PassHash string

	UserEmail string

	Perm  int
	Flags int
}

func (u *User) BucketingEnabled() bool {
	return u.Flags&1 != 0
}

func (u *User) IpfsAddEnabled() bool {
	return u.Flags&2 != 0
}

type AuthToken struct {
	gorm.Model
	Token  string `gorm:"unique"`
	User   uint
	Expiry time.Time
}

type InviteCode struct {
	gorm.Model
	Code      string `gorm:"unique"`
	CreatedBy uint
	ClaimedBy uint
}
