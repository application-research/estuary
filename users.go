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

	Perm int
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
