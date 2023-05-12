package model

import (
	"time"

	"gorm.io/gorm"
)

type Shuttle struct {
	gorm.Model
	Handle            string `gorm:"unique"`
	Token             string
	Host              string
	PeerID            string
	LastConnection    time.Time
	Private           bool
	Open              bool
	Priority          int
	ShuttlePreference []ShuttlePreference
}
