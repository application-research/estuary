package autoretrieve

import (
	"time"

	"gorm.io/gorm"
)

type Autoretrieve struct {
	gorm.Model

	Handle         string `gorm:"unique"`
	Token          string `gorm:"unique"`
	LastConnection time.Time
	PeerID         string `gorm:"unique"`
	Addresses      string
}
