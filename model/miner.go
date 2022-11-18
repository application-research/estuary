package model

import (
	"github.com/application-research/estuary/util"
	"gorm.io/gorm"
)

type StorageMiner struct {
	gorm.Model
	Address         util.DbAddr `gorm:"unique"`
	Suspended       bool
	SuspendedReason string
	Name            string
	Version         string
	Location        string
	Owner           uint
}
