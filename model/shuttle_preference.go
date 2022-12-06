package model

import "gorm.io/gorm"

type SP struct {
	gorm.Model
	SPID                  string
	Priority              int
	ShuttleSPPreferenceID uint
}

type ShuttleSPPreference struct {
	gorm.Model
	StorageProviders []SP
	ShuttleID        string
}
