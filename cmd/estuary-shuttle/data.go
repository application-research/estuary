package main

import (
	"time"

	"github.com/application-research/estuary/util"
	"gorm.io/gorm"
)

type Pin struct {
	ID        uint64    `gorm:"primarykey" json:"id"`
	CreatedAt time.Time `json:"-"`
	UpdatedAt time.Time `json:"-"`

	Content uint64 `gorm:"index"`

	Cid util.DbCID `json:"cid"`
	//Name        string     `json:"name"`
	UserID uint `json:"userId" gorm:"index"`
	//Description string     `json:"description"`
	Size   int64 `json:"size"`
	Active bool  `json:"active"`
	//Offloaded   bool       `json:"offloaded"`
	//Replication int `json:"replication"`

	AggregatedIn uint `json:"aggregatedIn"`
	Aggregate    bool `json:"aggregate"`

	Pinning bool   `json:"pinning"`
	PinMeta string `json:"pinMeta"`
	Failed  bool   `json:"failed"`

	DagSplit  bool   `json:"dagSplit"`
	SplitFrom uint64 `json:"splitFrom"`
}

type Object struct {
	ID   uint64     `gorm:"primarykey"`
	Cid  util.DbCID `gorm:"index"`
	Size uint64
	//Reads      int
	LastAccess time.Time
}

type ObjRef struct {
	ID     uint64 `gorm:"primarykey"`
	Pin    uint64 `gorm:"index"`
	Object uint64 `gorm:"index"`
	//Offloaded bool
}

func setupDatabase(dbval string) (*gorm.DB, error) {
	db, err := util.SetupDatabase(dbval)
	if err != nil {
		return nil, err
	}
	if err := migrateSchemas(db); err != nil {
		return nil, err
	}

	return db, nil
}

func migrateSchemas(db *gorm.DB) error {
	if err := db.AutoMigrate(
		&Pin{},
		&Object{},
		&ObjRef{}); err != nil {
		return err
	}
	return nil
}
