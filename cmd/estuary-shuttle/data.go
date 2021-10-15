package main

import (
	"time"

	"github.com/application-research/estuary/util"
	"gorm.io/gorm"
)

type Pin struct {
	ID        uint      `gorm:"primarykey" json:"id"`
	CreatedAt time.Time `json:"-"`
	UpdatedAt time.Time `json:"-"`

	Content uint `gorm:"index"`

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

	DagSplit bool `json:"dagSplit"`
}

type Object struct {
	ID   uint       `gorm:"primarykey"`
	Cid  util.DbCID `gorm:"index"`
	Size int
	//Reads      int
	LastAccess time.Time
}

type ObjRef struct {
	ID     uint `gorm:"primarykey"`
	Pin    uint `gorm:"index"`
	Object uint `gorm:"index"`
	//Offloaded bool
}

func setupDatabase(dbval string) (*gorm.DB, error) {
	db, err := util.SetupDatabase(dbval)
	if err != nil {
		return nil, err
	}

	db.AutoMigrate(&Pin{})
	db.AutoMigrate(&Object{})
	db.AutoMigrate(&ObjRef{})

	return db, nil
}
