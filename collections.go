package main

import "time"

type Collection struct {
	ID        uint      `gorm:"primarykey" json:"-"`
	CreatedAt time.Time `json:"createdAt"`

	UUID string `gorm:"index" json:"uuid"`

	Name        string `json:"name"`
	Description string `json:"description"`
	UserID      uint   `json:"userId"`
	CID         string `json:"cid"`
}

type CollectionRef struct {
	ID         uint `gorm:"primaryKey"`
	CreatedAt  time.Time
	Collection uint    `gorm:"index:,option:CONCURRENTLY; not null"`
	Content    uint    `gorm:"index:,option:CONCURRENTLY;not null"`
	Path       *string `gorm:"not null"`
}
