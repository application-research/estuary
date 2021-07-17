package main

import "gorm.io/gorm"

type Dealer struct {
	gorm.Model

	Handle string `gorm:"unique"`
	Token  string

	Host   string
	PeerID string
}
