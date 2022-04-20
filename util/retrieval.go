package util

import (
	"gorm.io/gorm"
)

type RetrievalFailureRecord struct {
	gorm.Model
	Miner   string `json:"miner"`
	Phase   string `json:"phase"`
	Message string `json:"message"`
	Content uint   `json:"content"`
	Cid     DbCID  `json:"cid"`
}

type RetrievalProgress struct {
	Wait   chan struct{}
	EndErr error
}
