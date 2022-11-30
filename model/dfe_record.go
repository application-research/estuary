package model

import (
	"github.com/libp2p/go-libp2p/core/protocol"
	"gorm.io/gorm"
)

type DfeRecord struct {
	gorm.Model
	Miner               string      `json:"miner"`
	DealUUID            string      `json:"deal_uuid"`
	Phase               string      `json:"phase"`
	Message             string      `json:"message"`
	Content             uint        `json:"content" gorm:"index"`
	MinerVersion        string      `json:"minerVersion"`
	UserID              uint        `json:"user_id" gorm:"index"`
	DealProtocolVersion protocol.ID `json:"deal_protocol_version"`
}
