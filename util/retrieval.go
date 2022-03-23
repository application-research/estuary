package util

import (
	"time"

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

type HeartbeatAutoretrieveResponse struct {
	Handle         string    `json:"handle"`
	LastConnection time.Time `json:"lastConnection"`
}

type AutoretrieveListResponse struct {
	Handle string `json:"handle"`
	Token  string `json:"token"`
	// Online         bool            `json:"online"`
	LastConnection time.Time `json:"lastConnection"`
	// AddrInfo       *peer.AddrInfo  `json:"addrInfo"`
	// Address        address.Address `json:"address"`
	// Hostname       string          `json:"hostname"`

}

type InitAutoretrieveResponse struct {
	Handle         string    `json:"handle"`
	Token          string    `json:"token"`
	LastConnection time.Time `json:"lastConnection"`
}
