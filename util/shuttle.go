package util

import (
	"time"

	"github.com/application-research/filclient"
	"github.com/filecoin-project/go-address"
	"github.com/libp2p/go-libp2p/core/peer"
)

type InitShuttleResponse struct {
	Handle string `json:"handle"`
	Token  string `json:"token"`
}

type ShuttleStorageStats struct {
	BlockstoreSize uint64 `json:"blockstoreSize"`
	BlockstoreFree uint64 `json:"blockstoreFree"`
	PinCount       int64  `json:"pinCount"`
	PinQueueLength int64  `json:"pinQueueLength"`
}

type ShuttleListResponse struct {
	Handle         string          `json:"handle"`
	Token          string          `json:"token"`
	Online         bool            `json:"online"`
	LastConnection time.Time       `json:"lastConnection"`
	AddrInfo       *peer.AddrInfo  `json:"addrInfo"`
	Address        address.Address `json:"address"`
	Hostname       string          `json:"hostname"`

	StorageStats *ShuttleStorageStats `json:"storageStats"`
}

type ShuttleCreateContentBody struct {
	ContentCreateBody
	Buckets      []string `json:"buckets"`
	DagSplitRoot uint     `json:"dagSplitRoot"`
	User         uint     `json:"user"`
}

type ChanTrack struct {
	Dbid uint
	Last *filclient.ChannelState
}
