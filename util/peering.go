package util

import (
	"github.com/application-research/estuary/node/modules/peering"
	"github.com/libp2p/go-libp2p-core/peer"
)

type PeeringPeerAddMessage struct {
	Message  string                `json:"message"`
	PeersAdd []peering.PeeringPeer `json:"peers"`
}

type PeeringPeerRemoveMessage struct {
	Message     string    `json:"message"`
	PeersRemove []peer.ID `json:"peers"`
}

//	generic response models
type GenericResponse struct {
	Message string `json:"message"`
}
