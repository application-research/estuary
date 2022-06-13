package util

import (
	"github.com/application-research/estuary/node/modules/peering"
	"github.com/libp2p/go-libp2p-core/peer"
)

type PeeringPeerAddMessage struct {
	Message  string                `json: Message`
	PeersAdd []peering.PeeringPeer `json: Peers`
}

type PeeringPeerRemoveMessage struct {
	Message     string    `json: Message`
	PeersRemove []peer.ID `json: Peers`
}
