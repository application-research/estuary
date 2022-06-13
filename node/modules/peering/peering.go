package peering

import (
	"github.com/ipfs/go-ipfs/peering"
	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/peer"
)

// A struct that is used to store the information of a peer.
type PeeringPeer struct {
	ID        string   `json:"ID"`
	Addrs     []string `json:"Addrs"`
	Connected bool     `json:"Connected,omitempty"`
}

// A wrapper for the `PeeringService` struct.
type EstuaryPeeringService struct {
	*peering.PeeringService
}

//	NewEstuaryPeeringService Construct a new Estuary Peering Service
func NewEstuaryPeeringService(host host.Host) *EstuaryPeeringService {
	return &EstuaryPeeringService{peering.NewPeeringService(host)}
}

// Start this function starts the EstuaryPeeringService
func (ps *EstuaryPeeringService) Start() error {
	return ps.PeeringService.Start()
}

// Stop this function stop the EstuaryPeeringService
func (ps *EstuaryPeeringService) Stop() error {
	return ps.PeeringService.Stop()
}

// ListPeers this function lists all peers on the current EstuaryPeeringService
func (ps *EstuaryPeeringService) ListPeers() []peer.AddrInfo {
	return ps.PeeringService.ListPeers()
}

// AddPeer this function adds a peer on the current EstuaryPeeringService
func (ps *EstuaryPeeringService) AddPeer(info peer.AddrInfo) {
	ps.PeeringService.AddPeer(info)
}

// RemovePeer this function removes a peer on the current EstuaryPeeringService
func (ps *EstuaryPeeringService) RemovePeer(peerId peer.ID) {
	ps.PeeringService.RemovePeer(peerId)
}
