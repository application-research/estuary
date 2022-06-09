package peering

import (
	"github.com/ipfs/go-ipfs/peering"
	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/peer"
)

type PeeringPeer struct {
	ID        string   `json:"ID"`
	Addrs     []string `json:"Addrs"`
	Connected bool     `json:"Connected,omitempty"`
}

type EstuaryPeeringService struct {
	*peering.PeeringService
}

type EstuaryPeeringNotifee peering.PeeringService

func NewEstuaryPeeringService(host host.Host) *EstuaryPeeringService {
	estuaryPeeringService := &EstuaryPeeringService{}
	estuaryPeeringService.PeeringService = peering.NewPeeringService(host)
	return estuaryPeeringService
}

func (ps *EstuaryPeeringService) Start() error {
	return ps.PeeringService.Start()
}

func (ps *EstuaryPeeringService) Stop() error {
	return ps.PeeringService.Stop()
}

func (ps *EstuaryPeeringService) ListPeers() []peer.AddrInfo {
	return ps.PeeringService.ListPeers()
}

func (ps *EstuaryPeeringService) AddPeer(info peer.AddrInfo) {
	ps.PeeringService.AddPeer(info)
}

func (ps *EstuaryPeeringService) RemovePeer(peerId peer.ID) {
	ps.PeeringService.RemovePeer(peerId)
}
