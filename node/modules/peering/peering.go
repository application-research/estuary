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

var DefaultPeers = []PeeringPeer{
	//Cloudflare
	{ID: "QmcfgsJsMtx6qJb74akCw1M24X1zFwgGo11h1cuhwQjtJP", Addrs: []string{"/ip6/2606:4700:60::6/tcp/4009", "/ip4/172.65.0.13/tcp/4009"}},

	//	NFT storage
	{ID: "12D3KooWEGeZ19Q79NdzS6CJBoCwFZwujqi5hoK8BtRcLa48fJdu", Addrs: []string{"/ip4/145.40.96.233/tcp/4001"}},
	{ID: "12D3KooWBnmsaeNRP6SCdNbhzaNHihQQBPDhmDvjVGsR1EbswncV", Addrs: []string{"/ip4/147.75.87.85/tcp/4001"}},
	{ID: "12D3KooWDLYiAdzUdM7iJHhWu5KjmCN62aWd7brQEQGRWbv8QcVb", Addrs: []string{"/ip4/136.144.57.203/tcp/4001"}},
	{ID: "12D3KooWFZmGztVoo2K1BcAoDEUmnp7zWFhaK5LcRHJ8R735T3eY", Addrs: []string{"/ip4/145.40.69.29/tcp/4001"}},
	{ID: "12D3KooWRJpsEsBtJ1TNik2zgdirqD4KFq5V4ar2vKCrEXUqFXPP", Addrs: []string{"/ip4/139.178.70.235/tcp/4001"}},
	{ID: "12D3KooWNxUGEN1SzRuwkJdbMDnHEVViXkRQEFCSuHRTdjFvD5uw", Addrs: []string{"/ip4/145.40.67.89/tcp/4001"}},
	{ID: "12D3KooWMZmMp9QwmfJdq3aXXstMbTCCB3FTWv9SNLdQGqyPMdUw", Addrs: []string{"/ip4/145.40.69.133/tcp/4001"}},
	{ID: "12D3KooWCpu8Nk4wmoXSsVeVSVzVHmrwBnEoC9jpcVpeWP7n67Bt", Addrs: []string{"/ip4/145.40.69.171/tcp/4001"}},
	{ID: "12D3KooWGx5pFFG7W2EG8N6FFwRLh34nHcCLMzoBSMSSpHcJYN7G", Addrs: []string{"/ip4/145.40.90.235/tcp/4001"}},
	{ID: "12D3KooWQsVxhA43ZjGNUDfF9EEiNYxb1PVEgCBMNj87E9cg92vT", Addrs: []string{"/ip4/139.178.69.135/tcp/4001"}},
	{ID: "12D3KooWMSrRXHgbBTsNGfxG1E44fLB6nJ5wpjavXj4VGwXKuz9X", Addrs: []string{"/ip4/147.75.32.99/tcp/4001"}},
	{ID: "12D3KooWE48wcXK7brQY1Hw7LhjF3xdiFegLnCAibqDtyrgdxNgn", Addrs: []string{"/ip4/147.75.86.227/tcp/4001"}},
	{ID: "12D3KooWSGCJYbM6uCvCF7cGWSitXSJTgEb7zjVCaxDyYNASTa8i", Addrs: []string{"/ip4/136.144.55.33/tcp/4001"}},
	{ID: "12D3KooWJbARcvvEEF4AAqvAEaVYRkEUNPC3Rv3joebqfPh4LaKq", Addrs: []string{"/ip4/136.144.57.127/tcp/4001"}},
	{ID: "12D3KooWNcshtC1XTbPxew2kq3utG2rRGLeMN8y5vSfAMTJMV7fE", Addrs: []string{"/ip4/147.75.87.249/tcp/4001"}},

	// 	Pinata
	{ID: "QmWaik1eJcGHq1ybTWe7sezRfqKNcDRNkeBaLnGwQJz1Cj", Addrs: []string{"/dnsaddr/fra1-1.hostnodes.pinata.cloud"}},
	{ID: "QmNfpLrQQZr5Ns9FAJKpyzgnDL2GgC6xBug1yUZozKFgu4", Addrs: []string{"/dnsaddr/fra1-2.hostnodes.pinata.cloud"}},
	{ID: "QmPo1ygpngghu5it8u4Mr3ym6SEU2Wp2wA66Z91Y1S1g29", Addrs: []string{"/dnsaddr/fra1-3.hostnodes.pinata.cloud"}},
	{ID: "QmRjLSisUCHVpFa5ELVvX3qVPfdxajxWJEHs9kN3EcxAW6", Addrs: []string{"/dnsaddr/nyc1-1.hostnodes.pinata.cloud"}},
	{ID: "QmPySsdmbczdZYBpbi2oq2WMJ8ErbfxtkG8Mo192UHkfGP", Addrs: []string{"/dnsaddr/nyc1-2.hostnodes.pinata.cloud"}},
	{ID: "QmSarArpxemsPESa6FNkmuu9iSE1QWqPX2R3Aw6f5jq4D5", Addrs: []string{"/dnsaddr/nyc1-3.hostnodes.pinata.cloud"}},

	//	Protocol Labs
	{ID: "QmUEMvxS2e7iDrereVYc5SWPauXPyNwxcy9BXZrC1QTcHE", Addrs: []string{"/dns/cluster0.fsn.dwebops.pub"}},
	{ID: "QmNSYxZAiJHeLdkBg38roksAR9So7Y5eojks1yjEcUtZ7i", Addrs: []string{"/dns/cluster1.fsn.dwebops.pub"}},
	{ID: "QmUd6zHcbkbcs7SMxwLs48qZVX3vpcM8errYS7xEczwRMA", Addrs: []string{"/dns/cluster2.fsn.dwebops.pub"}},
	{ID: "QmbVWZQhCGrS7DhgLqWbgvdmKN7JueKCREVanfnVpgyq8x", Addrs: []string{"/dns/cluster3.fsn.dwebops.pub"}},
	{ID: "QmdnXwLrC8p1ueiq2Qya8joNvk3TVVDAut7PrikmZwubtR", Addrs: []string{"/dns/cluster4.fsn.dwebops.pub"}},
	{ID: "12D3KooWCRscMgHgEo3ojm8ovzheydpvTEqsDtq7Vby38cMHrYjt", Addrs: []string{"/dns4/nft-storage-am6.nft.dwebops.net/tcp/18402"}},
	{ID: "12D3KooWQtpvNvUYFzAo1cRYkydgk15JrMSHp6B6oujqgYSnvsVm", Addrs: []string{"/dns4/nft-storage-dc13.nft.dwebops.net/tcp/18402"}},
	{ID: "12D3KooWQcgCwNCTYkyLXXQSZuL5ry1TzpM8PRe9dKddfsk1BxXZ", Addrs: []string{"/dns4/nft-storage-sv15.nft.dwebops.net/tcp/18402"}},

	//	Textile
	{ID: "QmR69wtWUMm1TWnmuD4JqC1TWLZcc8iR2KrTenfZZbiztd", Addrs: []string{"/ip4/104.210.43.77"}},

	//	Web3.Storage
	{ID: "12D3KooWR19qPPiZH4khepNjS3CLXiB7AbrbAD4ZcDjN1UjGUNE1", Addrs: []string{"/ip4/139.178.69.155/tcp/4001"}},
	{ID: "12D3KooWEDMw7oRqQkdCJbyeqS5mUmWGwTp8JJ2tjCzTkHboF6wK", Addrs: []string{"/ip4/139.178.68.91/tcp/4001"}},
	{ID: "12D3KooWPySxxWQjBgX9Jp6uAHQfVmdq8HG1gVvS1fRawHNSrmqW", Addrs: []string{"/ip4/147.75.33.191/tcp/4001"}},
	{ID: "12D3KooWNuoVEfVLJvU3jWY2zLYjGUaathsecwT19jhByjnbQvkj", Addrs: []string{"/ip4/147.75.32.73/tcp/4001"}},
	{ID: "12D3KooWSnniGsyAF663gvHdqhyfJMCjWJv54cGSzcPiEMAfanvU", Addrs: []string{"/ip4/145.40.89.195/tcp/4001"}},
	{ID: "12D3KooWKytRAd2ujxhGzaLHKJuje8sVrHXvjGNvHXovpar5KaKQ", Addrs: []string{"/ip4/136.144.56.153/tcp/4001"}},

	//	Estuary
	{ID: "12D3KooWCVXs8P7iq6ao4XhfAmKWrEeuKFWCJgqe9jGDMTqHYBjw", Addrs: []string{"/ip4/139.178.68.217/tcp/6744"}},
	{ID: "12D3KooWGBWx9gyUFTVQcKMTenQMSyE2ad9m7c9fpjS4NMjoDien", Addrs: []string{"/ip4/147.75.49.71/tcp/6745"}},
	{ID: "12D3KooWFrnuj5o3tx4fGD2ZVJRyDqTdzGnU3XYXmBbWbc8Hs8Nd", Addrs: []string{"/ip4/147.75.86.255/tcp/6745"}},
	{ID: "12D3KooWN8vAoGd6eurUSidcpLYguQiGZwt4eVgDvbgaS7kiGTup", Addrs: []string{"/ip4/3.134.223.177/tcp/6745"}},
	{ID: "12D3KooWLV128pddyvoG6NBvoZw7sSrgpMTPtjnpu3mSmENqhtL7", Addrs: []string{"/ip4/35.74.45.12/udp/6746/quic"}},
}
