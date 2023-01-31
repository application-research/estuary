package main

import (
	"context"
	"encoding/json"
	"sort"
	"time"

	"github.com/application-research/estuary/api/v1"
	"github.com/application-research/estuary/node"
	"github.com/filecoin-project/go-address"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/peerstore"
	"github.com/libp2p/go-libp2p/p2p/protocol/ping"
	"github.com/multiformats/go-multiaddr"
)

type PeerPingManager struct {
	Node     *node.Node
	HtClient *ShuttleHttpClient
	Result   PingManyResult
}

type SpHost struct {
	spAddr    address.Address
	multiAddr []multiaddr.Multiaddr
	peerID    peer.ID
}

type PingManyResult map[peer.ID]time.Duration

func NewPPM(node *node.Node, shtc *ShuttleHttpClient) *PeerPingManager {

	return &PeerPingManager{
		Node:     node,
		HtClient: shtc,
		Result:   make(map[peer.ID]time.Duration),
	}
}

// Kicks off a PPM task to get a list of SPs and ping them every <interval>
// Spawns a new go thread, and returns immedately
func (ppm *PeerPingManager) Run(interval time.Duration) {
	go func() {
		ctx := context.TODO()
		for {
			hosts, err := ppm.getSpList()
			if err != nil {
				log.Errorf("ppm could not get SP list %v", err)
			}

			ppm.PingMany(ctx, hosts)

			time.Sleep(interval)
		}
	}()
}

func (ppm *PeerPingManager) getSpList() ([]SpHost, error) {
	resp, err := ppm.HtClient.MakeRequest("GET", "/v2/storage-providers", nil, "")
	if err != nil {
		return nil, err
	}

	var out []api.MinerResp
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, err
	}

	var sps []SpHost

	for _, m := range out {
		sp := SpHost{
			spAddr:    m.Addr,
			peerID:    m.ChainInfo.PeerID,
			multiAddr: m.ChainInfo.Addresses,
		}
		sps = append(sps, sp)
	}

	return sps, nil
}

// Ping a list of hosts, returning RTT for each of them. Errors will be ignored, unresponsive hosts will not be returned
func (ppm *PeerPingManager) PingMany(ctx context.Context, hosts []SpHost) {
	result := make(PingManyResult)

	// TODO: Batch them in go funcs for faster performance
	for _, h := range hosts {

		// Try all multiaddrs until one is pingable, and take that result
		for _, addr := range h.multiAddr {
			res, err := ppm.pingOne(ctx, addr, h.peerID)

			if err == nil {
				result[h.peerID] = *res
				break
			}
		}
	}

	ppm.Result = result
}

// Perform a libp2p network ping to a specified host, returning the Round trip time (RTT)
// Note: this function may take as long as 1 second to complete, while it waits for pings back from the remote host
func (ppm *PeerPingManager) pingOne(ctx context.Context, addr multiaddr.Multiaddr, pID peer.ID) (*time.Duration, error) {
	// Add the host to the peerstore so it can be found
	ppm.Node.Host.Peerstore().AddAddr(pID, addr, peerstore.TempAddrTTL)

	tctx, cancel := context.WithTimeout(ctx, time.Duration(time.Millisecond*1000))
	defer cancel()

	t, err := netPing(tctx, ppm.Node.Host, pID)
	if err != nil {
		return nil, err
	}

	return &t, nil
}

// Returns a slice of the top-performing (lowest-latency) peers in the ping result.
// Output will be truncated to the top `n`
func (p PingManyResult) GetTopPeers(n int) []peer.ID {
	result := make([]peer.ID, 0, len(p))

	for k := range p {
		result = append(result, k)
	}

	sort.SliceStable(result, func(i, j int) bool {
		return p[result[i]] < p[result[j]]
	})

	return result[0:n]
}

func netPing(ctx context.Context, h host.Host, p peer.ID) (time.Duration, error) {
	result, ok := <-ping.Ping(ctx, h, p)
	if !ok {
		return result.RTT, ctx.Err()
	}
	return result.RTT, result.Error
}
