package main

import (
	"context"
	"sort"
	"time"

	"github.com/application-research/estuary/node"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/peerstore"
	"github.com/libp2p/go-libp2p/p2p/protocol/ping"
	"github.com/multiformats/go-multiaddr"
)

type PeerPingManager struct {
	Node   *node.Node
	Result PingManyResult
}

type Libp2pHost struct {
	addr   multiaddr.Multiaddr
	peerID peer.ID
}

type PingManyResult map[peer.ID]time.Duration

// Ping a list of hosts, returning RTT for each of them. Errors will be ignored, unresponsive hosts will not be returned
func (ppm PeerPingManager) PingMany(ctx context.Context, hosts []Libp2pHost) {
	result := make(PingManyResult)

	// TODO: Batch them in go funcs for faster performance
	for _, h := range hosts {
		res, err := ppm.pingOne(ctx, h.addr, h.peerID)

		if err == nil {
			result[h.peerID] = *res
		}
	}

	ppm.Result = result
}

// Perform a libp2p network ping to a specified host, returning the Round trip time (RTT)
func (ppm PeerPingManager) pingOne(ctx context.Context, addr multiaddr.Multiaddr, pID peer.ID) (*time.Duration, error) {
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
