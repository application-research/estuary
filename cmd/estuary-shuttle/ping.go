package main

import (
	"context"
	"time"

	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/peerstore"
	"github.com/libp2p/go-libp2p/p2p/protocol/ping"
	"github.com/multiformats/go-multiaddr"
)

type Libp2pHost struct {
	addr   multiaddr.Multiaddr
	peerID peer.ID
}

type PingManyResult map[peer.ID]time.Duration

// Ping a list of hosts, returning RTT for each of them. Errors will be ignored, unresponsive hosts will not be returned
func (s *Shuttle) PingMany(ctx context.Context, hosts []Libp2pHost) PingManyResult {
	result := make(PingManyResult)

	// TODO: Batch them in go funcs for faster performance
	for _, h := range hosts {
		res, err := s.PingOne(ctx, h.addr, h.peerID)

		if err == nil {
			result[h.peerID] = *res
		}
	}

	return result
}

// Perform a libp2p network ping to a specified host, returning the Round trip time (RTT)
func (s *Shuttle) PingOne(ctx context.Context, addr multiaddr.Multiaddr, pID peer.ID) (*time.Duration, error) {
	// Add the host to the peerstore so it can be found
	s.Node.Host.Peerstore().AddAddr(pID, addr, peerstore.TempAddrTTL)

	tctx, cancel := context.WithTimeout(ctx, time.Duration(time.Millisecond*1000))
	defer cancel()

	t, err := netPing(tctx, pID, s.Node.Host)
	if err != nil {
		return nil, err
	}

	return &t, nil
}

func netPing(ctx context.Context, p peer.ID, h host.Host) (time.Duration, error) {
	result, ok := <-ping.Ping(ctx, h, p)
	if !ok {
		return result.RTT, ctx.Err()
	}
	return result.RTT, result.Error
}
