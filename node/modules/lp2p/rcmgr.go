package lp2p

import (
	"context"
	"fmt"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	rcmgr "github.com/libp2p/go-libp2p/p2p/host/resource-manager"

	"github.com/application-research/estuary/metrics"

	logging "github.com/ipfs/go-log/v2"
	"go.opencensus.io/stats"
	"go.opencensus.io/tag"
)

var log = logging.Logger("rcmgr")

func NewResourceManager(limiter *rcmgr.Limiter) (network.ResourceManager, error) {
	var opts []rcmgr.Option
	libp2p.SetDefaultServiceLimits(&rcmgr.DefaultLimits)
	opts = append(opts, rcmgr.WithMetrics(rcmgrMetrics{}))
	mgr, err := rcmgr.NewResourceManager(*limiter, opts...)
	if err != nil {
		return nil, fmt.Errorf("error creating resource manager: %w", err)
	}
	return mgr, nil
}

type rcmgrMetrics struct{}

func (r rcmgrMetrics) Conn(dir network.Direction, usefd bool, op string) {
	ctx := context.Background()
	dirStr := "outbound"
	if dir == network.DirInbound {
		dirStr = "inbound"
	}
	ctx, _ = tag.New(ctx, tag.Upsert(metrics.Direction, dirStr))
	usefdStr := "false"
	if usefd {
		usefdStr = "true"
	}
	ctx, _ = tag.New(ctx, tag.Upsert(metrics.UseFD, usefdStr))
	opStr := "block"
	if op == "allow" {
		opStr = "allow"
	}
	ctx, _ = tag.New(ctx, tag.Upsert(metrics.Op, opStr))
	stats.Record(ctx, metrics.RcmgrConn.M(1))

}
func (r rcmgrMetrics) Stream(p peer.ID, dir network.Direction, op string) {
	ctx := context.Background()
	dirStr := "outbound"
	if dir == network.DirInbound {
		dirStr = "inbound"
	}
	ctx, _ = tag.New(ctx, tag.Upsert(metrics.Direction, dirStr))
	opStr := "block"
	if op == "allow" {
		opStr = "allow"
	}
	ctx, _ = tag.New(ctx, tag.Upsert(metrics.Op, opStr))
	stats.Record(ctx, metrics.RcmgrStream.M(1))
}

func (r rcmgrMetrics) Peer(op string) {
	ctx := context.Background()
	opStr := "block"
	if op == "allow" {
		opStr = "allow"
	}
	ctx, _ = tag.New(ctx, tag.Upsert(metrics.Op, opStr))
	stats.Record(ctx, metrics.RcmgrPeer.M(1))
}

func (r rcmgrMetrics) Proto(proto protocol.ID, op string) {
	ctx := context.Background()
	opStr := "block"
	if op == "allow" {
		opStr = "allow"
	}
	ctx, _ = tag.New(ctx, tag.Upsert(metrics.ProtocolID, string(proto)))
	ctx, _ = tag.New(ctx, tag.Upsert(metrics.Op, opStr))
	stats.Record(ctx, metrics.RcmgrProto.M(1))
}

func (r rcmgrMetrics) Svc(svc string, op string) {
	ctx := context.Background()
	opStr := "block"
	if op == "allow" {
		opStr = "allow"
	}
	ctx, _ = tag.New(ctx, tag.Upsert(metrics.ServiceID, string(svc)))
	ctx, _ = tag.New(ctx, tag.Upsert(metrics.Op, opStr))
	stats.Record(ctx, metrics.RcmgrSvc.M(1))
}

func (r rcmgrMetrics) Mem(size int, op string) {
	ctx := context.Background()
	opStr := "block"
	if op == "allow" {
		opStr = "allow"
	}
	ctx, _ = tag.New(ctx, tag.Upsert(metrics.Op, opStr))
	stats.Record(ctx, metrics.RcmgrMem.M(1))
}

func (r rcmgrMetrics) AllowConn(dir network.Direction, usefd bool) {
	r.Conn(dir, usefd, "allow")
}

func (r rcmgrMetrics) BlockConn(dir network.Direction, usefd bool) {
	r.Conn(dir, usefd, "block")
}

func (r rcmgrMetrics) AllowStream(p peer.ID, dir network.Direction) {
	r.Stream(p, dir, "allow")
}

func (r rcmgrMetrics) BlockStream(p peer.ID, dir network.Direction) {
	r.Stream(p, dir, "block")
}

func (r rcmgrMetrics) AllowPeer(p peer.ID) {
	r.Peer("allow")
}

func (r rcmgrMetrics) BlockPeer(p peer.ID) {
	r.Peer("block")
}

func (r rcmgrMetrics) AllowProtocol(proto protocol.ID) {
	r.Proto(proto, "allow")
}

func (r rcmgrMetrics) BlockProtocol(proto protocol.ID) {
	r.Proto(proto, "block")
}

func (r rcmgrMetrics) BlockProtocolPeer(proto protocol.ID, p peer.ID) {
	log.Infow("BlockProtocolPeer", "ProtocolID", proto, "PeerID", p)
}

func (r rcmgrMetrics) AllowService(svc string) {
	r.Svc(svc, "allow")
}

func (r rcmgrMetrics) BlockService(svc string) {
	r.Svc(svc, "block")
}

func (r rcmgrMetrics) BlockServicePeer(svc string, p peer.ID) {
	log.Infow("BlockServicePeer", "Svc", svc, "PeerID", p)
}

func (r rcmgrMetrics) AllowMemory(size int) {
	r.Mem(size, "allow")
}

func (r rcmgrMetrics) BlockMemory(size int) {
	r.Mem(size, "block")
}
