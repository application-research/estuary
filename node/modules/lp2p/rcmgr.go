package lp2p

import (
	"context"
	"fmt"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p-core/network"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/protocol"
	rcmgr "github.com/libp2p/go-libp2p-resource-manager"

	"github.com/application-research/estuary/metrics"

	"go.opencensus.io/stats"
	"go.opencensus.io/tag"
)

func NewDefaultLimiter() *rcmgr.BasicLimiter {
	return rcmgr.NewDefaultLimiter()
}

func NewResourceManager(limiter *rcmgr.BasicLimiter) (network.ResourceManager, error) {
	var opts []rcmgr.Option
	libp2p.SetDefaultServiceLimits(limiter)
	opts = append(opts, rcmgr.WithMetrics(rcmgrMetrics{}))
	mgr, err := rcmgr.NewResourceManager(limiter, opts...)
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
func (r rcmgrMetrics) Stream(dir network.Direction, usefd bool, op string) {
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
	stats.Record(ctx, metrics.RcmgrStream.M(1))
}

func (r rcmgrMetrics) AllowConn(dir network.Direction, usefd bool) {
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
	stats.Record(ctx, metrics.RcmgrAllowConn.M(1))
}

func (r rcmgrMetrics) BlockConn(dir network.Direction, usefd bool) {
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
	stats.Record(ctx, metrics.RcmgrBlockConn.M(1))
}

func (r rcmgrMetrics) AllowStream(p peer.ID, dir network.Direction) {
	ctx := context.Background()
	dirStr := "outbound"
	if dir == network.DirInbound {
		dirStr = "inbound"
	}
	ctx, _ = tag.New(ctx, tag.Upsert(metrics.Direction, dirStr))
	stats.Record(ctx, metrics.RcmgrAllowStream.M(1))
}

func (r rcmgrMetrics) BlockStream(p peer.ID, dir network.Direction) {
	ctx := context.Background()
	dirStr := "outbound"
	if dir == network.DirInbound {
		dirStr = "inbound"
	}
	ctx, _ = tag.New(ctx, tag.Upsert(metrics.Direction, dirStr))
	stats.Record(ctx, metrics.RcmgrBlockStream.M(1))
}

func (r rcmgrMetrics) AllowPeer(p peer.ID) {
	ctx := context.Background()
	stats.Record(ctx, metrics.RcmgrAllowPeer.M(1))
}

func (r rcmgrMetrics) BlockPeer(p peer.ID) {
	ctx := context.Background()
	stats.Record(ctx, metrics.RcmgrBlockPeer.M(1))
}

func (r rcmgrMetrics) AllowProtocol(proto protocol.ID) {
	ctx := context.Background()
	ctx, _ = tag.New(ctx, tag.Upsert(metrics.ProtocolID, string(proto)))
	stats.Record(ctx, metrics.RcmgrAllowProto.M(1))
}

func (r rcmgrMetrics) BlockProtocol(proto protocol.ID) {
	ctx := context.Background()
	ctx, _ = tag.New(ctx, tag.Upsert(metrics.ProtocolID, string(proto)))
	stats.Record(ctx, metrics.RcmgrBlockProto.M(1))
}

func (r rcmgrMetrics) BlockProtocolPeer(proto protocol.ID, p peer.ID) {
	ctx := context.Background()
	ctx, _ = tag.New(ctx, tag.Upsert(metrics.ProtocolID, string(proto)))
	stats.Record(ctx, metrics.RcmgrBlockProtoPeer.M(1))
}

func (r rcmgrMetrics) AllowService(svc string) {
	ctx := context.Background()
	ctx, _ = tag.New(ctx, tag.Upsert(metrics.ServiceID, svc))
	stats.Record(ctx, metrics.RcmgrAllowSvc.M(1))
}

func (r rcmgrMetrics) BlockService(svc string) {
	ctx := context.Background()
	ctx, _ = tag.New(ctx, tag.Upsert(metrics.ServiceID, svc))
	stats.Record(ctx, metrics.RcmgrBlockSvc.M(1))
}

func (r rcmgrMetrics) BlockServicePeer(svc string, p peer.ID) {
	ctx := context.Background()
	ctx, _ = tag.New(ctx, tag.Upsert(metrics.ServiceID, svc))
	stats.Record(ctx, metrics.RcmgrBlockSvcPeer.M(1))
}

func (r rcmgrMetrics) AllowMemory(size int) {
	stats.Record(context.Background(), metrics.RcmgrAllowMem.M(1))
}

func (r rcmgrMetrics) BlockMemory(size int) {
	stats.Record(context.Background(), metrics.RcmgrBlockMem.M(1))
}
