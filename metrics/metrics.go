package metrics

import (
	"context"
	"time"

	"go.opencensus.io/stats"
	"go.opencensus.io/stats/view"
	"go.opencensus.io/tag"

	rpcmetrics "github.com/filecoin-project/go-jsonrpc/metrics"

	"github.com/filecoin-project/lotus/blockstore"
)

// Distribution
var defaultMillisecondsDistribution = view.Distribution(0.01, 0.05, 0.1, 0.3, 0.6, 0.8, 1, 2, 3, 4, 5, 6, 8, 10, 13, 16, 20, 25, 30, 40, 50, 65, 80, 100, 130, 160, 200, 250, 300, 400, 500, 650, 800, 1000, 2000, 3000, 4000, 5000, 7500, 10000, 20000, 50000, 100000)
var workMillisecondsDistribution = view.Distribution(
	250, 500, 1000, 2000, 5000, 10_000, 30_000, 60_000, 2*60_000, 5*60_000, 10*60_000, 15*60_000, 30*60_000, // short sealing tasks
	40*60_000, 45*60_000, 50*60_000, 55*60_000, 60*60_000, 65*60_000, 70*60_000, 75*60_000, 80*60_000, 85*60_000, 100*60_000, 120*60_000, // PC2 / C2 range
	130*60_000, 140*60_000, 150*60_000, 160*60_000, 180*60_000, 200*60_000, 220*60_000, 260*60_000, 300*60_000, // PC1 range
	350*60_000, 400*60_000, 600*60_000, 800*60_000, 1000*60_000, 1300*60_000, 1800*60_000, 4000*60_000, 10000*60_000, // intel PC1 range
)

// Global Tags
var (
	// common
	Version, _     = tag.NewKey("version")
	Commit, _      = tag.NewKey("commit")
	NodeType, _    = tag.NewKey("node_type")
	PeerID, _      = tag.NewKey("peer_id")
	MinerID, _     = tag.NewKey("miner_id")
	FailureType, _ = tag.NewKey("failure_type")

	// rcmgr
	ServiceID, _  = tag.NewKey("svc")
	ProtocolID, _ = tag.NewKey("proto")
	Direction, _  = tag.NewKey("direction")
	UseFD, _      = tag.NewKey("use_fd")
)

// Measures
var (
	// common
	LotusInfo          = stats.Int64("info", "Arbitrary counter to tag lotus info to", stats.UnitDimensionless)
	PeerCount          = stats.Int64("peer/count", "Current number of FIL peers", stats.UnitDimensionless)
	APIRequestDuration = stats.Float64("api/request_duration_ms", "Duration of API requests", stats.UnitMilliseconds)

	// rcmgr
	RcmgrAllowConn      = stats.Int64("rcmgr/allow_conn", "Number of allowed connections", stats.UnitDimensionless)
	RcmgrBlockConn      = stats.Int64("rcmgr/block_conn", "Number of blocked connections", stats.UnitDimensionless)
	RcmgrAllowStream    = stats.Int64("rcmgr/allow_stream", "Number of allowed streams", stats.UnitDimensionless)
	RcmgrBlockStream    = stats.Int64("rcmgr/block_stream", "Number of blocked streams", stats.UnitDimensionless)
	RcmgrAllowPeer      = stats.Int64("rcmgr/allow_peer", "Number of allowed peer connections", stats.UnitDimensionless)
	RcmgrBlockPeer      = stats.Int64("rcmgr/block_peer", "Number of blocked peer connections", stats.UnitDimensionless)
	RcmgrAllowProto     = stats.Int64("rcmgr/allow_proto", "Number of allowed streams attached to a protocol", stats.UnitDimensionless)
	RcmgrBlockProto     = stats.Int64("rcmgr/block_proto", "Number of blocked blocked streams attached to a protocol", stats.UnitDimensionless)
	RcmgrBlockProtoPeer = stats.Int64("rcmgr/block_proto", "Number of blocked blocked streams attached to a protocol for a specific peer", stats.UnitDimensionless)
	RcmgrAllowSvc       = stats.Int64("rcmgr/allow_svc", "Number of allowed streams attached to a service", stats.UnitDimensionless)
	RcmgrBlockSvc       = stats.Int64("rcmgr/block_svc", "Number of blocked blocked streams attached to a service", stats.UnitDimensionless)
	RcmgrBlockSvcPeer   = stats.Int64("rcmgr/block_svc", "Number of blocked blocked streams attached to a service for a specific peer", stats.UnitDimensionless)
	RcmgrAllowMem       = stats.Int64("rcmgr/allow_mem", "Number of allowed memory reservations", stats.UnitDimensionless)
	RcmgrBlockMem       = stats.Int64("rcmgr/block_mem", "Number of blocked memory reservations", stats.UnitDimensionless)
)

var (
	InfoView = &view.View{
		Name:        "info",
		Description: "Lotus node information",
		Measure:     LotusInfo,
		Aggregation: view.LastValue(),
		TagKeys:     []tag.Key{Version, Commit, NodeType},
	}

	// rcmgr
	RcmgrAllowConnView = &view.View{
		Measure:     RcmgrAllowConn,
		Aggregation: view.Count(),
		TagKeys:     []tag.Key{Direction, UseFD},
	}
	RcmgrBlockConnView = &view.View{
		Measure:     RcmgrBlockConn,
		Aggregation: view.Count(),
		TagKeys:     []tag.Key{Direction, UseFD},
	}
	RcmgrAllowStreamView = &view.View{
		Measure:     RcmgrAllowStream,
		Aggregation: view.Count(),
		TagKeys:     []tag.Key{PeerID, Direction},
	}
	RcmgrBlockStreamView = &view.View{
		Measure:     RcmgrBlockStream,
		Aggregation: view.Count(),
		TagKeys:     []tag.Key{PeerID, Direction},
	}
	RcmgrAllowPeerView = &view.View{
		Measure:     RcmgrAllowPeer,
		Aggregation: view.Count(),
		TagKeys:     []tag.Key{PeerID},
	}
	RcmgrBlockPeerView = &view.View{
		Measure:     RcmgrBlockPeer,
		Aggregation: view.Count(),
		TagKeys:     []tag.Key{PeerID},
	}
	RcmgrAllowProtoView = &view.View{
		Measure:     RcmgrAllowProto,
		Aggregation: view.Count(),
		TagKeys:     []tag.Key{ProtocolID},
	}
	RcmgrBlockProtoView = &view.View{
		Measure:     RcmgrBlockProto,
		Aggregation: view.Count(),
		TagKeys:     []tag.Key{ProtocolID},
	}
	RcmgrBlockProtoPeerView = &view.View{
		Measure:     RcmgrBlockProtoPeer,
		Aggregation: view.Count(),
		TagKeys:     []tag.Key{ProtocolID, PeerID},
	}
	RcmgrAllowSvcView = &view.View{
		Measure:     RcmgrAllowSvc,
		Aggregation: view.Count(),
		TagKeys:     []tag.Key{ServiceID},
	}
	RcmgrBlockSvcView = &view.View{
		Measure:     RcmgrBlockSvc,
		Aggregation: view.Count(),
		TagKeys:     []tag.Key{ServiceID},
	}
	RcmgrBlockSvcPeerView = &view.View{
		Measure:     RcmgrBlockSvcPeer,
		Aggregation: view.Count(),
		TagKeys:     []tag.Key{ServiceID, PeerID},
	}
	RcmgrAllowMemView = &view.View{
		Measure:     RcmgrAllowMem,
		Aggregation: view.Count(),
	}
	RcmgrBlockMemView = &view.View{
		Measure:     RcmgrBlockMem,
		Aggregation: view.Count(),
	}
)

// DefaultViews is an array of OpenCensus views for metric gathering purposes
var ResourceManagerViews = func() []*view.View {
	views := []*view.View{
		InfoView,
		RcmgrAllowConnView,
		RcmgrBlockConnView,
		RcmgrAllowStreamView,
		RcmgrBlockStreamView,
		RcmgrAllowPeerView,
		RcmgrBlockPeerView,
		RcmgrAllowProtoView,
		RcmgrBlockProtoView,
		RcmgrBlockProtoPeerView,
		RcmgrAllowSvcView,
		RcmgrBlockSvcView,
		RcmgrBlockSvcPeerView,
		RcmgrAllowMemView,
		RcmgrBlockMemView,
	}
	views = append(views, blockstore.DefaultViews...)
	views = append(views, rpcmetrics.DefaultViews...)
	return views
}()

// SinceInMilliseconds returns the duration of time since the provide time as a float64.
func SinceInMilliseconds(startTime time.Time) float64 {
	return float64(time.Since(startTime).Nanoseconds()) / 1e6
}

// Timer is a function stopwatch, calling it starts the timer,
// calling the returned function will record the duration.
func Timer(ctx context.Context, m *stats.Float64Measure) func() {
	start := time.Now()
	return func() {
		stats.Record(ctx, m.M(SinceInMilliseconds(start)))
	}
}
