package metrics

import (
	"context"
	"os"
	"time"

	"go.opencensus.io/stats"
	"go.opencensus.io/stats/view"
	"go.opencensus.io/tag"

	rpcmetrics "github.com/filecoin-project/go-jsonrpc/metrics"

	"github.com/filecoin-project/lotus/blockstore"
)

// Global Tags
var (
	// common
	Version, _  = tag.NewKey("version")
	Commit, _   = tag.NewKey("commit")
	NodeType, _ = tag.NewKey("node_type")
	PeerID, _   = tag.NewKey("peer_id")

	// rcmgr
	ServiceID, _  = tag.NewKey("svc")
	ProtocolID, _ = tag.NewKey("proto")
	Direction, _  = tag.NewKey("direction")
	UseFD, _      = tag.NewKey("use_fd")
	Block, _      = tag.NewKey("block")
	Allow, _      = tag.NewKey("allow")
)

// Measures
var (
	// common
	LotusInfo          = stats.Int64("info", "Arbitrary counter to tag lotus info to", stats.UnitDimensionless)
	PeerCount          = stats.Int64("peer/count", "Current number of FIL peers", stats.UnitDimensionless)
	APIRequestDuration = stats.Float64("api/request_duration_ms", "Duration of API requests", stats.UnitMilliseconds)

	// rcmgr
	RcmgrConns          = stats.Int64("rcmgr/conns", "Number of connections", stats.UnitDimensionless)
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
	RcmgrConnsView = &view.View{
		Measure:     RcmgrConns,
		Aggregation: view.Count(),
		TagKeys:     []tag.Key{Direction, UseFD, Block, Allow},
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
var DefaultViews = func() []*view.View {
	views := []*view.View{
		InfoView,
		RcmgrConnsView,
		RcmgrAllowStreamView,
		RcmgrBlockStreamView,
		RcmgrAllowPeerView,
		RcmgrBlockPeerView,
		RcmgrAllowProtoView,
		RcmgrBlockProtoView,
		RcmgrAllowSvcView,
		RcmgrBlockSvcView,
		RcmgrBlockSvcPeerView,
		RcmgrAllowMemView,
		RcmgrBlockMemView,
	}
	views = append(views, blockstore.DefaultViews...)
	views = append(views, rpcmetrics.DefaultViews...)

	//	additional optional views based on environment variable
	if os.Getenv("ENABLE_RCMGR_BLOCK_PROTO_PEER_VIEW") == "true" {
		views = append(views, RcmgrBlockProtoPeerView)
	}

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
