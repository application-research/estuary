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
	Op, _         = tag.NewKey("op")
)

// Measures
var (
	// common
	LotusInfo          = stats.Int64("info", "Arbitrary counter to tag lotus info to", stats.UnitDimensionless)
	PeerCount          = stats.Int64("peer/count", "Current number of FIL peers", stats.UnitDimensionless)
	APIRequestDuration = stats.Float64("api/request_duration_ms", "Duration of API requests", stats.UnitMilliseconds)

	// rcmgr
	RcmgrConn   = stats.Int64("rcmgr/conn", "Number of connections", stats.UnitDimensionless)
	RcmgrStream = stats.Int64("rcmgr/stream", "Number of allowed streams", stats.UnitDimensionless)
	RcmgrPeer   = stats.Int64("rcmgr/peer", "Number of peers", stats.UnitDimensionless)
	RcmgrProto  = stats.Int64("rcmgr/proto", "Number of allowed streams attached to a protocol", stats.UnitDimensionless)
	RcmgrSvc    = stats.Int64("rcmgr/svc", "Number of streams attached to a service", stats.UnitDimensionless)
	RcmgrMem    = stats.Int64("rcmgr/mem", "Number of memory reservations", stats.UnitDimensionless)
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
	RcmgrConnView = &view.View{
		Measure:     RcmgrConn,
		Aggregation: view.Count(),
		TagKeys:     []tag.Key{Direction, UseFD, Op},
	}

	RcmgrStreamView = &view.View{
		Measure:     RcmgrStream,
		Aggregation: view.Count(),
		TagKeys:     []tag.Key{Direction, UseFD, Op},
	}

	RcmgrPeerView = &view.View{
		Measure:     RcmgrPeer,
		Aggregation: view.Count(),
		TagKeys:     []tag.Key{PeerID, Op},
	}

	RcmgrProtoView = &view.View{
		Measure:     RcmgrProto,
		Aggregation: view.Count(),
		TagKeys:     []tag.Key{ProtocolID, Op},
	}

	RcmgrSvcView = &view.View{
		Measure:     RcmgrSvc,
		Aggregation: view.Count(),
		TagKeys:     []tag.Key{ServiceID, Op},
	}

	RcmgrMemView = &view.View{
		Measure:     RcmgrMem,
		Aggregation: view.Count(),
	}
)

// DefaultViews is an array of OpenCensus views for metric gathering purposes
var DefaultViews = func() []*view.View {
	views := []*view.View{
		InfoView,
		RcmgrConnView,
		RcmgrStreamView,
		RcmgrPeerView,
		RcmgrProtoView,
		RcmgrSvcView,
		RcmgrMemView,
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
