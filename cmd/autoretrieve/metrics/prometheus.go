package metrics

import (
	"context"

	prometheus "github.com/ipfs/go-metrics-interface"
)

type Prometheus struct {
	Metrics

	activeRetrievals   prometheus.Gauge
	retrievalSuccesses prometheus.Counter
	retrievalFailures  prometheus.Counter
}

func NewPrometheus(ctx context.Context, inner Metrics) *Prometheus {
	metrics := &Prometheus{
		Metrics:            inner,
		activeRetrievals:   prometheus.NewCtx(ctx, "active_retrievals", "active retrieval count").Gauge(),
		retrievalSuccesses: prometheus.NewCtx(ctx, "retrieval_successes", "retrieval success count").Counter(),
		retrievalFailures:  prometheus.NewCtx(ctx, "retrieval_failures", "retrieval failure count").Counter(),
	}

	return metrics
}

// Resets all session data like active retrievals
func (prometheus *Prometheus) NewSession() {
	prometheus.activeRetrievals.Set(0)
}

func (metrics *Prometheus) RecordRetrieval(info CandidateInfo) {
	metrics.activeRetrievals.Inc()

	metrics.Metrics.RecordRetrieval(info)
}

func (metrics *Prometheus) RecordRetrievalResult(info CandidateInfo, result RetrievalResult) {
	metrics.activeRetrievals.Dec()

	if result.Err != nil {
		metrics.retrievalFailures.Inc()
	} else {
		metrics.retrievalSuccesses.Inc()
	}

	metrics.Metrics.RecordRetrievalResult(info, result)
}
