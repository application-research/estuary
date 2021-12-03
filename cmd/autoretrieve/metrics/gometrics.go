package metrics

import (
	"context"

	gometrics "github.com/ipfs/go-metrics-interface"
	gometricsprometheus "github.com/ipfs/go-metrics-prometheus"
)

type GoMetrics struct {
	Metrics

	activeRetrievals   gometrics.Gauge
	retrievalSuccesses gometrics.Counter
	retrievalFailures  gometrics.Counter
}

func GoMetricsInjectPrometheus() error {
	return gometricsprometheus.Inject()
}

func NewPrometheus(ctx context.Context, inner Metrics) *GoMetrics {

	scope := gometrics.CtxScope(ctx, "autoretrieve")
	metrics := &GoMetrics{
		Metrics:            inner,
		activeRetrievals:   gometrics.NewCtx(scope, "active_retrievals", "active retrieval count").Gauge(),
		retrievalSuccesses: gometrics.NewCtx(scope, "retrieval_successes", "retrieval success count").Counter(),
		retrievalFailures:  gometrics.NewCtx(scope, "retrieval_failures", "retrieval failure count").Counter(),
	}

	return metrics
}

// Resets all session data like active retrievals
func (gometrics *GoMetrics) NewSession() {
	gometrics.activeRetrievals.Set(0)
}

func (metrics *GoMetrics) RecordRetrieval(info CandidateInfo) {
	metrics.activeRetrievals.Inc()

	metrics.Metrics.RecordRetrieval(info)
}

func (metrics *GoMetrics) RecordRetrievalResult(info CandidateInfo, result RetrievalResult) {
	metrics.activeRetrievals.Dec()

	if result.Err != nil {
		metrics.retrievalFailures.Inc()
	} else {
		metrics.retrievalSuccesses.Inc()
	}

	metrics.Metrics.RecordRetrievalResult(info, result)
}
