package metrics

import (
	"net/http"
	//#nosec G108 - exposing the profiling endpoint is expected
	_ "net/http/pprof"

	"contrib.go.opencensus.io/exporter/prometheus"
	promclient "github.com/prometheus/client_golang/prometheus"
)

func Exporter() http.Handler {
	// Prometheus globals are exposed as interfaces, but the prometheus
	// OpenCensus exporter expects a concrete *Registry. The concrete type of
	// the globals are actually *Registry, so we downcast them, staying
	// defensive in case things change under the hood.
	registry, ok := promclient.DefaultRegisterer.(*promclient.Registry)
	if !ok {
		log.Warnf("failed to export default prometheus registry; some metrics will be unavailable; unexpected type: %T", promclient.DefaultRegisterer)
	}
	exporter, err := prometheus.NewExporter(prometheus.Options{
		Registry:  registry,
		Namespace: "estuary",
	})
	if err != nil {
		log.Errorf("could not create the prometheus stats exporter: %v", err)
	}

	return exporter
}
