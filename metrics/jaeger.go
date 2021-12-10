package metrics

import (
	logging "github.com/ipfs/go-log/v2"
	"go.opentelemetry.io/otel/exporters/jaeger"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.7.0"
)

var log = logging.Logger("metrics")

// NewJaegerTraceProvider returns a new and configured TracerProvider backed by Jaeger.
// based on https://github.com/open-telemetry/opentelemetry-go/blob/v0.20.0/example/jaeger/main.go
func NewJaegerTraceProvider(serviceName, agentEndpoint string, sampleRatio float64) (*sdktrace.TracerProvider, error) {
	log.Infow("creating jaeger trace provider", "serviceName", serviceName, "ratio", sampleRatio, "endpoint", agentEndpoint)
	var sampler sdktrace.Sampler
	if sampleRatio < 1 && sampleRatio > 0 {
		sampler = sdktrace.ParentBased(sdktrace.TraceIDRatioBased(sampleRatio))
	} else if sampleRatio == 1 {
		sampler = sdktrace.AlwaysSample()
	} else {
		sampler = sdktrace.NeverSample()
	}

	exp, err := jaeger.New(jaeger.WithCollectorEndpoint(jaeger.WithEndpoint(agentEndpoint)))
	if err != nil {
		return nil, err
	}
	tp := sdktrace.NewTracerProvider(
		// Always be sure to batch in production.
		sdktrace.WithBatcher(exp),
		sdktrace.WithSampler(sampler),
		sdktrace.WithResource(resource.NewWithAttributes(
			semconv.SchemaURL,
			semconv.ServiceNameKey.String(serviceName),
		)),
	)
	return tp, nil
}
