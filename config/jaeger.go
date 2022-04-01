package config

type Jaeger struct {
	EnableTracing bool
	ProviderUrl   string
	SamplerRatio  float64
}
