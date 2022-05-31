package config

type Jaeger struct {
	EnableTracing bool    `json:"enable_tracing"`
	ProviderUrl   string  `json:"provider_url"`
	SamplerRatio  float64 `json:"sampler_ratio"`
}
