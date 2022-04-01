package config

type JaegerConfig struct {
	EnableTracing bool
	ProviderUrl   string
	SamplerRatio  float64
}
