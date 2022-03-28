package config

type JaegerConfig struct {
	JaegerTracing      bool
	JaegerProviderUrl  string
	JaegerSamplerRatio float64
}
