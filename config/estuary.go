package config

type Estuary struct {
	Database       string
	StagingData    string
	DataDir        string
	ApiListen      string
	AutoRetrieve   bool
	LightstepToken string
	Hostname       string
	Node           Config
	Jaeger         JaegerConfig
	Deals          DealsConfig
	Content        ContentConfig
	LowMem         bool
	Replication    int
	Logging        LoggingConfig
}

func (cfg *Estuary) Load(filename string) error {
	return load(cfg, filename)
}

// save writes the config from `cfg` into `filename`.
func (cfg *Estuary) Save(filename string) error {
	return save(cfg, filename)
}
