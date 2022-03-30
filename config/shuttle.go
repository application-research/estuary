package config

import "errors"

type EstuaryRemoteConfig struct {
	Api       string
	Handle    string
	AuthToken string
}

type Shuttle struct {
	Database         string
	StagingData      string
	DataDir          string
	ApiListen        string
	AutoRetrieve     bool
	Hostname         string
	Private          bool
	Dev              bool
	NoReloadPinQueue bool
	Node             Config
	Jaeger           JaegerConfig
	Content          ContentConfig
	Logging          LoggingConfig
	Estuary          EstuaryRemoteConfig
}

func (cfg *Shuttle) Load(filename string) error {
	return load(cfg, filename)
}

// save writes the config from `cfg` into `filename`.
func (cfg *Shuttle) Save(filename string) error {
	return save(cfg, filename)
}

func (cfg *Shuttle) Validate() error {
	if cfg.Estuary.AuthToken == "" {
		return errors.New("no auth-token configured or specified on command line")
	}

	if cfg.Estuary.Handle == "" {
		return errors.New("no handle configured or specified on command line")
	}
	return nil
}
