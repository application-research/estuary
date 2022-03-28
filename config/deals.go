package config

type DealsConfig struct {
	FailOnTransferFailure bool `json:",omitempty"`
	Disable               bool `json:",omitempty"`
	NoStorageCron         bool `json:",omitempty"`
}
