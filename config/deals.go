package config

type DealConfig struct {
	FailOnTransferFailure bool `json:",omitempty"`
	Disable               bool `json:",omitempty"`
}
