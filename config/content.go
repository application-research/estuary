package config

type ContentConfig struct {
	Disable       bool `json:",omitempty"`
	GlobalDisable bool `json:",omitempty"` // not valid for shuttle
}
