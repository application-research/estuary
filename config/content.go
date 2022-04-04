package config

type Content struct {
	DisableLocalAdding  bool `json:",omitempty"`
	DisableGlobalAdding bool `json:",omitempty"` // not valid for shuttle
}
