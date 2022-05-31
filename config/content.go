package config

type Content struct {
	DisableLocalAdding  bool `json:"disable_local_adding"`
	DisableGlobalAdding bool `json:"disable_global_adding"` // not valid for shuttle
}
