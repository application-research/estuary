package config

type Content struct {
	DisableLocalAdding  bool  `json:"disable_local_adding"`
	DisableGlobalAdding bool  `json:"disable_global_adding"` // not valid for shuttle
	MinSize             int64 `json:"min_size"`
	MaxSize             int64 `json:"max_size"`
}
