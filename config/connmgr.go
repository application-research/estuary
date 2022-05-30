package config

type ConnectionManager struct {
	HighWater int `json:"high_water"`
	LowWater  int `json:"low_water"`
}
