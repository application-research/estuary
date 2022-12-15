package config

import "time"

// MinSize - minimum staging bucket size before it can be aggregated
// MaxSize - maximum staging bucket size before it can be aggregated
// AggregateInterval - interval to aggregate staging contents
type StagingBucket struct {
	Enabled           bool          `json:"enabled"`
	AggregateInterval time.Duration `json:"aggregate_interval"`
	CreationInterval  time.Duration `json:"creation_interval"`
}
