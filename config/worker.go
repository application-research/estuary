package config

import "time"

// MinSize - minimum staging bucket size before it can be aggregated
// MaxSize - maximum staging bucket size before it can be aggregated
// AggregateInterval - interval to aggregate staging contents
type WorkerIntervals struct {
	StagingZoneInterval time.Duration `json:"staging_zone_interval"`
	CommpInterval       time.Duration `json:"commp_interval"`
	DealInterval        time.Duration `json:"deal_interval"`
	SplitInterval       time.Duration `json:"split_interval"`
}
