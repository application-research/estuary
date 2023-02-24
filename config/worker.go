package config

import "time"

type WorkerIntervals struct {
	StagingZoneInterval time.Duration `json:"staging_zone_interval"`
	CommpInterval       time.Duration `json:"commp_interval"`
	DealInterval        time.Duration `json:"deal_interval"`
	SplitInterval       time.Duration `json:"split_interval"`
}
