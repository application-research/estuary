package config

import "time"

// MaxLifeTime - amount of time a staging zone will remain open before we aggregate it into a piece of content
// MaxContentAge - maximum amount of time a piece of content will go without either being aggregated or having a deal made for it
// MinSize - minimum staging bucket size before it can be aggregated
// MaxSize - maximum staging bucket size before it can be aggregated
// KeepAlive - staging zones will remain open for at least this long after the last piece of content is added to them (unless they are full)
// MaxItems - max number of items a bucket can hold before it is aggregated
// MinDealSize - minimum deal size that bucket must meet before it is ever considered for aggregation
// AggregateInterval - interval to aggregate staging contents
type StagingBucket struct {
	Enabled                 bool          `json:"enabled"`
	MinSize                 int64         `json:"min_size"`
	MaxSize                 int64         `json:"max_size"`
	MinDealSize             int64         `json:"min_deal_size"`
	IndividualDealThreshold int64         `json:"individual_deal_threshold"`
	MaxItems                int           `json:"max_items"`
	MaxContentAge           time.Duration `json:"max_content_age"`
	KeepAlive               time.Duration `json:"keep_alive"`
	MaxLifeTime             time.Duration `json:"max_life_time"`
	AggregateInterval       time.Duration `json:"aggregate_interval"`
}
