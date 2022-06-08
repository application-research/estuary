package config

import "time"

type FilClient struct {
	EventRateLimiter EventRateLimiter `json:"event_rate_limiter"`
}

type EventRateLimiter struct {
	TTL       time.Duration `json:"ttl"`
	CacheSize uint          `json:"cache_size"`
}
