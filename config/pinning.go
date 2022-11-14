package config

import "time"

type Pinning struct {
	RetryWorker RetryWorker `json:"retry_worker"`
}

type RetryWorker struct {
	Interval   time.Duration `json:"interval"`
	BatchLimit int           `json:"batch_limit"`
}
