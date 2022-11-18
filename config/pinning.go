package config

import "time"

type Pinning struct {
	RetryWorker RetryWorker `json:"retry_worker"`
}

type RetryWorker struct {
	Interval               time.Duration `json:"interval"`
	BatchSelectionLimit    int           `json:"batch_limit"`
	BatchSelectionDuration time.Duration `json:"batch_duration"`
}
