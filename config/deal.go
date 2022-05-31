package config

type Deal struct {
	FailOnTransferFailure bool `json:"fail_on_transfer_failure"`
	Disable               bool `json:"disable"`
	Verified              bool `json:"verified"`
}
