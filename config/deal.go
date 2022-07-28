package config

import "github.com/filecoin-project/go-state-types/abi"

type Deal struct {
	FailOnTransferFailure bool           `json:"fail_on_transfer_failure"`
	Disabled              bool           `json:"disabled"`
	Verified              bool           `json:"verified"`
	Duration              abi.ChainEpoch `json:"duration"`
}
