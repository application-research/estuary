package config

import (
	"github.com/application-research/filclient"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/big"
	"github.com/libp2p/go-libp2p/core/protocol"
)

const (
	cliDealProtocolv110 = "v110"
	cliDealProtocolv120 = "v120"
)

var DealProtocolsVersionsMap = map[string]protocol.ID{
	cliDealProtocolv110: filclient.DealProtocolv110,
	cliDealProtocolv120: filclient.DealProtocolv120,
}

type Deal struct {
	FailOnTransferFailure        bool                 `json:"fail_on_transfer_failure"`
	IsDisabled                   bool                 `json:"disabled"`
	IsVerified                   bool                 `json:"verified"`
	RemoveUnsealed               bool                 `json:"remove_unsealed"`
	Duration                     abi.ChainEpoch       `json:"duration"`
	EnabledDealProtocolsVersions map[protocol.ID]bool `json:"enabled_deal_protocol_versions"`
	MaxVerifiedPrice             big.Int              `json:"max_verified_price"`
	MaxPrice                     big.Int              `json:"max_price"`
}
