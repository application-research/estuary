//go:build calibnet
// +build calibnet

package build

import (
	"github.com/filecoin-project/go-address"
)

// Three miners with most power as of 2021-09-17
var calibnetMinerStrs = []string{
	"t03112",
	"t03149",
	"t01247",
}

func init() {
	SetAddressNetwork(address.Testnet)
	SetDefaultMiners(calibnetMinerStrs)
}
