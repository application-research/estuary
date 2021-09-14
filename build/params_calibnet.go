// +build calibnet

package build

import (
	"github.com/filecoin-project/go-address"
)

// TODO get miners sorted
var calibnetMinerStrs = []string{
	"t01000",
	"t01001",
	"t01002",
}

var defaultCalibnetDatabaseValue = "sqlite=estuary_calibnet.db"

func init() {
	SetAddressNetwork(address.Testnet)
	SetDefaultMiners(calibnetMinerStrs)
	SetDefaultDatabaseValue(defaultCalibnetDatabaseValue)
}
