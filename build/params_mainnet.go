// +build !calibnet

package build

import (
	"github.com/filecoin-project/go-address"
)

// miners from minerX spreadsheet
var mainnetMinerStrs = []string{
	"f02620",
	"f023971",
	"f022142",
	"f019551",
	"f01240",
	"f01247",
	"f01278",
	"f071624",
	"f0135078",
	"f022352",
	"f014768",
	"f022163",
	"f09848",
	"f02576",
	"f02606",
	"f019041",
	"f010617",
	"f023467",
	"f01276",
	"f02401",
	"f02387",
	"f019104",
	"f099608",
	"f062353",
	"f07998",
	"f019362",
	"f019100",
	"f014409",
	"f066596",
	"f01234",
	"f058369",
	"f08399",
	"f021716",
	"f010479",
	"f08403",
	"f01277",
	"f015927",
}

var defaultMainnetDatabaseValue = "sqlite=estuary.db"

func init() {
	SetAddressNetwork(address.Mainnet)
	SetDefaultMiners(mainnetMinerStrs)
	SetDefaultDatabaseValue(defaultMainnetDatabaseValue)
}
