package build

import (
	"github.com/filecoin-project/go-address"
)

func SetAddressNetwork(n address.Network) {
	address.CurrentNetwork = n
}

var DefaultMiners []address.Address

func SetDefaultMiners(minerStrs []string) {
	for _, s := range minerStrs {
		a, err := address.NewFromString(s)
		if err != nil {
			panic(err)
		}

		DefaultMiners = append(DefaultMiners, a)
	}
}
