package constants

import (
	"time"

	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/lotus/chain/types"
)

const ContentLocationLocal = "local"
const TopMinerSel = 15
const MinSafeDealLifetime = 2880 * 21 // three weeks

// DealDuration Making default deal duration be three weeks less than the maximum to ensure
// miners who start their deals early don't run into issues
const DealDuration = 1555200 - (2880 * 21)

// MinDealContentSize 3.6 GB
// 90% of the un-padded data size for a 4GB piece
// the 10% gap is to accommodate car file packing overhead, can probably do this better
const MinDealContentSize = int64((4 << 30) * 9 / 10)

// MaxDealContentSize 31.66 GB
const MaxDealContentSize = int64(34_000_000_000)

// how many contents to include per advertisement for autoretrieve
const AutoretrieveProviderBatchSize = uint(25000)

const DefaultIndexerURL = "https://cid.contact"

const TokenExpiryDurationAdmin = time.Hour * 24 * 365           // 1 year
const TokenExpiryDurationRegister = time.Hour * 24 * 7          // 1 week
const TokenExpiryDurationLogin = time.Hour * 24 * 30            // 30 days
const TokenExpiryDurationDefault = time.Hour * 24 * 30          // 30 days
const TokenExpiryDurationPermanent = time.Hour * 24 * 365 * 100 // 100 years

var DealMaxPrice abi.TokenAmount
var VerifiedDealMaxPrice = abi.NewTokenAmount(0)

func init() {
	max, err := types.ParseFIL("0.00000003")
	if err != nil {
		panic(err)
	}
	DealMaxPrice = abi.TokenAmount(max)
}
