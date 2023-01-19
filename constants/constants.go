package constants

import (
	"regexp"
	"time"

	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/lotus/chain/types"
)

const CacheSize = 256 * 1024 * 1024 // 256MB
const CacheDuration = time.Second * 10
const CachePurgeEveryDuration = time.Minute * 5

const ExtendedCacheSize = 16 * 1024 * 1024 // 16MB
const ExtendedCacheDuration = time.Minute * 60
const ExtendedCachePurgeEveryDuration = time.Minute * 120

const ContentLocationLocal = "local"
const TopMinerSel = 15
const MinSafeDealLifetime = 2880 * 21 // three weeks

// DealDuration Making default deal duration be three weeks less than the maximum to ensure
// miners who start their deals early don't run into issues
const DealDuration = 1555200 - (2880 * 21)

// MinDealContentSize 1 GB
const MinDealContentSize = int64(1_100_000_000)

// MaxDealContentSize 31.66 GB
const MaxDealContentSize = int64(34_000_000_000)

// how many contents to include per advertisement for autoretrieve
const AutoretrieveProviderBatchSize = uint64(25000)

const DefaultIndexerURL = "https://cid.contact"

const TokenExpiryDurationAdmin = time.Hour * 24 * 365           // 1 year
const TokenExpiryDurationRegister = time.Hour * 24 * 7          // 1 week
const TokenExpiryDurationLogin = time.Hour * 24 * 30            // 30 days
const TokenExpiryDurationDefault = time.Hour * 24 * 30          // 30 days
const TokenExpiryDurationPermanent = time.Hour * 24 * 365 * 100 // 100 years

var AdminUsernameAlphanumericRegex = regexp.MustCompile(`^[A-Za-z\d]{1,32}$`)

func IsAdminUsernameValid(username string) bool {
	return AdminUsernameAlphanumericRegex.MatchString(username)
}

var AdminPasswordLengthAndAlphanumericRegex = regexp.MustCompile(`^[A-Za-z\d]{8,}$`)
var AdminPasswordContainsAlphaRegex = regexp.MustCompile(`[A-Za-z]`)
var AdminPasswordContainsNumericRegex = regexp.MustCompile(`\d`)

var AdminPasswordRegexes = []*regexp.Regexp{
	AdminPasswordLengthAndAlphanumericRegex,
	AdminPasswordContainsAlphaRegex,
	AdminPasswordContainsNumericRegex,
}

func IsAdminPasswordValid(password string) bool {
	for _, regex := range AdminPasswordRegexes {
		ok := regex.MatchString(password)
		if !ok {
			return false
		}
	}
	return true
}

var DealMaxPrice abi.TokenAmount
var VerifiedDealMaxPrice = abi.NewTokenAmount(0)

func init() {
	max, err := types.ParseFIL("0.00000003")
	if err != nil {
		panic(err)
	}
	DealMaxPrice = abi.TokenAmount(max)
}
