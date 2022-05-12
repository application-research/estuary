package constants

import (
	"time"

	"github.com/filecoin-project/go-state-types/abi"
)

const DefaultReplication = 6

const DefaultContentSizeLimit = 34_000_000_000

// Making default deal duration be three weeks less than the maximum to ensure
// miners who start their deals early dont run into issues
const DealDuration = 1555200 - (2880 * 21)

// 90% of the unpadded data size for a 4GB piece
// the 10% gap is to accommodate car file packing overhead, can probably do this better
var IndividualDealThreshold = (abi.PaddedPieceSize(4<<30).Unpadded() * 9) / 10

var StagingZoneSizeLimit = (abi.PaddedPieceSize(16<<30).Unpadded() * 9) / 10

// amount of time a staging zone will remain open before we aggregate it into a piece of content
const MaxStagingZoneLifetime = time.Hour * 8

// maximum amount of time a piece of content will go without either being aggregated or having a deal made for it
const MaxContentAge = time.Hour * 24 * 7

// staging zones will remain open for at least this long after the last piece of content is added to them (unless they are full)
const StagingZoneKeepalive = time.Minute * 40

const MinDealSize = 256 << 20

const MaxBucketItems = 10000
