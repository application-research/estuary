package shuttlepreferencemgr

import (
	"context"
	"sort"

	"github.com/application-research/estuary/model"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type ShuttlePreferenceManager struct {
	DB          *gorm.DB
	MaxPriority int
}

type StorageProviderShuttlePing struct {
	StorageMinerID uint
	pings          []ShuttlePing
}

type ShuttlePing struct {
	ShuttleID uint
	ping      float64
}

func NewShuttlePreferenceManager(db *gorm.DB, maxPriority int) (*ShuttlePreferenceManager, error) {
	spref := &ShuttlePreferenceManager{
		DB:          db,
		MaxPriority: maxPriority,
	}

	return spref, nil
}

// Get SPs and priority in reference to a given shuttle
func (spm *ShuttlePreferenceManager) Query(sourceShuttle string) ([]model.ShuttlePreference, error) {
	var spl []model.ShuttlePreference

	if err := spm.DB.Find(&spl, "ShuttleID = ?", sourceShuttle).Error; err != nil {
		return nil, err
	}

	return spl, nil
}

// Creates or update SP/Shuttle preference records associated with the provided SP ids
func (spm *ShuttlePreferenceManager) PostSP(ctx context.Context, pings StorageProviderShuttlePing) error {

	var toCreate []model.ShuttlePreference
	sorted := sortShuttles(pings.pings)

	// Create entries for each SP/Shuttle Priority mapping
	for pri, shuttle := range sorted {
		// Only create records for this max priority #
		if pri == spm.MaxPriority {
			break
		}
		toCreate = append(toCreate, model.ShuttlePreference{
			StorageMinerID: pings.StorageMinerID,
			ShuttleID:      shuttle.ShuttleID,
			Priority:       uint(pri),
		})
	}

	// Save entry - Upsert -> Update existing entries (if SP's shuttle priority changes)
	spm.DB.Create(&toCreate).Clauses(clause.OnConflict{UpdateAll: true})
	return nil
}

// Sorts a list of shuttles in increasing order of their ping
func sortShuttles(m []ShuttlePing) []ShuttlePing {
	sort.Slice(m, func(i, j int) bool {
		return m[i].ping < m[j].ping
	})

	return m
}
