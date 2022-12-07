package shuttlepreference

import (
	"context"
	"sort"

	"github.com/application-research/estuary/model"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type SPPreferenceManager struct {
	DB *gorm.DB
}

func NewSPPreferenceManager(db *gorm.DB) (*SPPreferenceManager, error) {
	spref := &SPPreferenceManager{
		DB: db,
	}

	return spref, nil
}

// Get SPs and priority in reference to a given shuttle
func (spm *SPPreferenceManager) Query(sourceShuttle string) ([]model.ShuttlePreference, error) {
	var spl []model.ShuttlePreference

	if err := spm.DB.Find(&spl, "ShuttleID = ?", sourceShuttle).Error; err != nil {
		return nil, err
	}

	return spl, nil
}

type StorageProviderShuttlePing struct {
	StorageMinerID uint
	pings          []ShuttlePing
}

type ShuttlePing struct {
	ShuttleID uint
	ping      float64
}

// Creates or update SP/Shuttle preference records associated with the provided SP ids
func (spm *SPPreferenceManager) PostSP(ctx context.Context, pings StorageProviderShuttlePing) error {
	// Only create records for this max priority #
	// TODO: Extract to config
	maxPriorityToConsider := 2

	var toCreate []model.ShuttlePreference
	sorted := sortShuttles(pings.pings)

	// Create entries for each SP/Shuttle Priority mapping
	for pri, shuttle := range sorted {
		if pri == maxPriorityToConsider {
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
