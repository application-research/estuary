package splocator

import (
	"context"
	"log"

	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/lotus/api"
	"github.com/filecoin-project/lotus/chain/types"
	"github.com/multiformats/go-multiaddr"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type SpLocator struct {
	DB  *gorm.DB
	Api api.Gateway
}

type SP struct {
	ID             address.Address
	MultiAddresses []multiaddr.Multiaddr
}

type LocatedSP struct {
	gorm.Model
	ID             address.Address       `json:"id" gorm:"index"`
	MultiAddresses []multiaddr.Multiaddr `json:"address"`
	CountryName    string                `json:"country_name"`
	CountryCode    string                `json:"country_code"`
	Latitude       float64               `json:"latitude"`
	Longitude      float64               `json:"longitude"`
}

func NewSpLocator(db *gorm.DB, api api.Gateway) (*SpLocator, error) {
	ss := &SpLocator{
		DB:  db,
		Api: api,
	}

	return ss, nil
}

// Get SPs in a country. Returns all if country is empty string
func (ss *SpLocator) Query(country string) ([]LocatedSP, error) {
	var sps []LocatedSP
	if country != "" {
		// Query DB for all SP statistics in a given country code
		if err := ss.DB.Find(&sps, "country_code = ?", country).Error; err != nil {
			return nil, err
		}
	} else {
		// if unspecified, return all SPs
		if err := ss.DB.Find(&sps).Error; err != nil {
			return nil, err
		}
	}

	return sps, nil
}

// Creates or updates Storage Provider Selection records associated with the provided SP ids
func (ss *SpLocator) PostSP(ctx context.Context, spIDs []address.Address) {
	var sps []SP

	loc, err := getIpfsLocator(ctx)
	if err != nil {
		log.Println("error setting up ipfs-geoip lookups")
		return
	}

	for _, spID := range spIDs {
		sp := SP{
			ID: spID,
		}

		minfo, err := ss.Api.StateMinerInfo(ctx, spID, types.EmptyTSK)
		if err != nil {
			continue
		}

		for _, a := range minfo.Multiaddrs {
			ma, err := multiaddr.NewMultiaddrBytes(a)
			if err != nil {
				continue
			}
			sp.MultiAddresses = append(sp.MultiAddresses, ma)
		}

		sps = append(sps, sp)
	}

	// * No IPV6 Support yet
	located, err := LookupLocations(ctx, loc, sps)
	if err != nil {
		log.Println("error getting locations of sps")
		return
	}

	// Save entry - Upsert -> Update existing entries
	ss.DB.Create(&located).Clauses(clause.OnConflict{UpdateAll: true})
}

// Return a single Storage Provider's information
func (ss *SpLocator) GetSP(spid address.Address) (*LocatedSP, error) {

	var sp LocatedSP
	if err := ss.DB.Find(&sp, "ID = ?", spid).Error; err != nil {
		return nil, err
	}

	return &sp, nil
}
