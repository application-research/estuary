package server

import (
	"sort"
	"time"

	"github.com/application-research/estuary/util"
	"github.com/filecoin-project/go-address"
	"gorm.io/gorm"
)

const minerListTTL = time.Minute

type StorageMiner struct {
	gorm.Model
	Address         util.DbAddr `gorm:"unique"`
	Suspended       bool
	SuspendedReason string
	Name            string
	Version         string
	Location        string
	Owner           uint
}

func (cm *ContentManager) SortedMinerList() ([]address.Address, []*minerDealStats, error) {
	cm.minerLk.Lock()
	defer cm.minerLk.Unlock()
	if time.Since(cm.lastComputed) < minerListTTL {
		return cm.sortedMiners, cm.rawData, nil
	}

	sml, err := cm.ComputeSortedMinerList()
	if err != nil {
		return nil, nil, err
	}

	sortedAddrs := make([]address.Address, 0, len(sml))
	for _, m := range sml {
		sus, err := cm.minerIsSuspended(m.Miner)
		if err != nil {
			return nil, nil, err
		}

		if !sus {
			sortedAddrs = append(sortedAddrs, m.Miner)
		}
	}

	cm.rawData = sml
	cm.lastComputed = time.Now()
	cm.sortedMiners = sortedAddrs
	return sortedAddrs, sml, nil
}

func (cm *ContentManager) minerIsSuspended(m address.Address) (bool, error) {
	var miner StorageMiner
	if err := cm.DB.Find(&miner, "address = ?", m.String()).Error; err != nil {
		return false, err
	}

	return miner.Suspended, nil
}

type minerDealStats struct {
	Miner address.Address `json:"miner"`

	TotalDeals     int `json:"totalDeals"`
	ConfirmedDeals int `json:"confirmedDeals"`
	FailedDeals    int `json:"failedDeals"`
	DealFaults     int `json:"dealFaults"`
}

func (mds *minerDealStats) SuccessRatio() float64 {
	return float64(mds.ConfirmedDeals) / float64(mds.TotalDeals)
}

// The comparison function that decides 'miner X is better than miner Y'
func (mds *minerDealStats) Better(o *minerDealStats) bool {
	return mds.SuccessRatio() > o.SuccessRatio()
}

func (cm *ContentManager) ComputeSortedMinerList() ([]*minerDealStats, error) {
	var deals []ContentDeal
	if err := cm.DB.Find(&deals).Error; err != nil {
		return nil, err
	}

	stats := make(map[address.Address]*minerDealStats)
	for _, d := range deals {
		maddr, err := d.MinerAddr()
		if err != nil {
			return nil, err
		}

		st, ok := stats[maddr]
		if !ok {
			st = &minerDealStats{
				Miner: maddr,
			}
			stats[maddr] = st
		}

		st.TotalDeals++
		if d.DealID > 0 {
			if d.Failed {
				st.DealFaults++
			} else {
				st.ConfirmedDeals++
			}
		} else if d.Failed {
			st.FailedDeals++
		} else {
			// in progress
		}
	}

	minerStatsArr := make([]*minerDealStats, 0, len(stats))
	for _, st := range stats {
		minerStatsArr = append(minerStatsArr, st)
	}

	sort.Slice(minerStatsArr, func(i, j int) bool {
		return minerStatsArr[i].Better(minerStatsArr[j])
	})

	return minerStatsArr, nil
}
