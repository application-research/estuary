package miner

import (
	"sort"
	"time"

	"github.com/application-research/estuary/model"
	"github.com/filecoin-project/go-address"
)

const minerListTTL = time.Minute

func (mgr *MinerManager) SortedMinerList() ([]address.Address, []*minerDealStats, error) {
	mgr.minerLk.Lock()
	defer mgr.minerLk.Unlock()
	if time.Since(mgr.lastComputed) < minerListTTL {
		return mgr.sortedMiners, mgr.rawData, nil
	}

	sml, err := mgr.ComputeSortedMinerList()
	if err != nil {
		return nil, nil, err
	}

	sortedAddrs := make([]address.Address, 0, len(sml))
	for _, m := range sml {
		sus, err := mgr.minerIsSuspended(m.Miner)
		if err != nil {
			return nil, nil, err
		}

		if !sus {
			sortedAddrs = append(sortedAddrs, m.Miner)
		}
	}

	mgr.rawData = sml
	mgr.lastComputed = time.Now()
	mgr.sortedMiners = sortedAddrs
	return sortedAddrs, sml, nil
}

func (mgr *MinerManager) minerIsSuspended(m address.Address) (bool, error) {
	var miner model.StorageMiner
	if err := mgr.DB.Find(&miner, "address = ?", m.String()).Error; err != nil {
		return false, err
	}
	return miner.Suspended, nil
}

func (mgr *MinerManager) ComputeSortedMinerList() ([]*minerDealStats, error) {
	var deals []model.ContentDeal
	if err := mgr.DB.Find(&deals).Error; err != nil {
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
