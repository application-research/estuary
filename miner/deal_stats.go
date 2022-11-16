package miner

import "github.com/filecoin-project/go-address"

type minerDealStats struct {
	Miner          address.Address `json:"miner"`
	TotalDeals     int             `json:"totalDeals"`
	ConfirmedDeals int             `json:"confirmedDeals"`
	FailedDeals    int             `json:"failedDeals"`
	DealFaults     int             `json:"dealFaults"`
}

func (mds *minerDealStats) SuccessRatio() float64 {
	return float64(mds.ConfirmedDeals) / float64(mds.TotalDeals)
}

// The comparison function that decides 'miner X is better than miner Y'
func (mds *minerDealStats) Better(o *minerDealStats) bool {
	return mds.SuccessRatio() > o.SuccessRatio()
}
