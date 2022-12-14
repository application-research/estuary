package types

import (
	"time"

	"github.com/application-research/estuary/util"
)

type PinningStatus string

const (
	/*
	   - queued     # pinning operation is waiting in the queue; additional info can be returned in info[status_details]
	   - pinning    # pinning in progress; additional info can be returned in info[status_details]
	   - pinned     # pinned successfully
	   - failed     # pinning service was unable to finish pinning operation; additional info can be found in info[status_details]
	   - offloaded  # content has been offloaded
	*/
	PinningStatusPinning   PinningStatus = "pinning"
	PinningStatusPinned    PinningStatus = "pinned"
	PinningStatusFailed    PinningStatus = "failed"
	PinningStatusQueued    PinningStatus = "queued"
	PinningStatusOffloaded PinningStatus = "offloaded"
)

type IpfsPin struct {
	CID     string                 `json:"cid"`
	Name    string                 `json:"name"`
	Origins []string               `json:"origins"`
	Meta    map[string]interface{} `json:"meta"`
}

type IpfsPinStatusResponse struct {
	RequestID string                 `json:"requestid"`
	Status    PinningStatus          `json:"status"`
	Created   time.Time              `json:"created"`
	Delegates []string               `json:"delegates"`
	Info      map[string]interface{} `json:"info"`
	Pin       IpfsPin                `json:"pin"`
	Content   util.Content           `json:"content"`
}

type IpfsListPinStatusResponse struct {
	Count   int                      `json:"count"`
	Results []*IpfsPinStatusResponse `json:"results"`
}

func GetContentPinningStatus(cont util.Content) PinningStatus {
	status := PinningStatusQueued
	if cont.Active {
		status = PinningStatusPinned
	} else if cont.Failed {
		status = PinningStatusFailed
	} else if cont.Pinning {
		status = PinningStatusPinning
	} else if cont.Offloaded {
		status = PinningStatusOffloaded
	}
	return status
}
