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
	*/
	PinningStatusPinning PinningStatus = "pinning"
	PinningStatusPinned  PinningStatus = "pinned"
	PinningStatusFailed  PinningStatus = "failed"
	PinningStatusQueued  PinningStatus = "queued"
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
	}
	return status
}
