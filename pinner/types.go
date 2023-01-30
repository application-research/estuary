package pinner

import (
	"time"

	"github.com/application-research/estuary/pinner/status"
	"github.com/application-research/estuary/util"
)

type IpfsPin struct {
	CID     string                 `json:"cid"`
	Name    string                 `json:"name"`
	Origins []string               `json:"origins"`
	Meta    map[string]interface{} `json:"meta"`
}

type IpfsPinStatusResponse struct {
	RequestID string                 `json:"requestid"`
	Status    status.PinningStatus   `json:"status"`
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
