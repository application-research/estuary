package util

import (
	"github.com/application-research/filclient"
	datatransfer "github.com/filecoin-project/go-data-transfer"
	"github.com/ipfs/go-cid"
)

func TransferTerminated(st *filclient.ChannelState) bool {
	switch st.Status {
	case datatransfer.Cancelled,
		datatransfer.Failed,
		datatransfer.Completed:

		return true
	default:
		return false
	}

}

func ParseDealLabel(s string) (cid.Cid, error) {
	return cid.Decode(s)
}
