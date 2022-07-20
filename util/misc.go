package util

import (
	"github.com/application-research/filclient"
	datatransfer "github.com/filecoin-project/go-data-transfer"
	"github.com/ipfs/go-cid"
	ipld "github.com/ipfs/go-ipld-format"
	"github.com/multiformats/go-multihash"
	"net/http"
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

func FilterUnwalkableLinks(links []*ipld.Link) []*ipld.Link {
	out := make([]*ipld.Link, 0, len(links))

	for _, l := range links {
		if CidIsUnwalkable(l.Cid) {
			continue
		}
		out = append(out, l)
	}

	return out
}

func CidIsUnwalkable(c cid.Cid) bool {
	pref := c.Prefix()
	if pref.MhType == multihash.IDENTITY {
		return true
	}

	if pref.Codec == cid.FilCommitmentSealed || pref.Codec == cid.FilCommitmentUnsealed {
		return true
	}

	return false
}

func ErrorIfContentAddingDisabled(isContentAddingDisabled bool) error {
	if isContentAddingDisabled {
		return HttpError{
			Code:    http.StatusBadRequest,
			Reason:  ERR_CONTENT_ADDING_DISABLED,
			Details: "uploading content to this node is not allowed at the moment",
		}
	}
	return nil
}
