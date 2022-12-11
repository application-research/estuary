package shuttle

import (
	"context"

	rcpevent "github.com/application-research/estuary/shuttle/rpc/event"
	"github.com/ipfs/go-cid"
)

func (m *manager) PrepareForDataRequest(ctx context.Context, loc string, dbid uint, authToken string, propCid cid.Cid, payloadCid cid.Cid, size uint64) error {
	return m.sendRPCMessage(ctx, loc, &rcpevent.Command{
		Op: rcpevent.CMD_PrepareForDataRequest,
		Params: rcpevent.CmdParams{
			PrepareForDataRequest: &rcpevent.PrepareForDataRequest{
				DealDBID:    dbid,
				AuthToken:   authToken,
				ProposalCid: propCid,
				PayloadCid:  payloadCid,
				Size:        size,
			},
		},
	})
}

func (m *manager) CleanupPreparedRequest(ctx context.Context, loc string, dbid uint, authToken string) error {
	return m.sendRPCMessage(ctx, loc, &rcpevent.Command{
		Op: rcpevent.CMD_CleanupPreparedRequest,
		Params: rcpevent.CmdParams{
			CleanupPreparedRequest: &rcpevent.CleanupPreparedRequest{
				DealDBID:  dbid,
				AuthToken: authToken,
			},
		},
	})
}
