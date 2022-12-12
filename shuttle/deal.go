package shuttle

import (
	"context"

	rpcevent "github.com/application-research/estuary/shuttle/rpc/event"
	"github.com/ipfs/go-cid"
)

func (m *manager) PrepareForDataRequest(ctx context.Context, loc string, dbid uint, authToken string, propCid cid.Cid, payloadCid cid.Cid, size uint64) error {
	return m.sendRPCMessage(ctx, loc, &rpcevent.Command{
		Op: rpcevent.CMD_PrepareForDataRequest,
		Params: rpcevent.CmdParams{
			PrepareForDataRequest: &rpcevent.PrepareForDataRequest{
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
	return m.sendRPCMessage(ctx, loc, &rpcevent.Command{
		Op: rpcevent.CMD_CleanupPreparedRequest,
		Params: rpcevent.CmdParams{
			CleanupPreparedRequest: &rpcevent.CleanupPreparedRequest{
				DealDBID:  dbid,
				AuthToken: authToken,
			},
		},
	})
}
