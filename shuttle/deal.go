package shuttle

import (
	"context"

	"github.com/application-research/estuary/drpc"
	"github.com/ipfs/go-cid"
)

func (m *manager) PrepareForDataRequest(ctx context.Context, loc string, dbid uint, authToken string, propCid cid.Cid, payloadCid cid.Cid, size uint64) error {
	return m.sendRPCMessage(ctx, loc, &drpc.Command{
		Op: drpc.CMD_PrepareForDataRequest,
		Params: drpc.CmdParams{
			PrepareForDataRequest: &drpc.PrepareForDataRequest{
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
	return m.sendRPCMessage(ctx, loc, &drpc.Command{
		Op: drpc.CMD_CleanupPreparedRequest,
		Params: drpc.CmdParams{
			CleanupPreparedRequest: &drpc.CleanupPreparedRequest{
				DealDBID:  dbid,
				AuthToken: authToken,
			},
		},
	})
}
