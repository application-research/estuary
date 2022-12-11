package shuttle

import (
	"context"

	rcpevent "github.com/application-research/estuary/shuttle/rpc/event"
	"github.com/application-research/estuary/util"
	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p/core/peer"
)

func (m *manager) ConsolidateContent(ctx context.Context, loc string, contents []util.Content) error {
	m.log.Debugf("attempting to send consolidate content cmd to %s", loc)
	tc := &rcpevent.TakeContent{}
	for _, c := range contents {
		prs := make([]*peer.AddrInfo, 0)

		pr := m.AddrInfo(c.Location)
		if pr != nil {
			prs = append(prs, pr)
		}

		if pr == nil {
			m.log.Warnf("no addr info for node: %s", loc)
		}

		tc.Contents = append(tc.Contents, rcpevent.ContentFetch{
			ID:     c.ID,
			Cid:    c.Cid.CID,
			UserID: c.UserID,
			Peers:  prs,
		})
	}

	return m.sendRPCMessage(ctx, loc, &rcpevent.Command{
		Op: rcpevent.CMD_TakeContent,
		Params: rcpevent.CmdParams{
			TakeContent: tc,
		},
	})
}

func (m *manager) PinContent(ctx context.Context, loc string, cont util.Content, origins []*peer.AddrInfo) error {
	return m.sendRPCMessage(ctx, loc, &rcpevent.Command{
		Op: rcpevent.CMD_AddPin,
		Params: rcpevent.CmdParams{
			AddPin: &rcpevent.AddPin{
				DBID:   cont.ID,
				UserId: cont.UserID,
				Cid:    cont.Cid.CID,
				Peers:  origins,
			},
		},
	})
}

func (m *manager) CommPContent(ctx context.Context, loc string, data cid.Cid) error {
	return m.sendRPCMessage(ctx, loc, &rcpevent.Command{
		Op: rcpevent.CMD_ComputeCommP,
		Params: rcpevent.CmdParams{
			ComputeCommP: &rcpevent.ComputeCommP{
				Data: data,
			},
		},
	})
}

func (m *manager) UnpinContent(ctx context.Context, loc string, conts []uint) error {
	return m.sendRPCMessage(ctx, loc, &rcpevent.Command{
		Op: rcpevent.CMD_UnpinContent,
		Params: rcpevent.CmdParams{
			UnpinContent: &rcpevent.UnpinContent{
				Contents: conts,
			},
		},
	})
}

func (m *manager) AggregateContent(ctx context.Context, loc string, zone util.Content, zoneContents []util.Content) error {
	var aggrConts []rcpevent.AggregateContent
	for _, c := range zoneContents {
		aggrConts = append(aggrConts, rcpevent.AggregateContent{ID: c.ID, Name: c.Name, CID: c.Cid.CID})
	}

	return m.sendRPCMessage(ctx, loc, &rcpevent.Command{
		Op: rcpevent.CMD_AggregateContent,
		Params: rcpevent.CmdParams{
			AggregateContent: &rcpevent.AggregateContents{
				DBID:     zone.ID,
				UserID:   zone.UserID,
				Contents: aggrConts,
			},
		},
	})
}

func (m *manager) SplitContent(ctx context.Context, loc string, cont uint, size int64) error {
	return m.sendRPCMessage(ctx, loc, &rcpevent.Command{
		Op: rcpevent.CMD_SplitContent,
		Params: rcpevent.CmdParams{
			SplitContent: &rcpevent.SplitContent{
				Content: cont,
				Size:    size,
			},
		},
	})
}
