package shuttle

import (
	"context"

	rpcevent "github.com/application-research/estuary/shuttle/rpc/event"
	"github.com/application-research/estuary/util"
	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p/core/peer"
)

func (m *manager) ConsolidateContent(ctx context.Context, loc string, contents []util.Content) error {
	m.log.Debugf("attempting to send consolidate content cmd to %s", loc)
	tc := &rpcevent.TakeContent{}
	for _, c := range contents {
		prs := make([]*peer.AddrInfo, 0)

		pr, err := m.AddrInfo(c.Location)
		if err != nil {
			continue
		}

		if pr != nil {
			prs = append(prs, pr)
		}

		if pr == nil {
			m.log.Warnf("no addr info for node: %s", loc)
		}

		tc.Contents = append(tc.Contents, rpcevent.ContentFetch{
			ID:     c.ID,
			Cid:    c.Cid.CID,
			UserID: c.UserID,
			Peers:  prs,
		})
	}

	return m.sendRPCMessage(ctx, loc, &rpcevent.Command{
		Op: rpcevent.CMD_TakeContent,
		Params: rpcevent.CmdParams{
			TakeContent: tc,
		},
	})
}

func (m *manager) PinContent(ctx context.Context, loc string, cont util.Content, origins []*peer.AddrInfo) error {
	return m.sendRPCMessage(ctx, loc, &rpcevent.Command{
		Op: rpcevent.CMD_AddPin,
		Params: rpcevent.CmdParams{
			AddPin: &rpcevent.AddPin{
				DBID:   cont.ID,
				UserId: cont.UserID,
				Cid:    cont.Cid.CID,
				Peers:  origins,
			},
		},
	})
}

func (m *manager) CommPContent(ctx context.Context, loc string, data cid.Cid) error {
	m.log.Infof("sending commp")
	return m.sendRPCMessage(ctx, loc, &rpcevent.Command{
		Op: rpcevent.CMD_ComputeCommP,
		Params: rpcevent.CmdParams{
			ComputeCommP: &rpcevent.ComputeCommP{
				Data: data,
			},
		},
	})
}

func (m *manager) UnpinContent(ctx context.Context, loc string, conts []uint) error {
	return m.sendRPCMessage(ctx, loc, &rpcevent.Command{
		Op: rpcevent.CMD_UnpinContent,
		Params: rpcevent.CmdParams{
			UnpinContent: &rpcevent.UnpinContent{
				Contents: conts,
			},
		},
	})
}

func (m *manager) AggregateContent(ctx context.Context, loc string, zone util.Content, zoneContents []util.Content) error {
	var aggrConts []rpcevent.AggregateContent
	for _, c := range zoneContents {
		aggrConts = append(aggrConts, rpcevent.AggregateContent{ID: c.ID, Name: c.Name, CID: c.Cid.CID})
	}

	return m.sendRPCMessage(ctx, loc, &rpcevent.Command{
		Op: rpcevent.CMD_AggregateContent,
		Params: rpcevent.CmdParams{
			AggregateContent: &rpcevent.AggregateContents{
				DBID:     zone.ID,
				UserID:   zone.UserID,
				Contents: aggrConts,
			},
		},
	})
}

func (m *manager) SplitContent(ctx context.Context, loc string, cont uint, size int64) error {
	return m.sendRPCMessage(ctx, loc, &rpcevent.Command{
		Op: rpcevent.CMD_SplitContent,
		Params: rpcevent.CmdParams{
			SplitContent: &rpcevent.SplitContent{
				Content: cont,
				Size:    size,
			},
		},
	})
}
