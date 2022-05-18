package server

import (
	"context"
	"fmt"

	contentmanager "github.com/application-research/estuary/server/contentmanager"
	"github.com/application-research/estuary/util"
	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-fil-markets/retrievalmarket"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

func (s *Server) retrievalAsksForContent(ctx context.Context, contid uint) (map[address.Address]*retrievalmarket.QueryResponse, error) {
	ctx, span := s.Tracer.Start(ctx, "retrievalAsksForContent", trace.WithAttributes(
		attribute.Int("content", int(contid)),
	))
	defer span.End()

	var content util.Content
	if err := s.DB.First(&content, "id = ?", contid).Error; err != nil {
		return nil, err
	}

	var deals []contentmanager.ContentDeal
	if err := s.DB.Find(&deals, "content = ? and deal_id > 0", contid).Error; err != nil {
		return nil, err
	}

	fmt.Printf("looking at %d deals for content: %s\n", len(deals), content.Cid.CID)
	out := make(map[address.Address]*retrievalmarket.QueryResponse)
	for _, d := range deals {
		maddr, err := d.MinerAddr()
		if err != nil {
			return nil, err
		}

		resp, err := s.FilClient.RetrievalQuery(ctx, maddr, content.Cid.CID)
		if err != nil {
			s.CM.RecordRetrievalFailure(&util.RetrievalFailureRecord{
				Miner:   maddr.String(),
				Phase:   "query",
				Message: err.Error(),
				Content: content.ID,
				Cid:     content.Cid,
			})
			log.Errorf("failed to query miner %s: %s", maddr, err)
			continue
		}

		out[maddr] = resp
	}

	return out, nil
}

func (s *Server) retrieveContent(ctx context.Context, contid uint) error {
	ctx, span := s.Tracer.Start(ctx, "retrieveContent", trace.WithAttributes(
		attribute.Int("content", int(contid)),
	))
	defer span.End()

	content, err := s.CM.GetContent(contid)
	if err != nil {
		return err
	}

	asks, err := s.retrievalAsksForContent(ctx, contid)
	if err != nil {
		return err
	}

	if len(asks) == 0 {
		return fmt.Errorf("no retrieval asks for content")
	}

	for m, ask := range asks {
		if err := s.CM.TryRetrieve(ctx, m, content.Cid.CID, ask); err != nil {
			log.Errorw("failed to retrieve content", "miner", m, "content", content.Cid.CID, "err", err)
			s.CM.RecordRetrievalFailure(&util.RetrievalFailureRecord{
				Miner:   m.String(),
				Phase:   "retrieval",
				Message: err.Error(),
				Content: contid,
				Cid:     content.Cid,
			})
			continue
		}

		return nil
	}

	return nil
}
