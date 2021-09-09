package main

import (
	"context"
	"fmt"

	"github.com/application-research/estuary/drpc"
	"github.com/application-research/estuary/util"
	"github.com/application-research/filclient"
	"github.com/application-research/filclient/retrievehelper"
	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-fil-markets/retrievalmarket"
	"github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// NOTE: Copy pasted from the content manager in the top level estuary code
// At some point we should refactor and clean this up

type retrievalProgress struct {
	wait   chan struct{}
	endErr error
}

func (s *Shuttle) retrieveContent(ctx context.Context, contentToFetch uint, root cid.Cid, deals []drpc.StorageDeal) error {
	ctx, span := Tracer.Start(ctx, "retrieveContent", trace.WithAttributes(
		attribute.Int("content", int(contentToFetch)),
	))
	defer span.End()

	s.retrLk.Lock()
	prog, ok := s.retrievalsInProgress[contentToFetch]
	if !ok {
		prog = &retrievalProgress{
			wait: make(chan struct{}),
		}
		s.retrievalsInProgress[contentToFetch] = prog
	}
	s.retrLk.Unlock()

	if ok {
		select {
		case <-prog.wait:
			return prog.endErr
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	defer func() {
		s.retrLk.Lock()
		delete(s.retrievalsInProgress, contentToFetch)
		s.retrLk.Unlock()

		close(prog.wait)
	}()

	if err := s.runRetrieval(ctx, contentToFetch, deals, root, nil); err != nil {
		prog.endErr = err
		return err
	}

	return nil
}

func (s *Shuttle) runRetrieval(ctx context.Context, contentToFetch uint, deals []drpc.StorageDeal, root cid.Cid, sel ipld.Node) error {
	ctx, span := Tracer.Start(ctx, "runRetrieval")
	defer span.End()

	var pin Pin
	if err := s.DB.First(&pin, "content = ?", contentToFetch).Error; err != nil {
		return err
	}

	for _, deal := range deals {
		log.Infow("attempting retrieval deal", "content", contentToFetch, "miner", deal.Miner, "selector", sel != nil)

		ask, err := s.Filc.RetrievalQuery(ctx, deal.Miner, root)
		if err != nil {
			span.RecordError(err)

			log.Errorw("failed to query retrieval", "miner", deal.Miner, "content", root, "err", err)
			s.recordRetrievalFailure(&util.RetrievalFailureRecord{
				Miner:   deal.Miner.String(),
				Phase:   "query",
				Message: err.Error(),
				Content: contentToFetch,
				Cid:     util.DbCID{root},
			})
			continue
		}
		log.Infow("got retrieval ask", "content", contentToFetch, "miner", deal.Miner, "ask", ask)

		if err := s.tryRetrieve(ctx, deal.Miner, root, ask, sel); err != nil {
			span.RecordError(err)
			log.Errorw("failed to retrieve content", "miner", deal.Miner, "content", root, "err", err)
			s.recordRetrievalFailure(&util.RetrievalFailureRecord{
				Miner:   deal.Miner.String(),
				Phase:   "retrieval",
				Message: err.Error(),
				Content: contentToFetch,
				Cid:     util.DbCID{root},
			})
			continue
		}

		// success
		return nil
	}

	return fmt.Errorf("failed to retrieve with any miner we have deals with")
}

func (s *Shuttle) recordRetrievalFailure(rec *util.RetrievalFailureRecord) {

}

func (s *Shuttle) recordRetrievalSuccess(c cid.Cid, maddr address.Address, stats *filclient.RetrievalStats) {

}

func (s *Shuttle) tryRetrieve(ctx context.Context, maddr address.Address, c cid.Cid, ask *retrievalmarket.QueryResponse, sel ipld.Node) error {

	proposal, err := retrievehelper.RetrievalProposalForAsk(ask, c, sel)
	if err != nil {
		return err
	}

	stats, err := s.Filc.RetrieveContent(ctx, maddr, proposal)
	if err != nil {
		return err
	}

	s.recordRetrievalSuccess(c, maddr, stats)
	return nil
}
