package main

import (
	"context"
	"fmt"

	rpcevent "github.com/application-research/estuary/shuttle/rpc/event"
	"github.com/application-research/estuary/util"
	"github.com/application-research/filclient"
	"github.com/application-research/filclient/retrievehelper"
	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-fil-markets/retrievalmarket"
	"github.com/ipfs/go-blockservice"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-merkledag"
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

func (s *Shuttle) retrieveContent(ctx context.Context, req *rpcevent.RetrieveContent) error {
	ctx, span := s.Tracer.Start(ctx, "retrieveContent", trace.WithAttributes(
		attribute.Int("content", int(req.Content)),
	))
	defer span.End()

	s.retrLk.Lock()
	prog, ok := s.retrievalsInProgress[req.Content]
	if !ok {
		prog = &retrievalProgress{
			wait: make(chan struct{}),
		}
		s.retrievalsInProgress[req.Content] = prog
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
		delete(s.retrievalsInProgress, req.Content)
		s.retrLk.Unlock()

		close(prog.wait)
	}()

	if err := s.runRetrieval(ctx, req, nil); err != nil {
		prog.endErr = err
		return err
	}

	return nil
}

func (s *Shuttle) runRetrieval(ctx context.Context, req *rpcevent.RetrieveContent, sel ipld.Node) error {
	ctx, span := s.Tracer.Start(ctx, "runRetrieval")
	defer span.End()

	var pin Pin
	if err := s.DB.Find(&pin, "content = ?", req.Content).Error; err != nil {
		return err
	}

	// we already have this data locally
	if pin.ID > 0 {
		objects, err := s.objectsForPin(ctx, pin.ID)
		if err != nil {
			// weird case... probably should handle better?
			return fmt.Errorf("failed to get objects for pin: %w", err)
		}

		s.sendPinCompleteMessage(ctx, pin.Content, pin.Size, objects, req.Cid)
		return nil
	}

	var retrievedContent bool
	for _, deal := range req.Deals {
		log.Debugw("attempting retrieval deal", "content", req.Content, "miner", deal.Miner, "selector", sel != nil)

		ask, err := s.Filc.RetrievalQuery(ctx, deal.Miner, req.Cid)
		if err != nil {
			span.RecordError(err)

			log.Errorw("failed to query retrieval", "miner", deal.Miner, "content", req.Cid, "err", err)
			s.recordRetrievalFailure(&util.RetrievalFailureRecord{
				Miner:   deal.Miner.String(),
				Phase:   "query",
				Message: err.Error(),
				Content: req.Content,
				Cid:     util.DbCID{CID: req.Cid},
			})
			continue
		}
		log.Debugw("got retrieval ask", "content", req.Content, "miner", deal.Miner, "ask", ask)

		if err := s.tryRetrieve(ctx, deal.Miner, req.Cid, ask, sel); err != nil {
			span.RecordError(err)
			log.Errorw("failed to retrieve content", "miner", deal.Miner, "content", req.Cid, "err", err)
			s.recordRetrievalFailure(&util.RetrievalFailureRecord{
				Miner:   deal.Miner.String(),
				Phase:   "retrieval",
				Message: err.Error(),
				Content: req.Content,
				Cid:     util.DbCID{CID: req.Cid},
			})
			continue
		}

		retrievedContent = true
		break
	}

	if retrievedContent {
		newPin := &Pin{
			Content: req.Content,
			Cid:     util.DbCID{CID: req.Cid},
			UserID:  req.UserID,
			Active:  false,
			Pinning: true,
		}

		if err := s.DB.Create(newPin).Error; err != nil {
			return err
		}

		dserv := merkledag.NewDAGService(blockservice.New(s.Node.Blockstore, nil))
		totalSize, objects, err := s.addDatabaseTrackingToContent(ctx, req.Content, dserv, s.Node.Blockstore, req.Cid)
		if err != nil {
			log.Errorw("failed adding content to database after successful retrieval", "cont", req.Content, "err", err.Error())
			return err
		}

		s.sendPinCompleteMessage(ctx, req.Content, totalSize, objects, req.Cid)
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
