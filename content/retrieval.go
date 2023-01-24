package contentmgr

import (
	"context"
	"fmt"
	"math/rand"

	"github.com/application-research/estuary/constants"
	"github.com/application-research/estuary/model"
	"github.com/application-research/estuary/util"
	"github.com/application-research/filclient"
	"github.com/application-research/filclient/retrievehelper"
	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-fil-markets/retrievalmarket"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/xerrors"
)

func (m *manager) RecordRetrievalFailure(rfr *util.RetrievalFailureRecord) error {
	return m.db.Create(rfr).Error
}

func (m *manager) TryRetrieve(ctx context.Context, maddr address.Address, c cid.Cid, ask *retrievalmarket.QueryResponse) error {
	proposal, err := retrievehelper.RetrievalProposalForAsk(ask, c, nil)
	if err != nil {
		return err
	}

	stats, err := m.fc.RetrieveContent(ctx, maddr, proposal)
	if err != nil {
		return err
	}

	m.RecordRetrievalSuccess(c, maddr, stats)
	return nil
}

func (m *manager) RecordRetrievalSuccess(cc cid.Cid, ma address.Address, rstats *filclient.RetrievalStats) {
	if err := m.db.Create(&model.RetrievalSuccessRecord{
		Cid:          util.DbCID{CID: cc},
		Miner:        ma.String(),
		Peer:         rstats.Peer.String(),
		Size:         rstats.Size,
		DurationMs:   rstats.Duration.Milliseconds(),
		AverageSpeed: rstats.AverageSpeed,
		TotalPayment: rstats.TotalPayment.String(),
		NumPayments:  rstats.NumPayments,
		AskPrice:     rstats.AskPrice.String(),
	}).Error; err != nil {
		m.log.Errorf("failed to write retrieval success record: %s", err)
	}
}

func (m *manager) runRetrieval(ctx context.Context, contentToFetch uint64) error {
	ctx, span := m.tracer.Start(ctx, "runRetrieval")
	defer span.End()

	var content util.Content
	if err := m.db.First(&content, contentToFetch).Error; err != nil {
		return err
	}

	index := -1
	if content.AggregatedIn > 0 {
		rootContent := content.AggregatedIn
		ix, err := m.indexForAggregate(ctx, rootContent, contentToFetch)
		if err != nil {
			return err
		}
		index = ix
	}
	_ = index

	var deals []model.ContentDeal
	if err := m.db.Find(&deals, "content = ? and not failed", contentToFetch).Error; err != nil {
		return err
	}

	if len(deals) == 0 {
		return xerrors.Errorf("no active deals for content %d we are trying to retrieve", contentToFetch)
	}

	// TODO: probably need some better way to pick miners to retrieve from...
	perm := rand.Perm(len(deals))
	for _, i := range perm {
		deal := deals[i]

		maddr, err := deal.MinerAddr()
		if err != nil {
			m.log.Errorf("deal %d had bad miner address: %s", deal.ID, err)
			continue
		}

		m.log.Infow("attempting retrieval deal", "content", contentToFetch, "miner", maddr)

		ask, err := m.fc.RetrievalQuery(ctx, maddr, content.Cid.CID)
		if err != nil {
			span.RecordError(err)

			m.log.Errorw("failed to query retrieval", "miner", maddr, "content", content.Cid.CID, "err", err)
			if err := m.RecordRetrievalFailure(&util.RetrievalFailureRecord{
				Miner:   maddr.String(),
				Phase:   "query",
				Message: err.Error(),
				Content: content.ID,
				Cid:     content.Cid,
			}); err != nil {
				return xerrors.Errorf("failed to record deal failure: %w", err)
			}
			continue
		}
		m.log.Infow("got retrieval ask", "content", content, "miner", maddr, "ask", ask)

		if err := m.TryRetrieve(ctx, maddr, content.Cid.CID, ask); err != nil {
			span.RecordError(err)
			m.log.Errorw("failed to retrieve content", "miner", maddr, "content", content.Cid.CID, "err", err)
			if err := m.RecordRetrievalFailure(&util.RetrievalFailureRecord{
				Miner:   maddr.String(),
				Phase:   "retrieval",
				Message: err.Error(),
				Content: content.ID,
				Cid:     content.Cid,
			}); err != nil {
				return xerrors.Errorf("failed to record deal failure: %w", err)
			}
			continue
		}
		// success
		return nil
	}
	return fmt.Errorf("failed to retrieve with any miner we have deals with")
}

func (m *manager) sendRetrieveContentMessage(ctx context.Context, loc string, cont util.Content) error {
	return fmt.Errorf("not retrieving content yet until implementation is finished")
	/*
		var activeDeals []contentDeal
		if err := m.DB.Find(&activeDeals, "content = ? and not failed and deal_id > 0", cont.ID).Error; err != nil {
			return err
		}

		if len(activeDeals) == 0 {
			m.log.Errorf("attempted to retrieve content %d but have no active deals", cont.ID)
			return fmt.Errorf("no active deals for content %d, cannot retrieve", cont.ID)
		}

		var deals []drpc.StorageDeal
		for _, d := range activeDeals {
			ma, err := d.MinerAddr()
			if err != nil {
				m.log.Errorf("failed to parse miner addres for deal %d: %s", d.ID, err)
				continue
			}

			deals = append(deals, drpc.StorageDeal{
				Miner:  ma,
				DealID: d.DealID,
			})
		}

		return m.sendShuttleCommand(ctx, loc, &drpc.Command{
			Op: drpc.CMD_RetrieveContent,
			Params: drpc.CmdParams{
				RetrieveContent: &drpc.RetrieveContent{
					Content: cont.ID,
					Cid:     cont.Cid.CID,
					Deals:   deals,
				},
			},
		})
	*/
}

func (m *manager) retrieveContent(ctx context.Context, contentToFetch uint64) error {
	ctx, span := m.tracer.Start(ctx, "retrieveContent", trace.WithAttributes(
		attribute.Int("content", int(contentToFetch)),
	))
	defer span.End()

	m.retrLk.Lock()
	prog, ok := m.retrievalsInProgress[contentToFetch]
	if !ok {
		prog = &util.RetrievalProgress{
			Wait: make(chan struct{}),
		}
		m.retrievalsInProgress[contentToFetch] = prog
	}
	m.retrLk.Unlock()

	if ok {
		select {
		case <-prog.Wait:
			return prog.EndErr
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	defer func() {
		m.retrLk.Lock()
		delete(m.retrievalsInProgress, contentToFetch)
		m.retrLk.Unlock()

		close(prog.Wait)
	}()

	if err := m.runRetrieval(ctx, contentToFetch); err != nil {
		prog.EndErr = err
		return err
	}
	return nil
}

func (m *manager) RefreshContent(ctx context.Context, cont uint64) error {
	ctx, span := m.tracer.Start(ctx, "refreshContent")
	defer span.End()

	// TODO: this retrieval needs to mark all of its content as 'referenced'
	// until we can update its offloading status in the database
	var c util.Content
	if err := m.db.First(&c, "id = ?", cont).Error; err != nil {
		return err
	}

	loc, err := m.shuttleMgr.GetLocationForRetrieval(ctx, c)
	if err != nil {
		return err
	}
	m.log.Infof("refreshing content %d onto shuttle %s", cont, loc)

	switch loc {
	case constants.ContentLocationLocal:
		if err := m.retrieveContent(ctx, cont); err != nil {
			return err
		}

		if err := m.db.Model(&util.Content{}).Where("id = ?", cont).Update("offloaded", false).Error; err != nil {
			return err
		}

		if err := m.db.Model(&util.ObjRef{}).Where("content = ?", cont).Update("offloaded", 0).Error; err != nil {
			return err
		}
	default:
		return m.sendRetrieveContentMessage(ctx, loc, c)
	}
	return nil
}

func (m *manager) RefreshContentForCid(ctx context.Context, c cid.Cid) (blocks.Block, error) {
	ctx, span := m.tracer.Start(ctx, "refreshForCid", trace.WithAttributes(
		attribute.Stringer("cid", c),
	))
	defer span.End()

	var obj util.Object
	if err := m.db.First(&obj, "cid = ?", c.Bytes()).Error; err != nil {
		return nil, xerrors.Errorf("failed to get object from db: %s", err)
	}

	var refs []util.ObjRef
	if err := m.db.Find(&refs, "object = ?", obj.ID).Error; err != nil {
		return nil, err
	}

	var contentToFetch uint64
	switch len(refs) {
	case 0:
		return nil, xerrors.Errorf("have no object references for object %d in database", obj.ID)
	case 1:
		// easy case, fetch this thing.
		contentToFetch = refs[0].Content
	default:
		// have more than one reference for the same object. Need to pick one to retrieve

		// if one of the referenced contents has the requested cid as its root, then we should probably fetch that one

		var contents []util.Content
		if err := m.db.Find(&contents, "cid = ?", c.Bytes()).Error; err != nil {
			return nil, err
		}

		if len(contents) == 0 {
			// okay, this isnt anythings root cid. Just pick one I guess?
			contentToFetch = refs[0].Content
		} else {
			// good, this is a root cid, lets fetch that one.
			contentToFetch = contents[0].ID
		}
	}

	ch := m.notifyBlockstore.WaitFor(ctx, c)

	go func() {
		if err := m.retrieveContent(ctx, contentToFetch); err != nil {
			m.log.Errorf("failed to retrieve content to serve %d: %w", contentToFetch, err)
		}
	}()

	select {
	case blk := <-ch:
		return blk, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (m *manager) indexForAggregate(ctx context.Context, aggregateID, contID uint64) (int, error) {
	return 0, fmt.Errorf("selector based retrieval not yet implemented")
}
