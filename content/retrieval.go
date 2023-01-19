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

func (cm *ContentManager) RecordRetrievalFailure(rfr *util.RetrievalFailureRecord) error {
	return cm.db.Create(rfr).Error
}

func (cm *ContentManager) TryRetrieve(ctx context.Context, maddr address.Address, c cid.Cid, ask *retrievalmarket.QueryResponse) error {
	proposal, err := retrievehelper.RetrievalProposalForAsk(ask, c, nil)
	if err != nil {
		return err
	}

	stats, err := cm.filClient.RetrieveContent(ctx, maddr, proposal)
	if err != nil {
		return err
	}

	cm.RecordRetrievalSuccess(c, maddr, stats)
	return nil
}

func (cm *ContentManager) RecordRetrievalSuccess(cc cid.Cid, m address.Address, rstats *filclient.RetrievalStats) {
	if err := cm.db.Create(&model.RetrievalSuccessRecord{
		Cid:          util.DbCID{CID: cc},
		Miner:        m.String(),
		Peer:         rstats.Peer.String(),
		Size:         rstats.Size,
		DurationMs:   rstats.Duration.Milliseconds(),
		AverageSpeed: rstats.AverageSpeed,
		TotalPayment: rstats.TotalPayment.String(),
		NumPayments:  rstats.NumPayments,
		AskPrice:     rstats.AskPrice.String(),
	}).Error; err != nil {
		cm.log.Errorf("failed to write retrieval success record: %s", err)
	}
}

func (cm *ContentManager) runRetrieval(ctx context.Context, contentToFetch uint64) error {
	ctx, span := cm.tracer.Start(ctx, "runRetrieval")
	defer span.End()

	var content util.Content
	if err := cm.db.First(&content, contentToFetch).Error; err != nil {
		return err
	}

	index := -1
	if content.AggregatedIn > 0 {
		rootContent := content.AggregatedIn
		ix, err := cm.indexForAggregate(ctx, rootContent, contentToFetch)
		if err != nil {
			return err
		}
		index = ix
	}
	_ = index

	var deals []model.ContentDeal
	if err := cm.db.Find(&deals, "content = ? and not failed", contentToFetch).Error; err != nil {
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
			cm.log.Errorf("deal %d had bad miner address: %s", deal.ID, err)
			continue
		}

		cm.log.Infow("attempting retrieval deal", "content", contentToFetch, "miner", maddr)

		ask, err := cm.filClient.RetrievalQuery(ctx, maddr, content.Cid.CID)
		if err != nil {
			span.RecordError(err)

			cm.log.Errorw("failed to query retrieval", "miner", maddr, "content", content.Cid.CID, "err", err)
			if err := cm.RecordRetrievalFailure(&util.RetrievalFailureRecord{
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
		cm.log.Infow("got retrieval ask", "content", content, "miner", maddr, "ask", ask)

		if err := cm.TryRetrieve(ctx, maddr, content.Cid.CID, ask); err != nil {
			span.RecordError(err)
			cm.log.Errorw("failed to retrieve content", "miner", maddr, "content", content.Cid.CID, "err", err)
			if err := cm.RecordRetrievalFailure(&util.RetrievalFailureRecord{
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

func (cm *ContentManager) sendRetrieveContentMessage(ctx context.Context, loc string, cont util.Content) error {
	return fmt.Errorf("not retrieving content yet until implementation is finished")
	/*
		var activeDeals []contentDeal
		if err := cm.DB.Find(&activeDeals, "content = ? and not failed and deal_id > 0", cont.ID).Error; err != nil {
			return err
		}

		if len(activeDeals) == 0 {
			cm.log.Errorf("attempted to retrieve content %d but have no active deals", cont.ID)
			return fmt.Errorf("no active deals for content %d, cannot retrieve", cont.ID)
		}

		var deals []drpc.StorageDeal
		for _, d := range activeDeals {
			ma, err := d.MinerAddr()
			if err != nil {
				cm.log.Errorf("failed to parse miner addres for deal %d: %s", d.ID, err)
				continue
			}

			deals = append(deals, drpc.StorageDeal{
				Miner:  ma,
				DealID: d.DealID,
			})
		}

		return cm.sendShuttleCommand(ctx, loc, &drpc.Command{
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

func (cm *ContentManager) retrieveContent(ctx context.Context, contentToFetch uint64) error {
	ctx, span := cm.tracer.Start(ctx, "retrieveContent", trace.WithAttributes(
		attribute.Int("content", int(contentToFetch)),
	))
	defer span.End()

	cm.retrLk.Lock()
	prog, ok := cm.retrievalsInProgress[contentToFetch]
	if !ok {
		prog = &util.RetrievalProgress{
			Wait: make(chan struct{}),
		}
		cm.retrievalsInProgress[contentToFetch] = prog
	}
	cm.retrLk.Unlock()

	if ok {
		select {
		case <-prog.Wait:
			return prog.EndErr
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	defer func() {
		cm.retrLk.Lock()
		delete(cm.retrievalsInProgress, contentToFetch)
		cm.retrLk.Unlock()

		close(prog.Wait)
	}()

	if err := cm.runRetrieval(ctx, contentToFetch); err != nil {
		prog.EndErr = err
		return err
	}

	return nil
}

func (cm *ContentManager) RefreshContent(ctx context.Context, cont uint64) error {
	ctx, span := cm.tracer.Start(ctx, "refreshContent")
	defer span.End()

	// TODO: this retrieval needs to mark all of its content as 'referenced'
	// until we can update its offloading status in the database
	var c util.Content
	if err := cm.db.First(&c, "id = ?", cont).Error; err != nil {
		return err
	}

	loc, err := cm.shuttleMgr.GetLocationForRetrieval(ctx, c)
	if err != nil {
		return err
	}
	cm.log.Infof("refreshing content %d onto shuttle %s", cont, loc)

	switch loc {
	case constants.ContentLocationLocal:
		if err := cm.retrieveContent(ctx, cont); err != nil {
			return err
		}

		if err := cm.db.Model(&util.Content{}).Where("id = ?", cont).Update("offloaded", false).Error; err != nil {
			return err
		}

		if err := cm.db.Model(&util.ObjRef{}).Where("content = ?", cont).Update("offloaded", 0).Error; err != nil {
			return err
		}
	default:
		return cm.sendRetrieveContentMessage(ctx, loc, c)
	}
	return nil
}

func (cm *ContentManager) RefreshContentForCid(ctx context.Context, c cid.Cid) (blocks.Block, error) {
	ctx, span := cm.tracer.Start(ctx, "refreshForCid", trace.WithAttributes(
		attribute.Stringer("cid", c),
	))
	defer span.End()

	var obj util.Object
	if err := cm.db.First(&obj, "cid = ?", c.Bytes()).Error; err != nil {
		return nil, xerrors.Errorf("failed to get object from db: %s", err)
	}

	var refs []util.ObjRef
	if err := cm.db.Find(&refs, "object = ?", obj.ID).Error; err != nil {
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
		if err := cm.db.Find(&contents, "cid = ?", c.Bytes()).Error; err != nil {
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

	ch := cm.notifyBlockstore.WaitFor(ctx, c)

	go func() {
		if err := cm.retrieveContent(ctx, contentToFetch); err != nil {
			cm.log.Errorf("failed to retrieve content to serve %d: %w", contentToFetch, err)
		}
	}()

	select {
	case blk := <-ch:
		return blk, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (cm *ContentManager) indexForAggregate(ctx context.Context, aggregateID uint64, contID uint64) (int, error) {
	return 0, fmt.Errorf("selector based retrieval not yet implemented")
}
