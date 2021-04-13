package main

import (
	"context"
	"fmt"
	"math/rand"

	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-fil-markets/retrievalmarket"
	"github.com/ipfs/go-cid"
	"github.com/whyrusleeping/estuary/filclient"
	"gorm.io/gorm"
)

func (s *Server) retrievalAsksForContent(ctx context.Context, contid uint) (map[address.Address]*retrievalmarket.QueryResponse, error) {
	var content Content
	if err := s.DB.First(&content, "id = ?", contid).Error; err != nil {
		return nil, err
	}

	var deals []contentDeal
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
			s.CM.recordRetrievalFailure(&retrievalFailureRecord{
				Miner:   maddr.String(),
				Phase:   "query",
				Message: err.Error(),
			})
			log.Errorf("failed to query miner %s: %s", maddr, err)
			continue
		}

		out[maddr] = resp
	}

	return out, nil
}

type retrievalFailureRecord struct {
	gorm.Model
	Miner   string
	Phase   string
	Message string
}

func (cm *ContentManager) recordRetrievalFailure(rfr *retrievalFailureRecord) error {
	return cm.DB.Create(rfr).Error
}

func (s *Server) retrieveContent(ctx context.Context, contid uint) error {
	content, err := s.CM.getContent(contid)
	if err != nil {
		return err
	}

	asks, err := s.retrievalAsksForContent(ctx, contid)
	if err != nil {
		return err
	}

	fmt.Println("Got asks: ", len(asks))

	if len(asks) == 0 {
		return fmt.Errorf("no retrieval asks for content")
	}

	for m, ask := range asks {
		if err := s.CM.tryRetrieve(ctx, m, content.Cid.CID, ask); err != nil {
			log.Errorw("failed to retrieve content", "miner", m, "content", content.Cid.CID, "err", err)
			s.CM.recordRetrievalFailure(&retrievalFailureRecord{
				Miner:   m.String(),
				Phase:   "retrieval",
				Message: err.Error(),
			})
			continue
		}

		return nil
	}

	return nil
}

func (cm *ContentManager) tryRetrieve(ctx context.Context, maddr address.Address, c cid.Cid, ask *retrievalmarket.QueryResponse) error {
	proposal := &retrievalmarket.DealProposal{
		PayloadCID: c,
		ID:         retrievalmarket.DealID(rand.Int63n(1000000) + 100000),
		Params: retrievalmarket.Params{
			Selector:                nil,
			PieceCID:                nil,
			PricePerByte:            ask.MinPricePerByte,
			PaymentInterval:         ask.MaxPaymentInterval,
			PaymentIntervalIncrease: ask.MaxPaymentIntervalIncrease,
			UnsealPrice:             ask.UnsealPrice,
		},
	}

	stats, err := cm.FilClient.RetrieveContent(ctx, maddr, proposal)
	if err != nil {
		return err
	}

	cm.recordRetrievalSuccess(c, maddr, stats)
	return nil
}

type retrievalSuccessRecord struct {
	PropCid dbCID
	Miner   string

	Peer         string
	Size         uint64
	DurationMs   int64
	AverageSpeed uint64
	TotalPayment string
	NumPayments  int
	AskPrice     string
}

func (cm *ContentManager) recordRetrievalSuccess(cc cid.Cid, m address.Address, rstats *filclient.RetrievalStats) {
	if err := cm.DB.Create(&retrievalSuccessRecord{
		PropCid:      dbCID{cc},
		Miner:        m.String(),
		Peer:         rstats.Peer.String(),
		Size:         rstats.Size,
		DurationMs:   rstats.Duration.Milliseconds(),
		AverageSpeed: rstats.AverageSpeed,
		TotalPayment: rstats.TotalPayment.String(),
		NumPayments:  rstats.NumPayments,
		AskPrice:     rstats.AskPrice.String(),
	}).Error; err != nil {
		log.Errorf("failed to write retrieval success record: %s", err)
	}
}
