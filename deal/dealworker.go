package deal

import (
	"context"

	"github.com/application-research/estuary/model"
)

func (m *manager) runDealBackFillWorker(ctx context.Context) {
	// get content above minimum below max

}

func (m *manager) runDealWorker(ctx context.Context) error {
	// one process at a time for now, next pr will do multiples
	// get next ready content from queue
	payload := &model.DealQueue{}

	go func() {
		m.log.Infow("making more deals for content", "content", payload.ContID, "newDeals", payload.DealsCount)
		// if err := m.makeDealsForContent(ctx, content, payload.DealsCount, deals); err != nil {
		// 	m.log.Errorf("failed to make more deals: %s", err)
		// }
	}()
	return nil
}
