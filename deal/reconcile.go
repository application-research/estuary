package deal

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/application-research/estuary/constants"
	dealstatus "github.com/application-research/estuary/deal/status"
	"github.com/application-research/estuary/model"
	"github.com/application-research/estuary/util"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/xerrors"
	"gorm.io/gorm"
)

func (m *manager) ensureStorage(ctx context.Context, content util.Content, done func(time.Duration)) error {
	ctx, span := m.tracer.Start(ctx, "ensureStorage", trace.WithAttributes(
		attribute.Int("content", int(content.ID)),
	))
	defer span.End()

	// get content deals, if any
	var deals []model.ContentDeal
	if err := m.db.Find(&deals, "content = ? AND NOT failed", content.ID).Error; err != nil {
		if !xerrors.Is(err, gorm.ErrRecordNotFound) {
			return err
		}
	}

	// check on each of the existing deals, see if any needs fixing
	var countLk sync.Mutex
	var numSealed, numPublished, numProgress int
	var wg sync.WaitGroup

	errs := make([]error, len(deals))
	for i, d := range deals {
		dl := d
		wg.Add(1)
		go func(i int) {
			d := deals[i]
			defer wg.Done()

			status, err := m.checkDeal(ctx, &dl, content)
			if err != nil {
				var dfe *dealstatus.DealFailureError
				if xerrors.As(err, &dfe) {
					return
				} else {
					errs[i] = err
					return
				}
			}

			countLk.Lock()
			defer countLk.Unlock()
			switch status {
			case DEAL_CHECK_UNKNOWN, DEAL_NEARLY_EXPIRED, DEAL_CHECK_SLASHED:
				if err := m.repairDeal(&d); err != nil {
					errs[i] = xerrors.Errorf("repairing deal failed: %w", err)
					return
				}
			case DEAL_CHECK_SECTOR_ON_CHAIN:
				numSealed++
			case DEAL_CHECK_DEALID_ON_CHAIN:
				numPublished++
			case DEAL_CHECK_PROGRESS:
				numProgress++
			default:
				m.log.Errorf("unrecognized deal check status: %d", status)
			}
		}(i)
	}
	wg.Wait()

	// return the last error found, log the rest
	var retErr error
	for _, err := range errs {
		if err != nil {
			if retErr != nil {
				m.log.Errorf("check deal failure: %s", err)
			}
			retErr = err
		}
	}
	if retErr != nil {
		return fmt.Errorf("deal check errored: %w", retErr)
	}

	if content.Location != constants.ContentLocationLocal {
		// after reconciling content deals,
		// check If this is a shuttle content and that the shuttle is online and can start data transfer
		isOnline, err := m.shuttleMgr.IsOnline(content.Location)
		if err != nil || !isOnline {
			m.log.Warnf("content shuttle: %s, is not online", content.Location)
			done(time.Minute * 15)
			return err
		}
	}

	replicationFactor := m.cfg.Replication
	if content.Replication > 0 {
		replicationFactor = content.Replication
	}

	// check if content has enough good deals after reconcialiation,
	// if not enough good deals, go make more
	goodDeals := numSealed + numPublished + numProgress
	dealsToBeMade := replicationFactor - goodDeals
	if dealsToBeMade <= 0 {
		if numSealed >= replicationFactor {
			done(time.Hour * 24)
		} else if numSealed+numPublished >= replicationFactor {
			done(time.Hour)
		} else {
			done(time.Minute * 10)
		}
		return nil
	}

	m.log.Infof("getting commp for cont: %d", content.ID)
	_, _, _, err := m.commpMgr.GetOrRunPieceCommitment(context.Background(), content.Cid.CID, m.blockstore)
	if err != nil {
		return err
	}

	go func() {
		// make some more deals!
		m.log.Infow("making more deals for content", "content", content.ID, "curDealCount", len(deals), "newDeals", dealsToBeMade)
		if err := m.makeDealsForContent(ctx, content, dealsToBeMade, deals); err != nil {
			m.log.Errorf("failed to make more deals: %s", err)
		}
		done(time.Minute * 10)
	}()
	return nil
}