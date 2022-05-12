package server

import (
	"context"

	"github.com/application-research/estuary/node"
	contentmanager "github.com/application-research/estuary/server/contentmanager"
	"github.com/application-research/estuary/stagingbs"
	"github.com/application-research/estuary/util/gateway"
	"github.com/application-research/filclient"
	"github.com/filecoin-project/lotus/api"
	"github.com/ipfs/go-cid"
	"github.com/whyrusleeping/memo"
	"go.opencensus.io/trace"
	"golang.org/x/xerrors"
	"google.golang.org/appengine/log"
	"gorm.io/gorm"
)

type Server struct {
	Tracer     trace.Tracer
	Node       *node.Node
	DB         *gorm.DB
	FilClient  *filclient.FilClient
	Api        api.Gateway
	CM         *contentmanager.ContentManager
	StagingMgr *stagingbs.StagingBSMgr

	GwayHandler *gateway.GatewayHandler

	Cacher *memo.Cacher
}

func (s *Server) GarbageCollect(ctx context.Context) error {
	// since we're reference counting all the content, garbage collection becomes easy
	// its even easier if we don't care that its 'perfect'

	// We can probably even just remove stuff when its references are removed from the database
	keych, err := s.Node.Blockstore.AllKeysChan(ctx)
	if err != nil {
		return err
	}

	for c := range keych {
		keep, err := s.trackingObject(c)
		if err != nil {
			return err
		}

		if !keep {
			// can batch these deletes and execute them at the datastore layer for more perfs
			if err := s.Node.Blockstore.DeleteBlock(ctx, c); err != nil {
				return err
			}
		}
	}

	return nil
}

func (s *Server) trackingObject(c cid.Cid) (bool, error) {
	var count int64
	if err := s.DB.Model(&Object{}).Where("cid = ?", c.Bytes()).Count(&count).Error; err != nil {
		if xerrors.Is(err, gorm.ErrRecordNotFound) {
			return false, nil
		}
		return false, err
	}

	return count > 0, nil
}

func (s *Server) RestartAllTransfersForLocation(ctx context.Context, loc string) error {
	var deals []contentDeal
	if err := s.DB.Model(contentDeal{}).
		Joins("left join contents on contents.id = content_deals.content").
		Where("not content_deals.failed and content_deals.deal_id = 0 and content_deals.dt_chan != '' and location = ?", loc).
		Scan(&deals).Error; err != nil {
		return err
	}

	for _, d := range deals {
		chid, err := d.ChannelID()
		if err != nil {
			// Only legacy (push) transfers need to be restarted by Estuary.
			// Newer (pull) transfers are restarted by the Storage Provider.
			// So if it's not a legacy channel ID, ignore it.
			continue
		}

		if err := s.CM.RestartTransfer(ctx, loc, chid); err != nil {
			log.Errorf("failed to restart transfer: %s", err)
			continue
		}
	}

	return nil
}
