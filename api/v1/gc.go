package apiV1

import (
	"context"

	"github.com/application-research/estuary/util"
	"github.com/ipfs/go-cid"
	"golang.org/x/xerrors"
	"gorm.io/gorm"
)

func (s *apiV1) GarbageCollect(ctx context.Context) error {
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

func (s *apiV1) trackingObject(c cid.Cid) (bool, error) {
	var count int64
	if err := s.DB.Model(&util.Object{}).Where("cid = ?", c.Bytes()).Count(&count).Error; err != nil {
		if xerrors.Is(err, gorm.ErrRecordNotFound) {
			return false, nil
		}
		return false, err
	}
	return count > 0, nil
}
