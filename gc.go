package main

import (
	"context"

	"github.com/ipfs/go-cid"
	"golang.org/x/xerrors"
	"gorm.io/gorm"
)

func (cm *ContentManager) GarbageCollect(ctx context.Context) error {
	// since we're reference counting all the content, garbage collection becomes easy
	// its even easier if we don't care that its 'perfect'

	// We can probably even just remove stuff when its references are removed from the database
	keych, err := cm.Blockstore.AllKeysChan(ctx)
	if err != nil {
		return err
	}

	for c := range keych {
		_, err := cm.maybeRemoveObject(c)
		if err != nil {
			return err
		}
	}

	return nil
}

func (cm *ContentManager) maybeRemoveObject(c cid.Cid) (bool, error) {
	cm.contentLk.Lock()
	defer cm.contentLk.Unlock()
	keep, err := cm.trackingObject(c)
	if err != nil {
		return false, err
	}

	if !keep {
		// can batch these deletes and execute them at the datastore layer for more perfs
		if err := cm.Blockstore.DeleteBlock(c); err != nil {
			return false, err
		}

		return true, nil
	}

	return false, nil
}

func (cm *ContentManager) trackingObject(c cid.Cid) (bool, error) {
	var count int64
	if err := cm.DB.Model(&Object{}).Where("cid = ?", c.Bytes()).Count(&count).Error; err != nil {
		if xerrors.Is(err, gorm.ErrRecordNotFound) {
			return false, nil
		}
		return false, err
	}

	return count > 0, nil
}
