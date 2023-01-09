package contentmgr

import (
	"context"
	"fmt"

	"github.com/application-research/estuary/util"
	"github.com/ipfs/go-cid"
	"golang.org/x/xerrors"
	"gorm.io/gorm"
)

func (cm *ContentManager) isInflight(c cid.Cid) bool {
	cm.inflightCidsLk.Lock()
	defer cm.inflightCidsLk.Unlock()

	v, ok := cm.inflightCids[c]
	return ok && v > 0
}

func (cm *ContentManager) GarbageCollect(ctx context.Context) error {
	// since we're reference counting all the content, garbage collection becomes easy
	// its even easier if we don't care that its 'perfect'

	// We can probably even just remove stuff when its references are removed from the database
	keych, err := cm.blockstore.AllKeysChan(ctx)
	if err != nil {
		return err
	}

	for c := range keych {
		_, err := cm.maybeRemoveObject(ctx, c)
		if err != nil {
			return err
		}
	}

	return nil
}

func (cm *ContentManager) maybeRemoveObject(ctx context.Context, c cid.Cid) (bool, error) {
	cm.contentLk.Lock()
	defer cm.contentLk.Unlock()
	keep, err := cm.trackingObject(c)
	if err != nil {
		return false, err
	}

	if !keep {
		// can batch these deletes and execute them at the datastore layer for more perfs
		if err := cm.blockstore.DeleteBlock(ctx, c); err != nil {
			return false, err
		}
		return true, nil
	}
	return false, nil
}

func (cm *ContentManager) trackingObject(c cid.Cid) (bool, error) {
	cm.inflightCidsLk.Lock()
	ok := cm.isInflight(c)
	cm.inflightCidsLk.Unlock()

	if ok {
		return true, nil
	}

	var count int64
	if err := cm.db.Model(&util.Object{}).Where("cid = ?", c.Bytes()).Count(&count).Error; err != nil {
		if xerrors.Is(err, gorm.ErrRecordNotFound) {
			return false, nil
		}
		return false, err
	}
	return count > 0, nil
}

func (cm *ContentManager) RemoveContent(ctx context.Context, contID uint, now bool) error {
	ctx, span := cm.tracer.Start(ctx, "RemoveContent")
	defer span.End()

	cm.contentLk.Lock()
	defer cm.contentLk.Unlock()

	if err := cm.db.Delete(&util.Content{}, contID).Error; err != nil {
		return fmt.Errorf("failed to delete content from db: %w", err)
	}

	var objIds []struct {
		Object uint
	}

	if err := cm.db.Model(&util.ObjRef{}).Find(&objIds, "content = ?", contID).Error; err != nil {
		return fmt.Errorf("failed to gather referenced object IDs: %w", err)
	}

	if err := cm.db.Where("content = ?", contID).Delete(&util.ObjRef{}).Error; err != nil {
		return fmt.Errorf("failed to delete related object references: %w", err)
	}

	ids := make([]uint, len(objIds))
	for i, obj := range objIds {
		ids[i] = obj.Object
	}

	// Since im kinda bad at sql, this is going to be faster than the naive
	// query for now. Maybe can think of something more clever later
	batchSize := 100
	for i := 0; i < len(ids); i += 100 {
		count := batchSize
		if len(ids[i:]) < count {
			count = len(ids[i:])
		}

		slice := ids[i : i+count]

		subq := cm.db.Table("obj_refs").Select("1").Where("obj_refs.object = objects.id")
		if err := cm.db.Where("id IN ? and not exists (?)", slice, subq).Delete(&util.Object{}).Error; err != nil {
			return err
		}
	}

	if !now {
		return nil
	}

	// TODO: copied from the offloading method, need to refactor this into something better
	q := cm.db.Model(&util.ObjRef{}).
		Select("cid").
		Joins("left join objects on obj_refs.object = objects.id").
		Group("cid").
		Having("MIN(obj_refs.offloaded) = 1")

	rows, err := q.Rows()
	if err != nil {
		return err
	}

	defer rows.Close()
	for rows.Next() {
		var dbc util.DbCID
		if err := rows.Scan(&dbc); err != nil {
			return err
		}

		if err := cm.blockstore.DeleteBlock(ctx, dbc.CID); err != nil {
			return err
		}
	}
	return nil
}

func (cm *ContentManager) UnpinContent(ctx context.Context, contid uint) error {
	var pin util.Content
	if err := cm.db.First(&pin, "id = ?", contid).Error; err != nil {
		return err
	}

	objs, err := cm.objectsForPin(ctx, pin.ID)
	if err != nil {
		return err
	}

	// delete object refs rows for deleted content
	if err := cm.db.Where("content = ?", pin.ID).Delete(&util.ObjRef{}).Error; err != nil {
		return err
	}

	// delete objects that no longer have obj refs
	if err := cm.clearUnreferencedObjects(ctx, objs); err != nil {
		return err
	}

	// delete objects from blockstore that are no longer present in db
	for _, o := range objs {
		// TODO: this is safe, but... slow?
		if _, err := cm.deleteIfNotPinned(ctx, o); err != nil {
			return err
		}
	}

	// delete from contents table and adjust aggregate size, if applicable, in one tx
	return cm.db.Transaction(func(tx *gorm.DB) error {
		// delete contid row from contents table
		if err := tx.Model(util.Content{}).Delete(&util.Content{ID: pin.ID}).Error; err != nil {
			return err
		}

		if pin.AggregatedIn > 0 {
			// decrease aggregate's size by cont's size in contents table
			if err := tx.Model(util.Content{}).
				Where("id = ?", pin.AggregatedIn).
				UpdateColumn("size", gorm.Expr("size - ?", pin.Size)).Error; err != nil {
				return err
			}
		}
		return nil
	})
}

func (cm *ContentManager) deleteIfNotPinned(ctx context.Context, o *util.Object) (bool, error) {
	ctx, span := cm.tracer.Start(ctx, "deleteIfNotPinned")
	defer span.End()

	cm.contentLk.Lock()
	defer cm.contentLk.Unlock()

	return cm.deleteIfNotPinnedLock(ctx, o)
}

func (cm *ContentManager) deleteIfNotPinnedLock(ctx context.Context, o *util.Object) (bool, error) {
	ctx, span := cm.tracer.Start(ctx, "deleteIfNotPinnedLock")
	defer span.End()

	var objs []util.Object
	if err := cm.db.Limit(1).Model(util.Object{}).Where("id = ? OR cid = ?", o.ID, o.Cid).Find(&objs).Error; err != nil {
		return false, err
	}
	if len(objs) == 0 {
		return true, cm.node.Blockstore.DeleteBlock(ctx, o.Cid.CID)
	}
	return false, nil
}

func (cm *ContentManager) clearUnreferencedObjects(ctx context.Context, objs []*util.Object) error {
	var ids []uint
	for _, o := range objs {
		ids = append(ids, o.ID)
	}
	cm.contentLk.Lock()
	defer cm.contentLk.Unlock()

	if err := cm.db.Where("(?) = 0 and id in ?",
		cm.db.Model(util.ObjRef{}).Where("object = objects.id").Select("count(1)"), ids).
		Delete(util.Object{}).Error; err != nil {
		return err
	}
	return nil
}

func (cm *ContentManager) objectsForPin(ctx context.Context, cont uint) ([]*util.Object, error) {
	var objects []*util.Object
	if err := cm.db.Model(util.ObjRef{}).Where("content = ?", cont).
		Joins("left join objects on obj_refs.object = objects.id").
		Scan(&objects).Error; err != nil {
		return nil, err
	}
	return objects, nil
}
