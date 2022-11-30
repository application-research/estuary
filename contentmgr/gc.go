package contentmgr

import (
	"context"
	"fmt"

	"github.com/application-research/estuary/util"
	"github.com/ipfs/go-cid"
	"golang.org/x/xerrors"
	"gorm.io/gorm"
)

func (cm *ContentManager) GarbageCollect(ctx context.Context) error {
	// since we're reference counting all the content, garbage bucket becomes easy
	// its even easier if we don't care that its 'perfect'

	// We can probably even just remove stuff when its references are removed from the database
	keych, err := cm.Blockstore.AllKeysChan(ctx)
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
		if err := cm.Blockstore.DeleteBlock(ctx, c); err != nil {
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
	if err := cm.DB.Model(&util.Object{}).Where("cid = ?", c.Bytes()).Count(&count).Error; err != nil {
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

	if err := cm.DB.Delete(&util.Content{}, contID).Error; err != nil {
		return fmt.Errorf("failed to delete content from db: %w", err)
	}

	var objIds []struct {
		Object uint
	}

	if err := cm.DB.Model(&util.ObjRef{}).Find(&objIds, "content = ?", contID).Error; err != nil {
		return fmt.Errorf("failed to gather referenced object IDs: %w", err)
	}

	if err := cm.DB.Where("content = ?", contID).Delete(&util.ObjRef{}).Error; err != nil {
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

		subq := cm.DB.Table("obj_refs").Select("1").Where("obj_refs.object = objects.id")
		if err := cm.DB.Where("id IN ? and not exists (?)", slice, subq).Delete(&util.Object{}).Error; err != nil {
			return err
		}
	}

	if !now {
		return nil
	}

	// TODO: copied from the offloading method, need to refactor this into something better
	q := cm.DB.Model(&util.ObjRef{}).
		Select("cid").
		Joins("left join objects on obj_refs.object = objects.id").
		Group("cid").
		Having("MIN(obj_refs.offloaded) = 1")

	rows, err := q.Rows()
	if err != nil {
		return err
	}

	for rows.Next() {
		var dbc util.DbCID
		if err := rows.Scan(&dbc); err != nil {
			return err
		}

		if err := cm.Blockstore.DeleteBlock(ctx, dbc.CID); err != nil {
			return err
		}
	}
	return nil
}

func (cm *ContentManager) UnpinContent(ctx context.Context, contid uint) error {
	var pin util.Content
	if err := cm.DB.First(&pin, "id = ?", contid).Error; err != nil {
		return err
	}

	objs, err := cm.objectsForPin(ctx, pin.ID)
	if err != nil {
		return err
	}

	if err := cm.DB.Delete(&util.Content{ID: pin.ID}).Error; err != nil {
		return err
	}

	if err := cm.DB.Where("content = ?", pin.ID).Delete(&util.ObjRef{}).Error; err != nil {
		return err
	}

	if err := cm.clearUnreferencedObjects(ctx, objs); err != nil {
		return err
	}

	for _, o := range objs {
		// TODO: this is safe, but... slow?
		if _, err := cm.deleteIfNotPinned(ctx, o); err != nil {
			return err
		}
	}

	buckets, ok := cm.Buckets[pin.UserID]
	if ok {
		for _, bucket := range buckets {
			if _, err := cm.tryRemoveContent(bucket, pin); err != nil {
				return err
			}
		}
	}
	return nil
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
	if err := cm.DB.Limit(1).Model(util.Object{}).Where("id = ? OR cid = ?", o.ID, o.Cid).Find(&objs).Error; err != nil {
		return false, err
	}
	if len(objs) == 0 {
		return true, cm.Node.Blockstore.DeleteBlock(ctx, o.Cid.CID)
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

	if err := cm.DB.Where("(?) = 0 and id in ?",
		cm.DB.Model(util.ObjRef{}).Where("object = objects.id").Select("count(1)"), ids).
		Delete(util.Object{}).Error; err != nil {
		return err
	}
	return nil
}

func (cm *ContentManager) objectsForPin(ctx context.Context, content uint) ([]*util.Object, error) {
	var objects []*util.Object
	if err := cm.DB.Model(util.ObjRef{}).Where("content = ?", content).
		Joins("left join objects on obj_refs.object = objects.id").
		Scan(&objects).Error; err != nil {
		return nil, err
	}
	return objects, nil
}
