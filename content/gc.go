package contentmgr

import (
	"context"
	"fmt"

	"github.com/application-research/estuary/util"
	"github.com/ipfs/go-cid"
	"golang.org/x/xerrors"
	"gorm.io/gorm"
)

func (m *manager) isInflight(c cid.Cid) bool {
	m.inflightCidsLk.Lock()
	defer m.inflightCidsLk.Unlock()

	v, ok := m.inflightCids[c]
	return ok && v > 0
}

func (m *manager) GarbageCollect(ctx context.Context) error {
	// since we're reference counting all the content, garbage collection becomes easy
	// its even easier if we don't care that its 'perfect'

	// We can probably even just remove stuff when its references are removed from the database
	keych, err := m.blockstore.AllKeysChan(ctx)
	if err != nil {
		return err
	}

	for c := range keych {
		_, err := m.maybeRemoveObject(ctx, c)
		if err != nil {
			return err
		}
	}

	return nil
}

func (m *manager) maybeRemoveObject(ctx context.Context, c cid.Cid) (bool, error) {
	m.contentLk.Lock()
	defer m.contentLk.Unlock()
	keep, err := m.trackingObject(c)
	if err != nil {
		return false, err
	}

	if !keep {
		// can batch these deletes and execute them at the datastore layer for more perfs
		if err := m.blockstore.DeleteBlock(ctx, c); err != nil {
			return false, err
		}
		return true, nil
	}
	return false, nil
}

func (m *manager) trackingObject(c cid.Cid) (bool, error) {
	m.inflightCidsLk.Lock()
	ok := m.isInflight(c)
	m.inflightCidsLk.Unlock()

	if ok {
		return true, nil
	}

	var count int64
	if err := m.db.Model(&util.Object{}).Where("cid = ?", c.Bytes()).Count(&count).Error; err != nil {
		if xerrors.Is(err, gorm.ErrRecordNotFound) {
			return false, nil
		}
		return false, err
	}
	return count > 0, nil
}

func (m *manager) RemoveContent(ctx context.Context, contID uint, now bool) error {
	ctx, span := m.tracer.Start(ctx, "RemoveContent")
	defer span.End()

	m.contentLk.Lock()
	defer m.contentLk.Unlock()

	if err := m.db.Delete(&util.Content{}, contID).Error; err != nil {
		return fmt.Errorf("failed to delete content from db: %w", err)
	}

	var objIds []struct {
		Object uint
	}

	if err := m.db.Model(&util.ObjRef{}).Find(&objIds, "content = ?", contID).Error; err != nil {
		return fmt.Errorf("failed to gather referenced object IDs: %w", err)
	}

	if err := m.db.Where("content = ?", contID).Delete(&util.ObjRef{}).Error; err != nil {
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

		subq := m.db.Table("obj_refs").Select("1").Where("obj_refs.object = objects.id")
		if err := m.db.Where("id IN ? and not exists (?)", slice, subq).Delete(&util.Object{}).Error; err != nil {
			return err
		}
	}

	if !now {
		return nil
	}

	// TODO: copied from the offloading method, need to refactor this into something better
	q := m.db.Model(&util.ObjRef{}).
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

		if err := m.blockstore.DeleteBlock(ctx, dbc.CID); err != nil {
			return err
		}
	}
	return nil
}

func (m *manager) UnpinContent(ctx context.Context, contid uint) error {
	var pin util.Content
	if err := m.db.First(&pin, "id = ?", contid).Error; err != nil {
		return err
	}

	objs, err := m.objectsForPin(ctx, pin.ID)
	if err != nil {
		return err
	}

	// delete object refs rows for deleted content
	if err := m.db.Where("content = ?", pin.ID).Delete(&util.ObjRef{}).Error; err != nil {
		return err
	}

	// delete objects that no longer have obj refs
	if err := m.clearUnreferencedObjects(ctx, objs); err != nil {
		return err
	}

	// delete objects from blockstore that are no longer present in db
	for _, o := range objs {
		// TODO: this is safe, but... slow?
		if _, err := m.deleteIfNotPinned(ctx, o); err != nil {
			return err
		}
	}

	// delete from contents table and adjust aggregate size, if applicable, in one tx
	return m.db.Transaction(func(tx *gorm.DB) error {
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

func (m *manager) deleteIfNotPinned(ctx context.Context, o *util.Object) (bool, error) {
	ctx, span := m.tracer.Start(ctx, "deleteIfNotPinned")
	defer span.End()

	m.contentLk.Lock()
	defer m.contentLk.Unlock()

	return m.deleteIfNotPinnedLock(ctx, o)
}

func (m *manager) deleteIfNotPinnedLock(ctx context.Context, o *util.Object) (bool, error) {
	ctx, span := m.tracer.Start(ctx, "deleteIfNotPinnedLock")
	defer span.End()

	var objs []util.Object
	if err := m.db.Limit(1).Model(util.Object{}).Where("id = ? OR cid = ?", o.ID, o.Cid).Find(&objs).Error; err != nil {
		return false, err
	}
	if len(objs) == 0 {
		return true, m.node.Blockstore.DeleteBlock(ctx, o.Cid.CID)
	}
	return false, nil
}

func (m *manager) clearUnreferencedObjects(ctx context.Context, objs []*util.Object) error {
	var ids []uint64
	for _, o := range objs {
		ids = append(ids, o.ID)
	}
	m.contentLk.Lock()
	defer m.contentLk.Unlock()

	if err := m.db.Where("(?) = 0 and id in ?",
		m.db.Model(util.ObjRef{}).Where("object = objects.id").Select("count(1)"), ids).
		Delete(util.Object{}).Error; err != nil {
		return err
	}
	return nil
}

func (m *manager) objectsForPin(ctx context.Context, cont uint64) ([]*util.Object, error) {
	var objects []*util.Object
	if err := m.db.Model(util.ObjRef{}).Where("content = ?", cont).
		Joins("left join objects on obj_refs.object = objects.id").
		Scan(&objects).Error; err != nil {
		return nil, err
	}
	return objects, nil
}
