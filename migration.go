package main

import (
	"context"
	"errors"
	"time"

	"github.com/application-research/estuary/util"
	"gorm.io/gorm"
)

type Migration struct {
	Name        string `gorm:"primarykey"`
	StartedAt   time.Time
	CompletedAt time.Time
}

// ordered list of migrations. Data migrations are normally executed in the background so a migration function must
// not interfere with the running of the rest of estuary and should be stateless, interruptable and resumable. A
// migration may be marked as required in which case estuary will not start running until the migration has completed.
// A large data migration may be implemented in stages across multiple versions of estuary. The first stage would
// migrate the data in the background, retaining compatibility with existing queries. Once this is complete a later
// version of estuary may simplify code by removing the compatibility and updating the migration to be required. This
// will have no effect on systems that have already migrated but will allow older systems to be updated safely.
var migrations = []struct {
	name string
	fn   func(context.Context, *gorm.DB) error

	// when true, estuary will not start until the migration has completed
	required bool
}{

	{
		// Migration from cid to multihash for the objects table
		name: "object-cid-to-hash",
		fn: func(ctx context.Context, db *gorm.DB) error {
			var objs []*Object
			tx := db.Where("hash IS NULL").FindInBatches(&objs, 500, func(tx *gorm.DB, batch int) error {
				// Make sure migration is interruptable
				select {
				case <-ctx.Done():
					return ctx.Err()
				default:
				}

				for _, o := range objs {
					o.Hash = util.DbHashFromDbCID(&o.Cid)
				}

				tx.Save(&objs)
				log.Debugf("migrated %d cids to multihashes", tx.RowsAffected)
				return nil
			})
			return tx.Error
		},
	},
}

func maybePerformMigrations(ctx context.Context, db *gorm.DB) error {
	for _, minfo := range migrations {
		var mig Migration
		err := db.Where("name=?", minfo.name).First(&mig).Error
		if errors.Is(err, gorm.ErrRecordNotFound) {
			mig.Name = minfo.name
			mig.StartedAt = time.Now().UTC()
		} else if err != nil {
			return err
		} else if !mig.CompletedAt.IsZero() {
			// If the migration has completed then we can skip it
			continue
		}

		if err := db.Save(mig).Error; err != nil {
			return err
		}

		// Kick off the migration
		if minfo.required {
			// Migration must run to completion synchronously
			if err := runMigration(ctx, db, mig, minfo.fn); err != nil {
				return err
			}
		} else {
			// Migration runs asynch in the background
			go runMigration(ctx, db, mig, minfo.fn)
		}

	}

	return nil
}

func runMigration(ctx context.Context, db *gorm.DB, mig Migration, fn func(context.Context, *gorm.DB) error) error {
	log.Infof("%s migration started", mig.Name)
	err := fn(ctx, db)
	if err != nil {
		log.Errorf("%s migration failed: %v", mig.Name, err)
		return err
	}
	log.Infof("%s migration complete", mig.Name)

	mig.CompletedAt = time.Now().UTC()
	if err := db.Save(mig).Error; err != nil {
		log.Errorf("%s migration completed but failed to save status: %v", mig.Name, err)
		return err
	}

	return nil
}
