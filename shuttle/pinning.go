package shuttle

import (
	"context"
	"fmt"

	"github.com/application-research/estuary/drpc"
	"github.com/application-research/estuary/pinner/types"
	"github.com/application-research/estuary/util"
	"github.com/pkg/errors"
	"go.opentelemetry.io/otel/attribute"
	"golang.org/x/xerrors"
)

// even though there are 4 pin statuses, queued, pinning, pinned and failed
// the UpdatePinStatus only changes DB state for failed status
// when the content was added, status = pinning
// when the pin process is complete, status = pinned
func (cm *Manager) UpdatePinStatus(location string, contID uint, status types.PinningStatus) error {
	cm.log.Debugf("updating pin: %d, status: %s, loc: %s", contID, status, location)

	if status == types.PinningStatusFailed {
		var c util.Content
		if err := cm.db.First(&c, "id = ?", contID).Error; err != nil {
			return errors.Wrap(err, "failed to look up content")
		}

		if c.Active {
			return fmt.Errorf("got failed pin status message from location: %s where content(%d) was already active, refusing to do anything", location, contID)
		}

		if c.AggregatedIn > 0 {
			// unmark zone as consolidating if a staged content fails to pin
			cm.MarkFinishedConsolidating(c.AggregatedIn)
		}

		if err := cm.db.Model(util.Content{}).Where("id = ?", contID).UpdateColumns(map[string]interface{}{
			"active":  false,
			"pinning": false,
			"failed":  true,
			// TODO: consider if we should not clear aggregated_in, but instead filter for not failed contents when aggregating
			"aggregated_in": 0, // remove from staging zone so the zone can consolidate without it
		}).Error; err != nil {
			cm.log.Errorf("failed to mark content as failed in database: %s", err)
		}
	}
	return nil
}

func (cm *Manager) handlePinningComplete(ctx context.Context, handle string, pincomp *drpc.PinComplete) error {
	ctx, span := cm.tracer.Start(ctx, "handlePinningComplete")
	defer span.End()

	var cont util.Content
	if err := cm.db.First(&cont, "id = ?", pincomp.DBID).Error; err != nil {
		return xerrors.Errorf("got shuttle pin complete for unknown content %d (shuttle = %s): %w", pincomp.DBID, handle, err)
	}

	if cont.Active {
		// content already active, no need to add objects, just update location
		// this is used by consolidated contents
		if err := cm.db.Model(util.Content{}).Where("id = ?", cont.ID).UpdateColumns(map[string]interface{}{
			"pinning":  false,
			"location": handle,
		}).Error; err != nil {
			return err
		}
		return nil
	}

	if cont.Aggregate {
		// this is used by staging content aggregate
		if len(pincomp.Objects) != 1 {
			return fmt.Errorf("aggregate has more than 1 objects")
		}

		obj := &util.Object{
			Cid:  util.DbCID{CID: pincomp.Objects[0].Cid},
			Size: pincomp.Objects[0].Size,
		}
		if err := cm.db.Create(obj).Error; err != nil {
			return xerrors.Errorf("failed to create Object: %w", err)
		}

		if err := cm.db.Create(&util.ObjRef{
			Content: cont.ID,
			Object:  obj.ID,
		}).Error; err != nil {
			return xerrors.Errorf("failed to create Object reference: %w", err)
		}

		if err := cm.db.Model(util.Content{}).Where("id = ?", cont.ID).UpdateColumns(map[string]interface{}{
			"active":   true,
			"pinning":  false,
			"location": handle,
			"cid":      util.DbCID{CID: pincomp.CID},
			"size":     pincomp.Size,
		}).Error; err != nil {
			return xerrors.Errorf("failed to update content in database: %w", err)
		}

		// if the content is a consolidated aggregate, it means aggregation has been completed and we can mark as finished
		cm.MarkFinishedAggregating(cont.ID)

		// after aggregate is done, make deal for it
		cm.ToCheck(cont.ID)
		return nil
	}

	objects := make([]*util.Object, 0, len(pincomp.Objects))
	for _, o := range pincomp.Objects {
		objects = append(objects, &util.Object{
			Cid:  util.DbCID{CID: o.Cid},
			Size: o.Size,
		})
	}

	if err := cm.addObjectsToDatabase(ctx, pincomp.DBID, objects, handle); err != nil {
		return xerrors.Errorf("failed to add objects to database: %w", err)
	}

	cm.ToCheck(cont.ID)
	return nil
}

// addObjectsToDatabase creates entries on the estuary database for CIDs related to an already pinned CID (`root`)
// These entries are saved on the `objects` table, while metadata about the `root` CID is mostly kept on the `contents` table
// The link between the `objects` and `contents` tables is the `obj_refs` table
func (cm *Manager) addObjectsToDatabase(ctx context.Context, contID uint, objects []*util.Object, loc string) error {
	_, span := cm.tracer.Start(ctx, "addObjectsToDatabase")
	defer span.End()

	if err := cm.db.CreateInBatches(objects, 300).Error; err != nil {
		return xerrors.Errorf("failed to create objects in db: %w", err)
	}

	refs := make([]util.ObjRef, 0, len(objects))
	var totalSize int64
	for _, o := range objects {
		refs = append(refs, util.ObjRef{
			Content: contID,
			Object:  o.ID,
		})
		totalSize += int64(o.Size)
	}

	span.SetAttributes(
		attribute.Int64("totalSize", totalSize),
		attribute.Int("numObjects", len(objects)),
	)

	if err := cm.db.CreateInBatches(refs, 500).Error; err != nil {
		return xerrors.Errorf("failed to create refs: %w", err)
	}

	if err := cm.db.Model(util.Content{}).Where("id = ?", contID).UpdateColumns(map[string]interface{}{
		"active":   true,
		"size":     totalSize,
		"pinning":  false,
		"location": loc,
	}).Error; err != nil {
		return xerrors.Errorf("failed to update content in database: %w", err)
	}
	return nil
}
