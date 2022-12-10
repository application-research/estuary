package shuttle

import (
	"context"
	"fmt"

	"github.com/application-research/estuary/drpc"
	"github.com/application-research/estuary/model"
	"github.com/application-research/estuary/pinner/types"
	"github.com/application-research/estuary/util"
	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/pkg/errors"
	"go.opentelemetry.io/otel/attribute"
	"golang.org/x/xerrors"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

func (m *manager) ConsolidateContent(ctx context.Context, loc string, contents []util.Content) error {
	m.log.Debugf("attempting to send consolidate content cmd to %s", loc)
	tc := &drpc.TakeContent{}
	for _, c := range contents {
		prs := make([]*peer.AddrInfo, 0)

		pr := m.AddrInfo(c.Location)
		if pr != nil {
			prs = append(prs, pr)
		}

		if pr == nil {
			m.log.Warnf("no addr info for node: %s", loc)
		}

		tc.Contents = append(tc.Contents, drpc.ContentFetch{
			ID:     c.ID,
			Cid:    c.Cid.CID,
			UserID: c.UserID,
			Peers:  prs,
		})
	}

	return m.sendRPCMessage(ctx, loc, &drpc.Command{
		Op: drpc.CMD_TakeContent,
		Params: drpc.CmdParams{
			TakeContent: tc,
		},
	})
}

func (m *manager) PinContent(ctx context.Context, loc string, cont util.Content, origins []*peer.AddrInfo) error {
	return m.sendRPCMessage(ctx, loc, &drpc.Command{
		Op: drpc.CMD_AddPin,
		Params: drpc.CmdParams{
			AddPin: &drpc.AddPin{
				DBID:   cont.ID,
				UserId: cont.UserID,
				Cid:    cont.Cid.CID,
				Peers:  origins,
			},
		},
	})
}

func (m *manager) CommPContent(ctx context.Context, loc string, data cid.Cid) error {
	return m.sendRPCMessage(ctx, loc, &drpc.Command{
		Op: drpc.CMD_ComputeCommP,
		Params: drpc.CmdParams{
			ComputeCommP: &drpc.ComputeCommP{
				Data: data,
			},
		},
	})
}

func (m *manager) UnpinContent(ctx context.Context, loc string, conts []uint) error {
	return m.sendRPCMessage(ctx, loc, &drpc.Command{
		Op: drpc.CMD_UnpinContent,
		Params: drpc.CmdParams{
			UnpinContent: &drpc.UnpinContent{
				Contents: conts,
			},
		},
	})
}

func (m *manager) AggregateContent(ctx context.Context, loc string, cont util.Content, aggr []drpc.AggregateContent) error {
	return m.sendRPCMessage(ctx, loc, &drpc.Command{
		Op: drpc.CMD_AggregateContent,
		Params: drpc.CmdParams{
			AggregateContent: &drpc.AggregateContents{
				DBID:     cont.ID,
				UserID:   cont.UserID,
				Contents: aggr,
			},
		},
	})
}

func (m *manager) SplitContent(ctx context.Context, loc string, cont uint, size int64) error {
	return m.sendRPCMessage(ctx, loc, &drpc.Command{
		Op: drpc.CMD_SplitContent,
		Params: drpc.CmdParams{
			SplitContent: &drpc.SplitContent{
				Content: cont,
				Size:    size,
			},
		},
	})
}

func (m *manager) handleRpcGarbageCheck(ctx context.Context, handle string, param *drpc.GarbageCheck) error {
	var tounpin []uint
	for _, c := range param.Contents {
		var cont util.Content
		if err := m.db.First(&cont, "id = ?", c).Error; err != nil {
			if xerrors.Is(err, gorm.ErrRecordNotFound) {
				tounpin = append(tounpin, c)
			} else {
				return err
			}
		}

		if cont.Location != handle || cont.Offloaded {
			tounpin = append(tounpin, c)
		}
	}
	return m.UnpinContent(ctx, handle, tounpin)
}

// even though there are 4 pin statuses, queued, pinning, pinned and failed
// the UpdatePinStatus only changes DB state for failed status
// when the content was added, status = pinning
// when the pin process is complete, status = pinned
func (m *manager) handlePinUpdate(location string, contID uint, status types.PinningStatus) error {
	if status == types.PinningStatusFailed {
		m.log.Debugf("updating pin: %d, status: %s, loc: %s", contID, status, location)

		var c util.Content
		if err := m.db.First(&c, "id = ?", contID).Error; err != nil {
			return errors.Wrap(err, "failed to look up content")
		}

		// if content is already active, ignore it
		if c.Active {
			return nil
		}

		// if an aggregate zone is failing, zone is stuck
		// TODO - not sure if this is happening, but we should look (next pr will have a zone status), ignore for now
		if c.Aggregate {
			m.log.Errorf("a zone is stuck, as an aggregate zone: %d, failed to aggregate(pin) on location: %s", c.ID, location)
			return nil
		}

		if err := m.db.Model(util.Content{}).Where("id = ?", contID).UpdateColumns(map[string]interface{}{
			"active":        false,
			"pinning":       false,
			"failed":        true,
			"aggregated_in": 0, // remove from staging zone so the zone can consolidate without it
		}).Error; err != nil {
			m.log.Errorf("failed to mark content as failed in database: %s", err)
			return err
		}
	}
	return nil
}

func (m *manager) handlePinningComplete(ctx context.Context, handle string, pincomp *drpc.PinComplete) error {
	ctx, span := m.tracer.Start(ctx, "handlePinningComplete")
	defer span.End()

	var cont util.Content
	if err := m.db.First(&cont, "id = ?", pincomp.DBID).Error; err != nil {
		return xerrors.Errorf("got shuttle pin complete for unknown content %d (shuttle = %s): %w", pincomp.DBID, handle, err)
	}

	// if content already active, no need to add objects, just update location
	// this is used by consolidated contents
	if cont.Active {
		if err := m.db.Model(util.Content{}).Where("id = ?", cont.ID).UpdateColumns(map[string]interface{}{
			"pinning":  false,
			"location": handle,
		}).Error; err != nil {
			return err
		}
		return nil
	}

	// if content is an aggregate zone
	if cont.Aggregate {
		if len(pincomp.Objects) != 1 {
			return fmt.Errorf("aggregate has more than 1 objects")
		}

		obj := &util.Object{
			Cid:  util.DbCID{CID: pincomp.Objects[0].Cid},
			Size: pincomp.Objects[0].Size,
		}
		if err := m.db.Create(obj).Error; err != nil {
			return xerrors.Errorf("failed to create Object: %w", err)
		}

		if err := m.db.Create(&util.ObjRef{
			Content: cont.ID,
			Object:  obj.ID,
		}).Error; err != nil {
			return xerrors.Errorf("failed to create Object reference: %w", err)
		}

		if err := m.db.Model(util.Content{}).Where("id = ?", cont.ID).UpdateColumns(map[string]interface{}{
			"active":   false, //it will be activated by the staging worker
			"pinning":  false,
			"location": handle,
			"cid":      util.DbCID{CID: pincomp.CID},
			"size":     pincomp.Size,
		}).Error; err != nil {
			return xerrors.Errorf("failed to update content in database: %w", err)
		}
		return nil
	}

	// for individual content pin complete notification
	objects := make([]*util.Object, 0, len(pincomp.Objects))
	for _, o := range pincomp.Objects {
		objects = append(objects, &util.Object{
			Cid:  util.DbCID{CID: o.Cid},
			Size: o.Size,
		})
	}

	if err := m.addObjectsToDatabase(ctx, pincomp.DBID, objects, handle); err != nil {
		return xerrors.Errorf("failed to add objects to database: %w", err)
	}
	return nil
}

// addObjectsToDatabase creates entries on the estuary database for CIDs related to an already pinned CID (`root`)
// These entries are saved on the `objects` table, while metadata about the `root` CID is mostly kept on the `contents` table
// The link between the `objects` and `contents` tables is the `obj_refs` table
func (m *manager) addObjectsToDatabase(ctx context.Context, contID uint, objects []*util.Object, loc string) error {
	_, span := m.tracer.Start(ctx, "addObjectsToDatabase")
	defer span.End()

	if err := m.db.CreateInBatches(objects, 300).Error; err != nil {
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

	if err := m.db.CreateInBatches(refs, 500).Error; err != nil {
		return xerrors.Errorf("failed to create refs: %w", err)
	}

	if err := m.db.Model(util.Content{}).Where("id = ?", contID).UpdateColumns(map[string]interface{}{
		"active":   true,
		"size":     totalSize,
		"pinning":  false,
		"location": loc,
	}).Error; err != nil {
		return xerrors.Errorf("failed to update content in database: %w", err)
	}
	return nil
}

func (m *manager) handleRpcSplitComplete(ctx context.Context, handle string, param *drpc.SplitComplete) error {
	if param.ID == 0 {
		return fmt.Errorf("split complete send with ID = 0")
	}

	// TODO: do some sanity checks that the sub pieces were all made successfully...
	if err := m.db.Model(util.Content{}).Where("id = ?", param.ID).UpdateColumns(map[string]interface{}{
		"dag_split": true,
		"active":    false,
		"size":      0,
	}).Error; err != nil {
		return fmt.Errorf("failed to update content for split complete: %w", err)
	}

	if err := m.db.Delete(&util.ObjRef{}, "content = ?", param.ID).Error; err != nil {
		return fmt.Errorf("failed to delete object references for newly split object: %w", err)
	}
	return nil
}

func (m *manager) handleRpcCommPComplete(ctx context.Context, handle string, resp *drpc.CommPComplete) error {
	_, span := m.tracer.Start(ctx, "handleRpcCommPComplete")
	defer span.End()

	opcr := model.PieceCommRecord{
		Data:    util.DbCID{CID: resp.Data},
		Piece:   util.DbCID{CID: resp.CommP},
		Size:    resp.Size,
		CarSize: resp.CarSize,
	}

	return m.db.Clauses(clause.OnConflict{DoNothing: true}).Create(&opcr).Error
}
