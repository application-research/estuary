package server

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/application-research/estuary/constants"
	"github.com/application-research/estuary/drpc"
	"github.com/application-research/estuary/pinner"
	"github.com/application-research/estuary/types"
	"github.com/application-research/estuary/util"
	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p-core/peer"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/xerrors"
	"gorm.io/gorm/clause"
)

func (cm *ContentManager) updatePinStatus(handle string, cont uint, status string) {
	cm.pinLk.Lock()
	op, ok := cm.pinJobs[cont]
	cm.pinLk.Unlock()
	if !ok {
		log.Warnw("got pin status update for unknown content", "content", cont, "status", status, "shuttle", handle)
		return
	}

	op.SetStatus(status)
	if status == "failed" {
		var c util.Content
		if err := cm.DB.First(&c, "id = ?", cont).Error; err != nil {
			log.Errorf("failed to look up content: %s", err)
			return
		}

		if c.Active {
			log.Errorf("got failed pin status message from shuttle %s where content(%d) was already active, refusing to do anything", handle, cont)
			return
		}

		if err := cm.DB.Model(util.Content{}).Where("id = ?", cont).UpdateColumns(map[string]interface{}{
			"active":  false,
			"pinning": false,
			"failed":  true,
		}).Error; err != nil {
			log.Errorf("failed to mark content as failed in database: %s", err)
		}
	}
}

func (cm *ContentManager) handlePinningComplete(ctx context.Context, handle string, pincomp *drpc.PinComplete) error {
	ctx, span := cm.Tracer.Start(ctx, "handlePinningComplete")
	defer span.End()

	var cont util.Content
	if err := cm.DB.First(&cont, "id = ?", pincomp.DBID).Error; err != nil {
		return xerrors.Errorf("got shuttle pin complete for unknown content %d (shuttle = %s): %w", pincomp.DBID, handle, err)
	}

	if cont.Active {
		// content already active, no need to add objects, just update location
		if err := cm.DB.Model(util.Content{}).Where("id = ?", cont.ID).UpdateColumns(map[string]interface{}{
			"location": handle,
		}).Error; err != nil {
			return err
		}

		// TODO: should we recheck the staging zones?
		return nil
	}

	if cont.Aggregate {
		if err := cm.DB.Model(util.Content{}).Where("id = ?", cont.ID).UpdateColumns(map[string]interface{}{
			"active":   true,
			"pinning":  false,
			"location": handle,
		}).Error; err != nil {
			return xerrors.Errorf("failed to update content in database: %w", err)
		}
		return nil
	}

	objects := make([]*util.Object, 0, len(pincomp.Objects))
	for _, o := range pincomp.Objects {
		objects = append(objects, &util.Object{
			Cid:  util.DbCID{o.Cid},
			Size: o.Size,
		})
	}

	if err := cm.addObjectsToDatabase(ctx, pincomp.DBID, nil, cid.Cid{}, objects, handle); err != nil {
		return xerrors.Errorf("failed to add objects to database: %w", err)
	}

	cm.ToCheck <- cont.ID

	return nil
}

func (cm *ContentManager) PinStatus(cont util.Content) (*types.IpfsPinStatus, error) {
	cm.pinLk.Lock()
	po, ok := cm.pinJobs[cont.ID]
	cm.pinLk.Unlock()
	if !ok {
		var meta map[string]interface{}
		if cont.PinMeta != "" {
			if err := json.Unmarshal([]byte(cont.PinMeta), &meta); err != nil {
				log.Warnf("content %d has invalid pinmeta: %s", cont, err)
			}
		}

		ps := &types.IpfsPinStatus{
			Requestid: fmt.Sprintf("%d", cont.ID),
			Status:    "pinning",
			Created:   cont.CreatedAt,
			Pin: types.IpfsPin{
				Cid:  cont.Cid.CID.String(),
				Name: cont.Name,
				Meta: meta,
			},
			Delegates: cm.PinDelegatesForContent(cont),
			Info:      nil, // TODO: all sorts of extra info we could add...
		}

		if cont.Active {
			ps.Status = "pinned"
		}
		if cont.Failed {
			ps.Status = "failed"
		}

		return ps, nil
	}

	status := po.PinStatus()
	status.Delegates = cm.PinDelegatesForContent(cont)

	return status, nil
}

func (cm *ContentManager) PinDelegatesForContent(cont util.Content) []string {
	if cont.Location == "local" {
		var out []string
		for _, a := range cm.Host.Addrs() {
			out = append(out, fmt.Sprintf("%s/p2p/%s", a, cm.Host.ID()))
		}

		return out
	} else {
		ai, err := cm.addrInfoForShuttle(cont.Location)
		if err != nil {
			log.Errorf("failed to get address info for shuttle %q: %s", cont.Location, err)
			return nil
		}

		if ai == nil {
			log.Warnf("no address info for shuttle %s: %s", cont.Location, err)
			return nil
		}

		var out []string
		for _, a := range ai.Addrs {
			out = append(out, fmt.Sprintf("%s/p2p/%s", a, ai.ID))
		}
		return out
	}
}

func (cm *ContentManager) RefreshPinQueue() error {
	var toPin []util.Content
	if err := cm.DB.Find(&toPin, "active = false and pinning = true and not aggregate").Error; err != nil {
		return err
	}

	// TODO: this doesnt persist the replacement directives, so a queued
	// replacement, if ongoing during a restart of the node, will still
	// complete the pin when the process comes back online, but it wont delete
	// the old pin.
	// Need to fix this, probably best option is just to add a 'replace' field
	// to content, could be interesting to see the graph of replacements
	// anyways
	for _, c := range toPin {
		if c.Location == "local" {
			cm.addPinToQueue(c, nil, 0)
		} else {
			if err := cm.PinContentOnShuttle(context.TODO(), c, nil, 0, c.Location); err != nil {
				log.Errorf("failed to send pin message to shuttle: %s", err)
				time.Sleep(time.Millisecond * 100)
			}
		}
	}

	return nil
}

func (cm *ContentManager) PinContent(ctx context.Context, user uint, obj cid.Cid, name string, cols []*util.CollectionRef, peers []peer.AddrInfo, replace uint, meta map[string]interface{}) (*types.IpfsPinStatus, error) {
	loc, err := cm.SelectLocationForContent(ctx, obj, user)
	if err != nil {
		return nil, xerrors.Errorf("selecting location for content failed: %w", err)
	}

	var metab string
	if meta != nil {
		b, err := json.Marshal(meta)
		if err != nil {
			return nil, err
		}
		metab = string(b)
	}

	cont := util.Content{
		Cid: util.DbCID{obj},

		Name:        name,
		UserID:      user,
		Active:      false,
		Replication: constants.DefaultReplication,

		Pinning: true,
		PinMeta: metab,

		Location: loc,

		/*
			Size        int64  `json:"size"`
			Offloaded   bool   `json:"offloaded"`
		*/

	}
	if err := cm.DB.Create(&cont).Error; err != nil {
		return nil, err
	}

	if len(cols) > 0 {
		for _, c := range cols {
			c.Content = cont.ID
		}

		if err := cm.DB.Clauses(clause.OnConflict{
			Columns:   []clause.Column{{Name: "path"}, {Name: "collection"}},
			DoUpdates: clause.AssignmentColumns([]string{"created_at", "content"}),
		}).Create(cols).Error; err != nil {
			return nil, err
		}
	}

	if loc == "local" {
		cm.addPinToQueue(cont, peers, replace)
	} else {
		if err := cm.PinContentOnShuttle(ctx, cont, peers, replace, loc); err != nil {
			return nil, err
		}
	}

	return cm.PinStatus(cont)
}

func (cm *ContentManager) addPinToQueue(cont util.Content, peers []peer.AddrInfo, replace uint) {
	if cont.Location != "local" {
		log.Errorf("calling addPinToQueue on non-local content")
	}

	op := &pinner.PinningOperation{
		ContId:   cont.ID,
		UserId:   cont.UserID,
		Obj:      cont.Cid.CID,
		Name:     cont.Name,
		Peers:    peers,
		Started:  cont.CreatedAt,
		Status:   "queued",
		Replace:  replace,
		Location: cont.Location,
	}

	cm.pinLk.Lock()
	// TODO: check if we are overwriting anything here
	cm.pinJobs[cont.ID] = op
	cm.pinLk.Unlock()

	cm.PinMgr.Add(op)
}

func (cm *ContentManager) PinContentOnShuttle(ctx context.Context, cont util.Content, peers []peer.AddrInfo, replace uint, handle string) error {
	ctx, span := cm.Tracer.Start(ctx, "PinContentOnShuttle", trace.WithAttributes(
		attribute.String("handle", handle),
		attribute.String("CID", cont.Cid.CID.String()),
	))
	defer span.End()

	if err := cm.SendShuttleCommand(ctx, handle, &drpc.Command{
		Op: drpc.CMD_AddPin,
		Params: drpc.CmdParams{
			AddPin: &drpc.AddPin{
				DBID:   cont.ID,
				UserId: cont.UserID,
				Cid:    cont.Cid.CID,
				Peers:  peers,
			},
		},
	}); err != nil {
		return err
	}

	op := &pinner.PinningOperation{
		ContId:   cont.ID,
		UserId:   cont.UserID,
		Obj:      cont.Cid.CID,
		Name:     cont.Name,
		Peers:    peers,
		Started:  cont.CreatedAt,
		Status:   "queued",
		Replace:  replace,
		Location: handle,
	}

	cm.pinLk.Lock()
	// TODO: check if we are overwriting anything here
	cm.pinJobs[cont.ID] = op
	cm.pinLk.Unlock()

	return nil
}
