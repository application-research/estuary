package main

import (
	"context"
	"fmt"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"github.com/libp2p/go-libp2p-core/peer"
	drpc "github.com/whyrusleeping/estuary/drpc"
	"github.com/whyrusleeping/estuary/util"
)

type Shuttle struct {
	gorm.Model

	Handle string `gorm:"unique"`
	Token  string

	LastConnection time.Time
	Host           string
	PeerID         string

	Open bool
}

type shuttleConnection struct {
	handle string

	addrInfo peer.AddrInfo

	cmds    chan *drpc.Command
	closing chan struct{}
}

func (dc *shuttleConnection) sendMessage(ctx context.Context, cmd *drpc.Command) error {
	select {
	case dc.cmds <- cmd:
		return nil
	case <-dc.closing:
		return ErrNoShuttleConnection
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (cm *ContentManager) registerShuttleConnection(handle string, hello *drpc.Hello) (chan *drpc.Command, func(), error) {
	cm.shuttlesLk.Lock()
	defer cm.shuttlesLk.Unlock()
	_, ok := cm.shuttles[handle]
	if ok {
		log.Warn("registering shuttle but found existing connection")
		return nil, nil, fmt.Errorf("shuttle already connected")
	}

	d := &shuttleConnection{
		handle:   handle,
		addrInfo: hello.AddrInfo,
		cmds:     make(chan *drpc.Command, 32),
		closing:  make(chan struct{}),
	}

	cm.shuttles[handle] = d

	return d.cmds, func() {
		close(d.closing)
		cm.shuttlesLk.Lock()
		outd, ok := cm.shuttles[handle]
		if ok {
			if outd == d {
				delete(cm.shuttles, handle)
			}
		}
		cm.shuttlesLk.Unlock()

	}, nil
}

var ErrNilParams = fmt.Errorf("shuttle message had nil params")

func (cm *ContentManager) processShuttleMessage(handle string, msg *drpc.Message) error {
	ctx, span := cm.tracer.Start(context.TODO(), "processShuttleMessage")
	defer span.End()

	switch msg.Op {
	case drpc.OP_UpdatePinStatus:
		ups := msg.Params.UpdatePinStatus
		if ups == nil {
			return ErrNilParams
		}
		cm.UpdatePinStatus(handle, ups.DBID, ups.Status)
		return nil
	case drpc.OP_PinComplete:
		param := msg.Params.PinComplete
		if param == nil {
			return ErrNilParams
		}

		if err := cm.handlePinningComplete(ctx, handle, param); err != nil {
			log.Errorw("handling pin complete message failed", "shuttle", handle, "err", err)
		}
		return nil
	case drpc.OP_CommPComplete:
		param := msg.Params.CommPComplete
		if param == nil {
			return ErrNilParams
		}

		if err := cm.handleRpcCommPComplete(ctx, handle, param); err != nil {
			log.Errorf("handling commp complete message from shuttle %s: %s", handle, err)
		}
		return nil
	default:
		return fmt.Errorf("unrecognized message op: %q", msg.Op)
	}
}

var ErrNoShuttleConnection = fmt.Errorf("no connection to requested shuttle")

func (cm *ContentManager) sendShuttleCommand(ctx context.Context, handle string, cmd *drpc.Command) error {
	cm.shuttlesLk.Lock()
	d, ok := cm.shuttles[handle]
	cm.shuttlesLk.Unlock()
	if ok {
		return d.sendMessage(ctx, cmd)
	}

	return ErrNoShuttleConnection
}

func (cm *ContentManager) shuttleIsOnline(handle string) bool {
	cm.shuttlesLk.Lock()
	d, ok := cm.shuttles[handle]
	cm.shuttlesLk.Unlock()
	if !ok {
		return false
	}

	select {
	case <-d.closing:
		return false
	default:
		return true
	}
}

func (cm *ContentManager) handleRpcCommPComplete(ctx context.Context, handle string, resp *drpc.CommPComplete) error {
	ctx, span := cm.tracer.Start(ctx, "handleRpcCommPComplete")
	defer span.End()

	opcr := PieceCommRecord{
		Data:  util.DbCID{resp.Data},
		Piece: util.DbCID{resp.CommP},
		Size:  resp.Size,
	}

	if err := cm.DB.Clauses(clause.OnConflict{DoNothing: true}).Create(&opcr).Error; err != nil {
		return err
	}

	return nil
}
