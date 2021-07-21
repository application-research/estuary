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

type Dealer struct {
	gorm.Model

	Handle string `gorm:"unique"`
	Token  string

	LastConnection time.Time
	Host           string
	PeerID         string

	Open bool
}

type dealerConnection struct {
	handle string

	addrInfo peer.AddrInfo

	cmds    chan *drpc.Command
	closing chan struct{}
}

func (dc *dealerConnection) sendMessage(ctx context.Context, cmd *drpc.Command) error {
	select {
	case dc.cmds <- cmd:
		return nil
	case <-dc.closing:
		return ErrNoDealerConnection
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (cm *ContentManager) registerDealerConnection(handle string, hello *drpc.Hello) (chan *drpc.Command, func(), error) {
	cm.dealersLk.Lock()
	defer cm.dealersLk.Unlock()
	_, ok := cm.dealers[handle]
	if ok {
		log.Warn("registering dealer but found existing connection")
		return nil, nil, fmt.Errorf("dealer already connected")
	}

	d := &dealerConnection{
		handle:   handle,
		addrInfo: hello.AddrInfo,
		cmds:     make(chan *drpc.Command, 32),
		closing:  make(chan struct{}),
	}

	cm.dealers[handle] = d

	return d.cmds, func() {
		close(d.closing)
		cm.dealersLk.Lock()
		outd, ok := cm.dealers[handle]
		if ok {
			if outd == d {
				delete(cm.dealers, handle)
			}
		}
		cm.dealersLk.Unlock()

	}, nil
}

var ErrNilParams = fmt.Errorf("dealer message had nil params")

func (cm *ContentManager) processDealerMessage(handle string, msg *drpc.Message) error {
	ctx, span := cm.tracer.Start(context.TODO(), "processDealerMessage")
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
			log.Errorw("handling pin complete message failed", "dealer", handle, "err", err)
		}
		return nil
	case drpc.OP_CommPComplete:
		param := msg.Params.CommPComplete
		if param == nil {
			return ErrNilParams
		}

		if err := cm.handleRpcCommPComplete(ctx, handle, param); err != nil {
			log.Errorf("handling commp complete message from dealer %s: %s", handle, err)
		}
		return nil
	default:
		return fmt.Errorf("unrecognized message op: %q", msg.Op)
	}
}

var ErrNoDealerConnection = fmt.Errorf("no connection to requested dealer")

func (cm *ContentManager) sendDealerCommand(ctx context.Context, handle string, cmd *drpc.Command) error {
	cm.dealersLk.Lock()
	d, ok := cm.dealers[handle]
	cm.dealersLk.Unlock()
	if ok {
		return d.sendMessage(ctx, cmd)
	}

	return ErrNoDealerConnection
}

func (cm *ContentManager) dealerIsOnline(handle string) bool {
	cm.dealersLk.Lock()
	d, ok := cm.dealers[handle]
	cm.dealersLk.Unlock()
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
