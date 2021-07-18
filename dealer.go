package main

import (
	"context"
	"fmt"
	"time"

	"gorm.io/gorm"

	drpc "github.com/whyrusleeping/estuary/drpc"
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

func (s *Server) registerDealerConnection(handle string, hello *drpc.Hello) (chan *drpc.Command, func(), error) {
	s.dealersLk.Lock()
	defer s.dealersLk.Unlock()
	_, ok := s.dealers[handle]
	if ok {
		log.Warn("registering dealer but found existing connection")
		return nil, nil, fmt.Errorf("dealer already connected")
	}

	d := &dealerConnection{
		handle:  handle,
		cmds:    make(chan *drpc.Command, 32),
		closing: make(chan struct{}),
	}

	s.dealers[handle] = d

	return d.cmds, func() {
		close(d.closing)
		s.dealersLk.Lock()
		outd, ok := s.dealers[handle]
		if ok {
			if outd == d {
				delete(s.dealers, handle)
			}
		}
		s.dealersLk.Unlock()

	}, nil
}

var ErrNilParams = fmt.Errorf("dealer message had nil params")

func (s *Server) processDealerMessage(handle string, msg *drpc.Message) error {
	ctx, span := s.tracer.Start(context.TODO(), "processDealerMessage")
	defer span.End()

	switch msg.Op {
	case drpc.OP_UpdatePinStatus:
		ups := msg.Params.UpdatePinStatus
		if ups == nil {
			return ErrNilParams
		}
		s.UpdatePinStatus(handle, ups.DBID, ups.Status)
		return nil
	case drpc.OP_PinComplete:
		param := msg.Params.PinComplete
		if param == nil {
			return ErrNilParams
		}

		s.handlePinningComplete(ctx, handle, param)
		return nil
	default:
		return fmt.Errorf("unrecognized message op: %q", msg.Op)
	}
}

var ErrNoDealerConnection = fmt.Errorf("no connection to requested dealer")

func (s *Server) sendDealerCommand(ctx context.Context, handle string, cmd *drpc.Command) error {
	s.dealersLk.Lock()
	d, ok := s.dealers[handle]
	s.dealersLk.Unlock()
	if ok {
		return d.sendMessage(ctx, cmd)
	}

	return ErrNoDealerConnection
}

func (s *Server) dealerIsOnline(handle string) bool {
	s.dealersLk.Lock()
	d, ok := s.dealers[handle]
	s.dealersLk.Unlock()
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
