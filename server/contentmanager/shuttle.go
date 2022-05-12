package server

import (
	"context"
	"time"

	"gorm.io/gorm"

	drpc "github.com/application-research/estuary/drpc"
	"github.com/filecoin-project/go-address"
	"github.com/libp2p/go-libp2p-core/peer"
)

type Shuttle struct {
	gorm.Model

	Handle string `gorm:"unique"`
	Token  string

	LastConnection time.Time
	Host           string
	PeerID         string

	Private bool

	Open bool

	Priority int
}

type ShuttleConnection struct {
	handle  string
	cmds    chan *drpc.Command
	closing chan struct{}

	hostname string
	addrInfo peer.AddrInfo
	address  address.Address

	private bool

	spaceLow       bool
	blockstoreSize uint64
	blockstoreFree uint64
	pinCount       int64
	pinQueueLength int64
}

func (dc *ShuttleConnection) sendMessage(ctx context.Context, cmd *drpc.Command) error {
	select {
	case dc.cmds <- cmd:
		return nil
	case <-dc.closing:
		return ErrNoShuttleConnection
	case <-ctx.Done():
		return ctx.Err()
	}
}
