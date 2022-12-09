package shuttle

import (
	"context"
	"net/url"
	"time"

	"github.com/application-research/estuary/drpc"
	"github.com/application-research/estuary/model"
	"github.com/filecoin-project/go-address"
	"github.com/libp2p/go-libp2p/core/peer"
)

type ShuttleConnection struct {
	Handle                string
	ctx                   context.Context
	Hostname              string
	addrInfo              peer.AddrInfo
	address               address.Address
	private               bool
	ContentAddingDisabled bool
	spaceLow              bool
	blockstoreSize        uint64
	blockstoreFree        uint64
	pinCount              int64
	pinQueueLength        int64

	rcpQueue string
	cmds     chan *drpc.Command
}

func (sc *ShuttleConnection) sendMessage(ctx context.Context, cmd *drpc.Command) error {
	select {
	case sc.cmds <- cmd:
		return nil
	case <-sc.ctx.Done():
		return ErrNoShuttleConnection
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (cm *Manager) ConnectShuttle(handle string, hello *drpc.Hello) (chan *drpc.Command, func(), error) {
	cm.ShuttlesLk.Lock()
	defer cm.ShuttlesLk.Unlock()
	_, ok := cm.Shuttles[handle]
	if ok {
		return nil, nil, nil
	}

	_, err := url.Parse(hello.Host)
	if err != nil {
		cm.log.Errorf("shuttle had invalid hostname %q: %s", hello.Host, err)
		hello.Host = ""
	}

	if err := cm.db.Model(model.Shuttle{}).Where("handle = ?", handle).UpdateColumns(map[string]interface{}{
		"host":            hello.Host,
		"peer_id":         hello.AddrInfo.ID.String(),
		"last_connection": time.Now(),
		"private":         hello.Private,
	}).Error; err != nil {
		return nil, nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())

	sc := &ShuttleConnection{
		Handle:                handle,
		address:               hello.Address,
		addrInfo:              hello.AddrInfo,
		Hostname:              hello.Host,
		cmds:                  make(chan *drpc.Command, cm.cfg.RPCMessage.OutgoingQueueSize),
		ctx:                   ctx,
		private:               hello.Private,
		ContentAddingDisabled: hello.ContentAddingDisabled,
		rcpQueue:              hello.RPCQueue,
	}

	cm.Shuttles[handle] = sc

	return sc.cmds, func() {
		cancel()
		cm.ShuttlesLk.Lock()
		outd, ok := cm.Shuttles[handle]
		if ok {
			if outd == sc {
				delete(cm.Shuttles, handle)
			}
		}
		cm.ShuttlesLk.Unlock()
	}, nil
}
