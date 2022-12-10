package shuttle

import (
	"context"
	"fmt"
	"net/url"
	"time"

	"github.com/application-research/estuary/drpc"
	"github.com/application-research/estuary/model"
	"github.com/filecoin-project/go-address"
	"github.com/libp2p/go-libp2p/core/peer"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/net/websocket"
)

type connection struct {
	handle                string
	ctx                   context.Context
	hostname              string
	addrInfo              peer.AddrInfo
	address               address.Address
	private               bool
	contentAddingDisabled bool
	spaceLow              bool
	blockstoreSize        uint64
	blockstoreFree        uint64
	pinCount              int64
	pinQueueLength        int64

	useQueue bool
	cmds     chan *drpc.Command
}

func (m *manager) Connect(ws *websocket.Conn, handle string, done chan struct{}) (func() error, func(), error) {
	var hello drpc.Hello
	if err := websocket.JSON.Receive(ws, &hello); err != nil {
		return nil, nil, err
	}

	if err := websocket.JSON.Send(ws, &drpc.Hi{Handle: handle}); err != nil {
		return nil, nil, err
	}

	_, err := url.Parse(hello.Host)
	if err != nil {
		m.log.Errorf("shuttle had invalid hostname %q: %s", hello.Host, err)
		hello.Host = ""
	}

	if err := m.db.Model(model.Shuttle{}).Where("handle = ?", handle).UpdateColumns(map[string]interface{}{
		"host":            hello.Host,
		"peer_id":         hello.AddrInfo.ID.String(),
		"last_connection": time.Now(),
		"private":         hello.Private,
	}).Error; err != nil {
		return nil, nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())

	sc := &connection{
		handle:                handle,
		address:               hello.Address,
		addrInfo:              hello.AddrInfo,
		hostname:              hello.Host,
		cmds:                  make(chan *drpc.Command, m.cfg.RPCMessage.OutgoingQueueSize),
		ctx:                   ctx,
		private:               hello.Private,
		contentAddingDisabled: hello.ContentAddingDisabled,
		useQueue:              hello.UseQueue,
	}

	m.shuttlesLk.Lock()
	m.shuttles[handle] = sc
	m.shuttlesLk.Unlock()

	closeConn := func() {
		cancel()
		m.shuttlesLk.Lock()
		outd, ok := m.shuttles[handle]
		if ok {
			if outd == sc {
				delete(m.shuttles, handle)
			}
		}
		m.shuttlesLk.Unlock()
	}

	writeWebsocket := func() {
		for {
			select {
			case msg := <-sc.cmds:
				// Write
				err := websocket.JSON.Send(ws, msg)
				if err != nil {
					m.log.Errorf("failed to write command to shuttle: %s", err)
					return
				}
			case <-done:
				return
			}
		}
	}

	readWebsocket := func() error {
		var msg *drpc.Message
		if err := websocket.JSON.Receive(ws, &msg); err != nil {
			return err
		}
		msg.Handle = handle
		go func() {
			m.rpcWebsocket <- msg
		}()
		return nil
	}

	go writeWebsocket()

	return readWebsocket, closeConn, nil
}

func (m *manager) runWebsocketQueueProcessingWorkers(ctx context.Context, numHandlers int) {
	for i := 1; i <= numHandlers; i++ {
		go func() {
			for {
				select {
				case <-ctx.Done():
					return
				case msg := <-m.rpcWebsocket:
					if err := m.processMessage(msg); err != nil {
						m.log.Errorf("failed to process message from shuttle: %s", err)
					}
				}
			}
		}()
	}
}

func (m *manager) processMessage(msg *drpc.Message) error {
	ctx := context.TODO()

	// if the message contains a trace continue it here.
	if msg.HasTraceCarrier() {
		if sc := msg.TraceCarrier.AsSpanContext(); sc.IsValid() {
			ctx = trace.ContextWithRemoteSpanContext(ctx, sc)
		}
	}
	ctx, span := m.tracer.Start(ctx, "processShuttleMessage")
	defer span.End()

	m.log.Debugf("handling rpc message: %s, from shuttle: %s", msg.Op, msg.Handle)

	switch msg.Op {
	case drpc.OP_UpdatePinStatus:
		ups := msg.Params.UpdatePinStatus
		if ups == nil {
			return ErrNilParams
		}
		return m.handlePinUpdate(msg.Handle, ups.DBID, ups.Status)
	case drpc.OP_PinComplete:
		param := msg.Params.PinComplete
		if param == nil {
			return ErrNilParams
		}

		if err := m.handlePinningComplete(ctx, msg.Handle, param); err != nil {
			m.log.Errorw("handling pin complete message failed", "shuttle", msg.Handle, "err", err)
		}
		return nil
	case drpc.OP_CommPComplete:
		param := msg.Params.CommPComplete
		if param == nil {
			return ErrNilParams
		}

		if err := m.handleRpcCommPComplete(ctx, msg.Handle, param); err != nil {
			m.log.Errorf("handling commp complete message from shuttle %s: %s", msg.Handle, err)
		}
		return nil
	case drpc.OP_TransferStarted:
		param := msg.Params.TransferStarted
		if param == nil {
			return ErrNilParams
		}

		if err := m.handleRpcTransferStarted(ctx, msg.Handle, param); err != nil {
			m.log.Errorf("handling transfer started message from shuttle %s: %s", msg.Handle, err)
		}
		return nil
	case drpc.OP_TransferFinished:
		param := msg.Params.TransferFinished
		if param == nil {
			return ErrNilParams
		}

		if err := m.handleRpcTransferFinished(ctx, msg.Handle, param); err != nil {
			m.log.Errorf("handling transfer finished message from shuttle %s: %s", msg.Handle, err)
		}
		return nil
	case drpc.OP_TransferStatus:
		param := msg.Params.TransferStatus
		if param == nil {
			return ErrNilParams
		}

		if err := m.HandleRpcTransferStatus(ctx, msg.Handle, param); err != nil {
			m.log.Errorf("handling transfer status message from shuttle %s: %s", msg.Handle, err)
		}
		return nil
	case drpc.OP_ShuttleUpdate:
		param := msg.Params.ShuttleUpdate
		if param == nil {
			return ErrNilParams
		}

		if err := m.handleRpcShuttleUpdate(ctx, msg.Handle, param); err != nil {
			m.log.Errorf("handling shuttle update message from shuttle %s: %s", msg.Handle, err)
		}
		return nil
	case drpc.OP_GarbageCheck:
		param := msg.Params.GarbageCheck
		if param == nil {
			return ErrNilParams
		}

		if err := m.handleRpcGarbageCheck(ctx, msg.Handle, param); err != nil {
			m.log.Errorf("handling garbage check message from shuttle %s: %s", msg.Handle, err)
		}
		return nil
	case drpc.OP_SplitComplete:
		param := msg.Params.SplitComplete
		if param == nil {
			return ErrNilParams
		}

		if err := m.handleRpcSplitComplete(ctx, msg.Handle, param); err != nil {
			m.log.Errorf("handling split complete message from shuttle %s: %s", msg.Handle, err)
		}
		return nil
	case drpc.OP_SanityCheck:
		sc := msg.Params.SanityCheck
		if sc == nil {
			return ErrNilParams
		}
		m.sanityCheckMgr.HandleMissingBlocks(sc.CID, sc.ErrMsg)
		return nil
	default:
		return fmt.Errorf("unrecognized message op: %q", msg.Op)
	}
}
