package websocket

import (
	"context"
	"fmt"
	"net/url"
	"sync"
	"time"

	"github.com/application-research/estuary/model"
	rpcevent "github.com/application-research/estuary/shuttle/rpc/event"
	"github.com/application-research/estuary/shuttle/rpc/types"
	"github.com/filecoin-project/go-address"
	"github.com/libp2p/go-libp2p-core/peer"
	"golang.org/x/net/websocket"

	"github.com/application-research/estuary/config"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

var ErrNoShuttleConnection = fmt.Errorf("no connection to requested shuttle")

type Connection struct {
	Handle                string
	Ctx                   context.Context
	Hostname              string
	AddrInfo              peer.AddrInfo
	Address               address.Address
	Private               bool
	ContentAddingDisabled bool
	SpaceLow              bool
	BlockstoreSize        uint64
	BlockstoreFree        uint64
	PinCount              int64
	PinQueueLength        int64
	QueueEngEnabled       bool
	cmds                  chan *rpcevent.Command
}

type IEstuaryRpcEngine interface {
	Connect(ws *websocket.Conn, handle string, done chan struct{}) (func() error, func(), error)
	GetShuttleConnections() []*Connection
	GetShuttleConnection(handle string) (*Connection, bool)
}

type manager struct {
	db           *gorm.DB
	cfg          *config.Estuary
	log          *zap.SugaredLogger
	shuttlesLk   sync.Mutex
	shuttles     map[string]*Connection
	rpcWebsocket chan *rpcevent.Message
}

func NewEstuaryRpcEngine(ctx context.Context, db *gorm.DB, cfg *config.Estuary, log *zap.SugaredLogger, handlerFn types.MessageHandlerFn) IEstuaryRpcEngine {
	wbsMgr := &manager{
		db:           db,
		cfg:          cfg,
		log:          log,
		shuttles:     make(map[string]*Connection, 0),
		rpcWebsocket: make(chan *rpcevent.Message, cfg.RpcEngine.Websocket.IncomingQueueSize),
	}

	go wbsMgr.runWebsocketQueueProcessingWorkers(ctx, cfg.RpcEngine.Websocket.QueueHandlers, handlerFn)

	return wbsMgr
}

func (m *manager) Connect(ws *websocket.Conn, handle string, done chan struct{}) (func() error, func(), error) {
	var hello rpcevent.Hello
	if err := websocket.JSON.Receive(ws, &hello); err != nil {
		return nil, nil, err
	}

	// tell shuttle if api support queue engine
	if err := websocket.JSON.Send(ws, &rpcevent.Hi{QueueEngEnabled: m.cfg.RpcEngine.Queue.Enabled}); err != nil {
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

	sc := &Connection{
		Handle:                handle,
		Address:               hello.Address,
		AddrInfo:              hello.AddrInfo,
		Hostname:              hello.Host,
		cmds:                  make(chan *rpcevent.Command, m.cfg.RpcEngine.Websocket.OutgoingQueueSize),
		Ctx:                   ctx,
		Private:               hello.Private,
		ContentAddingDisabled: hello.ContentAddingDisabled,
		QueueEngEnabled:       hello.QueueEngEnabled,
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
		var msg *rpcevent.Message
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

func (sc *Connection) SendMessage(ctx context.Context, cmd *rpcevent.Command) error {
	select {
	case sc.cmds <- cmd:
		return nil
	case <-sc.Ctx.Done():
		return ErrNoShuttleConnection
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (m *manager) GetShuttleConnection(handle string) (*Connection, bool) {
	m.shuttlesLk.Lock()
	defer m.shuttlesLk.Unlock()
	conn, isConnected := m.shuttles[handle]
	return conn, isConnected
}

func (m *manager) GetShuttleConnections() []*Connection {
	m.shuttlesLk.Lock()
	defer m.shuttlesLk.Unlock()

	shts := make([]*Connection, 0)
	for _, sh := range m.shuttles {
		shts = append(shts, sh)
	}
	return shts
}

func (m *manager) runWebsocketQueueProcessingWorkers(ctx context.Context, numHandlers int, handlerFn types.MessageHandlerFn) {
	for i := 1; i <= numHandlers; i++ {
		go func() {
			for {
				select {
				case <-ctx.Done():
					return
				case msg := <-m.rpcWebsocket:
					if err := handlerFn(msg, "websocket"); err != nil {
						m.log.Errorf("failed to process message from shuttle: %s", err)
					}
				}
			}
		}()
	}
}
