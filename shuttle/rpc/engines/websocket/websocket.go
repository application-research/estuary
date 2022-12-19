package websocket

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"sync"
	"time"

	"github.com/application-research/estuary/model"
	rpcevent "github.com/application-research/estuary/shuttle/rpc/event"
	"github.com/application-research/estuary/shuttle/rpc/types"
	"github.com/application-research/estuary/util"
	"github.com/labstack/echo/v4"
	gwebsocket "golang.org/x/net/websocket"

	"github.com/application-research/estuary/config"
	"go.uber.org/zap"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

var ErrNoShuttleConnection = fmt.Errorf("no connection to requested shuttle")

type Connection struct {
	Handle string
	Ctx    context.Context
	cmds   chan *rpcevent.Command
}

type IEstuaryRpcEngine interface {
	Connect(c echo.Context, handle string, done chan struct{}) error
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

func (m *manager) Connect(c echo.Context, handle string, done chan struct{}) error {
	gwebsocket.Handler(func(ws *gwebsocket.Conn) {
		ws.MaxPayloadBytes = 128 << 20
		defer ws.Close()
		defer close(done)

		var helloBytes []byte
		if err := gwebsocket.Message.Receive(ws, &helloBytes); err != nil {
			return
		}

		var hello rpcevent.Hello
		if err := json.Unmarshal(helloBytes, &hello); err != nil {
			return
		}

		if _, err := url.Parse(hello.Host); err != nil {
			m.log.Errorf("shuttle had invalid hostname %q: %s", hello.Host, err)
			hello.Host = ""
		}

		s := &model.ShuttleConnection{
			Handle:                handle,
			Address:               util.DbAddr{Addr: hello.Address},
			AddrInfo:              util.DbAddrInfo{AddrInfo: hello.AddrInfo},
			Hostname:              hello.Host,
			Private:               hello.Private,
			ContentAddingDisabled: hello.ContentAddingDisabled,
			QueueEngEnabled:       hello.QueueEngEnabled,
			UpdatedAt:             time.Now().UTC(),
		}

		if err := m.db.Clauses(&clause.OnConflict{
			Columns:   []clause.Column{{Name: "handle"}},
			DoUpdates: clause.AssignmentColumns([]string{"address", "addr_info", "hostname", "private", "content_adding_disabled", "queue_eng_enabled"}),
		}).Create(&s).Error; err != nil {
			return
		}

		if err := m.db.Model(model.Shuttle{}).Where("handle = ?", handle).UpdateColumns(map[string]interface{}{
			"host":            hello.Host,
			"peer_id":         hello.AddrInfo.ID.String(),
			"last_connection": time.Now(),
			"private":         hello.Private,
		}).Error; err != nil {
			return
		}

		ctx, cancel := context.WithCancel(context.Background())

		sc := &Connection{
			cmds: make(chan *rpcevent.Command, m.cfg.RpcEngine.Websocket.OutgoingQueueSize),
			Ctx:  ctx,
		}

		m.shuttlesLk.Lock()
		m.shuttles[handle] = sc
		m.shuttlesLk.Unlock()

		// clean up on exit
		defer func() {
			cancel()
			m.shuttlesLk.Lock()
			outd, ok := m.shuttles[handle]
			if ok {
				if outd == sc {
					delete(m.shuttles, handle)
				}
			}
			m.shuttlesLk.Unlock()
		}()

		// write to shuttles
		go func() {
			for {
				select {
				case msg := <-sc.cmds:
					go func() {
						msgBytes, err := json.Marshal(msg)
						if err != nil {
							m.log.Errorf("failed to serialize message: %s", err)
							return
						}

						if err = gwebsocket.Message.Send(ws, msgBytes); err != nil {
							m.log.Errorf("failed to write command to shuttle: %s", err)
							return
						}
					}()
				case <-done:
					return
				}
			}
		}()

		readWebsocket := func() error {
			var msgBytes []byte
			if err := gwebsocket.Message.Receive(ws, &msgBytes); err != nil {
				return err
			}

			var msg *rpcevent.Message
			if err := json.Unmarshal(msgBytes, &msg); err != nil {
				return err
			}

			msg.Handle = handle
			go func() {
				m.rpcWebsocket <- msg
			}()
			return nil
		}

		for {
			if err := readWebsocket(); err != nil {
				m.log.Errorf("failed to read message from shuttle: %s, %s", handle, err)
				return
			}
		}
	}).ServeHTTP(c.Response(), c.Request())
	return nil
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
