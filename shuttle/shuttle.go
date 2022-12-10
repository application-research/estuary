package shuttle

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"sync"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
	"golang.org/x/net/websocket"
	"gorm.io/gorm"

	"github.com/application-research/estuary/config"
	"github.com/application-research/estuary/constants"
	shuttlequeue "github.com/application-research/estuary/shuttle/queue"

	contentqueue "github.com/application-research/estuary/content/queue"
	dealstatus "github.com/application-research/estuary/deal/status"
	"github.com/application-research/estuary/model"
	"github.com/application-research/estuary/sanitycheck"
	"github.com/application-research/filclient"
	datatransfer "github.com/filecoin-project/go-data-transfer"

	"github.com/ipfs/go-cid"
	"github.com/pkg/errors"

	transferstatus "github.com/application-research/estuary/deal/transfer/status"
	"github.com/application-research/estuary/drpc"

	"github.com/application-research/estuary/util"
	lru "github.com/hashicorp/golang-lru"
	"github.com/libp2p/go-libp2p/core/peer"
)

var ErrNilParams = fmt.Errorf("shuttle message had nil params")
var ErrNoShuttleConnection = fmt.Errorf("no connection to requested shuttle")

type IManager interface {
	Connect(ws *websocket.Conn, handle string, done chan struct{}) (func() error, func(), error)
	IsOnline(handle string) bool
	CanAddContent(handle string) bool
	HostName(handle string) string
	GetShuttlesConfig(u *util.User) (interface{}, error)
	StorageStats(handle string) *util.ShuttleStorageStats
	AddrInfo(handle string) *peer.AddrInfo
	StartTransfer(ctx context.Context, loc string, cd *model.ContentDeal, datacid cid.Cid) error
	RestartTransfer(ctx context.Context, loc string, chanid datatransfer.ChannelID, d model.ContentDeal) error
	GetTransferStatus(ctx context.Context, contLoc string, d *model.ContentDeal) (*filclient.ChannelState, error)
	UnpinContent(ctx context.Context, loc string, conts []uint) error
	PinContent(ctx context.Context, loc string, cont util.Content, origins []*peer.AddrInfo) error
	ConsolidateContent(ctx context.Context, loc string, contents []util.Content) error
	AggregateContent(ctx context.Context, loc string, cont util.Content, aggr []drpc.AggregateContent) error
	CommPContent(ctx context.Context, loc string, data cid.Cid) error
	SplitContent(ctx context.Context, loc string, cont uint, size int64) error
	GetLocationForRetrieval(ctx context.Context, cont util.Content) (string, error)
	GetLocationForStorage(ctx context.Context, obj cid.Cid, uid uint) (string, error)
	CleanupPreparedRequest(ctx context.Context, loc string, dbid uint, authToken string) error
	PrepareForDataRequest(ctx context.Context, loc string, dbid uint, authToken string, propCid cid.Cid, payloadCid cid.Cid, size uint64) error
	GetPreferredUploadEndpoints(u *util.User) ([]string, error)
	GetByAuth(auth string) (*model.Shuttle, error)
}

type manager struct {
	db                    *gorm.DB
	cfg                   *config.Estuary
	tracer                trace.Tracer
	transferStatuses      *lru.ARCCache
	log                   *zap.SugaredLogger
	transferStatusUpdater transferstatus.IUpdater
	dealStatusUpdater     dealstatus.IUpdater
	sanityCheckMgr        sanitycheck.IManager
	shuttlesLk            sync.Mutex
	shuttles              map[string]*connection
	rpcWebsocket          chan *drpc.Message
	rpcQueue              *shuttlequeue.Emanager

	cntQueueMgr contentqueue.IQueueManager
}

func NewManager(ctx context.Context, db *gorm.DB, cfg *config.Estuary, log *zap.SugaredLogger, sanitycheckMgr sanitycheck.IManager, cntQueueMgr contentqueue.IQueueManager) (IManager, error) {
	cache, err := lru.NewARC(50000)
	if err != nil {
		return nil, err
	}

	mgr := &manager{
		cfg:                   cfg,
		db:                    db,
		transferStatuses:      cache,
		shuttles:              make(map[string]*connection),
		tracer:                otel.Tracer("replicator"),
		log:                   log,
		transferStatusUpdater: transferstatus.NewUpdater(db),
		dealStatusUpdater:     dealstatus.NewUpdater(db, log),
		sanityCheckMgr:        sanitycheckMgr,
		rpcWebsocket:          make(chan *drpc.Message, cfg.RPCMessage.IncomingQueueSize),
	}

	if cfg.RPCQueue.Enabled {
		emgr, err := shuttlequeue.NewEstuaryQueueMgr(cfg, mgr.processMessage)
		if err != nil {
			return nil, err
		}
		mgr.rpcQueue = emgr
	}

	// register workers/handlers to process websocket messages from a channel(queue)
	go mgr.runWebsocketQueueProcessingWorkers(ctx, cfg.RPCMessage.QueueHandlers)
	return mgr, nil
}

func (m *manager) IsOnline(handle string) bool {
	m.shuttlesLk.Lock()
	sc, ok := m.shuttles[handle]
	m.shuttlesLk.Unlock()
	if !ok {
		return false
	}

	select {
	case <-sc.ctx.Done():
		return false
	default:
		return true
	}
}

func (m *manager) CanAddContent(handle string) bool {
	m.shuttlesLk.Lock()
	defer m.shuttlesLk.Unlock()
	d, ok := m.shuttles[handle]
	if ok {
		return !d.contentAddingDisabled
	}
	return true
}

func (m *manager) AddrInfo(handle string) *peer.AddrInfo {
	m.shuttlesLk.Lock()
	defer m.shuttlesLk.Unlock()
	d, ok := m.shuttles[handle]
	if !ok {
		return nil
	}
	return &d.addrInfo
}

func (m *manager) HostName(handle string) string {
	m.shuttlesLk.Lock()
	defer m.shuttlesLk.Unlock()
	d, ok := m.shuttles[handle]
	if ok {
		return d.hostname
	}
	return ""
}

func (m *manager) StorageStats(handle string) *util.ShuttleStorageStats {
	m.shuttlesLk.Lock()
	defer m.shuttlesLk.Unlock()
	d, ok := m.shuttles[handle]
	if !ok {
		return nil
	}

	return &util.ShuttleStorageStats{
		BlockstoreSize: d.blockstoreSize,
		BlockstoreFree: d.blockstoreFree,
		PinCount:       d.pinCount,
		PinQueueLength: d.pinQueueLength,
	}
}

func (m *manager) GetShuttlesConfig(u *util.User) (interface{}, error) {
	var shts []interface{}
	for _, sh := range m.shuttles {
		if sh.hostname == "" {
			m.log.Warnf("failed to get shuttle(%s) config, shuttle hostname is not set", sh.handle)
			continue
		}

		out, err := getShuttleConfig(sh.hostname, u.AuthToken.Token)
		if err != nil {
			return nil, err
		}
		shts = append(shts, out)
	}
	return shts, nil
}

func getShuttleConfig(hostname string, authToken string) (interface{}, error) {
	u, err := url.Parse(hostname)
	if err != nil {
		return nil, errors.Errorf("failed to parse url for shuttle(%s) config: %s", hostname, err)
	}
	u.Path = ""

	req, err := http.NewRequest("GET", fmt.Sprintf("%s://%s/admin/system/config", u.Scheme, u.Host), nil)
	if err != nil {
		return nil, errors.Errorf("failed to build GET request for shuttle(%s) config: %s", hostname, err)
	}
	req.Header.Set("Authorization", "Bearer "+authToken)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, errors.Errorf("failed to request shuttle(%s) config: %s", hostname, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		bodyBytes, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return nil, errors.Errorf("failed to read shuttle(%s) config err resp: %s", hostname, err)
		}
		return nil, errors.Errorf("failed to get shuttle(%s) config: %s", hostname, bodyBytes)
	}

	var out interface{}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, errors.Errorf("failed to decode shuttle config response: %s", err)
	}
	return out, nil
}

func (sc *connection) sendMessage(ctx context.Context, cmd *drpc.Command) error {

	select {
	case sc.cmds <- cmd:
		return nil
	case <-sc.ctx.Done():
		return ErrNoShuttleConnection
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (m *manager) sendRPCMessage(ctx context.Context, handle string, cmd *drpc.Command) error {
	if handle == "" || handle == constants.ContentLocationLocal {
		return fmt.Errorf("attempted to send command to empty shuttle handle or local")
	}

	m.log.Debugf("sending rpc message:%s, to shuttle:%s", cmd.Op, handle)

	m.shuttlesLk.Lock()
	d, ok := m.shuttles[handle]
	m.shuttlesLk.Unlock()
	if ok {
		return d.sendMessage(ctx, cmd)
	}
	return ErrNoShuttleConnection
}

func (m *manager) handleRpcShuttleUpdate(ctx context.Context, handle string, param *drpc.ShuttleUpdate) error {
	m.shuttlesLk.Lock()
	defer m.shuttlesLk.Unlock()
	d, ok := m.shuttles[handle]
	if !ok {
		return fmt.Errorf("shuttle connection not found while handling update for %q", handle)
	}

	d.spaceLow = param.BlockstoreFree < (param.BlockstoreSize / 10)
	d.blockstoreFree = param.BlockstoreFree
	d.blockstoreSize = param.BlockstoreSize
	d.pinCount = param.NumPins
	d.pinQueueLength = int64(param.PinQueueSize)

	return nil
}

func (m *manager) GetByAuth(auth string) (*model.Shuttle, error) {
	var shuttle *model.Shuttle
	if err := m.db.First(&shuttle, "token = ?", auth).Error; err != nil {
		return nil, err
	}
	return shuttle, nil
}
