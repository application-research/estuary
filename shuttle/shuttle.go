package shuttle

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
	"golang.org/x/xerrors"
	"gorm.io/gorm"

	"github.com/application-research/estuary/config"
	rpc "github.com/application-research/estuary/shuttle/rpc"
	"github.com/labstack/echo/v4"

	contentqueue "github.com/application-research/estuary/content/queue"
	dealstatus "github.com/application-research/estuary/deal/status"
	"github.com/application-research/estuary/model"
	"github.com/application-research/estuary/sanitycheck"
	"github.com/application-research/filclient"
	datatransfer "github.com/filecoin-project/go-data-transfer"

	"github.com/ipfs/go-cid"
	"github.com/pkg/errors"

	transferstatus "github.com/application-research/estuary/deal/transfer/status"
	rpcevent "github.com/application-research/estuary/shuttle/rpc/event"

	"github.com/application-research/estuary/util"
	"github.com/libp2p/go-libp2p/core/peer"
)

var ErrNilParams = fmt.Errorf("shuttle message had nil params")
var ErrNoShuttleConnection = fmt.Errorf("no connection to requested shuttle")

type IManager interface {
	Connect(c echo.Context, handle string, done chan struct{}) error
	IsOnline(handle string) (bool, error)
	CanAddContent(handle string) (bool, error)
	HostName(handle string) (string, error)
	StorageStats(handle string) (*util.ShuttleStorageStats, error)
	AddrInfo(handle string) (*peer.AddrInfo, error)

	GetShuttlesConfig(u *util.User) (interface{}, error)
	StartTransfer(ctx context.Context, loc string, cd *model.ContentDeal, datacid cid.Cid) error
	RestartTransfer(ctx context.Context, loc string, chanid datatransfer.ChannelID, d model.ContentDeal) error
	GetTransferStatus(ctx context.Context, contLoc string, d *model.ContentDeal) (*filclient.ChannelState, error)
	UnpinContent(ctx context.Context, loc string, conts []uint) error
	PinContent(ctx context.Context, loc string, cont util.Content, origins []*peer.AddrInfo) error
	ConsolidateContent(ctx context.Context, loc string, contents []util.Content) error
	AggregateContent(ctx context.Context, loc string, zone util.Content, zoneContents []util.Content) error
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
	log                   *zap.SugaredLogger
	transferStatusUpdater transferstatus.IUpdater
	dealStatusUpdater     dealstatus.IUpdater
	rpcMgr                rpc.IManager
}

func NewManager(ctx context.Context, db *gorm.DB, cfg *config.Estuary, log *zap.SugaredLogger, sanitycheckMgr sanitycheck.IManager, cntQueueMgr contentqueue.IQueueManager) (IManager, error) {
	rpcMgr, err := rpc.NewEstuaryRpcManager(ctx, db, cfg, log, sanitycheckMgr, cntQueueMgr)
	if err != nil {
		return nil, err
	}

	return &manager{
		db:                    db,
		cfg:                   cfg,
		tracer:                otel.Tracer("shuttle"),
		log:                   log,
		transferStatusUpdater: transferstatus.NewUpdater(db),
		dealStatusUpdater:     dealstatus.NewUpdater(db, log),
		rpcMgr:                rpcMgr,
	}, nil
}

// replace this with ping
func (m *manager) IsOnline(handle string) (bool, error) {
	d, err := m.getConnectionByHandle(handle)
	if err != nil {
		return false, err
	}
	if d == nil {
		return false, err
	}
	// if connection not updated in the last 5 minutes
	return time.Now().Add(-5 * time.Minute).Before(d.UpdatedAt), nil
}

func (m *manager) CanAddContent(handle string) (bool, error) {
	d, err := m.getConnectionByHandle(handle)
	if err != nil {
		return false, err
	}

	if d != nil {
		return !d.ContentAddingDisabled, nil
	}
	return true, nil
}

func (m *manager) AddrInfo(handle string) (*peer.AddrInfo, error) {
	d, err := m.getConnectionByHandle(handle)
	if err != nil {
		return nil, err
	}

	if d == nil {
		return nil, nil
	}
	return &d.AddrInfo.AddrInfo, nil
}

func (m *manager) HostName(handle string) (string, error) {
	d, err := m.getConnectionByHandle(handle)
	if err != nil {
		return "", err
	}

	if d != nil {
		return d.Hostname, nil
	}
	return "", nil
}

func (m *manager) StorageStats(handle string) (*util.ShuttleStorageStats, error) {
	d, err := m.getConnectionByHandle(handle)
	if err != nil {
		return nil, err
	}

	if d != nil {
		return &util.ShuttleStorageStats{
			BlockstoreSize: d.BlockstoreSize,
			BlockstoreFree: d.BlockstoreFree,
			PinCount:       d.PinCount,
			PinQueueLength: d.PinQueueLength,
		}, nil
	}
	return nil, nil
}

func (m *manager) GetShuttlesConfig(u *util.User) (interface{}, error) {
	var shts []interface{}
	connectedShuttles, err := m.getConnections()
	if err != nil {
		return nil, err
	}

	for _, sh := range connectedShuttles {
		if sh.Hostname == "" {
			m.log.Warnf("failed to get shuttle(%s) config, shuttle hostname is not set", sh.Handle)
			continue
		}

		out, err := getShuttleConfig(sh.Hostname, u.AuthToken.Token)
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

func (m *manager) sendRPCMessage(ctx context.Context, handle string, cmd *rpcevent.Command) error {
	return m.rpcMgr.SendRPCMessage(ctx, handle, cmd)
}

func (m *manager) Connect(c echo.Context, handle string, done chan struct{}) error {
	return m.rpcMgr.Connect(c, handle, done)
}

func (m *manager) GetByAuth(auth string) (*model.Shuttle, error) {
	var shuttle *model.Shuttle
	if err := m.db.First(&shuttle, "token = ?", auth).Error; err != nil {
		return nil, err
	}
	return shuttle, nil
}

func (m *manager) getConnectionByHandle(handle string) (*model.ShuttleConnection, error) {
	var shuttle *model.ShuttleConnection
	if err := m.db.First(&shuttle, "handle = ?", handle).Error; err != nil {
		if !xerrors.Is(err, gorm.ErrRecordNotFound) {
			return nil, err
		}
	}
	return shuttle, nil
}

func (m *manager) getConnections() ([]*model.ShuttleConnection, error) {
	var shuttles []*model.ShuttleConnection
	if err := m.db.Find(&shuttles).Error; err != nil {
		return nil, err
	}
	return shuttles, nil
}
