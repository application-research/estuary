package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	_ "net/http/pprof"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/urfave/cli/v2"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"golang.org/x/net/websocket"
	"golang.org/x/sys/unix"
	"golang.org/x/xerrors"
	"gorm.io/gorm"

	"github.com/application-research/estuary/drpc"
	node "github.com/application-research/estuary/node"
	"github.com/application-research/estuary/pinner"
	"github.com/application-research/estuary/stagingbs"
	"github.com/application-research/estuary/util"
	"github.com/application-research/filclient"
	datatransfer "github.com/filecoin-project/go-data-transfer"
	"github.com/filecoin-project/lotus/api"
	lcli "github.com/filecoin-project/lotus/cli"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-blockservice"
	"github.com/ipfs/go-cid"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	chunker "github.com/ipfs/go-ipfs-chunker"
	exchange "github.com/ipfs/go-ipfs-exchange-interface"
	offline "github.com/ipfs/go-ipfs-exchange-offline"
	ipld "github.com/ipfs/go-ipld-format"
	logging "github.com/ipfs/go-log"
	"github.com/ipfs/go-merkledag"
	"github.com/ipfs/go-unixfs/importer"
	uio "github.com/ipfs/go-unixfs/io"
	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
	promhttp "github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/whyrusleeping/memo"
)

var Tracer = otel.Tracer("shuttle")

var log = logging.Logger("shuttle")

func init() {
	if os.Getenv("FULLNODE_API_INFO") == "" {
		os.Setenv("FULLNODE_API_INFO", "wss://api.chain.love")
	}
}

func main() {
	logging.SetLogLevel("dt-impl", "debug")
	logging.SetLogLevel("shuttle", "debug")
	logging.SetLogLevel("paych", "debug")
	logging.SetLogLevel("filclient", "debug")
	logging.SetLogLevel("dt_graphsync", "debug")
	logging.SetLogLevel("dt-chanmon", "debug")
	logging.SetLogLevel("markets", "debug")
	logging.SetLogLevel("data_transfer_network", "debug")
	logging.SetLogLevel("rpc", "info")
	logging.SetLogLevel("bs-wal", "info")
	logging.SetLogLevel("bs-migrate", "info")

	app := cli.NewApp()
	app.Flags = []cli.Flag{
		&cli.StringFlag{
			Name:  "repo",
			Value: "~/.lotus",
		},
		&cli.StringFlag{
			Name:    "database",
			Value:   "sqlite=estuary-shuttle.db",
			EnvVars: []string{"ESTUARY_SHUTTLE_DATABASE"},
		},
		&cli.StringFlag{
			Name: "blockstore",
		},
		&cli.StringFlag{
			Name:  "write-log",
			Usage: "enable write log blockstore in specified directory",
		},
		&cli.StringFlag{
			Name:    "apilisten",
			Usage:   "address for the api server to listen on",
			Value:   ":3005",
			EnvVars: []string{"ESTUARY_SHUTTLE_API_LISTEN"},
		},
		&cli.StringFlag{
			Name:    "datadir",
			Usage:   "directory to store data in",
			Value:   ".",
			EnvVars: []string{"ESTUARY_SHUTTLE_DATADIR"},
		},
		&cli.StringFlag{
			Name:  "estuary-api",
			Usage: "api endpoint for master estuary node",
			Value: "api.estuary.tech",
		},
		&cli.StringFlag{
			Name:     "auth-token",
			Usage:    "auth token for connecting to estuary",
			Required: true,
		},
		&cli.StringFlag{
			Name:     "handle",
			Usage:    "estuary shuttle handle to use",
			Required: true,
		},
		&cli.StringFlag{
			Name:  "host",
			Usage: "url that this node is publicly dialable at",
		},
		&cli.BoolFlag{
			Name: "logging",
		},
		&cli.BoolFlag{
			Name: "write-log-flush",
		},
		&cli.BoolFlag{
			Name: "write-log-truncate",
		},
		&cli.BoolFlag{
			Name: "private",
		},
	}

	app.Action = func(cctx *cli.Context) error {
		ddir := cctx.String("datadir")

		bsdir := cctx.String("blockstore")
		if bsdir == "" {
			bsdir = filepath.Join(ddir, "blocks")
		} else if bsdir[0] != '/' && bsdir[0] != ':' {
			bsdir = filepath.Join(ddir, bsdir)
		}

		wlog := cctx.String("write-log")
		if wlog != "" && wlog[0] != '/' {
			wlog = filepath.Join(ddir, wlog)
		}

		cfg := &node.Config{
			ListenAddrs: []string{
				"/ip4/0.0.0.0/tcp/6745",
			},
			Blockstore:        bsdir,
			WriteLog:          wlog,
			HardFlushWriteLog: cctx.Bool("write-log-flush"),
			WriteLogTruncate:  cctx.Bool("write-log-truncate"),
			Libp2pKeyFile:     filepath.Join(ddir, "peer.key"),
			Datastore:         filepath.Join(ddir, "leveldb"),
			WalletDir:         filepath.Join(ddir, "wallet"),
		}

		nd, err := node.Setup(context.TODO(), cfg)
		if err != nil {
			return err
		}

		api, closer, err := lcli.GetGatewayAPI(cctx)
		if err != nil {
			return err
		}

		defer closer()

		defaddr, err := nd.Wallet.GetDefault()
		if err != nil {
			return err
		}

		filc, err := filclient.NewClient(nd.Host, api, nd.Wallet, defaddr, nd.Blockstore, nd.Datastore, ddir)
		if err != nil {
			return err
		}

		db, err := setupDatabase(cctx.String("database"))
		if err != nil {
			return err
		}

		commpMemo := memo.NewMemoizer(func(ctx context.Context, k string) (interface{}, error) {
			start := time.Now()

			c, err := cid.Decode(k)
			if err != nil {
				return nil, err
			}

			commpcid, size, err := filclient.GeneratePieceCommitment(ctx, c, nd.Blockstore)
			if err != nil {
				return nil, err
			}

			log.Infof("commp generation over %d bytes took: %s", size, time.Since(start))

			res := &commpResult{
				CommP: commpcid,
				Size:  size,
			}

			return res, nil
		})

		sbm, err := stagingbs.NewStagingBSMgr(filepath.Join(ddir, "staging"))
		if err != nil {
			return err
		}

		d := &Shuttle{
			Node:       nd,
			Api:        api,
			DB:         db,
			Filc:       filc,
			StagingMgr: sbm,
			Private:    cctx.Bool("private"),

			commpMemo: commpMemo,

			trackingChannels: make(map[string]*chanTrack),

			outgoing: make(chan *drpc.Message),

			hostname:      cctx.String("host"),
			estuaryHost:   cctx.String("estuary-api"),
			shuttleHandle: cctx.String("handle"),
			shuttleToken:  cctx.String("auth-token"),
		}
		d.PinMgr = pinner.NewPinManager(d.doPinning, d.onPinStatusUpdate, &pinner.PinManagerOpts{
			MaxActivePerUser: 30,
		})

		go d.PinMgr.Run(100)

		if err := d.refreshPinQueue(); err != nil {
			log.Errorf("failed to refresh pin queue: %s", err)
		}

		d.Filc.SubscribeToDataTransferEvents(func(event datatransfer.Event, st datatransfer.ChannelState) {
			chid := st.ChannelID().String()
			d.tcLk.Lock()
			defer d.tcLk.Unlock()
			trk, ok := d.trackingChannels[chid]
			if !ok {
				return
			}

			if trk.last == nil || trk.last.Status != st.Status() {
				cst := filclient.ChannelStateConv(st)
				trk.last = cst

				log.Infof("event(%d) message: %s", event.Code, event.Message)
				go d.sendTransferStatusUpdate(context.TODO(), &drpc.TransferStatus{
					Chanid:   chid,
					DealDBID: trk.dbid,
					State:    cst,
				})
			}
		})

		go func() {
			http.Handle("/debug/metrics/prometheus", promhttp.Handler())
			if err := http.ListenAndServe("127.0.0.1:3105", nil); err != nil {
				log.Errorf("failed to start http server for pprof endpoints: %s", err)
			}
		}()

		go func() {
			if err := d.RunRpcConnection(); err != nil {
				log.Errorf("failed to run rpc connection: %s", err)
			}
		}()

		go func() {
			upd, err := d.getUpdatePacket()
			if err != nil {
				log.Errorf("failed to get update packet: %s", err)
			}

			if err := d.sendRpcMessage(context.TODO(), &drpc.Message{
				Op: drpc.OP_ShuttleUpdate,
				Params: drpc.MsgParams{
					ShuttleUpdate: upd,
				},
			}); err != nil {
				log.Errorf("failed to send shuttle update: %s", err)
			}
			for range time.Tick(time.Minute) {
				upd, err := d.getUpdatePacket()
				if err != nil {
					log.Errorf("failed to get update packet: %s", err)
				}

				if err := d.sendRpcMessage(context.TODO(), &drpc.Message{
					Op: drpc.OP_ShuttleUpdate,
					Params: drpc.MsgParams{
						ShuttleUpdate: upd,
					},
				}); err != nil {
					log.Errorf("failed to send shuttle update: %s", err)
				}
			}

		}()

		return d.ServeAPI(cctx.String("apilisten"), cctx.Bool("logging"))
	}

	if err := app.Run(os.Args); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

type Shuttle struct {
	Node       *node.Node
	Api        api.Gateway
	DB         *gorm.DB
	PinMgr     *pinner.PinManager
	Filc       *filclient.FilClient
	StagingMgr *stagingbs.StagingBSMgr

	tcLk             sync.Mutex
	trackingChannels map[string]*chanTrack

	addPinLk sync.Mutex

	outgoing chan *drpc.Message

	Private bool

	hostname      string
	estuaryHost   string
	shuttleHandle string
	shuttleToken  string

	commpMemo *memo.Memoizer
}

type chanTrack struct {
	dbid uint
	last *filclient.ChannelState
}

func (d *Shuttle) RunRpcConnection() error {
	for {
		conn, err := d.dialConn()
		if err != nil {
			log.Errorf("failed to dial estuary rpc endpoint: %s", err)
			time.Sleep(time.Second * 10)
			continue
		}

		if err := d.runRpc(conn); err != nil {
			log.Errorf("rpc routine exited with an error: %s", err)
			time.Sleep(time.Second * 10)
			continue
		}

		log.Warnf("rpc routine exited with no error, reconnecting...")
		time.Sleep(time.Second)
	}
}

func (d *Shuttle) runRpc(conn *websocket.Conn) error {
	log.Infof("connecting to primary estuary node")
	defer conn.Close()

	readDone := make(chan struct{})

	// Send hello message
	hello, err := d.getHelloMessage()
	if err != nil {
		return err
	}

	if err := websocket.JSON.Send(conn, hello); err != nil {
		return err
	}

	go func() {
		defer close(readDone)

		for {
			var cmd drpc.Command
			if err := websocket.JSON.Receive(conn, &cmd); err != nil {
				log.Errorf("failed to read command from websocket: %s", err)
				return
			}

			go func(cmd *drpc.Command) {
				if err := d.handleRpcCmd(cmd); err != nil {
					log.Errorf("failed to handle rpc command: %s", err)
				}
			}(&cmd)
		}
	}()

	for {
		select {
		case <-readDone:
			return fmt.Errorf("read routine exited, assuming socket is closed")
		case msg := <-d.outgoing:
			conn.SetWriteDeadline(time.Now().Add(time.Second * 30))
			if err := websocket.JSON.Send(conn, msg); err != nil {
				log.Errorf("failed to send message: %s", err)
			}
			conn.SetWriteDeadline(time.Time{})
		}
	}
}

func (d *Shuttle) getHelloMessage() (*drpc.Hello, error) {
	addr, err := d.Node.Wallet.GetDefault()
	if err != nil {
		return nil, err
	}

	log.Infow("sending hello", "hostname", d.hostname, "address", addr, "pid", d.Node.Host.ID())
	return &drpc.Hello{
		Host:    d.hostname,
		PeerID:  d.Node.Host.ID().Pretty(),
		Address: addr,
		Private: d.Private,
		AddrInfo: peer.AddrInfo{
			ID:    d.Node.Host.ID(),
			Addrs: d.Node.Host.Addrs(),
		},
	}, nil
}

func (d *Shuttle) dialConn() (*websocket.Conn, error) {
	cfg, err := websocket.NewConfig("wss://"+d.estuaryHost+"/shuttle/conn", "http://localhost")
	if err != nil {
		return nil, err
	}

	cfg.Header.Set("Authorization", "Bearer "+d.shuttleToken)

	conn, err := websocket.DialConfig(cfg)
	if err != nil {
		return nil, err
	}

	return conn, nil
}

type User struct {
	ID       uint
	Username string
	Perms    int

	AuthToken       string `json:"-"` // this struct shouldnt ever be serialized, but just in case...
	StorageDisabled bool
}

func (d *Shuttle) checkTokenAuth(token string) (*User, error) {
	req, err := http.NewRequest("GET", "https://"+d.estuaryHost+"/viewer", nil)
	if err != nil {
		return nil, err
	}

	req.Header.Set("Authorization", "Bearer "+token)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}

	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		var herr util.HttpError
		if err := json.NewDecoder(resp.Body).Decode(&herr); err != nil {
			return nil, fmt.Errorf("authentication check returned unexpected error, code %d", resp.StatusCode)
		}

		return nil, fmt.Errorf("authentication check failed: %s(%d)", herr.Message, herr.Code)
	}

	var out util.ViewerResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, err
	}

	return &User{
		ID:              out.ID,
		Username:        out.Username,
		Perms:           out.Perms,
		AuthToken:       token,
		StorageDisabled: out.Settings.ContentAddingDisabled,
	}, nil
}

func (d *Shuttle) AuthRequired(level int) echo.MiddlewareFunc {
	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c echo.Context) error {
			auth, err := util.ExtractAuth(c)
			if err != nil {
				return err
			}

			u, err := d.checkTokenAuth(auth)
			if err != nil {
				return err
			}

			if u.Perms >= level {
				c.Set("user", u)
				return next(c)
			}

			log.Warnw("User not authorized", "user", u.ID, "perms", u.Perms, "required", level)

			return &util.HttpError{
				Code:    401,
				Message: util.ERR_NOT_AUTHORIZED,
			}
		}
	}
}

func withUser(f func(echo.Context, *User) error) func(echo.Context) error {
	return func(c echo.Context) error {
		u, ok := c.Get("user").(*User)
		if !ok {
			return fmt.Errorf("endpoint not called with proper authentication")
		}

		return f(c, u)
	}
}

func (s *Shuttle) ServeAPI(listen string, logging bool) error {
	e := echo.New()

	if logging {
		e.Use(middleware.Logger())
	}

	e.Use(middleware.CORS())

	e.HTTPErrorHandler = func(err error, ctx echo.Context) {
		log.Errorf("handler error: %s", err)
		var herr *util.HttpError
		if xerrors.As(err, &herr) {
			res := map[string]string{
				"error": herr.Message,
			}
			if herr.Details != "" {
				res["details"] = herr.Details
			}
			ctx.JSON(herr.Code, res)
			return
		}

		var echoErr *echo.HTTPError
		if xerrors.As(err, &echoErr) {
			ctx.JSON(echoErr.Code, map[string]interface{}{
				"error": echoErr.Message,
			})
			return
		}

		// TODO: returning all errors out to the user smells potentially bad
		_ = ctx.JSON(500, map[string]interface{}{
			"error": err.Error(),
		})
	}

	e.GET("/health", s.handleHealth)

	content := e.Group("/content")
	content.Use(s.AuthRequired(util.PermLevelUser))
	content.POST("/add", withUser(s.handleAdd))
	content.GET("/read/:cont", withUser(s.handleReadContent))
	//content.POST("/add-ipfs", withUser(d.handleAddIpfs))
	//content.POST("/add-car", withUser(d.handleAddCar))

	admin := e.Group("/admin")
	admin.Use(s.AuthRequired(util.PermLevelAdmin))
	admin.GET("/health/:cid", s.handleContentHealthCheck)
	admin.POST("/resend/pincomplete/:content", s.handleResendPinComplete)

	return e.Start(listen)
}

func (s *Shuttle) handleAdd(c echo.Context, u *User) error {
	ctx := c.Request().Context()

	if u.StorageDisabled {
		return &util.HttpError{
			Code:    400,
			Message: util.ERR_CONTENT_ADDING_DISABLED,
		}
	}

	form, err := c.MultipartForm()
	if err != nil {
		return err
	}
	defer form.RemoveAll()

	mpf, err := c.FormFile("data")
	if err != nil {
		return err
	}

	fname := mpf.Filename
	fi, err := mpf.Open()
	if err != nil {
		return err
	}

	defer fi.Close()

	collection := c.FormValue("collection")

	bsid, bs, err := s.StagingMgr.AllocNew()
	if err != nil {
		return err
	}

	defer func() {
		go func() {
			if err := s.StagingMgr.CleanUp(bsid); err != nil {
				log.Errorf("failed to clean up staging blockstore: %s", err)
			}
		}()
	}()

	bserv := blockservice.New(bs, nil)
	dserv := merkledag.NewDAGService(bserv)

	nd, err := s.importFile(ctx, dserv, fi)
	if err != nil {
		return err
	}

	contid, err := s.createContent(ctx, u, nd.Cid(), fname, collection)
	if err != nil {
		return err
	}

	pin := &Pin{
		Content: contid,
		Cid:     util.DbCID{nd.Cid()},
		UserID:  u.ID,

		Active:  false,
		Pinning: true,
	}

	if err := s.DB.Create(pin).Error; err != nil {
		return err
	}

	if err := s.addDatabaseTrackingToContent(ctx, contid, dserv, bs, nd.Cid(), func(int64) {}); err != nil {
		return xerrors.Errorf("encountered problem computing object references: %w", err)
	}

	if err := s.dumpBlockstoreTo(ctx, bs, s.Node.Blockstore); err != nil {
		return xerrors.Errorf("failed to move data from staging to main blockstore: %w", err)
	}

	subCtx, cancel := context.WithTimeout(ctx, time.Second*10)
	defer cancel()
	if err := s.Node.Dht.Provide(subCtx, nd.Cid(), true); err != nil {
		log.Warnf("failed to provide newly added content: %s", err)
	}

	go func() {
		if err := s.Node.Provider.Provide(nd.Cid()); err != nil {
			fmt.Println("providing failed: ", err)
		}
		fmt.Println("providing complete")
	}()

	return c.JSON(200, &util.AddFileResponse{
		Cid:       nd.Cid().String(),
		EstuaryId: contid,
		Providers: s.addrsForShuttle(),
	})
}

func (s *Shuttle) addrsForShuttle() []string {
	var out []string
	for _, a := range s.Node.Host.Addrs() {
		out = append(out, fmt.Sprintf("%s/p2p/%s", a, s.Node.Host.ID()))
	}
	return out
}

type createContentBody struct {
	Root        cid.Cid  `json:"root"`
	Name        string   `json:"name"`
	Collections []string `json:"collections"`
	Location    string   `json:"location"`
}

type createContentResponse struct {
	ID uint `json:"id"`
}

func (s *Shuttle) createContent(ctx context.Context, u *User, root cid.Cid, fname, collection string) (uint, error) {
	var cols []string
	if collection != "" {
		cols = []string{collection}
	}

	data, err := json.Marshal(createContentBody{
		Root:        root,
		Name:        fname,
		Collections: cols,
		Location:    s.shuttleHandle,
	})
	if err != nil {
		return 0, err
	}

	req, err := http.NewRequest("POST", "https://"+s.estuaryHost+"/content/create", bytes.NewReader(data))
	if err != nil {
		return 0, err
	}

	req.Header.Set("Authorization", "Bearer "+u.AuthToken)
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, err
	}

	defer resp.Body.Close()

	var rbody createContentResponse
	if err := json.NewDecoder(resp.Body).Decode(&rbody); err != nil {
		return 0, err
	}

	if rbody.ID == 0 {
		return 0, fmt.Errorf("create content request failed, got back content ID zero")
	}

	return rbody.ID, nil
}

// TODO: mostly copy paste from estuary, dedup code
func (d *Shuttle) doPinning(ctx context.Context, op *pinner.PinningOperation, cb pinner.PinProgressCB) error {
	ctx, span := Tracer.Start(ctx, "doPinning")
	defer span.End()

	for _, pi := range op.Peers {
		if err := d.Node.Host.Connect(ctx, pi); err != nil {
			log.Warnf("failed to connect to origin node for pinning operation: %s", err)
		}
	}

	bserv := blockservice.New(d.Node.Blockstore, d.Node.Bitswap)
	dserv := merkledag.NewDAGService(bserv)
	dsess := merkledag.NewSession(ctx, dserv)

	if err := d.addDatabaseTrackingToContent(ctx, op.ContId, dsess, d.Node.Blockstore, op.Obj, cb); err != nil {
		// pinning failed, we wont try again. mark pin as dead
		/* maybe its fine if we retry later?
		if err := d.DB.Model(Pin{}).Where("content = ?", op.ContId).UpdateColumns(map[string]interface{}{
			"pinning": false,
		}).Error; err != nil {
			log.Errorf("failed to update failed pin status: %s", err)
		}
		*/

		return err
	}

	/*
		if op.Replace > 0 {
			if err := s.CM.RemoveContent(ctx, op.Replace, true); err != nil {
				log.Infof("failed to remove content in replacement: %d", op.Replace)
			}
		}
	*/

	// this provide call goes out immediately
	if err := d.Node.FullRT.Provide(ctx, op.Obj, true); err != nil {
		log.Infof("provider broadcast failed: %s", err)
	}

	// this one adds to a queue
	if err := d.Node.Provider.Provide(op.Obj); err != nil {
		log.Infof("providing failed: %s", err)
	}

	return nil
}

const noDataTimeout = time.Minute * 10

// TODO: mostly copy paste from estuary, dedup code
func (d *Shuttle) addDatabaseTrackingToContent(ctx context.Context, contid uint, dserv ipld.NodeGetter, bs blockstore.Blockstore, root cid.Cid, cb func(int64)) error {
	ctx, span := Tracer.Start(ctx, "computeObjRefsUpdate")
	defer span.End()

	var dbpin Pin
	if err := d.DB.First(&dbpin, "content = ?", contid).Error; err != nil {
		return err
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	gotData := make(chan struct{}, 1)
	go func() {
		nodata := time.NewTimer(noDataTimeout)
		defer nodata.Stop()

		for {
			select {
			case <-nodata.C:
				cancel()
			case <-gotData:
				nodata.Reset(noDataTimeout)
			case <-ctx.Done():
				return
			}
		}
	}()

	var objects []*Object
	var totalSize int64
	cset := cid.NewSet()

	err := merkledag.Walk(ctx, func(ctx context.Context, c cid.Cid) ([]*ipld.Link, error) {
		node, err := dserv.Get(ctx, c)
		if err != nil {
			return nil, err
		}

		cb(int64(len(node.RawData())))

		select {
		case gotData <- struct{}{}:
		case <-ctx.Done():
		}

		objects = append(objects, &Object{
			Cid:  util.DbCID{c},
			Size: len(node.RawData()),
		})

		totalSize += int64(len(node.RawData()))

		if c.Type() == cid.Raw {
			return nil, nil
		}

		return node.Links(), nil
	}, root, cset.Visit, merkledag.Concurrent())
	if err != nil {
		return err
	}

	span.SetAttributes(
		attribute.Int64("totalSize", totalSize),
		attribute.Int("numObjects", len(objects)),
	)

	if err := d.DB.CreateInBatches(objects, 300).Error; err != nil {
		return xerrors.Errorf("failed to create objects in db: %w", err)
	}

	if err := d.DB.Model(Pin{}).Where("content = ?", contid).UpdateColumns(map[string]interface{}{
		"active":  true,
		"size":    totalSize,
		"pinning": false,
	}).Error; err != nil {
		return xerrors.Errorf("failed to update content in database: %w", err)
	}

	refs := make([]ObjRef, len(objects))
	for i := range refs {
		refs[i].Pin = dbpin.ID
		refs[i].Object = objects[i].ID
	}

	if err := d.DB.CreateInBatches(refs, 500).Error; err != nil {
		return xerrors.Errorf("failed to create refs: %w", err)
	}

	d.sendPinCompleteMessage(ctx, dbpin.Content, totalSize, objects)

	return nil
}

func (d *Shuttle) onPinStatusUpdate(cont uint, status string) {
	log.Infof("updating pin status: %d %s", cont, status)
	if status == "failed" {
		if err := d.DB.Model(Pin{}).Where("content = ?", cont).UpdateColumns(map[string]interface{}{
			"pinning": false,
			"active":  false,
			"failed":  true,
		}).Error; err != nil {
			log.Errorf("failed to mark pin as failed in database: %s", err)
		}
	}

	go func() {
		if err := d.sendRpcMessage(context.TODO(), &drpc.Message{
			Op: "UpdatePinStatus",
			Params: drpc.MsgParams{
				UpdatePinStatus: &drpc.UpdatePinStatus{
					DBID:   cont,
					Status: status,
				},
			},
		}); err != nil {
			log.Errorf("failed to send pin status update: %s", err)
		}
	}()
}

func (s *Shuttle) refreshPinQueue() error {
	var toPin []Pin
	if err := s.DB.Find(&toPin, "active = false and pinning = true").Error; err != nil {
		return err
	}

	// TODO: this doesnt persist the replacement directives, so a queued
	// replacement, if ongoing during a restart of the node, will still
	// complete the pin when the process comes back online, but it wont delete
	// the old pin.
	// Need to fix this, probably best option is just to add a 'replace' field
	// to content, could be interesting to see the graph of replacements
	// anyways
	log.Infof("refreshing %d pins", len(toPin))
	for _, c := range toPin {
		s.addPinToQueue(c, nil, 0)
	}

	return nil
}

func (s *Shuttle) addPinToQueue(p Pin, peers []peer.AddrInfo, replace uint) {
	op := &pinner.PinningOperation{
		ContId:  p.Content,
		UserId:  p.UserID,
		Obj:     p.Cid.CID,
		Peers:   peers,
		Started: p.CreatedAt,
		Status:  "queued",
		Replace: replace,
	}

	/*

		s.pinLk.Lock()
		// TODO: check if we are overwriting anything here
		s.pinJobs[cont.ID] = op
		s.pinLk.Unlock()
	*/

	s.PinMgr.Add(op)
}

func (s *Shuttle) importFile(ctx context.Context, dserv ipld.DAGService, fi io.Reader) (ipld.Node, error) {
	_, span := Tracer.Start(ctx, "importFile")
	defer span.End()

	spl := chunker.NewSizeSplitter(fi, 1024*1024)
	return importer.BuildDagFromReader(dserv, spl)
}

func (s *Shuttle) dumpBlockstoreTo(ctx context.Context, from, to blockstore.Blockstore) error {
	ctx, span := Tracer.Start(ctx, "blockstoreCopy")
	defer span.End()

	// TODO: smarter batching... im sure ive written this logic before, just gotta go find it
	keys, err := from.AllKeysChan(ctx)
	if err != nil {
		return err
	}

	var batch []blocks.Block

	for k := range keys {
		blk, err := from.Get(k)
		if err != nil {
			return err
		}

		batch = append(batch, blk)

		if len(batch) > 500 {
			if err := to.PutMany(batch); err != nil {
				return err
			}
			batch = batch[:0]
		}
	}

	if len(batch) > 0 {
		if err := to.PutMany(batch); err != nil {
			return err
		}
	}

	return nil
}

func (s *Shuttle) getUpdatePacket() (*drpc.ShuttleUpdate, error) {
	var upd drpc.ShuttleUpdate

	upd.PinQueueSize = s.PinMgr.PinQueueSize()

	var st unix.Statfs_t
	if err := unix.Statfs(s.Node.StorageDir, &st); err != nil {
		log.Errorf("failed to get blockstore disk usage: %s", err)
	}

	upd.BlockstoreSize = st.Blocks * uint64(st.Bsize)
	upd.BlockstoreFree = st.Bavail * uint64(st.Bsize)

	if err := s.DB.Model(Pin{}).Where("active").Count(&upd.NumPins).Error; err != nil {
		return nil, err
	}

	return &upd, nil
}

func (s *Shuttle) handleHealth(c echo.Context) error {
	return c.JSON(200, map[string]string{
		"status": "ok",
	})
}

func (s *Shuttle) Unpin(ctx context.Context, contid uint) error {
	var pin Pin
	if err := s.DB.First(&pin, "id = ?", contid).Error; err != nil {
		return err
	}

	objs, err := s.objectsForPin(ctx, pin.ID)
	if err != nil {
		return err
	}

	if err := s.DB.Delete(Pin{}, pin.ID).Error; err != nil {
		return err
	}

	if err := s.DB.Where("pin = ?", pin.ID).Delete(ObjRef{}).Error; err != nil {
		return err
	}

	if err := s.clearUnreferencedObjects(ctx, objs); err != nil {
		return err
	}

	for _, o := range objs {
		// TODO: this is safe, but... slow?
		if err := s.deleteIfNotPinned(o); err != nil {
			return err
		}
	}

	return nil
}

func (s *Shuttle) deleteIfNotPinned(o *Object) error {
	s.addPinLk.Lock()
	defer s.addPinLk.Unlock()

	var c int64
	if err := s.DB.Model(Object{}).Where("id = ? or cid = ?", o.ID, o.Cid).Count(&c).Error; err != nil {
		return err
	}
	if c == 0 {
		return s.Node.Blockstore.DeleteBlock(o.Cid.CID)
	}
	return nil
}

func (s *Shuttle) clearUnreferencedObjects(ctx context.Context, objs []*Object) error {
	var ids []uint
	for _, o := range objs {
		ids = append(ids, o.ID)
	}
	s.addPinLk.Lock()
	defer s.addPinLk.Unlock()

	if err := s.DB.Where("(?) = 0 and id in ?",
		s.DB.Model(ObjRef{}).Where("object = objects.id").Select("count(1)"), ids).
		Delete(Object{}).Error; err != nil {
		return err
	}

	return nil
}

func (s *Shuttle) handleReadContent(c echo.Context, u *User) error {
	cont, err := strconv.Atoi(c.Param("cont"))
	if err != nil {
		return err
	}

	var pin Pin
	if err := s.DB.First(&pin, "content = ?", cont).Error; err != nil {
		return err
	}

	bserv := blockservice.New(s.Node.Blockstore, offline.Exchange(s.Node.Blockstore))
	dserv := merkledag.NewDAGService(bserv)

	ctx := context.Background()
	nd, err := dserv.Get(ctx, pin.Cid.CID)
	if err != nil {
		return c.JSON(400, map[string]string{
			"error": err.Error(),
		})
	}
	r, err := uio.NewDagReader(ctx, nd, dserv)
	if err != nil {
		return c.JSON(400, map[string]string{
			"error": err.Error(),
		})
	}

	io.Copy(c.Response(), r)
	return nil
}

func (s *Shuttle) handleContentHealthCheck(c echo.Context) error {
	ctx := c.Request().Context()
	cc, err := cid.Decode(c.Param("cid"))
	if err != nil {
		return err
	}

	var obj Object
	if err := s.DB.First(&obj, "cid = ?", cc.Bytes()).Error; err != nil {
		return c.JSON(404, map[string]interface{}{
			"error": "object not found in database",
		})
	}

	var pins []Pin
	if err := s.DB.Model(ObjRef{}).Joins("left join pins on obj_refs.pin = pins.id").Where("object = ?", obj.ID).Select("pins.*").Scan(&pins).Error; err != nil {
		log.Errorf("failed to find pins for cid: %s", err)
	}

	_, rootFetchErr := s.Node.Blockstore.Get(cc)
	if rootFetchErr != nil {
		log.Errorf("failed to fetch root: %s", rootFetchErr)
	}

	var exch exchange.Interface
	if c.QueryParam("fetch") != "" {
		exch = s.Node.Bitswap
	}

	bserv := blockservice.New(s.Node.Blockstore, exch)
	dserv := merkledag.NewDAGService(bserv)

	cset := cid.NewSet()
	err = merkledag.Walk(ctx, func(ctx context.Context, c cid.Cid) ([]*ipld.Link, error) {
		node, err := dserv.Get(ctx, c)
		if err != nil {
			return nil, err
		}

		if c.Type() == cid.Raw {
			return nil, nil
		}

		return node.Links(), nil
	}, cc, cset.Visit, merkledag.Concurrent())

	errstr := ""
	if err != nil {
		errstr = err.Error()
	}

	rferrstr := ""
	if rootFetchErr != nil {
		rferrstr = rootFetchErr.Error()
	}

	return c.JSON(200, map[string]interface{}{
		"pins":          pins,
		"cid":           cc,
		"traverseError": errstr,
		"foundBlocks":   cset.Len(),
		"rootFetchErr":  rferrstr,
	})
}

func (s *Shuttle) handleResendPinComplete(c echo.Context) error {
	ctx := c.Request().Context()
	cont, err := strconv.Atoi(c.Param("content"))
	if err != nil {
		return err
	}

	var p Pin
	if err := s.DB.First(&p, "content = ?", cont).Error; err != nil {
		return err
	}

	objects, err := s.objectsForPin(ctx, p.ID)
	if err != nil {
		return fmt.Errorf("failed to get objects for pin: %w", err)
	}

	s.sendPinCompleteMessage(ctx, p.Content, p.Size, objects)

	return c.JSON(200, map[string]string{})
}
