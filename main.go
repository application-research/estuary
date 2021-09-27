package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/application-research/estuary/node"
	"github.com/application-research/estuary/pinner"
	"github.com/application-research/estuary/stagingbs"
	"github.com/application-research/estuary/util"
	"github.com/application-research/filclient"
	"github.com/filecoin-project/go-address"
	"github.com/google/uuid"
	"github.com/ipfs/go-cid"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	logging "github.com/ipfs/go-log/v2"
	"go.opentelemetry.io/otel"

	//_ "go.opentelemetry.io/otel/exporters/prometheus"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/xerrors"

	"github.com/filecoin-project/lotus/api"
	lcli "github.com/filecoin-project/lotus/cli"
	cli "github.com/urfave/cli/v2"

	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

var log = logging.Logger("estuary")

var defaultMiners []address.Address

var defaultDatabaseValue = "sqlite=estuary.db"

func init() {
	// miners from minerX spreadsheet
	minerStrs := []string{
		"f02620",
		"f023971",
		"f022142",
		"f019551",
		"f01240",
		"f01247",
		"f01278",
		"f071624",
		"f0135078",
		"f022352",
		"f014768",
		"f022163",
		"f09848",
		"f02576",
		"f02606",
		"f019041",
		"f010617",
		"f023467",
		"f01276",
		"f02401",
		"f02387",
		"f019104",
		"f099608",
		"f062353",
		"f07998",
		"f019362",
		"f019100",
		"f014409",
		"f066596",
		"f01234",
		"f058369",
		"f08399",
		"f021716",
		"f010479",
		"f08403",
		"f01277",
		"f015927",
	}

	for _, s := range minerStrs {
		a, err := address.NewFromString(s)
		if err != nil {
			panic(err)
		}

		defaultMiners = append(defaultMiners, a)
	}
}

type storageMiner struct {
	gorm.Model
	Address         util.DbAddr `gorm:"unique"`
	Suspended       bool
	SuspendedReason string
	Name            string
	Version         string
	Location        string
	Owner           uint
}

type Content struct {
	ID        uint           `gorm:"primarykey" json:"id"`
	CreatedAt time.Time      `json:"-"`
	UpdatedAt time.Time      `json:"-"`
	DeletedAt gorm.DeletedAt `gorm:"index" json:"-"`

	Cid         util.DbCID `json:"cid"`
	Name        string     `json:"name"`
	UserID      uint       `json:"userId" gorm:"index"`
	Description string     `json:"description"`
	Size        int64      `json:"size"`
	Active      bool       `json:"active"`
	Offloaded   bool       `json:"offloaded"`
	Replication int        `json:"replication"`

	// TODO: shift most of the 'state' booleans in here into a single state
	// field, should make reasoning about things much simpler
	AggregatedIn uint `json:"aggregatedIn" gorm:"index:,option:CONCURRENTLY"`
	Aggregate    bool `json:"aggregate"`

	Pinning bool   `json:"pinning"`
	PinMeta string `json:"pinMeta"`

	Failed bool `json:"failed"`

	Location string `json:"location"`
	// TODO: shift location tracking to just use the ID of the shuttle
	// Also move towards recording content movement intentions in the database,
	// making that process more resilient to failures
	// LocID     uint   `json:"locID"`
	// LocIntent uint   `json:"locIntent"`

	// If set, this content is part of a split dag.
	// In such a case, the 'root' content should be advertised on the dht, but
	// not have deals made for it, and the children should have deals made for
	// them (unlike with aggregates)
	// For efficiencys sake, we will reuse the aggregate fields for describing
	// the structure of splitdags
	DagSplit bool `json:"dagSplit"`
}

type Object struct {
	ID         uint       `gorm:"primarykey"`
	Cid        util.DbCID `gorm:"index"`
	Size       int
	Reads      int
	LastAccess time.Time
}

type ObjRef struct {
	ID        uint `gorm:"primarykey"`
	Content   uint `gorm:"index:,option:CONCURRENTLY"`
	Object    uint `gorm:"index:,option:CONCURRENTLY"`
	Offloaded uint
}

func main() {
	logging.SetLogLevel("dt-impl", "debug")
	logging.SetLogLevel("estuary", "debug")
	logging.SetLogLevel("paych", "debug")
	logging.SetLogLevel("filclient", "debug")
	logging.SetLogLevel("dt_graphsync", "debug")
	logging.SetLogLevel("graphsync_allocator", "debug")
	logging.SetLogLevel("dt-chanmon", "debug")
	logging.SetLogLevel("markets", "debug")
	logging.SetLogLevel("data_transfer_network", "debug")
	logging.SetLogLevel("rpc", "info")
	logging.SetLogLevel("bs-wal", "info")
	logging.SetLogLevel("provider.batched", "info")

	app := cli.NewApp()
	app.Flags = []cli.Flag{
		&cli.StringFlag{
			Name:  "repo",
			Value: "~/.lotus",
		},
		&cli.StringFlag{
			Name:    "database",
			Usage:   "specify connection string for estuary database",
			Value:   defaultDatabaseValue,
			EnvVars: []string{"ESTUARY_DATABASE"},
		},
		&cli.StringFlag{
			Name:    "apilisten",
			Usage:   "address for the api server to listen on",
			Value:   ":3004",
			EnvVars: []string{"ESTUARY_API_LISTEN"},
		},
		&cli.StringFlag{
			Name:    "datadir",
			Usage:   "directory to store data in",
			Value:   ".",
			EnvVars: []string{"ESTUARY_DATADIR"},
		},
		&cli.StringFlag{
			Name:   "write-log",
			Usage:  "enable write log blockstore in specified directory",
			Hidden: true,
		},
		&cli.BoolFlag{
			Name:  "no-storage-cron",
			Usage: "run estuary without processing files into deals",
		},
		&cli.BoolFlag{
			Name:  "logging",
			Usage: "enable api endpoint logging",
		},
		&cli.BoolFlag{
			Name:   "enable-auto-retrieve",
			Hidden: true,
		},
		&cli.StringFlag{
			Name:    "lightstep-token",
			Usage:   "specify lightstep access token for enabling trace exports",
			EnvVars: []string{"ESTUARY_LIGHTSTEP_TOKEN"},
		},
		&cli.StringFlag{
			Name:  "hostname",
			Usage: "specify hostname this node will be reachable at",
			Value: "http://localhost:3004",
		},
		&cli.BoolFlag{
			Name:  "fail-deals-on-transfer-failure",
			Usage: "consider deals failed when the transfer to the miner fails",
		},
		&cli.BoolFlag{
			Name:  "disable-deal-making",
			Usage: "do not create any new deals (existing deals will still be processed)",
		},
		&cli.BoolFlag{
			Name:  "disable-content-adding",
			Usage: "disallow new content ingestion globally",
		},
		&cli.BoolFlag{
			Name:  "disable-local-content-adding",
			Usage: "disallow new content ingestion on this node (shuttles are unaffected)",
		},
		&cli.StringFlag{
			Name:  "blockstore",
			Usage: "specify blockstore parameters",
		},
		&cli.BoolFlag{
			Name: "write-log-truncate",
		},
		&cli.IntFlag{
			Name:  "default-replication",
			Value: 6,
		},
	}
	app.Commands = []*cli.Command{
		{
			Name:  "setup",
			Usage: "Creates an initial auth token under new user \"admin\"",
			Action: func(cctx *cli.Context) error {
				db, err := setupDatabase(cctx)
				if err != nil {
					return err
				}

				quietdb := db.Session(&gorm.Session{
					Logger: logger.Discard,
				})

				username := "admin"
				passHash := ""

				if err := quietdb.First(&User{}, "username = ?", username).Error; err == nil {
					return fmt.Errorf("an admin user already exists")
				}

				newUser := &User{
					UUID:     uuid.New().String(),
					Username: username,
					PassHash: passHash,
					Perm:     100,
				}
				if err := db.Create(newUser).Error; err != nil {
					return fmt.Errorf("admin user creation failed: %w", err)
				}

				authToken := &AuthToken{
					Token:  "EST" + uuid.New().String() + "ARY",
					User:   newUser.ID,
					Expiry: time.Now().Add(time.Hour * 24 * 365),
				}
				if err := db.Create(authToken).Error; err != nil {
					return fmt.Errorf("admin token creation failed: %w", err)
				}

				fmt.Printf("Auth Token: %v\n", authToken.Token)

				return nil
			},
		},
	}
	app.Action = func(cctx *cli.Context) error {
		ddir := cctx.String("datadir")

		bstore := filepath.Join(ddir, "estuary-blocks")
		if bs := cctx.String("blockstore"); bs != "" {
			bstore = bs
		}
		cfg := &node.Config{
			ListenAddrs: []string{
				"/ip4/0.0.0.0/tcp/6744",
			},
			Blockstore:       bstore,
			Libp2pKeyFile:    filepath.Join(ddir, "estuary-peer.key"),
			Datastore:        filepath.Join(ddir, "estuary-leveldb"),
			WalletDir:        filepath.Join(ddir, "estuary-wallet"),
			WriteLogTruncate: cctx.Bool("write-log-truncate"),
		}

		if wl := cctx.String("write-log"); wl != "" {
			if wl[0] == '/' {
				cfg.WriteLog = wl
			} else {
				cfg.WriteLog = filepath.Join(ddir, wl)
			}
		}

		db, err := setupDatabase(cctx)
		if err != nil {
			return err
		}

		defaultReplication = cctx.Int("default-replication")

		cfg.KeyProviderFunc = func(rpctx context.Context) (<-chan cid.Cid, error) {
			log.Infof("running key provider func")
			out := make(chan cid.Cid)
			go func() {
				defer close(out)

				var contents []Content
				if err := db.Find(&contents, "active").Error; err != nil {
					log.Errorf("failed to load contents for reproviding: %s", err)
					return
				}
				log.Infof("key provider func returning %d values", len(contents))

				for _, c := range contents {
					select {
					case out <- c.Cid.CID:
					case <-rpctx.Done():
						return
					}
				}
			}()
			return out, nil
		}

		var trackingBstore *TrackingBlockstore
		cfg.BlockstoreWrap = func(bs blockstore.Blockstore) (blockstore.Blockstore, error) {
			trackingBstore = NewTrackingBlockstore(bs, db)
			return trackingBstore, nil
		}

		nd, err := node.Setup(context.Background(), cfg)
		if err != nil {
			return err
		}

		addr, err := nd.Wallet.GetDefault()
		if err != nil {
			return err
		}

		sbmgr, err := stagingbs.NewStagingBSMgr(filepath.Join(ddir, "stagingdata"))
		if err != nil {
			return err
		}

		api, closer, err := lcli.GetGatewayAPI(cctx)
		// api, closer, err := lcli.GetFullNodeAPI(cctx)
		if err != nil {
			return err
		}
		defer closer()

		s := &Server{
			Node:       nd,
			Api:        api,
			StagingMgr: sbmgr,
			tracer:     otel.Tracer("api"),
			quickCache: make(map[string]endpointCache),
		}

		// TODO: this is an ugly self referential hack... should fix
		pinmgr := pinner.NewPinManager(s.doPinning, nil, &pinner.PinManagerOpts{
			MaxActivePerUser: 20,
		})

		go pinmgr.Run(50)

		fc, err := filclient.NewClient(nd.Host, api, nd.Wallet, addr, nd.Blockstore, nd.Datastore, ddir)
		if err != nil {
			return err
		}

		s.FilClient = fc

		for _, a := range nd.Host.Addrs() {
			fmt.Printf("%s/p2p/%s\n", a, nd.Host.ID())
		}

		go func() {
			for _, ai := range node.BootstrapPeers {
				if err := nd.Host.Connect(context.TODO(), ai); err != nil {
					fmt.Println("failed to connect to bootstrapper: ", err)
					continue
				}
			}

			if err := nd.Dht.Bootstrap(context.TODO()); err != nil {
				fmt.Println("dht bootstrapping failed: ", err)
			}
		}()

		s.DB = db

		cm, err := NewContentManager(db, api, fc, trackingBstore, s.Node.NotifBlockstore, nd.Provider, pinmgr, nd, cctx.String("hostname"))
		if err != nil {
			return err
		}

		fc.SetPieceCommFunc(cm.getPieceCommitment)

		cm.FailDealOnTransferFailure = cctx.Bool("fail-deals-on-transfer-failure")

		cm.isDealMakingDisabled = cctx.Bool("disable-deal-making")
		cm.contentAddingDisabled = cctx.Bool("disable-content-adding")
		cm.localContentAddingDisabled = cctx.Bool("disable-local-content-adding")

		cm.tracer = otel.Tracer("replicator")

		if cctx.Bool("enable-auto-retrive") {
			trackingBstore.SetCidReqFunc(cm.RefreshContentForCid)
		}

		if !cctx.Bool("no-storage-cron") {
			go cm.ContentWatcher()
		}

		s.CM = cm

		if !cm.contentAddingDisabled {
			go func() {
				// wait for shuttles to reconnect
				// This is a bit of a hack, and theres probably a better way to
				// solve this. but its good enough for now
				time.Sleep(time.Second * 10)

				if err := cm.refreshPinQueue(); err != nil {
					log.Errorf("failed to refresh pin queue: %s", err)
				}
			}()
		}

		return s.ServeAPI(cctx.String("apilisten"), cctx.Bool("logging"), cctx.String("lightstep-token"), filepath.Join(ddir, "cache"))
	}

	if err := app.Run(os.Args); err != nil {
		fmt.Println(err)
	}
}

func setupDatabase(cctx *cli.Context) (*gorm.DB, error) {
	dbval := cctx.String("database")

	/* TODO: change this default
	ddir := cctx.String("datadir")
	if dbval == defaultDatabaseValue && ddir != "." {
		dbval = "sqlite=" + filepath.Join(ddir, "estuary.db")
	}
	*/

	db, err := util.SetupDatabase(dbval)
	if err != nil {
		return nil, err
	}

	db.AutoMigrate(&Content{})
	db.AutoMigrate(&Object{})
	db.AutoMigrate(&ObjRef{})
	db.AutoMigrate(&Collection{})
	db.AutoMigrate(&CollectionRef{})

	db.AutoMigrate(&contentDeal{})
	db.AutoMigrate(&dfeRecord{})
	db.AutoMigrate(&PieceCommRecord{})
	db.AutoMigrate(&proposalRecord{})
	db.AutoMigrate(&util.RetrievalFailureRecord{})
	db.AutoMigrate(&retrievalSuccessRecord{})

	db.AutoMigrate(&minerStorageAsk{})
	db.AutoMigrate(&storageMiner{})

	db.AutoMigrate(&User{})
	db.AutoMigrate(&AuthToken{})
	db.AutoMigrate(&InviteCode{})

	db.AutoMigrate(&Shuttle{})

	var count int64
	if err := db.Model(&storageMiner{}).Count(&count).Error; err != nil {
		return nil, err
	}

	if count == 0 {
		fmt.Println("adding default miner list to database...")
		for _, m := range defaultMiners {
			db.Create(&storageMiner{Address: util.DbAddr{m}})
		}

	}
	return db, nil
}

type Server struct {
	tracer     trace.Tracer
	Node       *node.Node
	DB         *gorm.DB
	FilClient  *filclient.FilClient
	Api        api.Gateway
	CM         *ContentManager
	StagingMgr *stagingbs.StagingBSMgr

	cacheLk    sync.Mutex
	quickCache map[string]endpointCache
}

type endpointCache struct {
	lastComputed time.Time
	val          interface{}
}

func (s *Server) checkCache(endpoint string, ttl time.Duration) (interface{}, bool) {
	s.cacheLk.Lock()
	defer s.cacheLk.Unlock()

	ec, ok := s.quickCache[endpoint]
	if !ok {
		return nil, false
	}

	if time.Since(ec.lastComputed) < ttl {
		return ec.val, true
	}

	return ec.val, false
}

func (s *Server) setCache(endpoint string, val interface{}) {
	s.cacheLk.Lock()
	defer s.cacheLk.Unlock()
	s.quickCache[endpoint] = endpointCache{
		lastComputed: time.Now(),
		val:          val,
	}
}

func (s *Server) GarbageCollect(ctx context.Context) error {
	// since we're reference counting all the content, garbage collection becomes easy
	// its even easier if we don't care that its 'perfect'

	// We can probably even just remove stuff when its references are removed from the database
	keych, err := s.Node.Blockstore.AllKeysChan(ctx)
	if err != nil {
		return err
	}

	for c := range keych {
		keep, err := s.trackingObject(c)
		if err != nil {
			return err
		}

		if !keep {
			// can batch these deletes and execute them at the datastore layer for more perfs
			if err := s.Node.Blockstore.DeleteBlock(c); err != nil {
				return err
			}
		}
	}

	return nil
}

func (s *Server) trackingObject(c cid.Cid) (bool, error) {
	var count int64
	if err := s.DB.Model(&Object{}).Where("cid = ?", c.Bytes()).Count(&count).Error; err != nil {
		if xerrors.Is(err, gorm.ErrRecordNotFound) {
			return false, nil
		}
		return false, err
	}

	return count > 0, nil
}

func jsondump(o interface{}) {
	data, _ := json.MarshalIndent(o, "", "  ")
	fmt.Println(string(data))
}
