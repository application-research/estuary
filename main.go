package main

import (
	"context"
	crand "crypto/rand"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/filecoin-project/go-address"
	lmdb "github.com/filecoin-project/go-bs-lmdb"
	"github.com/ipfs/go-bitswap"
	bsnet "github.com/ipfs/go-bitswap/network"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	levelds "github.com/ipfs/go-ds-leveldb"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	batched "github.com/ipfs/go-ipfs-provider/batched"
	queue "github.com/ipfs/go-ipfs-provider/queue"
	logging "github.com/ipfs/go-log"
	"github.com/libp2p/go-libp2p"
	connmgr "github.com/libp2p/go-libp2p-connmgr"
	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/metrics"
	"github.com/libp2p/go-libp2p-core/peer"
	crypto "github.com/libp2p/go-libp2p-crypto"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	fullrt "github.com/libp2p/go-libp2p-kad-dht/fullrt"
	"github.com/multiformats/go-multiaddr"
	"github.com/whyrusleeping/estuary/filclient"
	"github.com/whyrusleeping/estuary/keystore"
	bsm "github.com/whyrusleeping/go-bs-measure"
	"go.opentelemetry.io/otel"

	//_ "go.opentelemetry.io/otel/exporters/prometheus"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/xerrors"

	"github.com/filecoin-project/lotus/api"
	"github.com/filecoin-project/lotus/chain/types"
	"github.com/filecoin-project/lotus/chain/wallet"
	lcli "github.com/filecoin-project/lotus/cli"
	cli "github.com/urfave/cli/v2"

	"gorm.io/driver/postgres"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

var log = logging.Logger("estuary")

var bootstrappers = []string{
	"/dnsaddr/bootstrap.libp2p.io/p2p/QmNnooDu7bfjPFoTZYxMNLWUQJyrVwtbZg5gBMjTezGAJN",
	"/dnsaddr/bootstrap.libp2p.io/p2p/QmQCU2EcMqAqQPR2i9bChDtGNJchTbq5TbXJJ16u19uLTa",
	"/dnsaddr/bootstrap.libp2p.io/p2p/QmbLHAnMoJPWSCR5Zhtx6BHJX9KiKNN6tpvbUcqanj75Nb",
	"/dnsaddr/bootstrap.libp2p.io/p2p/QmcZf59bWwK5XFi76CZX8cbJ4BhTzzA3gU1ZjYZcYW3dwt",
}

var defaultMiners []address.Address

func init() {
	//miners from minerX spreadsheet
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
	Address         dbAddr `gorm:"unique"`
	Suspended       bool
	SuspendedReason string
	Name            string
	Version         string
}

type dbAddr struct {
	Addr address.Address
}

func (dba *dbAddr) Scan(v interface{}) error {
	s, ok := v.(string)
	if !ok {
		return fmt.Errorf("dbAddrs must be strings")
	}

	addr, err := address.NewFromString(s)
	if err != nil {
		return err
	}

	dba.Addr = addr
	return nil
}

func (dba dbAddr) Value() (driver.Value, error) {
	return dba.Addr.String(), nil
}

type dbCID struct {
	CID cid.Cid
}

func (dbc *dbCID) Scan(v interface{}) error {
	b, ok := v.([]byte)
	if !ok {
		return fmt.Errorf("dbcids must get bytes!")
	}

	c, err := cid.Cast(b)
	if err != nil {
		return err
	}

	dbc.CID = c
	return nil
}

func (dbc dbCID) Value() (driver.Value, error) {
	return dbc.CID.Bytes(), nil
}

func (dbc dbCID) MarshalJSON() ([]byte, error) {
	return json.Marshal(dbc.CID.String())
}

func (dbc *dbCID) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return err
	}

	c, err := cid.Decode(s)
	if err != nil {
		return err
	}

	dbc.CID = c
	return nil
}

type Content struct {
	ID        uint           `gorm:"primarykey" json:"id"`
	CreatedAt time.Time      `json:"-"`
	UpdatedAt time.Time      `json:"-"`
	DeletedAt gorm.DeletedAt `gorm:"index" json:"-"`

	Cid         dbCID  `json:"cid"`
	Name        string `json:"name"`
	UserID      uint   `json:"userId" gorm:"index"`
	Description string `json:"description"`
	Size        int64  `json:"size"`
	Active      bool   `json:"active"`
	Offloaded   bool   `json:"offloaded"`
	Replication int    `json:"replication"`

	AggregatedIn uint `json:"aggregatedIn"`
	Aggregate    bool `json:"aggregate"`
}

type Object struct {
	ID         uint  `gorm:"primarykey"`
	Cid        dbCID `gorm:"index"`
	Size       int
	Reads      int
	LastAccess time.Time
}

type ObjRef struct {
	ID        uint `gorm:"primarykey"`
	Content   uint
	Object    uint
	Offloaded uint
}

func setupWallet(dir string) (*wallet.LocalWallet, error) {
	kstore, err := keystore.OpenOrInitKeystore(dir)
	if err != nil {
		return nil, err
	}

	wallet, err := wallet.NewWallet(kstore)
	if err != nil {
		return nil, err
	}

	addrs, err := wallet.WalletList(context.TODO())
	if err != nil {
		return nil, err
	}

	if len(addrs) == 0 {
		_, err := wallet.WalletNew(context.TODO(), types.KTBLS)
		if err != nil {
			return nil, err
		}
	}

	defaddr, err := wallet.GetDefault()
	if err != nil {
		return nil, err
	}

	fmt.Println("Wallet address is: ", defaddr)

	return wallet, nil
}

func main() {
	logging.SetLogLevel("dt-impl", "debug")
	logging.SetLogLevel("estuary", "debug")
	logging.SetLogLevel("paych", "debug")
	logging.SetLogLevel("filclient", "debug")
	logging.SetLogLevel("dt_graphsync", "debug")
	logging.SetLogLevel("dt-chanmon", "debug")
	logging.SetLogLevel("markets", "debug")
	logging.SetLogLevel("data_transfer_network", "debug")
	logging.SetLogLevel("rpc", "info")

	app := cli.NewApp()
	app.Flags = []cli.Flag{
		&cli.StringFlag{
			Name:  "repo",
			Value: "~/.lotus",
		},
		&cli.StringFlag{
			Name:    "database",
			Value:   "sqlite=estuary.db",
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
		&cli.BoolFlag{
			Name: "no-storage-cron",
		},
		&cli.BoolFlag{
			Name: "logging",
		},
		&cli.BoolFlag{
			Name: "enable-auto-retrieve",
		},
		&cli.StringFlag{
			Name:    "lightstep-token",
			Usage:   "specify lightstep access token for enabling trace exports",
			EnvVars: []string{"ESTUARY_LIGHTSTEP_TOKEN"},
		},
		&cli.StringFlag{
			Name:  "https-domain",
			Usage: "specify domain name to run ssl for",
		},
		&cli.BoolFlag{
			Name: "fail-deals-on-transfer-failure",
		},
	}
	app.Action = func(cctx *cli.Context) error {
		ddir := cctx.String("datadir")
		cfg := &Config{
			ListenAddrs: []string{
				"/ip4/0.0.0.0/tcp/6744",
			},
			Blockstore:    filepath.Join(ddir, "estuary-blocks"),
			Libp2pKeyFile: filepath.Join(ddir, "estuary-peer.key"),
			Datastore:     filepath.Join(ddir, "estuary-leveldb"),
			WalletDir:     filepath.Join(ddir, "estuary-wallet"),
		}

		api, closer, err := lcli.GetGatewayAPI(cctx)
		//api, closer, err := lcli.GetFullNodeAPI(cctx)
		if err != nil {
			return err
		}

		defer closer()

		db, err := setupDatabase(cctx)
		if err != nil {
			return err
		}

		nd, err := setup(context.Background(), cfg, db)
		if err != nil {
			return err
		}

		addr, err := nd.Wallet.GetDefault()
		if err != nil {
			return err
		}

		sbmgr, err := NewStagingBSMgr(filepath.Join(ddir, "stagingdata"))
		if err != nil {
			return err
		}

		s := &Server{
			Node:       nd,
			Api:        api,
			StagingMgr: sbmgr,
			tracer:     otel.Tracer("api"),
			quickCache: make(map[string]endpointCache),
			pinJobs:    make(map[uint]*pinningOperation),
		}

		fc, err := filclient.NewClient(nd.Host, api, nd.Wallet, addr, nd.Blockstore, nd.Datastore, ddir)
		if err != nil {
			return err
		}

		s.FilClient = fc

		for _, a := range nd.Host.Addrs() {
			fmt.Printf("%s/p2p/%s\n", a, nd.Host.ID())
		}

		go func() {
			for _, bsp := range bootstrappers {

				ma, err := multiaddr.NewMultiaddr(bsp)
				if err != nil {
					fmt.Println("failed to parse bootstrap address: ", err)
					continue
				}
				ai, err := peer.AddrInfoFromP2pAddr(ma)
				if err != nil {
					fmt.Println("failed to create address info: ", err)
					continue
				}

				if err := nd.Host.Connect(context.TODO(), *ai); err != nil {
					fmt.Println("failed to connect to bootstrapper: ", err)
					continue
				}
			}

			if err := nd.Dht.Bootstrap(context.TODO()); err != nil {
				fmt.Println("dht bootstrapping failed: ", err)
			}
		}()

		s.DB = db

		cm := NewContentManager(db, api, fc, s.Node.TrackingBlockstore, nd.Provider)
		fc.SetPieceCommFunc(cm.getPieceCommitment)

		cm.FailDealOnTransferFailure = cctx.Bool("fail-deals-on-transfer-failure")

		cm.tracer = otel.Tracer("replicator")

		if cctx.Bool("enable-auto-retrive") {
			nd.TrackingBlockstore.SetCidReqFunc(cm.RefreshContentForCid)
		}

		if !cctx.Bool("no-storage-cron") {
			go cm.ContentWatcher()
		}

		s.CM = cm

		return s.ServeAPI(cctx.String("apilisten"), cctx.Bool("logging"), cctx.String("https-domain"), cctx.String("lightstep-token"), filepath.Join(ddir, "cache"))
	}

	if err := app.Run(os.Args); err != nil {
		fmt.Println(err)
	}
}

func setupDatabase(cctx *cli.Context) (*gorm.DB, error) {
	dbval := cctx.String("database")
	parts := strings.SplitN(dbval, "=", 2)
	if len(parts) == 1 {
		return nil, fmt.Errorf("format for database string is 'DBTYPE=PARAMS'")
	}

	var dial gorm.Dialector
	switch parts[0] {
	case "sqlite":
		dial = sqlite.Open(parts[1])
	case "postgres":
		dial = postgres.Open(parts[1])
	default:
		return nil, fmt.Errorf("unsupported or unrecognized db type: %s", parts[0])
	}

	db, err := gorm.Open(dial, &gorm.Config{})
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
	db.AutoMigrate(&retrievalFailureRecord{})
	db.AutoMigrate(&retrievalSuccessRecord{})

	db.AutoMigrate(&minerStorageAsk{})
	db.AutoMigrate(&storageMiner{})

	db.AutoMigrate(&User{})
	db.AutoMigrate(&AuthToken{})
	db.AutoMigrate(&InviteCode{})

	var count int64
	if err := db.Model(&storageMiner{}).Count(&count).Error; err != nil {
		return nil, err
	}

	if count == 0 {
		fmt.Println("adding default miner list to database...")
		for _, m := range defaultMiners {
			db.Create(&storageMiner{Address: dbAddr{m}})
		}

	}
	return db, nil
}

type Server struct {
	tracer     trace.Tracer
	Node       *Node
	DB         *gorm.DB
	FilClient  *filclient.FilClient
	Api        api.Gateway
	CM         *ContentManager
	StagingMgr *StagingBSMgr

	cacheLk    sync.Mutex
	quickCache map[string]endpointCache

	pinLk   sync.Mutex
	pinJobs map[uint]*pinningOperation
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

	return nil, false
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

type Node struct {
	Dht      *dht.IpfsDHT
	Provider *batched.BatchProvidingSystem
	Host     host.Host

	Datastore datastore.Batching

	Blockstore         blockstore.Blockstore
	TrackingBlockstore *TrackingBlockstore
	Bitswap            *bitswap.Bitswap

	Wallet *wallet.LocalWallet

	Bwc *metrics.BandwidthCounter

	Config *Config
}

type Config struct {
	ListenAddrs []string

	Blockstore string

	Libp2pKeyFile string

	Datastore string

	WalletDir string
}

func setup(ctx context.Context, cfg *Config, db *gorm.DB) (*Node, error) {
	peerkey, err := loadOrInitPeerKey(cfg.Libp2pKeyFile)
	if err != nil {
		return nil, err
	}

	bwc := metrics.NewBandwidthCounter()

	h, err := libp2p.New(ctx,
		libp2p.ListenAddrStrings(cfg.ListenAddrs...),
		libp2p.NATPortMap(),
		libp2p.ConnectionManager(connmgr.NewConnManager(500, 800, time.Minute)),
		libp2p.Identity(peerkey),
		libp2p.BandwidthReporter(bwc),
	)
	if err != nil {
		return nil, err
	}

	frt, err := fullrt.NewFullRT(h, dht.ProtocolDHT)
	if err != nil {
		return nil, err
	}

	dht, err := dht.New(ctx, h)
	if err != nil {
		return nil, err
	}

	bstore, err := lmdb.Open(&lmdb.Options{
		Path:   cfg.Blockstore,
		NoSync: true,
	})
	if err != nil {
		return nil, err
	}

	mbs := bsm.New("estuary", bstore)

	ds, err := levelds.NewDatastore(cfg.Datastore, nil)
	if err != nil {
		return nil, err
	}

	tbs := NewTrackingBlockstore(mbs, db)

	bsnet := bsnet.NewFromIpfsHost(h, frt)
	bswap := bitswap.New(ctx, bsnet, tbs)

	wallet, err := setupWallet(cfg.WalletDir)
	if err != nil {
		return nil, err
	}

	provq, err := queue.NewQueue(context.Background(), "provq", ds)
	if err != nil {
		return nil, err
	}

	kprov := batched.KeyProvider(func(rpctx context.Context) (<-chan cid.Cid, error) {
		out := make(chan cid.Cid)
		go func() {
			defer close(out)

			var contents []Content
			if err := db.Find(&contents, "active").Error; err != nil {
				log.Errorf("failed to load contents for reproviding: %s", err)
				return
			}

			for _, c := range contents {
				select {
				case out <- c.Cid.CID:
				case <-rpctx.Done():
					return
				}
			}

		}()
		return out, nil
	})

	prov, err := batched.New(frt, provq, kprov)
	if err != nil {
		return nil, err
	}

	return &Node{
		Dht:                dht,
		Provider:           prov,
		Host:               h,
		Blockstore:         mbs,
		Datastore:          ds,
		Bitswap:            bswap.(*bitswap.Bitswap),
		TrackingBlockstore: tbs,
		Wallet:             wallet,
		Bwc:                bwc,
		Config:             cfg,
	}, nil
}

func loadOrInitPeerKey(kf string) (crypto.PrivKey, error) {
	data, err := ioutil.ReadFile(kf)
	if err != nil {
		if !os.IsNotExist(err) {
			return nil, err
		}

		k, _, err := crypto.GenerateEd25519Key(crand.Reader)
		if err != nil {
			return nil, err
		}

		data, err := crypto.MarshalPrivateKey(k)
		if err != nil {
			return nil, err
		}

		if err := ioutil.WriteFile(kf, data, 0600); err != nil {
			return nil, err
		}

		return k, nil
	}
	return crypto.UnmarshalPrivateKey(data)
}

func jsondump(o interface{}) {
	data, _ := json.MarshalIndent(o, "", "  ")
	fmt.Println(string(data))
}
