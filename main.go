package main

import (
	"context"
	crand "crypto/rand"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/filecoin-project/go-address"
	lmdb "github.com/filecoin-project/go-bs-lmdb"
	datatransfer "github.com/filecoin-project/go-data-transfer"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/ipfs/go-bitswap"
	bsnet "github.com/ipfs/go-bitswap/network"
	"github.com/ipfs/go-blockservice"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	levelds "github.com/ipfs/go-ds-leveldb"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	chunker "github.com/ipfs/go-ipfs-chunker"
	logging "github.com/ipfs/go-log"
	"github.com/ipfs/go-merkledag"
	importer "github.com/ipfs/go-unixfs/importer"
	"github.com/libp2p/go-libp2p"
	connmgr "github.com/libp2p/go-libp2p-connmgr"
	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/peer"
	crypto "github.com/libp2p/go-libp2p-crypto"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	"github.com/multiformats/go-multiaddr"
	"github.com/whyrusleeping/estuary/filclient"
	"golang.org/x/xerrors"

	"github.com/filecoin-project/lotus/api"
	"github.com/filecoin-project/lotus/chain/types"
	"github.com/filecoin-project/lotus/chain/wallet"
	lcli "github.com/filecoin-project/lotus/cli"
	cli "github.com/urfave/cli/v2"

	"gorm.io/driver/postgres"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
)

var log = logging.Logger("estuary")

var bootstrappers = []string{
	"/dnsaddr/bootstrap.libp2p.io/p2p/QmNnooDu7bfjPFoTZYxMNLWUQJyrVwtbZg5gBMjTezGAJN",
	"/dnsaddr/bootstrap.libp2p.io/p2p/QmQCU2EcMqAqQPR2i9bChDtGNJchTbq5TbXJJ16u19uLTa",
	"/dnsaddr/bootstrap.libp2p.io/p2p/QmbLHAnMoJPWSCR5Zhtx6BHJX9KiKNN6tpvbUcqanj75Nb",
	"/dnsaddr/bootstrap.libp2p.io/p2p/QmcZf59bWwK5XFi76CZX8cbJ4BhTzzA3gU1ZjYZcYW3dwt",
}

var miners []address.Address

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

		miners = append(miners, a)
	}
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

type Content struct {
	gorm.Model
	Cid    dbCID
	Name   string
	User   string
	Size   int64
	Active bool
}

type Object struct {
	ID    uint `gorm:"primarykey"`
	Cid   dbCID
	Size  int
	Reads int
}

type ObjRef struct {
	ID      uint `gorm:"primarykey"`
	Content uint
	Object  uint
}

func setupWallet(dir string) (*wallet.LocalWallet, error) {
	kstore, err := OpenOrInitKeystore(dir)
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

	app := cli.NewApp()
	app.Flags = []cli.Flag{
		&cli.StringFlag{
			Name:  "repo",
			Value: "~/.lotus",
		},
		&cli.StringFlag{
			Name:  "database",
			Value: "sqlite=estuary.db",
		},
	}
	app.Action = func(cctx *cli.Context) error {
		cfg := &Config{
			ListenAddrs: []string{
				"/ip4/0.0.0.0/tcp/6744",
			},
			Blockstore:    "estuary-blocks",
			Libp2pKeyFile: "estuary-peer.key",
			Datastore:     "estuary-leveldb",
		}

		//api, closer, err := lcli.GetGatewayAPI(cctx)
		api, closer, err := lcli.GetFullNodeAPI(cctx)
		if err != nil {
			return err
		}

		defer closer()

		nd, err := setup(context.Background(), cfg)
		if err != nil {
			return err
		}

		addr, err := nd.Wallet.GetDefault()
		if err != nil {
			return err
		}

		s := &Server{
			Node: nd,
			Api:  api,
		}

		fc, err := filclient.NewClient(nd.Host, api, nd.Wallet, addr, nd.Blockstore, nd.Datastore, s.getPieceCommitment)
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

		db, err := setupDatabase(cctx)
		if err != nil {
			return err
		}

		s.DB = db

		cm := NewContentManager(db, api, fc)
		go cm.ContentWatcher()

		s.CM = cm

		e := echo.New()
		e.HTTPErrorHandler = func(err error, ctx echo.Context) {
			log.Errorf("handler error: %s", err)
		}
		e.Use(middleware.CORS())
		e.POST("/content/add", s.handleAdd)
		e.GET("/content/stats", s.handleStats)
		e.GET("/content/ensure-replication/:datacid", s.handleEnsureReplication)
		e.GET("/content/status/:id", s.handleContentStatus)
		e.GET("/content/list", s.handleListContent)

		e.GET("/deals/query/:miner", s.handleQueryAsk)
		e.POST("/deals/make/:miner", s.handleMakeDeal)
		e.POST("/deals/transfer/start/:miner/:propcid/:datacid", s.handleTransferStart)
		e.POST("/deals/transfer/status", s.handleTransferStatus)
		e.GET("/deals/transfer/in-progress", s.handleTransferInProgress)
		e.POST("/deals/transfer/restart", s.handleTransferRestart)
		e.GET("/deals/status/:miner/:propcid", s.handleDealStatus)

		e.GET("/admin/balance", s.handleAdminBalance)
		e.GET("/admin/add-escrow/:amt", s.handleAdminAddEscrow)

		return e.Start(":3004")
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

	db.AutoMigrate(&contentDeal{})
	db.AutoMigrate(&dfeRecord{})
	db.AutoMigrate(&PieceCommRecord{})

	return db, nil
}

type Server struct {
	Node      *Node
	DB        *gorm.DB
	FilClient *filclient.FilClient
	Api       api.GatewayAPI
	CM        *ContentManager
}

type statsResp struct {
	Cid           cid.Cid
	File          string
	BWUsed        int64
	TotalRequests int64
}

func (s *Server) handleStats(c echo.Context) error {
	var contents []Content
	if err := s.DB.Find(&contents).Error; err != nil {
		return err
	}

	var out []statsResp
	for _, c := range contents {
		q := `select *
from obj_refs
left join objects on objects.id == obj_refs.object
where obj_refs.content = ?`
		var objects []Object
		if err := s.DB.Raw(q, c.ID).Find(&objects).Error; err != nil {
			return xerrors.Errorf("object lookup failed: %w", err)
		}

		counts, err := s.Node.TrackingBlockstore.GetCounts(objects)
		if err != nil {
			return err
		}

		st := statsResp{
			Cid:  c.Cid.CID,
			File: c.Name,
		}

		for i, count := range counts {
			st.TotalRequests += int64(count)
			st.BWUsed += int64(count * objects[i].Size)
		}
		out = append(out, st)
	}

	return c.JSON(200, out)
}

func (s *Server) handleAdd(c echo.Context) error {
	vals, err := c.FormParams()
	if err != nil {
		return err
	}

	namevals := vals["name"]
	if len(namevals) == 0 || namevals[0] == "" {
		return c.JSON(400, map[string]string{"error": "must specify a filename"})
	}
	fname := namevals[0]

	mpf, err := c.FormFile("data")
	if err != nil {
		return err
	}
	fi, err := mpf.Open()
	if err != nil {
		return err
	}

	bserv := blockservice.New(s.Node.Blockstore, nil)
	dserv := merkledag.NewDAGService(bserv)
	// TODO: use a temporary on-disk blockstore to store the uploaded data until the user pays us
	spl := chunker.DefaultSplitter(fi)
	nd, err := importer.BuildDagFromReader(dserv, spl)
	if err != nil {
		return err
	}

	var objects []*Object
	var totalSize int64
	cset := cid.NewSet()
	err = merkledag.Walk(context.TODO(), dserv.GetLinks, nd.Cid(), func(c cid.Cid) bool {
		if cset.Visit(c) {
			size, err := s.Node.Blockstore.GetSize(c)
			if err != nil {
				log.Errorf("failed to get object size in walk %s: %s", c, err)
			}
			objects = append(objects, &Object{
				Cid:  dbCID{c},
				Size: size,
			})
			totalSize += int64(size)

			return true
		}
		return false
	})
	if err != nil {
		return err
	}

	if err := s.DB.Create(objects).Error; err != nil {
		return xerrors.Errorf("failed to create objects in db: %w", err)
	}

	// okay cool, we added the content, now track it
	content := &Content{
		Cid:    dbCID{nd.Cid()},
		Size:   totalSize,
		Name:   fname,
		Active: true,
	}

	if err := s.DB.Create(content).Error; err != nil {
		return xerrors.Errorf("failed to track new content in database: %w", err)
	}

	refs := make([]ObjRef, len(objects))
	for i := range refs {
		refs[i].Content = content.ID
		refs[i].Object = objects[i].ID
	}

	if err := s.DB.Create(refs).Error; err != nil {
		return xerrors.Errorf("failed to create refs: %w", err)
	}

	s.CM.ToCheck <- content.ID

	go func() {
		if err := s.Node.Dht.Provide(context.TODO(), nd.Cid(), true); err != nil {
			fmt.Println("providing failed: ", err)
		}
		fmt.Println("providing complete")
	}()
	return c.JSON(200, map[string]string{"cid": nd.Cid().String()})
}

func (s *Server) handleEnsureReplication(c echo.Context) error {
	data, err := cid.Decode(c.Param("datacid"))
	if err != nil {
		return err
	}

	var content Content
	if err := s.DB.Find(&content, "cid = ?", data.Bytes()).Error; err != nil {
		return err
	}

	fmt.Println("Content: ", content.Cid.CID, data)

	s.CM.ToCheck <- content.ID
	return nil
}

func (s *Server) handleListContent(c echo.Context) error {
	var contents []Content
	if err := s.DB.Find(&contents, "active").Error; err != nil {
		return err
	}

	var ids []uint
	for _, c := range contents {
		ids = append(ids, c.ID)
	}

	return c.JSON(200, ids)
}

type dealStatus struct {
	Deal           contentDeal             `json:"deal"`
	TransferStatus *filclient.ChannelState `json:"transfer"`
}

func (s *Server) handleContentStatus(c echo.Context) error {
	ctx := context.TODO()
	val, err := strconv.Atoi(c.Param("id"))
	if err != nil {
		return err
	}

	var content Content
	if err := s.DB.First(&content, "id = ?", val).Error; err != nil {
		return err
	}

	var deals []contentDeal
	if err := s.DB.Find(&deals, "content = ?", content.ID).Error; err != nil {
		return err
	}

	var ds []dealStatus
	for _, d := range deals {
		maddr, err := d.MinerAddr()
		if err != nil {
			return err
		}
		chanst, err := s.FilClient.TransferStatusForContent(ctx, content.Cid.CID, maddr)
		if err != nil && err != filclient.ErrNoTransferFound {
			return err
		}

		ds = append(ds, dealStatus{
			Deal:           d,
			TransferStatus: chanst,
		})
	}

	var failures []dfeRecord
	if err := s.DB.Find(&failures, "content = ?", content.ID).Error; err != nil {
		return err
	}

	return c.JSON(200, map[string]interface{}{
		"content":  content,
		"deals":    ds,
		"failures": failures,
	})
}

func (s *Server) handleQueryAsk(c echo.Context) error {
	addr, err := address.NewFromString(c.Param("miner"))
	if err != nil {
		return err
	}

	ask, err := s.FilClient.GetAsk(context.TODO(), addr)
	if err != nil {
		return err
	}

	return c.JSON(200, ask)
}

type dealRequest struct {
	Cid      cid.Cid
	Price    types.BigInt
	Duration abi.ChainEpoch
}

func (s *Server) handleMakeDeal(c echo.Context) error {
	ctx := context.TODO()

	addr, err := address.NewFromString(c.Param("miner"))
	if err != nil {
		return err
	}

	var req dealRequest
	if err := c.Bind(&req); err != nil {
		return err
	}

	proposal, err := s.FilClient.MakeDeal(ctx, addr, req.Cid, req.Price, req.Duration)
	if err != nil {
		return err
	}

	raw, err := json.MarshalIndent(proposal, "", "  ")
	if err != nil {
		return err
	}
	fmt.Println("deal proposal: ", string(raw))

	resp, err := s.FilClient.SendProposal(ctx, proposal)
	if err != nil {
		return err
	}

	return c.JSON(200, resp)
}

func (s *Server) handleTransferStatus(c echo.Context) error {
	var chanid datatransfer.ChannelID
	if err := c.Bind(&chanid); err != nil {
		return err
	}

	status, err := s.FilClient.TransferStatus(context.TODO(), &chanid)
	if err != nil {
		return err
	}

	return c.JSON(200, status)
}

func (s *Server) handleTransferInProgress(c echo.Context) error {
	ctx := context.TODO()

	transfers, err := s.FilClient.TransfersInProgress(ctx)
	if err != nil {
		return err
	}

	out := make(map[string]*filclient.ChannelState)
	for chanid, state := range transfers {
		out[chanid.String()] = filclient.ChannelStateConv(state)
	}

	return c.JSON(200, out)
}

func (s *Server) handleTransferRestart(c echo.Context) error {
	var chanid datatransfer.ChannelID
	if err := c.Bind(&chanid); err != nil {
		return err
	}

	err := s.FilClient.RestartTransfer(context.TODO(), &chanid)
	if err != nil {
		return err
	}

	return nil
}

func (s *Server) handleTransferStart(c echo.Context) error {
	addr, err := address.NewFromString(c.Param("miner"))
	if err != nil {
		return err
	}

	propCid, err := cid.Decode(c.Param("propcid"))
	if err != nil {
		return err
	}

	dataCid, err := cid.Decode(c.Param("datacid"))
	if err != nil {
		return err
	}

	chanid, err := s.FilClient.StartDataTransfer(context.TODO(), addr, propCid, dataCid)
	if err != nil {
		return err
	}

	return c.JSON(200, chanid)
}

func (s *Server) handleDealStatus(c echo.Context) error {
	ctx := context.TODO()

	addr, err := address.NewFromString(c.Param("miner"))
	if err != nil {
		return err
	}

	propCid, err := cid.Decode(c.Param("propcid"))
	if err != nil {
		return err
	}

	status, err := s.FilClient.DealStatus(ctx, addr, propCid)
	if err != nil {
		return xerrors.Errorf("getting deal status: %w", err)
	}

	return c.JSON(200, status)
}

func (s *Server) handleAdminBalance(c echo.Context) error {
	balance, err := s.FilClient.Balance(context.TODO())
	if err != nil {
		return err
	}

	return c.JSON(200, balance)
}

func (s *Server) handleAdminAddEscrow(c echo.Context) error {
	amt, err := types.ParseFIL(c.Param("amt"))
	if err != nil {
		return err
	}

	resp, err := s.FilClient.LockMarketFunds(context.TODO(), amt)
	if err != nil {
		return err
	}

	return c.JSON(200, resp)
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

type PieceCommRecord struct {
	Data  dbCID `gorm:"unique"`
	Piece dbCID
	Size  abi.UnpaddedPieceSize
}

func (s *Server) getPieceCommitment(rt abi.RegisteredSealProof, data cid.Cid, bs blockstore.Blockstore) (cid.Cid, abi.UnpaddedPieceSize, error) {
	var pcr PieceCommRecord
	err := s.DB.First(&pcr, "data = ?", data.Bytes()).Error
	if err == nil {
		fmt.Println("database response!!!")
		if !pcr.Piece.CID.Defined() {
			return cid.Undef, 0, fmt.Errorf("got an undefined thing back from database")
		}
		return pcr.Piece.CID, pcr.Size, nil
	}

	if !xerrors.Is(err, gorm.ErrRecordNotFound) {
		return cid.Undef, 0, err
	}

	pc, size, err := filclient.GeneratePieceCommitment(rt, data, bs)
	if err != nil {
		return cid.Undef, 0, xerrors.Errorf("failed to generate piece commitment: %w", err)
	}

	pcr = PieceCommRecord{
		Data:  dbCID{data},
		Piece: dbCID{pc},
		Size:  size,
	}

	if err := s.DB.Clauses(clause.OnConflict{DoNothing: true}).Create(&pcr).Error; err != nil {
		return cid.Undef, 0, err
	}

	return pc, size, nil
}

type Node struct {
	Dht  *dht.IpfsDHT
	Host host.Host

	Datastore datastore.Batching

	Blockstore         blockstore.Blockstore
	TrackingBlockstore *TrackingBlockstore
	Bitswap            *bitswap.Bitswap

	Wallet *wallet.LocalWallet
}

type Config struct {
	ListenAddrs []string

	Blockstore string

	Libp2pKeyFile string

	Datastore string
}

func setup(ctx context.Context, cfg *Config) (*Node, error) {
	peerkey, err := loadOrInitPeerKey(cfg.Libp2pKeyFile)
	if err != nil {
		return nil, err
	}

	h, err := libp2p.New(ctx,
		libp2p.ListenAddrStrings(cfg.ListenAddrs...),
		libp2p.NATPortMap(),
		libp2p.ConnectionManager(connmgr.NewConnManager(500, 800, time.Minute)),
		libp2p.Identity(peerkey),
	)
	if err != nil {
		return nil, err
	}

	dht, err := dht.New(ctx, h)
	if err != nil {
		return nil, err
	}

	bstore, err := lmdb.Open(&lmdb.Options{
		Path: cfg.Blockstore,
	})
	if err != nil {
		return nil, err
	}

	ds, err := levelds.NewDatastore(cfg.Datastore, nil)
	if err != nil {
		return nil, err
	}

	tbs := NewTrackingBlockstore(bstore, nil)

	bsnet := bsnet.NewFromIpfsHost(h, dht)
	bswap := bitswap.New(ctx, bsnet, tbs)

	wallet, err := setupWallet("estuary-wallet")
	if err != nil {
		return nil, err
	}

	return &Node{
		Dht:                dht,
		Host:               h,
		Blockstore:         bstore,
		Datastore:          ds,
		Bitswap:            bswap.(*bitswap.Bitswap),
		TrackingBlockstore: tbs,
		Wallet:             wallet,
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
