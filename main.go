package main

import (
	"bytes"
	"context"
	crand "crypto/rand"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"time"

	"github.com/filecoin-project/go-address"
	lmdb "github.com/filecoin-project/go-bs-lmdb"
	cborutil "github.com/filecoin-project/go-cbor-util"
	datatransfer "github.com/filecoin-project/go-data-transfer"
	"github.com/filecoin-project/go-fil-markets/storagemarket"
	"github.com/filecoin-project/go-fil-markets/storagemarket/network"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/big"
	"github.com/filecoin-project/specs-actors/actors/builtin/market"
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

	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"github.com/labstack/echo/v4"
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
	Cid  dbCID
	Name string
	User string
	Size int64
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

var testMarketsCmd = &cli.Command{
	Name: "test-markets",
	Action: func(cctx *cli.Context) error {
		return nil
		/*
			ctx := context.TODO()
			h, err := libp2p.New(ctx)
			if err != nil {
				return err
			}

			api, closer, err := lcli.GetGatewayAPI(cctx)
			if err != nil {
				return err
			}

			defer closer()

			fc, err := filc.NewClient(h, api, wallet, addr)
			if err != nil {
				return err
			}

			addr, err := address.NewFromString(cctx.Args().First())
			if err != nil {
				return err
			}

			fmt.Println("calling get ask", addr)
			ask, err := fc.GetAsk(ctx, addr)
			if err != nil {
				return err
			}

			fmt.Printf("ASK: %#v\n", ask)

		*/
	},
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
	app.Commands = []*cli.Command{
		testMarketsCmd,
	}
	app.Flags = []cli.Flag{
		&cli.StringFlag{
			Name:  "repo",
			Value: "~/.lotus",
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

		db, err := gorm.Open(sqlite.Open("estuary.db"), &gorm.Config{})
		if err != nil {
			return err
		}
		db.AutoMigrate(&Content{})
		db.AutoMigrate(&Object{})
		db.AutoMigrate(&ObjRef{})

		db.AutoMigrate(&contentDeal{})
		db.AutoMigrate(&dfeRecord{})
		db.AutoMigrate(&PieceCommRecord{})

		s.DB = db

		e := echo.New()
		e.HTTPErrorHandler = func(err error, ctx echo.Context) {
			log.Errorf("handler error: %s", err)
		}

		e.POST("/content/add", s.handleAdd)
		e.GET("/content/stats", s.handleStats)
		e.GET("/content/ensure-replication/:datacid", s.handleEnsureReplication)

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

type Server struct {
	Node      *Node
	DB        *gorm.DB
	FilClient *filclient.FilClient
	Api       api.GatewayAPI
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
		Cid:  dbCID{nd.Cid()},
		Size: totalSize,
		Name: fname,
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

	return s.EnsureStorage(data)

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

func (s *Server) pickMiners(n int) ([]address.Address, error) {
	perm := rand.Perm(len(miners))[:n]
	out := make([]address.Address, n)
	for ix, val := range perm {
		out[ix] = miners[val]
	}
	return out, nil
}

type contentDeal struct {
	gorm.Model
	Content  uint
	PropCid  dbCID
	Miner    string
	DealID   int64
	Failed   bool
	FailedAt time.Time
}

func (cd contentDeal) MinerAddr() (address.Address, error) {
	return address.NewFromString(cd.Miner)
}

func (s *Server) EnsureStorage(c cid.Cid) error {
	ctx := context.TODO()

	// check if content has enough deals made for it
	// if not enough deals, go make more
	// check all existing deals, ensure they are still active
	// if not active, repair!
	var content Content
	if err := s.DB.Find(&content, "cid = ?", c.Bytes()).Error; err != nil {
		return err
	}

	fmt.Println("Content: ", content.Cid.CID, c)

	var deals []contentDeal
	if err := s.DB.Find(&deals, "content = ? AND NOT failed", content.ID).Error; err != nil {
		if !xerrors.Is(err, gorm.ErrRecordNotFound) {
			return err
		}
	}

	replicationFactor := 10

	if len(deals) < replicationFactor {
		// make some more deals!
		fmt.Printf("Content only has %d deals, making %d more.\n", len(deals), replicationFactor-len(deals))
		if err := s.makeDealsForContent(ctx, content, replicationFactor-len(deals)); err != nil {
			return err
		}
	}

	// check on each of the existing deals, see if they need fixing
	for _, d := range deals {
		ok, err := s.checkDeal(&d)
		if err != nil {
			var dfe *DealFailureError
			if xerrors.As(err, &dfe) {
				s.recordDealFailure(dfe)
				continue
			} else {
				return err
			}
		}

		if !ok {
			if err := s.repairDeal(&d); err != nil {
				return xerrors.Errorf("repairing deal failed: %w", err)
			}
		}
	}

	return nil
}

func (s *Server) getContent(id uint) (*Content, error) {
	var content Content
	if err := s.DB.First(&content, "id = ?", id).Error; err != nil {
		return nil, err
	}
	return &content, nil
}

func (s *Server) checkDeal(d *contentDeal) (bool, error) {
	ctx := context.TODO()

	maddr, err := d.MinerAddr()
	if err != nil {
		return false, err
	}

	if d.DealID != 0 {
		return s.FilClient.CheckChainDeal(ctx, abi.DealID(d.DealID))
	}

	// case where deal isnt yet on chain...

	fmt.Println("checking proposal cid: ", d.PropCid.CID)
	provds, err := s.FilClient.DealStatus(ctx, maddr, d.PropCid.CID)
	if err != nil {
		// if we cant get deal status from a miner and the data hasnt landed on
		// chain what do we do?
		s.recordDealFailure(&DealFailureError{
			Miner:   maddr,
			Phase:   "check-status",
			Message: err.Error(),
			Content: d.Content,
		})
		return false, nil
	}

	content, err := s.getContent(d.Content)
	if err != nil {
		return false, err
	}

	if provds.PublishCid != nil {
		id, err := s.getDealID(ctx, *provds.PublishCid, d)
		if err != nil {
			return false, xerrors.Errorf("failed to check deal id: %w", err)
		}

		fmt.Println("Got deal ID for deal!", id)
		d.DealID = int64(id)
		if err := s.DB.Save(&d).Error; err != nil {
			return false, xerrors.Errorf("failed to update database entry: %w", err)
		}
		return true, nil
	}

	head, err := s.Api.ChainHead(ctx)
	if err != nil {
		return false, err
	}

	if provds.Proposal.StartEpoch < head.Height() {
		// deal expired, miner didnt start it in time
		return false, nil
	}
	// miner still has time...

	fmt.Println("Checking transfer status...")
	status, err := s.FilClient.TransferStatusForContent(ctx, content.Cid.CID, maddr)
	if err != nil {
		if err != filclient.ErrNoTransferFound {
			return false, err
		}

		// no transfer found for this pair, need to create a new one
	}

	switch status.Status {
	case datatransfer.Failed:
		fmt.Println("Transfer failed!", status.Message)
		s.recordDealFailure(&DealFailureError{
			Miner:   maddr,
			Phase:   "data-transfer",
			Message: fmt.Sprintf("transfer failed: %s", status.Message),
			Content: content.ID,
		})
	case datatransfer.Cancelled:
		fmt.Println("Transfer canceled!", status.Message)
		s.recordDealFailure(&DealFailureError{
			Miner:   maddr,
			Phase:   "data-transfer",
			Message: fmt.Sprintf("transfer cancelled: %s", status.Message),
			Content: content.ID,
		})
	case datatransfer.TransferFinished, datatransfer.Finalizing, datatransfer.Completing, datatransfer.Completed:
		// these are all okay
		fmt.Println("Transfer is finished-ish!", status.Status)
	case datatransfer.Ongoing:
		fmt.Println("transfer status is ongoing!")
		// expected, this is fine
	default:
		fmt.Printf("Unexpected data transfer state: %d (msg = %s)\n", status.Status, status.Message)
	}

	return true, nil
}

func (s *Server) getDealID(ctx context.Context, pubcid cid.Cid, d *contentDeal) (abi.DealID, error) {
	mlookup, err := s.Api.StateSearchMsg(ctx, pubcid)
	if err != nil {
		return 0, xerrors.Errorf("could not find published deal on chain: %w", err)
	}

	if mlookup.Message != pubcid {
		// TODO: can probably deal with this by checking the message contents?
		return 0, xerrors.Errorf("publish deal message was replaced on chain")
	}

	msg, err := s.Api.ChainGetMessage(ctx, mlookup.Message)
	if err != nil {
		return 0, err
	}

	var params market.PublishStorageDealsParams
	if err := params.UnmarshalCBOR(bytes.NewReader(msg.Params)); err != nil {
		return 0, err
	}

	dealix := -1
	for i, pd := range params.Deals {
		nd, err := cborutil.AsIpld(&pd)
		if err != nil {
			return 0, xerrors.Errorf("failed to compute deal proposal ipld node: %w", err)
		}

		if nd.Cid() == d.PropCid.CID {
			dealix = i
			break
		}
	}

	if dealix == -1 {
		return 0, fmt.Errorf("our deal was not in this publish message")
	}

	if mlookup.Receipt.ExitCode != 0 {
		return 0, xerrors.Errorf("miners deal publish failed (exit: %d)", mlookup.Receipt.ExitCode)
	}

	var retval market.PublishStorageDealsReturn
	if err := retval.UnmarshalCBOR(bytes.NewReader(mlookup.Receipt.Return)); err != nil {
		return 0, xerrors.Errorf("publish deal return was improperly formatted: %w", err)
	}

	if len(retval.IDs) != len(params.Deals) {
		return 0, fmt.Errorf("return value from publish deals did not match length of params")
	}

	return retval.IDs[dealix], nil
}

func (s *Server) repairDeal(d *contentDeal) error {
	d.Failed = true
	d.FailedAt = time.Now()
	if err := s.DB.Save(d).Error; err != nil {
		return err
	}

	return nil
}

func (s *Server) priceIsTooHigh(price abi.TokenAmount) bool {
	return false
}

func (s *Server) makeDealsForContent(ctx context.Context, content Content, count int) error {
	ms, err := s.pickMiners(count)
	if err != nil {
		return err
	}

	asks := make([]*network.AskResponse, len(ms))
	var successes int
	for i, m := range ms {
		ask, err := s.FilClient.GetAsk(ctx, m)
		if err != nil {
			s.recordDealFailure(&DealFailureError{
				Miner:   m,
				Phase:   "query-ask",
				Message: err.Error(),
				Content: content.ID,
			})
			fmt.Printf("failed to get ask for miner %s: %s\n", m, err)
			continue
		}
		asks[i] = ask
		successes++
	}

	proposals := make([]*network.Proposal, len(ms))
	for i, m := range ms {
		if asks[i] == nil {
			continue
		}

		prop, err := s.FilClient.MakeDeal(ctx, m, content.Cid.CID, asks[i].Ask.Ask.Price, 1000000)
		if err != nil {
			return xerrors.Errorf("failed to construct a deal proposal: %w", err)
		}

		proposals[i] = prop
	}

	responses := make([]*network.SignedResponse, len(ms))
	for i, p := range proposals {
		if p == nil {
			continue
		}

		dealresp, err := s.FilClient.SendProposal(ctx, p)
		if err != nil {
			s.recordDealFailure(&DealFailureError{
				Miner:   ms[i],
				Phase:   "send-proposal",
				Message: err.Error(),
				Content: content.ID,
			})
			fmt.Println("failed to propose deal with miner: ", err)
			continue
		}

		// TODO: verify signature!
		switch dealresp.Response.State {
		case storagemarket.StorageDealError:
			if err := s.recordDealFailure(&DealFailureError{
				Miner:   miners[i],
				Phase:   "propose",
				Message: dealresp.Response.Message,
				Content: content.ID,
			}); err != nil {
				return err
			}
		case storagemarket.StorageDealProposalRejected:
			if err := s.recordDealFailure(&DealFailureError{
				Miner:   miners[i],
				Phase:   "propose",
				Message: dealresp.Response.Message,
				Content: content.ID,
			}); err != nil {
				return err
			}
		default:
			if err := s.recordDealFailure(&DealFailureError{
				Miner:   miners[i],
				Phase:   "propose",
				Message: fmt.Sprintf("unrecognized response state %d: %s", dealresp.Response.State, dealresp.Response.Message),
				Content: content.ID,
			}); err != nil {
				return err
			}
		case storagemarket.StorageDealWaitingForData, storagemarket.StorageDealProposalAccepted:
			fmt.Println("good deal state!", dealresp.Response.State)
			responses[i] = dealresp

			cd := &contentDeal{
				Content: content.ID,
				PropCid: dbCID{dealresp.Response.Proposal},
				Miner:   ms[i].String(),
			}

			if err := s.DB.Create(cd).Error; err != nil {
				return xerrors.Errorf("failed to create database entry for deal: %w", err)
			}
		}
	}

	// Now start up some data transfers!
	// note: its okay if we dont start all the data transfers, we can just do it next time around
	for i, resp := range responses {
		if resp == nil {
			continue
		}

		chanid, err := s.FilClient.StartDataTransfer(ctx, ms[i], resp.Response.Proposal, content.Cid.CID)
		if err != nil {
			if oerr := s.recordDealFailure(&DealFailureError{
				Miner:   ms[i],
				Phase:   "start-data-transfer",
				Message: err.Error(),
				Content: content.ID,
			}); oerr != nil {
				return oerr
			}
		}

		log.Infow("Started data transfer", "chanid", chanid)
	}

	return nil
}

func (s *Server) recordDealFailure(dfe *DealFailureError) error {
	log.Infow("deal failure error", "miner", dfe.Miner, "phase", dfe.Phase, "msg", dfe.Message, "content", dfe.Content)
	return s.DB.Create(dfe.Record()).Error
}

type DealFailureError struct {
	Miner   address.Address
	Phase   string
	Message string
	Content uint
}

type dfeRecord struct {
	gorm.Model
	Miner   string
	Phase   string
	Message string
	Content uint
}

func (dfe *DealFailureError) Record() *dfeRecord {
	return &dfeRecord{
		Miner:   dfe.Miner.String(),
		Phase:   dfe.Phase,
		Message: dfe.Message,
		Content: dfe.Content,
	}
}

func (dfe *DealFailureError) Error() string {
	return fmt.Sprintf("deal with miner %s failed in phase %s: %s", dfe.Message, dfe.Phase, dfe.Message)

}

func averageAskPrice(asks []*network.AskResponse) types.FIL {
	total := abi.NewTokenAmount(0)
	for _, a := range asks {
		total = types.BigAdd(total, a.Ask.Ask.Price)
	}

	return types.FIL(big.Div(total, big.NewInt(int64(len(asks)))))
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
