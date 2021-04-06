package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http/pprof"
	"strconv"

	"github.com/filecoin-project/go-address"
	datatransfer "github.com/filecoin-project/go-data-transfer"
	"github.com/filecoin-project/go-padreader"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/lotus/chain/types"
	"github.com/ipfs/go-blockservice"
	"github.com/ipfs/go-cid"
	chunker "github.com/ipfs/go-ipfs-chunker"
	"github.com/ipfs/go-merkledag"
	importer "github.com/ipfs/go-unixfs/importer"
	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
	"github.com/whyrusleeping/estuary/filclient"
	"golang.org/x/sys/unix"
	"golang.org/x/xerrors"
	"gorm.io/gorm/clause"
)

func (s *Server) ServeAPI(srv string, logging bool) error {
	e := echo.New()
	e.HTTPErrorHandler = func(err error, ctx echo.Context) {
		log.Errorf("handler error: %s", err)
	}

	e.GET("/debug/pprof/:prof", serveProfile)

	if logging {
		e.Use(middleware.Logger())
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
	e.GET("/deals/estimate", s.handleEstimateDealCost)

	e.GET("/retrieval/querytest/:content", s.handleRetrievalCheck)

	e.GET("/admin/balance", s.handleAdminBalance)
	e.GET("/admin/add-escrow/:amt", s.handleAdminAddEscrow)
	e.GET("/admin/dealstats", s.handleDealStats)
	e.GET("/admin/disk-info", s.handleDiskSpaceCheck)
	e.POST("/admin/add-miner/:miner", s.handleAdminAddMiner)

	return e.Start(srv)
}

func serveProfile(c echo.Context) error {
	pprof.Handler(c.Param("prof")).ServeHTTP(c.Response().Writer, c.Request())
	return nil
}

type statsResp struct {
	Cid           cid.Cid `json:"cid"`
	File          string  `json:"file"`
	BWUsed        int64   `json:"bw_used"`
	TotalRequests int64   `json:"total_requests"`
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

	mpf, err := c.FormFile("data")
	if err != nil {
		return err
	}

	fname := mpf.Filename
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

	if err := s.DB.CreateInBatches(objects, 300).Error; err != nil {
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

	if err := s.DB.CreateInBatches(refs, 500).Error; err != nil {
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
	Cid      cid.Cid        `json:"cid"`
	Price    types.BigInt   `json:"price"`
	Duration abi.ChainEpoch `json:"duration"`
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

func (s *Server) handleAdminAddMiner(c echo.Context) error {
	m, err := address.NewFromString(c.Param("miner"))
	if err != nil {
		return err
	}

	return s.DB.Clauses(&clause.OnConflict{DoNothing: true}).Create(&storageMiner{Address: dbAddr{m}}).Error
}

type contentDealStats struct {
	NumDeals     int `json:"num_deals"`
	NumConfirmed int `json:"num_confirmed"`
	NumFailed    int `json:"num_failed"`

	TotalSpending     abi.TokenAmount `json:"total_spending"`
	ConfirmedSpending abi.TokenAmount `json:"confirmed_spending"`
}

func (s *Server) handleDealStats(c echo.Context) error {
	ctx := context.TODO()

	var alldeals []contentDeal
	if err := s.DB.Find(&alldeals).Error; err != nil {
		return err
	}

	sbc := make(map[uint]*contentDealStats)

	for _, d := range alldeals {
		maddr, err := d.MinerAddr()
		if err != nil {
			return err
		}

		st, err := s.FilClient.DealStatus(ctx, maddr, d.PropCid.CID)
		if err != nil {
			log.Errorf("checking deal status failed (%s): %s", maddr, err)
			continue
		}

		fee := st.Proposal.TotalStorageFee()

		cds, ok := sbc[d.Content]
		if !ok {
			cds = &contentDealStats{
				TotalSpending:     abi.NewTokenAmount(0),
				ConfirmedSpending: abi.NewTokenAmount(0),
			}
			sbc[d.Content] = cds
		}

		if d.Failed {
			cds.NumFailed++
			continue
		}

		cds.TotalSpending = types.BigAdd(cds.TotalSpending, fee)
		cds.NumDeals++

		if d.DealID != 0 {
			cds.ConfirmedSpending = types.BigAdd(cds.ConfirmedSpending, fee)
			cds.NumConfirmed++
		}
	}

	return c.JSON(200, sbc)
}

type diskSpaceInfo struct {
	BstoreSize uint64 `json:"bstore_size"`
	BstoreFree uint64 `json:"bstore_free"`
}

func (s *Server) handleDiskSpaceCheck(c echo.Context) error {
	var st unix.Statfs_t
	if err := unix.Statfs(s.Node.Config.Blockstore, &st); err != nil {
		return err
	}

	return c.JSON(200, &diskSpaceInfo{
		BstoreSize: st.Blocks * uint64(st.Bsize),

		BstoreFree: st.Bavail * uint64(st.Bsize),
	})
}

func (s *Server) handleRetrievalCheck(c echo.Context) error {
	ctx := context.TODO()
	contid, err := strconv.Atoi(c.Param("content"))
	if err != nil {
		return err
	}
	if err := s.retrieveContent(ctx, uint(contid)); err != nil {
		return err
	}

	return c.JSON(200, "We did a thing")

}

type estimateDealBody struct {
	Size         uint64 `json:"size"`
	Replication  int    `json:"replication"`
	DurationBlks int    `json:"duration_blks"`
	Verified     bool   `json:"verified"`
}

type priceEstimateResponse struct {
	Total string
}

func (s *Server) handleEstimateDealCost(c echo.Context) error {
	ctx := context.TODO()
	var body estimateDealBody
	if err := c.Bind(&body); err != nil {
		return err
	}

	rounded := padreader.PaddedSize(body.Size)

	total, err := s.CM.estimatePrice(ctx, body.Replication, rounded.Padded(), abi.ChainEpoch(body.DurationBlks), body.Verified)
	if err != nil {
		return err
	}

	return c.JSON(200, &priceEstimateResponse{
		Total: types.FIL(*total).String(),
	})
}
