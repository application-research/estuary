package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/pprof"
	"strconv"
	"strings"
	"time"

	"github.com/filecoin-project/go-address"
	datatransfer "github.com/filecoin-project/go-data-transfer"
	"github.com/filecoin-project/go-padreader"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/lotus/chain/types"
	"github.com/google/uuid"
	"github.com/ipfs/go-blockservice"
	"github.com/ipfs/go-cid"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	chunker "github.com/ipfs/go-ipfs-chunker"
	offline "github.com/ipfs/go-ipfs-exchange-offline"
	"github.com/ipfs/go-merkledag"
	importer "github.com/ipfs/go-unixfs/importer"
	uio "github.com/ipfs/go-unixfs/io"
	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
	"github.com/whyrusleeping/estuary/filclient"
	"golang.org/x/sys/unix"
	"golang.org/x/xerrors"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

const (
	ERR_INVALID_TOKEN        = "ERR_INVALID_TOKEN"
	ERR_TOKEN_EXPIRED        = "ERR_TOKEN_EXPIRED"
	ERR_AUTH_MISSING         = "ERR_AUTH_MISSING"
	ERR_INVALID_AUTH         = "ERR_INVALID_AUTH"
	ERR_AUTH_MISSING_BEARER  = "ERR_AUTH_MISSING_BEARER"
	ERR_NOT_AUTHORIZED       = "ERR_NOT_AUTHORIZED"
	ERR_INVALID_INVITE       = "ERR_INVALID_INVITE"
	ERR_USERNAME_TAKEN       = "ERR_USERNAME_TAKEN"
	ERR_USER_CREATION_FAILED = "ERR_USER_CREATION_FAILED"
	ERR_USER_NOT_FOUND       = "ERR_USER_NOT_FOUND"
	ERR_INVALID_PASSWORD     = "ERR_INVALID_PASSWORD"
)

const (
	PermLevelUser  = 2
	PermLevelAdmin = 10
)

func (s *Server) ServeAPI(srv string, logging bool) error {
	e := echo.New()
	e.HTTPErrorHandler = func(err error, ctx echo.Context) {
		log.Errorf("handler error: %s", err)
		var herr *httpError
		if xerrors.As(err, &herr) {
			ctx.JSON(herr.Code, map[string]string{
				"error": herr.Message,
			})
			return
		}

		var echoErr *echo.HTTPError
		if xerrors.As(err, &echoErr) {
			ctx.JSON(echoErr.Code, map[string]interface{}{
				"error": echoErr.Message,
			})
			return
		}

		ctx.NoContent(500)
	}

	e.GET("/debug/pprof/:prof", serveProfile)

	if logging {
		e.Use(middleware.Logger())
	}
	e.Use(middleware.CORS())

	e.POST("/register", s.handleRegisterUser)
	e.POST("/login", s.handleLoginUser)

	content := e.Group("/content")
	content.Use(s.AuthRequired(PermLevelUser))
	content.POST("/add", withUser(s.handleAdd))
	content.GET("/stats", withUser(s.handleStats))
	content.GET("/ensure-replication/:datacid", s.handleEnsureReplication)
	content.GET("/status/:id", withUser(s.handleContentStatus))
	content.GET("/list", withUser(s.handleListContent))
	content.GET("/failures/:content", withUser(s.handleGetContentFailures))

	deals := e.Group("/deals")
	content.Use(s.AuthRequired(PermLevelUser))
	deals.GET("/query/:miner", s.handleQueryAsk)
	deals.POST("/make/:miner", s.handleMakeDeal)
	deals.POST("/transfer/start/:miner/:propcid/:datacid", s.handleTransferStart)
	deals.POST("/transfer/status", s.handleTransferStatus)
	deals.GET("/transfer/in-progress", s.handleTransferInProgress)
	deals.POST("/transfer/restart", s.handleTransferRestart)
	deals.GET("/status/:miner/:propcid", s.handleDealStatus)
	deals.GET("/estimate", s.handleEstimateDealCost)

	// explicitly public, for now
	e.GET("/miners/failures/:miner", s.handleGetMinerFailures)
	e.GET("/miners/deals/:miner", s.handleGetMinerDeals)

	// should probably remove this
	e.GET("/retrieval/querytest/:content", s.handleRetrievalCheck)

	admin := e.Group("/admin")
	admin.Use(s.AuthRequired(PermLevelAdmin))
	admin.GET("/balance", s.handleAdminBalance)
	admin.GET("/add-escrow/:amt", s.handleAdminAddEscrow)
	admin.GET("/dealstats", s.handleDealStats)
	admin.GET("/disk-info", s.handleDiskSpaceCheck)
	admin.POST("/add-miner/:miner", s.handleAdminAddMiner)

	admin.GET("/cm/read/:content", s.handleReadLocalContent)
	admin.GET("/cm/offload/candidates", s.handleGetOffloadingCandidates)
	admin.POST("/cm/offload/:content", s.handleOffloadContent)
	admin.GET("/cm/refresh/:content", s.handleRefreshContent)

	admin.POST("/invite/:code", withUser(s.handleAdminCreateInvite))

	return e.Start(srv)
}

type httpError struct {
	Code    int
	Message string
}

func (he httpError) Error() string {
	return he.Message
}

func serveProfile(c echo.Context) error {
	pprof.Handler(c.Param("prof")).ServeHTTP(c.Response().Writer, c.Request())
	return nil
}

type statsResp struct {
	Cid           cid.Cid `json:"cid"`
	File          string  `json:"file"`
	BWUsed        int64   `json:"bwUsed"`
	TotalRequests int64   `json:"totalRequests"`
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

func (s *Server) handleStats(c echo.Context, u *User) error {
	var contents []Content
	if err := s.DB.Find(&contents, "user_id = ?", u.ID).Error; err != nil {
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

func (s *Server) handleAdd(c echo.Context, u *User) error {
	mpf, err := c.FormFile("data")
	if err != nil {
		return err
	}

	fname := mpf.Filename
	fi, err := mpf.Open()
	if err != nil {
		return err
	}

	replication := defaultReplication
	replVal := c.FormValue("replication")
	if replVal != "" {
		parsed, err := strconv.Atoi(replVal)
		if err != nil {
			log.Errorf("failed to parse replication value in form data, assuming default for now: %s", err)
		} else {
			replication = parsed
		}
	}

	bsid, bs, err := s.StagingMgr.AllocNew()
	if err != nil {
		return err
	}

	bserv := blockservice.New(bs, nil)
	dserv := merkledag.NewDAGService(bserv)
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
			size, err := bs.GetSize(c)
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
		Cid:         dbCID{nd.Cid()},
		Size:        totalSize,
		Name:        fname,
		Active:      true,
		UserID:      u.ID,
		Replication: replication,
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

	if err := dumpBlockstoreTo(bs, s.Node.Blockstore); err != nil {
		return xerrors.Errorf("failed to move data from staging to main blockstore: %w", err)
	}

	if err := s.StagingMgr.CleanUp(bsid); err != nil {
		log.Errorf("failed to clean up staging blockstore: %s", err)
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

func dumpBlockstoreTo(from, to blockstore.Blockstore) error {
	// TODO: smarter batching... im sure ive written this logic before, just gotta go find it
	keys, err := from.AllKeysChan(context.TODO())
	if err != nil {
		return err
	}

	for k := range keys {
		blk, err := from.Get(k)
		if err != nil {
			return err
		}

		if err := to.Put(blk); err != nil {
			return err
		}
	}

	return nil
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

func (s *Server) handleListContent(c echo.Context, u *User) error {
	var contents []Content
	if err := s.DB.Find(&contents, "active and user_id = ?", u.ID).Error; err != nil {
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

func (s *Server) handleContentStatus(c echo.Context, u *User) error {
	ctx := context.TODO()
	val, err := strconv.Atoi(c.Param("id"))
	if err != nil {
		return err
	}

	var content Content
	if err := s.DB.First(&content, "id = ? and user_id = ?", val, u.ID).Error; err != nil {
		return err
	}

	var deals []contentDeal
	if err := s.DB.Find(&deals, "content = ?", content.ID).Error; err != nil {
		return err
	}

	var ds []dealStatus
	for _, d := range deals {

		chanst, err := s.CM.GetTransferStatus(ctx, &d, content.Cid.CID)
		if err != nil {
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

	proposal, err := s.FilClient.MakeDeal(ctx, addr, req.Cid, req.Price, 0, req.Duration)
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

func (s *Server) handleAdminCreateInvite(c echo.Context, u *User) error {
	code := c.Param("code")
	invite := &InviteCode{
		Code:      code,
		CreatedBy: u.ID,
	}
	if err := s.DB.Create(invite).Error; err != nil {
		return err
	}

	return c.JSON(200, map[string]string{
		"code": invite.Code,
	})
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
	NumDeals     int `json:"numDeals"`
	NumConfirmed int `json:"numConfirmed"`
	NumFailed    int `json:"numFailed"`

	TotalSpending     abi.TokenAmount `json:"totalSpending"`
	ConfirmedSpending abi.TokenAmount `json:"confirmedSpending"`
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
	BstoreSize uint64 `json:"bstoreSize"`
	BstoreFree uint64 `json:"bstoreFree"`
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
	DurationBlks int    `json:"durationBlks"`
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

func (s *Server) handleGetMinerFailures(c echo.Context) error {
	maddr, err := address.NewFromString(c.Param("miner"))
	if err != nil {
		return err
	}

	var merrs []dfeRecord
	if err := s.DB.Find(&merrs, "miner = ?", maddr.String()).Error; err != nil {
		return err
	}

	return c.JSON(200, merrs)
}

func (s *Server) handleGetMinerDeals(c echo.Context) error {
	maddr, err := address.NewFromString(c.Param("miner"))
	if err != nil {
		return err
	}

	var deals []contentDeal
	if err := s.DB.Find(&deals, "miner = ?", maddr.String()).Error; err != nil {
		return err
	}

	return c.JSON(200, deals)
}

func (s *Server) handleGetContentFailures(c echo.Context, u *User) error {
	cont, err := strconv.Atoi(c.Param("content"))
	if err != nil {
		return err
	}

	var errs []dfeRecord
	if err := s.DB.Find(&errs, "content = ?", cont).Error; err != nil {
		return err
	}

	return c.JSON(200, errs)
}

func (s *Server) handleGetOffloadingCandidates(c echo.Context) error {
	conts, err := s.CM.getRemovalCandidates()
	if err != nil {
		return err
	}

	return c.JSON(200, conts)
}

func (s *Server) handleOffloadContent(c echo.Context) error {
	cont, err := strconv.Atoi(c.Param("content"))
	if err != nil {
		return err
	}

	ctx := context.Background()
	s.CM.OffloadContent(ctx, uint(cont))
	return nil
}

func (s *Server) handleRefreshContent(c echo.Context) error {
	cont, err := strconv.Atoi(c.Param("content"))
	if err != nil {
		return err
	}

	ctx := context.Background()
	return s.CM.RefreshContent(ctx, uint(cont))
}

func (s *Server) handleReadLocalContent(c echo.Context) error {
	cont, err := strconv.Atoi(c.Param("content"))
	if err != nil {
		return err
	}

	var content Content
	if err := s.DB.First(&content, "id = ?", cont).Error; err != nil {
		return err
	}

	bserv := blockservice.New(s.Node.Blockstore, offline.Exchange(s.Node.Blockstore))
	dserv := merkledag.NewDAGService(bserv)

	ctx := context.Background()
	nd, err := dserv.Get(ctx, content.Cid.CID)
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

func (s *Server) checkTokenAuth(token string) (*User, error) {
	var authToken AuthToken
	if err := s.DB.First(&authToken, "token = ?", token).Error; err != nil {
		if xerrors.Is(err, gorm.ErrRecordNotFound) {
			return nil, &httpError{
				Code:    http.StatusForbidden,
				Message: ERR_INVALID_TOKEN,
			}
		}
		return nil, err
	}

	if authToken.Expiry.Before(time.Now()) {
		return nil, &httpError{
			Code:    http.StatusForbidden,
			Message: ERR_TOKEN_EXPIRED,
		}
	}

	var user User
	if err := s.DB.First(&user, "id = ?", authToken.User).Error; err != nil {
		if xerrors.Is(err, gorm.ErrRecordNotFound) {
			return nil, &httpError{
				Code:    http.StatusForbidden,
				Message: ERR_INVALID_TOKEN,
			}
		}
		return nil, err
	}

	return &user, nil
}

func (s *Server) AuthRequired(level int) echo.MiddlewareFunc {
	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c echo.Context) error {
			auth := c.Request().Header.Get("Authorization")
			if auth == "" {
				return &httpError{
					Code:    403,
					Message: ERR_AUTH_MISSING,
				}
			}

			parts := strings.Split(auth, " ")
			if len(parts) != 2 {
				return &httpError{
					Code:    403,
					Message: ERR_INVALID_AUTH,
				}
			}

			if parts[0] != "Bearer" {
				return &httpError{
					Code:    403,
					Message: ERR_AUTH_MISSING_BEARER,
				}
			}

			u, err := s.checkTokenAuth(parts[1])
			if err != nil {
				return err
			}

			if u.Perm >= level {
				c.Set("user", u)
				return next(c)
			}

			return &httpError{
				Code:    401,
				Message: ERR_NOT_AUTHORIZED,
			}
		}
	}
}

type registerBody struct {
	Username     string `json:"username"`
	PasswordHash string `json:"passwordHash"`
	InviteCode   string `json:"inviteCode"`
}

func (s *Server) handleRegisterUser(c echo.Context) error {
	var reg registerBody
	if err := c.Bind(&reg); err != nil {
		return err
	}

	var invite InviteCode
	if err := s.DB.First(&invite, "code = ?", reg.InviteCode).Error; err != nil {
		if xerrors.Is(err, gorm.ErrRecordNotFound) {
			return &httpError{
				Code:    http.StatusForbidden,
				Message: ERR_INVALID_INVITE,
			}
		}

	}

	var exist User
	if err := s.DB.First(&exist, "username = ?", reg.Username).Error; err == nil {
		return &httpError{
			Code:    http.StatusForbidden,
			Message: ERR_USERNAME_TAKEN,
		}
	}

	newUser := &User{
		Username: reg.Username,
		UUID:     uuid.New().String(),
		PassHash: reg.PasswordHash,
	}
	if err := s.DB.Create(newUser).Error; err != nil {
		herr := &httpError{
			Code:    http.StatusForbidden,
			Message: ERR_USER_CREATION_FAILED,
		}

		return fmt.Errorf("user creation failed: %s: %w", err, herr)
	}

	authToken := &AuthToken{
		Token:  "EST" + uuid.New().String() + "ARY",
		User:   newUser.ID,
		Expiry: time.Now().Add(time.Hour * 24 * 7),
	}

	if err := s.DB.Create(authToken).Error; err != nil {
		return err
	}

	return c.JSON(200, &loginResponse{
		Token:  authToken.Token,
		Expiry: authToken.Expiry,
	})
}

type loginBody struct {
	Username string `json:"username"`
	PassHash string `json:"passwordHash"`
}

type loginResponse struct {
	Token  string    `json:"token"`
	Expiry time.Time `json:"expiry"`
}

func (s *Server) handleLoginUser(c echo.Context) error {
	var body loginBody
	if err := c.Bind(&body); err != nil {
		return err
	}

	var user User
	if err := s.DB.First(&user, "username = ?", body.Username).Error; err != nil {
		if xerrors.Is(err, gorm.ErrRecordNotFound) {
			return &httpError{
				Code:    http.StatusForbidden,
				Message: ERR_USER_NOT_FOUND,
			}
		}
		return err
	}

	if user.PassHash != body.PassHash {
		return &httpError{
			Code:    http.StatusForbidden,
			Message: ERR_INVALID_PASSWORD,
		}
	}

	authToken := &AuthToken{
		Token:  "EST" + uuid.New().String() + "ARY",
		User:   user.ID,
		Expiry: time.Now().Add(time.Hour * 24 * 7),
	}

	if err := s.DB.Create(authToken).Error; err != nil {
		return err
	}

	return c.JSON(200, &loginResponse{
		Token:  authToken.Token,
		Expiry: authToken.Expiry,
	})
}
