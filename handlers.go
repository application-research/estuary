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
	"sync"
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
	ipld "github.com/ipfs/go-ipld-format"
	"github.com/ipfs/go-merkledag"
	"github.com/ipfs/go-unixfs/importer"
	uio "github.com/ipfs/go-unixfs/io"
	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
	"github.com/lightstep/otel-launcher-go/launcher"
	"github.com/whyrusleeping/estuary/filclient"
	"golang.org/x/sys/unix"
	"golang.org/x/xerrors"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/semconv"
	"go.opentelemetry.io/otel/trace"
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
	ERR_INVITE_ALREADY_USED  = "ERR_INVITE_ALREADY_USED"
)

const (
	PermLevelUser  = 2
	PermLevelAdmin = 10
)

func (s *Server) ServeAPI(srv string, logging bool, lsteptok string) error {
	if lsteptok != "" {
		ls := launcher.ConfigureOpentelemetry(
			launcher.WithServiceName("estuary-api"),
			launcher.WithAccessToken(lsteptok),
		)

		defer ls.Shutdown()
	}

	e := echo.New()

	e.Use(s.tracingMiddleware)
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
	e.GET("/health", s.handleHealth)

	e.GET("/test-error", s.handleTestError)

	e.GET("/viewer", withUser(s.handleGetViewer), s.AuthRequired(PermLevelUser))

	user := e.Group("/user")
	user.Use(s.AuthRequired(PermLevelUser))
	user.GET("/test-error", s.handleTestError)
	user.GET("/api-keys", withUser(s.handleUserGetApiKeys))
	user.POST("/api-keys", withUser(s.handleUserCreateApiKey))
	user.DELETE("/api-keys/:key", withUser(s.handleUserRevokeApiKey))

	content := e.Group("/content")
	content.Use(s.AuthRequired(PermLevelUser))
	content.POST("/add", withUser(s.handleAdd))
	content.GET("/stats", withUser(s.handleStats))
	content.GET("/ensure-replication/:datacid", s.handleEnsureReplication)
	content.GET("/status/:id", withUser(s.handleContentStatus))
	content.GET("/list", withUser(s.handleListContent))
	content.GET("/failures/:content", withUser(s.handleGetContentFailures))
	content.GET("/bw-usage/:content", withUser(s.handleGetContentBandwidth))

	deals := e.Group("/deals")
	content.Use(s.AuthRequired(PermLevelUser))
	deals.GET("/query/:miner", s.handleQueryAsk)
	deals.POST("/make/:miner", s.handleMakeDeal)
	deals.POST("/transfer/start/:miner/:propcid/:datacid", s.handleTransferStart)
	deals.POST("/transfer/status", s.handleTransferStatus)
	deals.GET("/transfer/in-progress", s.handleTransferInProgress)
	deals.POST("/transfer/restart", s.handleTransferRestart)
	deals.GET("/status/:miner/:propcid", s.handleDealStatus)
	deals.POST("/estimate", s.handleEstimateDealCost)

	cols := e.Group("/collections")
	cols.Use(s.AuthRequired(PermLevelUser))
	cols.GET("/list", withUser(s.handleListCollections))
	cols.POST("/create", withUser(s.handleCreateCollection))
	cols.POST("/add-content", withUser(s.handleAddContentsToCollection))
	cols.GET("/content/:coluuid", withUser(s.handleGetCollectionContents))

	// explicitly public, for now
	public := e.Group("/public")

	public.GET("/stats", s.handlePublicStats)

	miners := public.Group("/miners")
	miners.GET("", s.handleAdminGetMiners)
	miners.GET("/failures/:miner", s.handleGetMinerFailures)
	miners.GET("/deals/:miner", s.handleGetMinerDeals)
	miners.GET("/stats/:miner", s.handleGetMinerStats)
	miners.GET("/storage/query/:miner", s.handleQueryAsk)

	admin := e.Group("/admin")
	admin.Use(s.AuthRequired(PermLevelAdmin))
	admin.GET("/balance", s.handleAdminBalance)
	admin.POST("/add-escrow/:amt", s.handleAdminAddEscrow)
	admin.GET("/dealstats", s.handleDealStats)
	admin.GET("/disk-info", s.handleDiskSpaceCheck)
	admin.GET("/stats", s.handleAdminStats)
	admin.POST("/miners/add/:miner", s.handleAdminAddMiner)
	admin.POST("/miners/rm/:miner", s.handleAdminRemoveMiner)
	admin.POST("/miners/suspend/:miner", s.handleAdminSuspendMiner)
	admin.PUT("/miners/unsuspend/:miner", s.handleAdminUnsuspendMiner)
	admin.GET("/miners", s.handleAdminGetMiners)
	admin.GET("/miners/stats", s.handleAdminGetMinerStats)

	admin.GET("/cm/read/:content", s.handleReadLocalContent)
	admin.GET("/cm/offload/candidates", s.handleGetOffloadingCandidates)
	admin.POST("/cm/offload/:content", s.handleOffloadContent)
	admin.GET("/cm/refresh/:content", s.handleRefreshContent)

	admin.GET("/retrieval/querytest/:content", s.handleRetrievalCheck)
	admin.GET("/retrieval/stats", s.handleGetRetrievalInfo)

	admin.POST("/invite/:code", withUser(s.handleAdminCreateInvite))
	admin.GET("/invites", s.handleAdminGetInvites)

	users := admin.Group("/users")
	users.GET("", s.handleAdminGetUsers)

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
	Offloaded     bool    `json:"offloaded"`
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

// TODO: delete me when debugging done
func (s *Server) handleTestError(c echo.Context) error {
	return fmt.Errorf("i am a scary error, log me please")
}

func (s *Server) handleStats(c echo.Context, u *User) error {
	var contents []Content
	if err := s.DB.Find(&contents, "user_id = ?", u.ID).Error; err != nil {
		return err
	}

	out := []statsResp{}
	for _, c := range contents {
		st := statsResp{
			Cid:  c.Cid.CID,
			File: c.Name,
		}

		var res struct {
			Bw         int64
			TotalReads int64
		}

		if err := s.DB.Model(ObjRef{}).
			Select("SUM(size * reads) as bw, SUM(reads) as total_reads").
			Where("obj_refs.content = ?", c.ID).
			Joins("left join objects on obj_refs.object = objects.id").
			Scan(&res).Error; err != nil {
			return err
		}

		st.TotalRequests = res.TotalReads
		st.BWUsed = res.Bw

		out = append(out, st)
	}

	return c.JSON(200, out)
}

func (s *Server) handleAdd(c echo.Context, u *User) error {
	ctx := c.Request().Context()

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

	collection := c.FormValue("collection")
	var col *Collection
	if collection != "" {
		var srchCol Collection
		if err := s.DB.First(&srchCol, "uuid = ? and user_id = ?", collection, u.ID).Error; err != nil {
			return err
		}

		col = &srchCol
	}

	bsid, bs, err := s.StagingMgr.AllocNew()
	if err != nil {
		return err
	}

	bserv := blockservice.New(bs, nil)
	dserv := merkledag.NewDAGService(bserv)

	nd, err := s.importFile(ctx, dserv, fi)
	if err != nil {
		return err
	}

	content, err := s.addDatabaseTracking(ctx, u, dserv, bs, nd.Cid(), fname, replication)
	if err != nil {
		return xerrors.Errorf("encountered problem computing object references: %w", err)
	}

	if col != nil {
		if err := s.DB.Create(&CollectionRef{
			Collection: col.ID,
			Content:    content.ID,
		}).Error; err != nil {
			log.Errorf("failed to add content to requested collection: %s", err)
		}
	}

	if err := s.dumpBlockstoreTo(ctx, bs, s.Node.Blockstore); err != nil {
		return xerrors.Errorf("failed to move data from staging to main blockstore: %w", err)
	}

	go func() {
		if err := s.StagingMgr.CleanUp(bsid); err != nil {
			log.Errorf("failed to clean up staging blockstore: %s", err)
		}
	}()

	s.CM.ToCheck <- content.ID

	go func() {
		if err := s.Node.Dht.Provide(context.TODO(), nd.Cid(), true); err != nil {
			fmt.Println("providing failed: ", err)
		}
		fmt.Println("providing complete")
	}()
	return c.JSON(200, map[string]string{"cid": nd.Cid().String()})
}

func (s *Server) importFile(ctx context.Context, dserv ipld.DAGService, fi io.Reader) (ipld.Node, error) {
	_, span := s.tracer.Start(ctx, "importFile")
	defer span.End()

	spl := chunker.DefaultSplitter(fi)
	return importer.BuildDagFromReader(dserv, spl)
}

func (s *Server) addDatabaseTracking(ctx context.Context, u *User, dserv ipld.LinkGetter, bs blockstore.Blockstore, root cid.Cid, fname string, replication int) (*Content, error) {
	ctx, span := s.tracer.Start(ctx, "computeObjRefs")
	defer span.End()

	var objects []*Object
	var totalSize int64
	cset := cid.NewSet()
	err := merkledag.Walk(ctx, dserv.GetLinks, root, func(c cid.Cid) bool {
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
		return nil, err
	}

	if err := s.DB.CreateInBatches(objects, 300).Error; err != nil {
		return nil, xerrors.Errorf("failed to create objects in db: %w", err)
	}

	// okay cool, we added the content, now track it
	content := &Content{
		Cid:         dbCID{root},
		Size:        totalSize,
		Name:        fname,
		Active:      true,
		UserID:      u.ID,
		Replication: replication,
	}

	if err := s.DB.Create(content).Error; err != nil {
		return nil, xerrors.Errorf("failed to track new content in database: %w", err)
	}

	refs := make([]ObjRef, len(objects))
	for i := range refs {
		refs[i].Content = content.ID
		refs[i].Object = objects[i].ID
	}

	if err := s.DB.CreateInBatches(refs, 500).Error; err != nil {
		return nil, xerrors.Errorf("failed to create refs: %w", err)
	}

	return content, nil
}

func (s *Server) dumpBlockstoreTo(ctx context.Context, from, to blockstore.Blockstore) error {
	ctx, span := s.tracer.Start(ctx, "blockstoreCopy")
	defer span.End()

	// TODO: smarter batching... im sure ive written this logic before, just gotta go find it
	keys, err := from.AllKeysChan(ctx)
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

	return c.JSON(200, contents)
}

type onChainDealState struct {
	SectorStartEpoch abi.ChainEpoch `json:"sectorStartEpoch"`
	LastUpdatedEpoch abi.ChainEpoch `json:"lastUpdatedEpoch"`
	SlashEpoch       abi.ChainEpoch `json:"slashEpoch"`
}

type dealStatus struct {
	Deal           contentDeal             `json:"deal"`
	TransferStatus *filclient.ChannelState `json:"transfer"`
	OnChainState   *onChainDealState       `json:"onChainState"`
}

func (s *Server) handleContentStatus(c echo.Context, u *User) error {
	ctx := c.Request().Context()
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

	ds := make([]dealStatus, len(deals))
	var wg sync.WaitGroup
	for i := range deals {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			d := deals[i]
			dstatus := dealStatus{
				Deal: d,
			}

			chanst, err := s.CM.GetTransferStatus(ctx, &d, content.Cid.CID)
			if err != nil {
				log.Errorf("failed to get transfer status: %s", err)
				return
			}

			dstatus.TransferStatus = chanst

			if d.DealID > 0 {
				markDeal, err := s.Api.StateMarketStorageDeal(ctx, abi.DealID(d.DealID), types.EmptyTSK)
				if err != nil {
					log.Warnw("failed to get deal info from market actor", "dealID", d.DealID, "error", err)
				} else {
					dstatus.OnChainState = &onChainDealState{
						SectorStartEpoch: markDeal.State.SectorStartEpoch,
						LastUpdatedEpoch: markDeal.State.LastUpdatedEpoch,
						SlashEpoch:       markDeal.State.SlashEpoch,
					}
				}
			}

			ds[i] = dstatus
		}(i)
	}

	wg.Wait()

	var failCount int64
	if err := s.DB.Model(&dfeRecord{}).Where("content = ?", content.ID).Count(&failCount).Error; err != nil {
		return err
	}

	return c.JSON(200, map[string]interface{}{
		"content":       content,
		"deals":         ds,
		"failuresCount": failCount,
	})
}

func (s *Server) handleQueryAsk(c echo.Context) error {
	addr, err := address.NewFromString(c.Param("miner"))
	if err != nil {
		return err
	}

	ask, err := s.FilClient.GetAsk(c.Request().Context(), addr)
	if err != nil {
		return err
	}

	return c.JSON(200, toDBAsk(ask))
}

type dealRequest struct {
	Cid      cid.Cid        `json:"cid"`
	Price    types.BigInt   `json:"price"`
	Duration abi.ChainEpoch `json:"duration"`
	Verified bool           `json:"verified"`
}

func (s *Server) handleMakeDeal(c echo.Context) error {
	ctx := c.Request().Context()

	addr, err := address.NewFromString(c.Param("miner"))
	if err != nil {
		return err
	}

	var req dealRequest
	if err := c.Bind(&req); err != nil {
		return err
	}

	proposal, err := s.FilClient.MakeDeal(ctx, addr, req.Cid, req.Price, 0, req.Duration, req.Verified)
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

type getInvitesResp struct {
	Code      string `json:"code"`
	Username  string `json:"createdBy"`
	ClaimedBy string `json:"claimedBy"`
}

func (s *Server) handleAdminGetInvites(c echo.Context) error {
	var invites []getInvitesResp
	if err := s.DB.Debug().Model(&InviteCode{}).
		Select("code, username, (?) as claimed_by", s.DB.Table("users").Select("username").Where("id = invite_codes.claimed_by")).
		//Where("claimed_by IS NULL").
		Joins("left join users on users.id = invite_codes.created_by").
		Scan(&invites).Error; err != nil {
		return err
	}

	return c.JSON(200, invites)
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
	balance, err := s.FilClient.Balance(c.Request().Context())
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

type adminStatsResponse struct {
	TotalDealAttempted   int64 `json:"totalDealsAttempted"`
	TotalDealsSuccessful int64 `json:"totalDealsSuccessful"`
	TotalDealsFailed     int64 `json:"totalDealsFailed"`

	NumMiners int64 `json:"numMiners"`
	NumUsers  int64 `json:"numUsers"`
	NumFiles  int64 `json:"numFiles"`

	NumRetrievals      int64 `json:"numRetrievals"`
	NumRetrFailures    int64 `json:"numRetrievalFailures"`
	NumStorageFailures int64 `json:"numStorageFailures"`
}

func (s *Server) handleAdminStats(c echo.Context) error {

	var dealsTotal int64
	if err := s.DB.Model(&contentDeal{}).Count(&dealsTotal).Error; err != nil {
		return err
	}

	var dealsSuccessful int64
	if err := s.DB.Model(&contentDeal{}).Where("deal_id > 0").Count(&dealsSuccessful).Error; err != nil {
		return err
	}

	var dealsFailed int64
	if err := s.DB.Model(&contentDeal{}).Where("failed").Count(&dealsFailed).Error; err != nil {
		return err
	}

	var numMiners int64
	if err := s.DB.Model(&storageMiner{}).Count(&numMiners).Error; err != nil {
		return err
	}

	var numUsers int64
	if err := s.DB.Model(&User{}).Count(&numUsers).Error; err != nil {
		return err
	}

	var numFiles int64
	if err := s.DB.Model(&Content{}).Where("active").Count(&numFiles).Error; err != nil {
		return err
	}

	var numRetrievals int64
	if err := s.DB.Model(&retrievalSuccessRecord{}).Count(&numRetrievals).Error; err != nil {
		return err
	}

	var numRetrievalFailures int64
	if err := s.DB.Model(&retrievalFailureRecord{}).Count(&numRetrievalFailures).Error; err != nil {
		return err
	}

	var numStorageFailures int64
	if err := s.DB.Model(&dfeRecord{}).Count(&numStorageFailures).Error; err != nil {
		return err
	}

	return c.JSON(200, &adminStatsResponse{
		TotalDealAttempted:   dealsTotal,
		TotalDealsSuccessful: dealsSuccessful,
		TotalDealsFailed:     dealsFailed,
		NumMiners:            numMiners,
		NumUsers:             numUsers,
		NumFiles:             numFiles,
		NumRetrievals:        numRetrievals,
		NumRetrFailures:      numRetrievalFailures,
		NumStorageFailures:   numStorageFailures,
	})
}

type minerResp struct {
	Addr            address.Address `json:"addr"`
	Suspended       bool            `json:"suspended"`
	SuspendedReason string          `json:"suspendedReason,omitempty"`
}

func (s *Server) handleAdminGetMiners(c echo.Context) error {
	var miners []storageMiner
	if err := s.DB.Find(&miners).Error; err != nil {
		return err
	}

	out := make([]minerResp, len(miners))
	for i, m := range miners {
		out[i].Addr = m.Address.Addr
		out[i].Suspended = m.Suspended
		out[i].SuspendedReason = m.SuspendedReason
	}

	return c.JSON(200, out)
}

func (s *Server) handleAdminGetMinerStats(c echo.Context) error {
	sml, err := s.CM.computeSortedMinerList()
	if err != nil {
		return err
	}

	return c.JSON(200, sml)
}

func (s *Server) handleAdminRemoveMiner(c echo.Context) error {
	m, err := address.NewFromString(c.Param("miner"))
	if err != nil {
		return err
	}

	return s.DB.Where("address = ?", m.String()).Delete(&storageMiner{}).Error
}

type suspendMinerBody struct {
	Reason string `json:"reason"`
}

func (s *Server) handleAdminSuspendMiner(c echo.Context) error {
	m, err := address.NewFromString(c.Param("miner"))
	if err != nil {
		return err
	}

	var body suspendMinerBody
	if err := c.Bind(&body); err != nil {
		return err
	}

	if err := s.DB.Model(&storageMiner{}).Where("address = ?", m.String()).Updates(map[string]interface{}{
		"suspended":        true,
		"suspended_reason": body.Reason,
	}).Error; err != nil {
		return err
	}

	return c.JSON(200, map[string]string{})
}

func (s *Server) handleAdminUnsuspendMiner(c echo.Context) error {
	m, err := address.NewFromString(c.Param("miner"))
	if err != nil {
		return err
	}

	if err := s.DB.Model(&storageMiner{}).Where("address = ?", m.String()).Update("suspended", false).Error; err != nil {
		return err
	}

	return c.JSON(200, map[string]string{})
}

func (s *Server) handleAdminAddMiner(c echo.Context) error {
	m, err := address.NewFromString(c.Param("miner"))
	if err != nil {
		return err
	}

	if err := s.DB.Clauses(&clause.OnConflict{DoNothing: true}).Create(&storageMiner{Address: dbAddr{m}}).Error; err != nil {
		return err
	}

	return c.JSON(200, map[string]string{})
}

type contentDealStats struct {
	NumDeals     int `json:"numDeals"`
	NumConfirmed int `json:"numConfirmed"`
	NumFailed    int `json:"numFailed"`

	TotalSpending     abi.TokenAmount `json:"totalSpending"`
	ConfirmedSpending abi.TokenAmount `json:"confirmedSpending"`
}

func (s *Server) handleDealStats(c echo.Context) error {
	ctx, span := s.tracer.Start(c.Request().Context(), "handleDealStats")
	defer span.End()

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

func (s *Server) handleGetRetrievalInfo(c echo.Context) error {
	var infos []retrievalSuccessRecord
	if err := s.DB.Find(&infos).Error; err != nil {
		return err
	}

	return c.JSON(200, infos)
}

func (s *Server) handleRetrievalCheck(c echo.Context) error {
	ctx := c.Request().Context()
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

type askResponse struct {
	Miner         string           `json:"miner"`
	Price         *abi.TokenAmount `json:"price"`
	VerifiedPrice *abi.TokenAmount `json:"verifiedPrice"`
	MinDealSize   int64            `json:"minDealSize"`
}

type priceEstimateResponse struct {
	TotalStr string `json:"totalFil"`
	Total    string `json:"totalAttoFil"`
	Asks     []*minerStorageAsk
}

func (s *Server) handleEstimateDealCost(c echo.Context) error {
	ctx := c.Request().Context()

	var body estimateDealBody
	if err := c.Bind(&body); err != nil {
		return err
	}

	rounded := padreader.PaddedSize(body.Size)

	estimate, err := s.CM.estimatePrice(ctx, body.Replication, rounded.Padded(), abi.ChainEpoch(body.DurationBlks), body.Verified)
	if err != nil {
		return err
	}

	return c.JSON(200, &priceEstimateResponse{
		TotalStr: types.FIL(*estimate.Total).String(),
		Total:    estimate.Total.String(),
		Asks:     estimate.Asks,
	})
}

func (s *Server) handleGetMinerFailures(c echo.Context) error {
	maddr, err := address.NewFromString(c.Param("miner"))
	if err != nil {
		return err
	}

	var merrs []dfeRecord
	if err := s.DB.Limit(1000).Order("created_at desc").Find(&merrs, "miner = ?", maddr.String()).Error; err != nil {
		return err
	}

	return c.JSON(200, merrs)
}

type minerStatsResp struct {
	Miner           address.Address `json:"miner"`
	UsedByEstuary   bool            `json:"usedByEstuary"`
	DealCount       int64           `json:"dealCount"`
	ErrorCount      int64           `json:"errorCount"`
	Suspended       bool            `json:"suspended"`
	SuspendedReason string          `json:"suspendedReason"`
}

func (s *Server) handleGetMinerStats(c echo.Context) error {
	maddr, err := address.NewFromString(c.Param("miner"))
	if err != nil {
		return err
	}

	var m storageMiner
	if err := s.DB.First(&m, "address = ?", maddr.String()).Error; err != nil {
		if xerrors.Is(err, gorm.ErrRecordNotFound) {
			return c.JSON(200, &minerStatsResp{
				Miner:         maddr,
				UsedByEstuary: false,
			})
		}
		return err
	}

	var dealscount int64
	if err := s.DB.Model(&contentDeal{}).Where("miner = ?", maddr.String()).Count(&dealscount).Error; err != nil {
		return err
	}

	var errorcount int64
	if err := s.DB.Model(&dfeRecord{}).Where("miner = ?", maddr.String()).Count(&errorcount).Error; err != nil {
		return err
	}

	return c.JSON(200, &minerStatsResp{
		Miner:           maddr,
		UsedByEstuary:   true,
		DealCount:       dealscount,
		ErrorCount:      errorcount,
		Suspended:       m.Suspended,
		SuspendedReason: m.SuspendedReason,
	})
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

type bandwidthResponse struct {
	TotalOut int64 `json:"totalOut"`
}

func (s *Server) handleGetContentBandwidth(c echo.Context, u *User) error {
	cont, err := strconv.Atoi(c.Param("content"))
	if err != nil {
		return err
	}

	var content Content
	if err := s.DB.First(&content, cont).Error; err != nil {
		return err
	}

	if content.UserID != u.ID {
		return &httpError{
			Code:    401,
			Message: ERR_NOT_AUTHORIZED,
		}
	}

	// select SUM(size * reads) from obj_refs left join objects on obj_refs.object = objects.id where obj_refs.content = 42;
	var bw int64
	if err := s.DB.Model(ObjRef{}).
		Select("SUM(size * reads)").
		Where("obj_refs.content = ?", content.ID).
		Joins("left join objects on obj_refs.object = objects.id").
		Scan(&bw).Error; err != nil {
		return err
	}

	return c.JSON(200, &bandwidthResponse{
		TotalOut: bw,
	})
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

	if err := s.CM.OffloadContent(c.Request().Context(), uint(cont)); err != nil {
		return err
	}

	return c.JSON(200, map[string]interface{}{})
}

func (s *Server) handleRefreshContent(c echo.Context) error {
	cont, err := strconv.Atoi(c.Param("content"))
	if err != nil {
		return err
	}

	return s.CM.RefreshContent(c.Request().Context(), uint(cont))
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

			log.Warnw("User not authorized", "user", u.ID, "perm", u.Perm, "required", level)

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

	if invite.ClaimedBy != 0 {
		return &httpError{
			Code:    http.StatusForbidden,
			Message: ERR_INVITE_ALREADY_USED,
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
		Perm:     PermLevelUser,
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

	invite.ClaimedBy = newUser.ID
	if err := s.DB.Save(&invite).Error; err != nil {
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

	authToken, err := s.newAuthTokenForUser(&user)
	if err != nil {
		return err
	}

	return c.JSON(200, &loginResponse{
		Token:  authToken.Token,
		Expiry: authToken.Expiry,
	})
}

func (s *Server) newAuthTokenForUser(user *User) (*AuthToken, error) {
	authToken := &AuthToken{
		Token:  "EST" + uuid.New().String() + "ARY",
		User:   user.ID,
		Expiry: time.Now().Add(time.Hour * 24 * 7),
	}

	if err := s.DB.Create(authToken).Error; err != nil {
		return nil, err
	}

	return authToken, nil
}

type userSettings struct {
	Replication  int  `json:"replication"`
	Verified     bool `json:"verified"`
	DealDuration int  `json:"dealDuration"`
}

type viewerResponse struct {
	Username string `json:"username"`
	Perms    int    `json:"perms"`

	Settings userSettings `json:"settings"`
}

func (s *Server) handleGetViewer(c echo.Context, u *User) error {

	return c.JSON(200, &viewerResponse{
		Username: u.Username,
		Perms:    u.Perm,
		Settings: userSettings{
			Replication:  6,
			Verified:     true,
			DealDuration: 2880 * 365,
		},
	})
}

func (s *Server) handleHealth(c echo.Context) error {
	return c.JSON(200, map[string]string{
		"status": "ok",
	})
}

type getApiKeysResp struct {
	Token  string    `json:"token"`
	Expiry time.Time `json:"expiry"`
}

func (s *Server) handleUserRevokeApiKey(c echo.Context, u *User) error {
	kval := c.Param("key")

	if err := s.DB.Delete(&AuthToken{}, "\"user\" = ? AND token = ?", u.ID, kval).Error; err != nil {
		return err
	}

	return c.NoContent(200)
}

func (s *Server) handleUserCreateApiKey(c echo.Context, u *User) error {
	authToken, err := s.newAuthTokenForUser(u)
	if err != nil {
		return err
	}

	return c.JSON(200, &getApiKeysResp{
		Token:  authToken.Token,
		Expiry: authToken.Expiry,
	})
}

func (s *Server) handleUserGetApiKeys(c echo.Context, u *User) error {
	var keys []AuthToken
	if err := s.DB.Find(&keys, "auth_tokens.user = ?", u.ID).Error; err != nil {
		return err
	}

	out := []getApiKeysResp{}
	for _, k := range keys {
		out = append(out, getApiKeysResp{
			Token:  k.Token,
			Expiry: k.Expiry,
		})
	}

	return c.JSON(200, out)
}

type createCollectionBody struct {
	Name        string `json:"name"`
	Description string `json:"description"`
}

func (s *Server) handleCreateCollection(c echo.Context, u *User) error {
	var body createCollectionBody
	if err := c.Bind(&body); err != nil {
		return err
	}

	col := &Collection{
		UUID:        uuid.New().String(),
		Name:        body.Name,
		Description: body.Description,
		UserID:      u.ID,
	}

	if err := s.DB.Create(col).Error; err != nil {
		return err
	}

	return c.JSON(200, col)
}

func (s *Server) handleListCollections(c echo.Context, u *User) error {
	var cols []Collection
	if err := s.DB.Find(&cols, "user_id = ?", u.ID).Error; err != nil {
		return err
	}

	return c.JSON(200, cols)
}

type addContentToCollectionParams struct {
	Contents   []uint `json:"contents"`
	Collection string `json:"collection"`
}

func (s *Server) handleAddContentsToCollection(c echo.Context, u *User) error {
	var params addContentToCollectionParams
	if err := c.Bind(&params); err != nil {
		return err
	}

	if len(params.Contents) > 128 {
		return fmt.Errorf("too many contents specified: %d (max 128)", len(params.Contents))
	}

	var col Collection
	if err := s.DB.First(&col, "user_id = ?", u.ID).Error; err != nil {
		return err
	}

	var contents []Content
	if err := s.DB.Find(&contents, "id in ? and user_id = ?", params.Contents, u.ID).Error; err != nil {
		return err
	}

	if len(contents) != len(params.Contents) {
		return fmt.Errorf("%d specified content(s) were not found or user missing permissions", len(params.Contents)-len(contents))
	}

	var colrefs []CollectionRef
	for _, cont := range contents {
		colrefs = append(colrefs, CollectionRef{
			Collection: col.ID,
			Content:    cont.ID,
		})
	}

	if err := s.DB.Create(colrefs).Error; err != nil {
		return err
	}

	return c.JSON(200, map[string]string{})
}

func (s *Server) handleGetCollectionContents(c echo.Context, u *User) error {
	colid := c.Param("coluuid")

	var col Collection
	if err := s.DB.First(&col, "uuid = ? and user_id = ?", colid, u.ID).Error; err != nil {
		return err
	}

	contents := []Content{}
	if err := s.DB.Debug().
		Model(CollectionRef{}).
		Where("collection = ?", col.ID).
		Joins("left join contents on contents.id = collection_refs.collection").
		Scan(&contents).Error; err != nil {
		return err
	}

	return c.JSON(200, contents)
}

func (s *Server) tracingMiddleware(next echo.HandlerFunc) echo.HandlerFunc {
	return func(c echo.Context) error {

		r := c.Request()
		tctx, span := s.tracer.Start(context.Background(),
			"HTTP "+r.Method+" "+c.Path(),
			trace.WithAttributes(
				semconv.HTTPMethodKey.String(r.Method),
				semconv.HTTPRouteKey.String(r.URL.Path),
				semconv.HTTPClientIPKey.String(r.RemoteAddr),
				semconv.HTTPRequestContentLengthKey.Int64(c.Request().ContentLength),
			),
		)
		defer span.End()

		r = r.WithContext(tctx)
		c.SetRequest(r)

		if err := next(c); err != nil {
			span.SetStatus(codes.Error, err.Error())
			span.RecordError(err)
			c.Error(err)
		} else {
			span.SetStatus(codes.Ok, "")
		}

		span.SetAttributes(
			semconv.HTTPStatusCodeKey.Int(c.Response().Status),
			semconv.HTTPResponseContentLengthKey.Int64(c.Response().Size),
		)

		return nil
	}
}

type adminUserResponse struct {
	Id       uint   `json:"id"`
	Username string `json:"username"`

	SpaceUsed int `json:"spaceUsed"`
	NumFiles  int `json:"numFiles"`
}

func (s *Server) handleAdminGetUsers(c echo.Context) error {
	var resp []adminUserResponse
	if err := s.DB.Model(Content{}).
		Select("user_id as id,(?) as username,SUM(size) as space_used,count(*) as num_files", s.DB.Model(&User{}).Select("username").Where("id = user_id")).
		Group("user_id").Scan(&resp).Error; err != nil {
		return err
	}

	return c.JSON(200, resp)
}

type publicStatsResponse struct {
	TotalStorage     int64 `json:"totalStorage"`
	TotalFilesStored int   `json:"totalFiles"`
}

func (s *Server) handlePublicStats(c echo.Context) error {
	var stats publicStatsResponse
	if err := s.DB.Model(Content{}).Select("SUM(size) as total_storage,COUNT(*) as total_files_stored").Scan(&stats).Error; err != nil {
		return err
	}

	return c.JSON(200, stats)
}
