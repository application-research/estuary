package main

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	httpprof "net/http/pprof"
	"path/filepath"
	"runtime/pprof"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	drpc "github.com/application-research/estuary/drpc"
	"github.com/application-research/estuary/util"
	"github.com/application-research/filclient"
	"github.com/filecoin-project/go-address"
	datatransfer "github.com/filecoin-project/go-data-transfer"
	"github.com/filecoin-project/go-padreader"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/crypto"
	"github.com/filecoin-project/lotus/chain/types"
	"github.com/filecoin-project/lotus/lib/sigs"
	"github.com/filecoin-project/specs-actors/actors/builtin/market"
	"github.com/google/uuid"
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
	car "github.com/ipld/go-car"
	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/lightstep/otel-launcher-go/launcher"
	"github.com/multiformats/go-multiaddr"
	"golang.org/x/crypto/acme/autocert"
	"golang.org/x/net/websocket"
	"golang.org/x/sys/unix"
	"golang.org/x/xerrors"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/semconv"
	"go.opentelemetry.io/otel/trace"
)

func (s *Server) ServeAPI(srv string, logging bool, domain string, lsteptok string, cachedir string) error {
	if lsteptok != "" {
		ls := launcher.ConfigureOpentelemetry(
			launcher.WithServiceName("estuary-api"),
			launcher.WithAccessToken(lsteptok),
		)

		defer ls.Shutdown()
	}

	e := echo.New()

	if domain != "" {
		e.AutoTLSManager.Cache = autocert.DirCache(filepath.Join(cachedir, "certs"))
		e.AutoTLSManager.HostPolicy = autocert.HostWhitelist(domain)
	}

	if logging {
		e.Use(middleware.Logger())
	}

	e.Use(s.tracingMiddleware)
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

		ctx.NoContent(500)
	}

	e.GET("/debug/pprof/:prof", serveProfile)
	e.GET("/debug/cpuprofile", serveCpuProfile)

	e.Use(middleware.CORS())

	e.POST("/register", s.handleRegisterUser)
	e.POST("/login", s.handleLoginUser)
	e.GET("/health", s.handleHealth)

	e.GET("/test-error", s.handleTestError)

	e.GET("/viewer", withUser(s.handleGetViewer), s.AuthRequired(util.PermLevelUser))

	user := e.Group("/user")
	user.Use(s.AuthRequired(util.PermLevelUser))
	user.GET("/test-error", s.handleTestError)
	user.GET("/api-keys", withUser(s.handleUserGetApiKeys))
	user.POST("/api-keys", withUser(s.handleUserCreateApiKey))
	user.DELETE("/api-keys/:key", withUser(s.handleUserRevokeApiKey))
	user.GET("/export", withUser(s.handleUserExportData))
	user.PUT("/password", withUser(s.handleUserChangePassword))
	user.PUT("/address", withUser(s.handleUserChangeAddress))
	user.GET("/stats", withUser(s.handleGetUserStats))

	userMiner := user.Group("/miner")
	userMiner.POST("/claim", withUser(s.handleUserClaimMiner))
	userMiner.GET("/claim/:miner", withUser(s.handleUserGetClaimMinerMsg))
	userMiner.POST("/suspend/:miner", withUser(s.handleSuspendMiner))
	userMiner.PUT("/unsuspend/:miner", withUser(s.handleUnsuspendMiner))
	userMiner.PUT("/set-info/:miner", withUser(s.handleMinersSetInfo))

	content := e.Group("/content")
	content.Use(s.AuthRequired(util.PermLevelUser))
	content.POST("/add", withUser(s.handleAdd))
	content.POST("/add-ipfs", withUser(s.handleAddIpfs))
	content.POST("/add-car", withUser(s.handleAddCar))
	content.GET("/by-cid/:cid", s.handleGetContentByCid)
	content.GET("/stats", withUser(s.handleStats))
	content.GET("/ensure-replication/:datacid", s.handleEnsureReplication)
	content.GET("/status/:id", withUser(s.handleContentStatus))
	content.GET("/list", withUser(s.handleListContent))
	content.GET("/deals", withUser(s.handleListContentWithDeals))
	content.GET("/failures/:content", withUser(s.handleGetContentFailures))
	content.GET("/bw-usage/:content", withUser(s.handleGetContentBandwidth))
	content.GET("/staging-zones", withUser(s.handleGetStagingZoneForUser))
	content.GET("/aggregated/:content", withUser(s.handleGetAggregatedForContent))
	content.GET("/all-deals", withUser(s.handleGetAllDealsForUser))
	content.POST("/create", withUser(s.handleCreateContent))

	// TODO: the commented out routes here are still fairly useful, but maybe
	// need to have some sort of 'super user' permission level in order to use
	// them? Can easily cause harm using them
	deals := e.Group("/deals")
	deals.Use(s.AuthRequired(util.PermLevelUser))
	deals.GET("/status/:deal", withUser(s.handleGetDealStatus))
	deals.GET("/status-by-proposal/:propcid", withUser(s.handleGetDealStatusByPropCid))
	deals.GET("/query/:miner", s.handleQueryAsk)
	deals.POST("/make/:miner", withUser(s.handleMakeDeal))
	//deals.POST("/transfer/start/:miner/:propcid/:datacid", s.handleTransferStart)
	deals.POST("/transfer/status", s.handleTransferStatus)
	deals.GET("/transfer/in-progress", s.handleTransferInProgress)
	//deals.POST("/transfer/restart", s.handleTransferRestart)
	deals.GET("/status/:miner/:propcid", s.handleDealStatus)
	deals.POST("/estimate", s.handleEstimateDealCost)
	deals.GET("/proposal/:propcid", s.handleGetProposal)
	deals.GET("/info/:dealid", s.handleGetDealInfo)
	deals.GET("/failures", s.handleStorageFailures)

	cols := e.Group("/collections")
	cols.Use(s.AuthRequired(util.PermLevelUser))
	cols.GET("/list", withUser(s.handleListCollections))
	cols.POST("/create", withUser(s.handleCreateCollection))
	cols.POST("/add-content", withUser(s.handleAddContentsToCollection))
	cols.GET("/content/:coluuid", withUser(s.handleGetCollectionContents))

	pinning := e.Group("/pinning")
	pinning.Use(s.AuthRequired(util.PermLevelUser))
	pinning.GET("/pins", withUser(s.handleListPins))
	pinning.POST("/pins", withUser(s.handleAddPin))
	pinning.GET("/pins/:requestid", withUser(s.handleGetPin))
	pinning.POST("/pins/:requestid", withUser(s.handleReplacePin))
	pinning.DELETE("/pins/:requestid", withUser(s.handleDeletePin))

	// explicitly public, for now
	public := e.Group("/public")

	public.GET("/stats", s.handlePublicStats)
	public.GET("/by-cid/:cid", s.handleGetContentByCid)
	public.GET("/deals/failures", s.handleStorageFailures)

	metrics := public.Group("/metrics")
	metrics.GET("/deals-on-chain", s.handleMetricsDealOnChain)

	netw := public.Group("/net")
	netw.GET("/peers", s.handleNetPeers)
	netw.GET("/addrs", s.handleNetAddrs)

	miners := public.Group("/miners")
	miners.GET("", s.handleAdminGetMiners)
	miners.GET("/failures/:miner", s.handleGetMinerFailures)
	miners.GET("/deals/:miner", s.handleGetMinerDeals)
	miners.GET("/stats/:miner", s.handleGetMinerStats)
	miners.GET("/storage/query/:miner", s.handleQueryAsk)

	admin := e.Group("/admin")
	admin.Use(s.AuthRequired(util.PermLevelAdmin))
	admin.GET("/balance", s.handleAdminBalance)
	admin.POST("/add-escrow/:amt", s.handleAdminAddEscrow)
	admin.GET("/dealstats", s.handleDealStats)
	admin.GET("/disk-info", s.handleDiskSpaceCheck)
	admin.GET("/stats", s.handleAdminStats)

	// miners
	admin.POST("/miners/add/:miner", s.handleAdminAddMiner)
	admin.POST("/miners/rm/:miner", s.handleAdminRemoveMiner)
	admin.POST("/miners/suspend/:miner", withUser(s.handleSuspendMiner))
	admin.PUT("/miners/unsuspend/:miner", withUser(s.handleUnsuspendMiner))
	admin.PUT("/miners/set-info/:miner", withUser(s.handleMinersSetInfo))
	admin.GET("/miners", s.handleAdminGetMiners)
	admin.GET("/miners/stats", s.handleAdminGetMinerStats)

	admin.GET("/cm/progress", s.handleAdminGetProgress)
	admin.GET("/cm/all-deals", s.handleDebugGetAllDeals)
	admin.GET("/cm/read/:content", s.handleReadLocalContent)
	admin.GET("/cm/staging/all", s.handleAdminGetStagingZones)
	admin.GET("/cm/offload/candidates", s.handleGetOffloadingCandidates)
	admin.POST("/cm/offload/:content", s.handleOffloadContent)
	admin.POST("/cm/offload/collect", s.handleRunOffloadingCollection)
	admin.GET("/cm/refresh/:content", s.handleRefreshContent)
	admin.GET("/cm/buckets", s.handleGetBucketDiag)
	admin.GET("/cm/health/:id", s.handleContentHealthCheck)
	admin.POST("/cm/dealmaking", s.handleSetDealMaking)

	admnetw := admin.Group("/net")
	admnetw.GET("/peers", s.handleNetPeers)

	admin.GET("/retrieval/querytest/:content", s.handleRetrievalCheck)
	admin.GET("/retrieval/stats", s.handleGetRetrievalInfo)

	admin.POST("/invite/:code", withUser(s.handleAdminCreateInvite))
	admin.GET("/invites", s.handleAdminGetInvites)

	admin.GET("/fixdeals", s.handleFixupDeals)
	admin.POST("/loglevel", s.handleLogLevel)

	users := admin.Group("/users")
	users.GET("", s.handleAdminGetUsers)

	shuttle := admin.Group("/shuttle")
	shuttle.POST("/init", s.handleShuttleInit)
	shuttle.GET("/list", s.handleShuttleList)

	e.GET("/shuttle/conn", s.handleShuttleConnection)

	if domain != "" {
		return e.StartAutoTLS(srv)
	} else {
		return e.Start(srv)
	}
}

func serveCpuProfile(c echo.Context) error {
	if err := pprof.StartCPUProfile(c.Response()); err != nil {
		return err
	}

	defer pprof.StopCPUProfile()

	select {
	case <-c.Request().Context().Done():
		return c.Request().Context().Err()
	case <-time.After(time.Second * 30):
	}

	return nil
}

func serveProfile(c echo.Context) error {
	httpprof.Handler(c.Param("prof")).ServeHTTP(c.Response().Writer, c.Request())
	return nil
}

type statsResp struct {
	ID              uint    `json:"id"`
	Cid             cid.Cid `json:"cid"`
	File            string  `json:"file"`
	BWUsed          int64   `json:"bwUsed"`
	TotalRequests   int64   `json:"totalRequests"`
	Offloaded       bool    `json:"offloaded"`
	AggregatedFiles int64   `json:"aggregatedFiles"`
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
	return fmt.Errorf("i am a scary error, log me please more")
}

func (s *Server) handleStats(c echo.Context, u *User) error {
	limit := 500
	if limstr := c.QueryParam("limit"); limstr != "" {
		nlim, err := strconv.Atoi(limstr)
		if err != nil {
			return err
		}

		if nlim > 0 {
			limit = nlim
		}
	}

	offset := 0
	if offstr := c.QueryParam("offset"); offstr != "" {
		noff, err := strconv.Atoi(offstr)
		if err != nil {
			return err
		}

		if noff > 0 {
			offset = noff
		}
	}

	var contents []Content
	if err := s.DB.Limit(limit).Offset(offset).Order("created_at desc").Find(&contents, "user_id = ? and active", u.ID).Error; err != nil {
		return err
	}

	out := make([]statsResp, 0, len(contents))
	for _, c := range contents {
		st := statsResp{
			ID:   c.ID,
			Cid:  c.Cid.CID,
			File: c.Name,
		}

		if false {
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
		}

		if c.Aggregate {
			if err := s.DB.Model(Content{}).Where("aggregated_in = ?", c.ID).Count(&st.AggregatedFiles).Error; err != nil {
				return err
			}
		}

		out = append(out, st)
	}

	return c.JSON(200, out)
}

type addFromIpfsParams struct {
	Root       string   `json:"root"`
	Name       string   `json:"name"`
	Collection string   `json:"collection"`
	Peers      []string `json:"peers"`
}

func (s *Server) handleAddIpfs(c echo.Context, u *User) error {
	ctx := c.Request().Context()

	if s.CM.contentAddingDisabled || u.StorageDisabled {
		return &util.HttpError{
			Code:    400,
			Message: util.ERR_CONTENT_ADDING_DISABLED,
		}
	}

	var params addFromIpfsParams
	if err := c.Bind(&params); err != nil {
		return err
	}

	var cols []*Collection
	if params.Collection != "" {
		var srchCol Collection
		if err := s.DB.First(&srchCol, "uuid = ? and user_id = ?", params.Collection, u.ID).Error; err != nil {
			return err
		}

		cols = []*Collection{&srchCol}
	}

	var addrInfos []peer.AddrInfo
	for _, p := range params.Peers {
		ai, err := peer.AddrInfoFromString(p)
		if err != nil {
			return err
		}

		addrInfos = append(addrInfos, *ai)
	}

	rcid, err := cid.Decode(params.Root)
	if err != nil {
		return err
	}

	if c.QueryParam("ignore-dupes") == "true" {
		var count int64
		if err := s.DB.Model(Content{}).Where("cid = ? and user_id = ?", rcid.Bytes(), u.ID).Count(&count).Error; err != nil {
			return err
		}
		if count > 0 {
			return c.JSON(302, map[string]string{"message": "content with given cid already preserved"})
		}
	}

	filename := params.Name
	if filename == "" {
		filename = params.Root
	}

	pinstatus, err := s.CM.pinContent(ctx, u.ID, rcid, filename, cols, addrInfos, 0, nil)
	if err != nil {
		return err
	}

	return c.JSON(202, pinstatus)
}

func (s *Server) handleAddCar(c echo.Context, u *User) error {
	ctx := c.Request().Context()

	if s.CM.contentAddingDisabled || u.StorageDisabled {
		return &util.HttpError{
			Code:    400,
			Message: util.ERR_CONTENT_ADDING_DISABLED,
		}
	}

	bsid, sbs, err := s.StagingMgr.AllocNew()
	if err != nil {
		return err
	}

	defer c.Request().Body.Close()
	header, err := s.loadCar(ctx, sbs, c.Request().Body)
	if err != nil {
		return err
	}

	if len(header.Roots) != 1 {
		// if someone wants this feature, let me know
		return c.JSON(400, map[string]string{"error": "cannot handle uploading car files with multiple roots"})
	}

	// TODO: how to specify filename?
	filename := header.Roots[0].String()

	bserv := blockservice.New(sbs, nil)
	dserv := merkledag.NewDAGService(bserv)

	cont, err := s.CM.addDatabaseTracking(ctx, u, dserv, s.Node.Blockstore, header.Roots[0], filename, defaultReplication)
	if err != nil {
		return err
	}

	if err := s.dumpBlockstoreTo(ctx, sbs, s.Node.Blockstore); err != nil {
		return xerrors.Errorf("failed to move data from staging to main blockstore: %w", err)
	}

	go func() {
		if err := s.StagingMgr.CleanUp(bsid); err != nil {
			log.Errorf("failed to clean up staging blockstore: %s", err)
		}
	}()

	go func() {
		// TODO: we should probably have a queue to throw these in instead of putting them out in goroutines...
		s.CM.ToCheck <- cont.ID
	}()

	go func() {
		if err := s.Node.Provider.Provide(header.Roots[0]); err != nil {
			fmt.Println("providing failed: ", err)
		}
		fmt.Println("providing complete")
	}()
	return c.JSON(200, map[string]interface{}{"content": cont})

	return nil
}

func (s *Server) loadCar(ctx context.Context, bs blockstore.Blockstore, r io.Reader) (*car.CarHeader, error) {
	_, span := s.tracer.Start(ctx, "loadCar")
	defer span.End()

	return car.LoadCar(bs, r)
}

func (s *Server) handleAdd(c echo.Context, u *User) error {
	ctx, span := s.tracer.Start(c.Request().Context(), "handleAdd", trace.WithAttributes(attribute.Int("user", int(u.ID))))
	defer span.End()

	if s.CM.contentAddingDisabled || u.StorageDisabled || s.CM.localContentAddingDisabled {
		var details string
		if s.CM.localContentAddingDisabled {
			uep, err := s.getPreferredUploadEndpoints(u)
			if err != nil {
				log.Warnf("failed to get preferred upload endpoints: %s", err)
			} else {
				details = fmt.Sprintf("this estuary instance has disabled adding new content, please redirect your request to one of the following endpoints: %v", uep)
			}
		}
		return &util.HttpError{
			Code:    400,
			Message: util.ERR_CONTENT_ADDING_DISABLED,
			Details: details,
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

	content, err := s.CM.addDatabaseTracking(ctx, u, dserv, bs, nd.Cid(), fname, replication)
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

	go func() {
		s.CM.ToCheck <- content.ID
	}()

	if c.QueryParam("lazy-provide") != "true" {
		subctx, cancel := context.WithTimeout(ctx, time.Second*10)
		defer cancel()
		if err := s.Node.FullRT.Provide(subctx, nd.Cid(), true); err != nil {
			span.RecordError(fmt.Errorf("provide error: %w", err))
			log.Errorf("fullrt provide call errored: %s", err)
		}
	}

	go func() {
		if err := s.Node.Provider.Provide(nd.Cid()); err != nil {
			fmt.Println("providing failed: ", err)
		}
		fmt.Println("providing complete")
	}()

	return c.JSON(200, map[string]interface{}{
		"cid":       nd.Cid().String(),
		"estuaryId": content.ID,
		"providers": s.CM.pinDelegatesForContent(*content),
	})
}

func (s *Server) importFile(ctx context.Context, dserv ipld.DAGService, fi io.Reader) (ipld.Node, error) {
	_, span := s.tracer.Start(ctx, "importFile")
	defer span.End()

	spl := chunker.DefaultSplitter(fi)
	return importer.BuildDagFromReader(dserv, spl)
}

var noDataTimeout = time.Minute * 10

func (cm *ContentManager) addDatabaseTrackingToContent(ctx context.Context, cont uint, dserv ipld.NodeGetter, bs blockstore.Blockstore, root cid.Cid) error {
	ctx, span := cm.tracer.Start(ctx, "computeObjRefsUpdate")
	defer span.End()

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
	cset := cid.NewSet()

	err := merkledag.Walk(ctx, func(ctx context.Context, c cid.Cid) ([]*ipld.Link, error) {
		node, err := dserv.Get(ctx, c)
		if err != nil {
			return nil, err
		}

		select {
		case gotData <- struct{}{}:
		case <-ctx.Done():
		}

		objects = append(objects, &Object{
			Cid:  util.DbCID{c},
			Size: len(node.RawData()),
		})

		if c.Type() == cid.Raw {
			return nil, nil
		}

		return node.Links(), nil
	}, root, cset.Visit, merkledag.Concurrent())
	if err != nil {
		return err
	}

	if err := cm.addObjectsToDatabase(ctx, cont, objects); err != nil {
		return err
	}

	return nil
}

func (cm *ContentManager) addDatabaseTracking(ctx context.Context, u *User, dserv ipld.NodeGetter, bs blockstore.Blockstore, root cid.Cid, fname string, replication int) (*Content, error) {
	ctx, span := cm.tracer.Start(ctx, "computeObjRefs")
	defer span.End()

	content := &Content{
		Cid:         util.DbCID{root},
		Name:        fname,
		Active:      false,
		Pinning:     true,
		UserID:      u.ID,
		Replication: replication,
		Location:    "local",
	}

	if err := cm.DB.Create(content).Error; err != nil {
		return nil, xerrors.Errorf("failed to track new content in database: %w", err)
	}

	if err := cm.addDatabaseTrackingToContent(ctx, content.ID, dserv, bs, root); err != nil {
		return nil, err
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

type expandedContent struct {
	Content
	AggregatedFiles int64 `json:"aggregatedFiles"`
}

func (s *Server) handleListContentWithDeals(c echo.Context, u *User) error {

	var limit int = 20
	if limstr := c.QueryParam("limit"); limstr != "" {
		l, err := strconv.Atoi(limstr)
		if err != nil {
			return err
		}
		limit = l
	}

	var offset int
	if offstr := c.QueryParam("offset"); offstr != "" {
		o, err := strconv.Atoi(offstr)
		if err != nil {
			return err
		}
		offset = o
	}

	var contents []Content
	if err := s.DB.Limit(limit).Offset(offset).Order("id desc").Find(&contents, "active and user_id = ? and not aggregated_in > 0", u.ID).Error; err != nil {
		return err
	}

	out := make([]expandedContent, 0, len(contents))
	for _, cont := range contents {
		if !s.CM.contentInStagingZone(c.Request().Context(), cont) {
			ec := expandedContent{
				Content: cont,
			}
			if cont.Aggregate {
				if err := s.DB.Model(Content{}).Where("aggregated_in = ?", cont.ID).Count(&ec.AggregatedFiles).Error; err != nil {
					return err
				}

			}
			out = append(out, ec)
		}
	}

	return c.JSON(200, out)
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

			chanst, err := s.CM.GetTransferStatus(ctx, &d, &content)
			if err != nil {
				log.Errorf("failed to get transfer status: %s", err)
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

	sort.Slice(ds, func(i, j int) bool {
		return ds[i].Deal.CreatedAt.Before(ds[j].Deal.CreatedAt)
	})

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

func (s *Server) handleGetDealStatus(c echo.Context, u *User) error {
	ctx := c.Request().Context()

	val, err := strconv.Atoi(c.Param("deal"))
	if err != nil {
		return err
	}

	dstatus, err := s.dealStatusByID(ctx, uint(val))
	if err != nil {
		return err
	}

	return c.JSON(200, dstatus)
}

func (s *Server) handleGetDealStatusByPropCid(c echo.Context, u *User) error {
	ctx := c.Request().Context()

	propcid, err := cid.Decode(c.Param("propcid"))
	if err != nil {
		return err
	}

	var deal contentDeal
	if err := s.DB.First(&deal, "prop_cid = ?", propcid.Bytes()).Error; err != nil {
		return err
	}

	dstatus, err := s.dealStatusByID(ctx, deal.ID)
	if err != nil {
		return err
	}

	return c.JSON(200, dstatus)
}

func (s *Server) dealStatusByID(ctx context.Context, dealid uint) (*dealStatus, error) {
	var deal contentDeal
	if err := s.DB.First(&deal, "id = ?", dealid).Error; err != nil {
		return nil, err
	}

	var content Content
	if err := s.DB.First(&content, "id = ?", deal.Content).Error; err != nil {
		return nil, err
	}

	chanst, err := s.CM.GetTransferStatus(ctx, &deal, &content)
	if err != nil {
		log.Errorf("failed to get transfer status: %s", err)
	}

	dstatus := dealStatus{
		Deal:           deal,
		TransferStatus: chanst,
	}

	if deal.DealID > 0 {
		markDeal, err := s.Api.StateMarketStorageDeal(ctx, abi.DealID(deal.DealID), types.EmptyTSK)
		if err != nil {
			log.Warnw("failed to get deal info from market actor", "dealID", deal.DealID, "error", err)
		} else {
			dstatus.OnChainState = &onChainDealState{
				SectorStartEpoch: markDeal.State.SectorStartEpoch,
				LastUpdatedEpoch: markDeal.State.LastUpdatedEpoch,
				SlashEpoch:       markDeal.State.SlashEpoch,
			}
		}
	}

	return &dstatus, nil
}

type getContentResponse struct {
	Content      *Content       `json:"content"`
	AggregatedIn *Content       `json:"aggregatedIn,omitempty"`
	Deals        []*contentDeal `json:"deals"`
}

func (s *Server) handleGetContentByCid(c echo.Context) error {
	obj, err := cid.Decode(c.Param("cid"))
	if err != nil {
		return err
	}

	// TODO: check both cidv0 and v1 for dag-pb cids

	var contents []Content
	if err := s.DB.Find(&contents, "cid = ?", obj.Bytes()).Error; err != nil {
		return err
	}

	var out []getContentResponse
	for i, cont := range contents {
		resp := getContentResponse{
			Content: &contents[i],
		}

		id := cont.ID

		if cont.AggregatedIn > 0 {
			var aggr Content
			if err := s.DB.First(&aggr, "id = ?", cont.AggregatedIn).Error; err != nil {
				return err
			}

			resp.AggregatedIn = &aggr
			id = cont.AggregatedIn
		}

		var deals []*contentDeal
		if err := s.DB.Find(&deals, "content = ? and deal_id > 0 and not failed", id).Error; err != nil {
			return err
		}

		resp.Deals = deals

		out = append(out, resp)
	}

	return c.JSON(200, out)
}

func (s *Server) handleQueryAsk(c echo.Context) error {
	addr, err := address.NewFromString(c.Param("miner"))
	if err != nil {
		return err
	}

	ask, err := s.FilClient.GetAsk(c.Request().Context(), addr)
	if err != nil {
		return c.JSON(500, map[string]string{"error": err.Error()})
	}

	if err := s.CM.updateMinerVersion(c.Request().Context(), addr); err != nil {
		return err
	}

	return c.JSON(200, toDBAsk(ask))
}

type dealRequest struct {
	Content uint            `json:"content"`
	Miner   address.Address `json:"miner"`
}

func (s *Server) handleMakeDeal(c echo.Context, u *User) error {
	ctx := c.Request().Context()

	if u.Perm < util.PermLevelAdmin {
		return util.HttpError{
			Code:    401,
			Message: util.ERR_INVALID_AUTH,
		}
	}

	addr, err := address.NewFromString(c.Param("miner"))
	if err != nil {
		return err
	}

	var req dealRequest
	if err := c.Bind(&req); err != nil {
		return err
	}

	var cont Content
	if err := s.DB.First(&cont, "id = ?", req.Content).Error; err != nil {
		return err
	}

	id, err := s.CM.makeDealWithMiner(ctx, cont, addr, true)
	if err != nil {
		return err
	}

	return c.JSON(200, map[string]interface{}{
		"deal": id,
	})
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

	chanid, err := s.FilClient.StartDataTransfer(c.Request().Context(), addr, propCid, dataCid)
	if err != nil {
		return err
	}

	return c.JSON(200, chanid)
}

func (s *Server) handleDealStatus(c echo.Context) error {
	ctx := c.Request().Context()

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

func (s *Server) handleGetProposal(c echo.Context) error {
	propCid, err := cid.Decode(c.Param("propcid"))
	if err != nil {
		return err
	}

	var proprec proposalRecord
	if err := s.DB.First(&proprec, "prop_cid = ?", propCid.Bytes()).Error; err != nil {
		return err
	}

	var prop market.ClientDealProposal
	if err := prop.UnmarshalCBOR(bytes.NewReader(proprec.Data)); err != nil {
		return err
	}

	return c.JSON(200, prop)
}

func (s *Server) handleGetDealInfo(c echo.Context) error {
	dealid, err := strconv.ParseInt(c.Param("dealid"), 10, 64)
	if err != nil {
		return err
	}

	deal, err := s.Api.StateMarketStorageDeal(c.Request().Context(), abi.DealID(dealid), types.EmptyTSK)
	if err != nil {
		return err
	}

	return c.JSON(200, deal)
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

	PinQueueSize int `json:"pinQueueSize"`
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
		PinQueueSize:         s.CM.pinMgr.PinQueueSize(),
	})
}

type minerResp struct {
	Addr            address.Address `json:"addr"`
	Name            string          `json:"name"`
	Suspended       bool            `json:"suspended"`
	SuspendedReason string          `json:"suspendedReason,omitempty"`
	Version         string          `json:"version"`
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
		out[i].Name = m.Name
		out[i].Version = m.Version
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

type minerSetInfoParams struct {
	Name string `json:"name"`
}

func (s *Server) handleMinersSetInfo(c echo.Context, u *User) error {
	m, err := address.NewFromString(c.Param("miner"))
	if err != nil {
		return err
	}

	var sm storageMiner
	if err := s.DB.First(&sm, "address = ?", m.String()).Error; err != nil {
		return err
	}

	if !(u.Perm >= util.PermLevelAdmin || sm.Owner == u.ID) {
		return &util.HttpError{
			Code:    401,
			Message: util.ERR_NOT_AUTHORIZED,
		}
	}

	var params minerSetInfoParams
	if err := c.Bind(&params); err != nil {
		return err
	}

	if err := s.DB.Model(storageMiner{}).Where("address = ?", m.String()).Update("name", params.Name).Error; err != nil {
		return err
	}

	return c.JSON(200, map[string]string{})
}

func (s *Server) handleAdminRemoveMiner(c echo.Context) error {
	m, err := address.NewFromString(c.Param("miner"))
	if err != nil {
		return err
	}

	if err := s.DB.Where("address = ?", m.String()).Delete(&storageMiner{}).Error; err != nil {
		return err
	}

	return c.JSON(200, map[string]string{})
}

type suspendMinerBody struct {
	Reason string `json:"reason"`
}

func (s *Server) handleSuspendMiner(c echo.Context, u *User) error {
	m, err := address.NewFromString(c.Param("miner"))
	if err != nil {
		return err
	}

	var sm storageMiner
	if err := s.DB.First(&sm, "address = ?", m.String()).Error; err != nil {
		return err
	}

	if !(u.Perm >= util.PermLevelAdmin || sm.Owner == u.ID) {
		return &util.HttpError{
			Code:    401,
			Message: util.ERR_NOT_AUTHORIZED,
		}
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

func (s *Server) handleUnsuspendMiner(c echo.Context, u *User) error {
	m, err := address.NewFromString(c.Param("miner"))
	if err != nil {
		return err
	}

	var sm storageMiner
	if err := s.DB.First(&sm, "address = ?", m.String()).Error; err != nil {
		return err
	}

	if !(u.Perm >= util.PermLevelAdmin || sm.Owner == u.ID) {
		return &util.HttpError{
			Code:    401,
			Message: util.ERR_NOT_AUTHORIZED,
		}
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

	if err := s.DB.Clauses(&clause.OnConflict{DoNothing: true}).Create(&storageMiner{Address: util.DbAddr{m}}).Error; err != nil {
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

type lmdbStat struct {
	PSize         uint   `json:"pSize"`
	Depth         uint   `json:"depth"`
	BranchPages   uint64 `json:"branchPages"`
	LeafPages     uint64 `json:"leafPages"`
	OverflowPages uint64 `json:"overflowPages"`
	Entries       uint64 `json:"entries"`
}

type diskSpaceInfo struct {
	BstoreSize uint64 `json:"bstoreSize"`
	BstoreFree uint64 `json:"bstoreFree"`

	LmdbUsage uint64 `json:"lmdbUsage"`

	LmdbStat lmdbStat `json:"lmdbStat"`
}

func (s *Server) handleDiskSpaceCheck(c echo.Context) error {
	/*
		lmst, err := s.Node.Lmdb.Stat()
		if err != nil {
			return err
		}
	*/

	var st unix.Statfs_t
	if err := unix.Statfs(s.Node.Config.Blockstore, &st); err != nil {
		return err
	}

	return c.JSON(200, &diskSpaceInfo{
		BstoreSize: st.Blocks * uint64(st.Bsize),
		BstoreFree: st.Bavail * uint64(st.Bsize),
		/*
			LmdbUsage:  uint64(lmst.PSize) * (lmst.BranchPages + lmst.OverflowPages + lmst.LeafPages),
			LmdbStat: lmdbStat{
				PSize:         lmst.PSize,
				Depth:         lmst.Depth,
				BranchPages:   lmst.BranchPages,
				LeafPages:     lmst.LeafPages,
				OverflowPages: lmst.OverflowPages,
				Entries:       lmst.Entries,
			},
		*/
	})
}

func (s *Server) handleGetRetrievalInfo(c echo.Context) error {
	var infos []retrievalSuccessRecord
	if err := s.DB.Find(&infos).Error; err != nil {
		return err
	}

	var failures []retrievalFailureRecord
	if err := s.DB.Find(&failures).Error; err != nil {
		return err
	}

	return c.JSON(200, map[string]interface{}{
		"records":  infos,
		"failures": failures,
	})
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
	Name            string          `json:"name"`
	Version         string          `json:"version"`
	UsedByEstuary   bool            `json:"usedByEstuary"`
	DealCount       int64           `json:"dealCount"`
	ErrorCount      int64           `json:"errorCount"`
	Suspended       bool            `json:"suspended"`
	SuspendedReason string          `json:"suspendedReason"`

	ChainInfo *minerChainInfo `json:"chainInfo"`
}

type minerChainInfo struct {
	PeerID    string   `json:"peerId"`
	Addresses []string `json:"addresses"`

	Owner  string `json:"owner"`
	Worker string `json:"worker"`
}

func (s *Server) handleGetMinerStats(c echo.Context) error {
	ctx, span := s.tracer.Start(c.Request().Context(), "handleGetMinerStats")
	defer span.End()

	maddr, err := address.NewFromString(c.Param("miner"))
	if err != nil {
		return err
	}

	minfo, err := s.Api.StateMinerInfo(ctx, maddr, types.EmptyTSK)
	if err != nil {
		return err
	}

	ci := minerChainInfo{
		Owner:  minfo.Owner.String(),
		Worker: minfo.Worker.String(),
	}

	if minfo.PeerId != nil {
		ci.PeerID = minfo.PeerId.String()
	}
	for _, a := range minfo.Multiaddrs {
		ma, err := multiaddr.NewMultiaddrBytes(a)
		if err != nil {
			return err
		}
		ci.Addresses = append(ci.Addresses, ma.String())
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
		Name:            m.Name,
		Version:         m.Version,
		ChainInfo:       &ci,
	})
}

func (s *Server) handleGetMinerDeals(c echo.Context) error {
	maddr, err := address.NewFromString(c.Param("miner"))
	if err != nil {
		return err
	}

	var deals []contentDeal
	if err := s.DB.Order("created_at desc").Find(&deals, "miner = ?", maddr.String()).Error; err != nil {
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
		return &util.HttpError{
			Code:    401,
			Message: util.ERR_NOT_AUTHORIZED,
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

func (s *Server) handleGetAggregatedForContent(c echo.Context, u *User) error {
	cont, err := strconv.Atoi(c.Param("content"))
	if err != nil {
		return err
	}

	var content Content
	if err := s.DB.First(&content, "id = ?", cont).Error; err != nil {
		return err
	}

	if content.UserID != u.ID {
		return &util.HttpError{
			Code:    403,
			Message: util.ERR_NOT_AUTHORIZED,
		}
	}

	var sub []Content
	if err := s.DB.Find(&sub, "aggregated_in = ?", cont).Error; err != nil {
		return err
	}

	return c.JSON(200, sub)
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

func (s *Server) handleAdminGetStagingZones(c echo.Context) error {
	s.CM.bucketLk.Lock()
	defer s.CM.bucketLk.Unlock()

	return c.JSON(200, s.CM.buckets)
}

func (s *Server) handleGetOffloadingCandidates(c echo.Context) error {
	conts, err := s.CM.getRemovalCandidates(c.Request().Context(), c.QueryParam("all") == "true")
	if err != nil {
		return err
	}

	return c.JSON(200, conts)
}

func (s *Server) handleRunOffloadingCollection(c echo.Context) error {
	var body struct {
		Execute        bool  `json:"execute"`
		SpaceRequested int64 `json:"spaceRequested"`
	}

	if err := c.Bind(&body); err != nil {
		return err
	}

	res, err := s.CM.ClearUnused(c.Request().Context(), body.SpaceRequested, !body.Execute)
	if err != nil {
		return err
	}

	return c.JSON(200, res)
}

func (s *Server) handleOffloadContent(c echo.Context) error {
	cont, err := strconv.Atoi(c.Param("content"))
	if err != nil {
		return err
	}

	removed, err := s.CM.OffloadContents(c.Request().Context(), []uint{uint(cont)})
	if err != nil {
		return err
	}

	return c.JSON(200, map[string]interface{}{
		"blocksRemoved": removed,
	})
}

func (s *Server) handleRefreshContent(c echo.Context) error {
	cont, err := strconv.Atoi(c.Param("content"))
	if err != nil {
		return err
	}

	if err := s.CM.RefreshContent(c.Request().Context(), uint(cont)); err != nil {
		return c.JSON(500, map[string]string{"error": err.Error()})
	}

	return c.JSON(200, map[string]string{})
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
			return nil, &util.HttpError{
				Code:    http.StatusForbidden,
				Message: util.ERR_INVALID_TOKEN,
			}
		}
		return nil, err
	}

	if authToken.Expiry.Before(time.Now()) {
		return nil, &util.HttpError{
			Code:    http.StatusForbidden,
			Message: util.ERR_TOKEN_EXPIRED,
		}
	}

	var user User
	if err := s.DB.First(&user, "id = ?", authToken.User).Error; err != nil {
		if xerrors.Is(err, gorm.ErrRecordNotFound) {
			return nil, &util.HttpError{
				Code:    http.StatusForbidden,
				Message: util.ERR_INVALID_TOKEN,
			}
		}
		return nil, err
	}

	return &user, nil
}

func (s *Server) AuthRequired(level int) echo.MiddlewareFunc {
	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c echo.Context) error {
			auth, err := util.ExtractAuth(c)
			if err != nil {
				return err
			}

			u, err := s.checkTokenAuth(auth)
			if err != nil {
				return err
			}

			if u.Perm >= level {
				c.Set("user", u)
				return next(c)
			}

			log.Warnw("User not authorized", "user", u.ID, "perm", u.Perm, "required", level)

			return &util.HttpError{
				Code:    401,
				Message: util.ERR_NOT_AUTHORIZED,
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
			return &util.HttpError{
				Code:    http.StatusForbidden,
				Message: util.ERR_INVALID_INVITE,
			}
		}
	}

	if invite.ClaimedBy != 0 {
		return &util.HttpError{
			Code:    http.StatusForbidden,
			Message: util.ERR_INVITE_ALREADY_USED,
		}
	}

	username := strings.ToLower(reg.Username)

	var exist User
	if err := s.DB.First(&exist, "username = ?", username).Error; err == nil {
		return &util.HttpError{
			Code:    http.StatusForbidden,
			Message: util.ERR_USERNAME_TAKEN,
		}
	}

	newUser := &User{
		Username: username,
		UUID:     uuid.New().String(),
		PassHash: reg.PasswordHash,
		Perm:     util.PermLevelUser,
	}
	if err := s.DB.Create(newUser).Error; err != nil {
		herr := &util.HttpError{
			Code:    http.StatusForbidden,
			Message: util.ERR_USER_CREATION_FAILED,
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
	if err := s.DB.First(&user, "username = ?", strings.ToLower(body.Username)).Error; err != nil {
		if xerrors.Is(err, gorm.ErrRecordNotFound) {
			return &util.HttpError{
				Code:    http.StatusForbidden,
				Message: util.ERR_USER_NOT_FOUND,
			}
		}
		return err
	}

	if user.PassHash != body.PassHash {
		return &util.HttpError{
			Code:    http.StatusForbidden,
			Message: util.ERR_INVALID_PASSWORD,
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

type changePasswordParams struct {
	NewPassHash string `json:"newPasswordHash"`
}

func (s *Server) handleUserChangePassword(c echo.Context, u *User) error {
	var params changePasswordParams
	if err := c.Bind(&params); err != nil {
		return err
	}

	if err := s.DB.Model(User{}).Where("id = ?", u.ID).Update("pass_hash", params.NewPassHash).Error; err != nil {
		return err
	}

	return c.JSON(200, map[string]string{})
}

type changeAddressParams struct {
	Address string `json:"address"`
}

func (s *Server) handleUserChangeAddress(c echo.Context, u *User) error {
	var params changeAddressParams
	if err := c.Bind(&params); err != nil {
		return err
	}

	addr, err := address.NewFromString(params.Address)
	if err != nil {
		log.Warnf("invalid filecoin address in change address request body: %w", err)

		return &util.HttpError{
			Code:    401,
			Message: "invalid address in request body",
		}
	}

	if err := s.DB.Model(User{}).Where("id = ?", u.ID).Update("address", addr.String()).Error; err != nil {
		return err
	}

	return c.JSON(200, map[string]string{})
}

type userStatsResponse struct {
	TotalSize int64 `json:"totalSize"`
	NumPins   int64 `json:"numPins"`
}

func (s *Server) handleGetUserStats(c echo.Context, u *User) error {
	var stats userStatsResponse
	if err := s.DB.Model(Content{}).Where("user_id = ?", u.ID).
		Select("SUM(size) as total_size,COUNT(1) as num_pins").
		Scan(&stats).Error; err != nil {
		return err
	}

	return c.JSON(200, stats)
}

func (s *Server) newAuthTokenForUser(user *User) (*AuthToken, error) {
	authToken := &AuthToken{
		Token:  "EST" + uuid.New().String() + "ARY",
		User:   user.ID,
		Expiry: time.Now().Add(time.Hour * 24 * 30),
	}

	if err := s.DB.Create(authToken).Error; err != nil {
		return nil, err
	}

	return authToken, nil
}

func (s *Server) handleGetViewer(c echo.Context, u *User) error {
	uep, err := s.getPreferredUploadEndpoints(u)
	if err != nil {
		return err
	}

	return c.JSON(200, &util.ViewerResponse{
		ID:       u.ID,
		Username: u.Username,
		Perms:    u.Perm,
		Address:  u.Address.Addr.String(),
		Settings: util.UserSettings{
			Replication:           6,
			Verified:              true,
			DealDuration:          dealDuration,
			MaxStagingWait:        maxStagingZoneLifetime,
			FileStagingThreshold:  int64(individualDealThreshold),
			ContentAddingDisabled: s.CM.contentAddingDisabled || u.StorageDisabled,
			DealMakingDisabled:    s.CM.dealMakingDisabled(),
			UploadEndpoints:       uep,
		},
	})
}

func (s *Server) getPreferredUploadEndpoints(u *User) ([]string, error) {
	var out []string

	// TODO: this should be a lotttttt smarter
	s.CM.shuttlesLk.Lock()
	defer s.CM.shuttlesLk.Unlock()
	for _, sh := range s.CM.shuttles {
		if sh.hostname != "" {
			out = append(out, "https://"+sh.hostname+"/content/add")
		}
	}
	if !s.CM.localContentAddingDisabled {
		out = append(out, "https://api.estuary.tech/content/add")
	}
	return out, nil
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

		attrs := []attribute.KeyValue{
			semconv.HTTPMethodKey.String(r.Method),
			semconv.HTTPRouteKey.String(r.URL.Path),
			semconv.HTTPClientIPKey.String(r.RemoteAddr),
			semconv.HTTPRequestContentLengthKey.Int64(c.Request().ContentLength),
		}

		if reqid := r.Header.Get("EstClientReqID"); reqid != "" {
			if len(reqid) > 64 {
				reqid = reqid[:64]
			}
			attrs = append(attrs, attribute.String("ClientReqID", reqid))
		}

		tctx, span := s.tracer.Start(context.Background(),
			"HTTP "+r.Method+" "+c.Path(),
			trace.WithAttributes(attrs...),
		)
		defer span.End()

		r = r.WithContext(tctx)
		c.SetRequest(r)

		err := next(c)
		if err != nil {
			span.SetStatus(codes.Error, err.Error())
			span.RecordError(err)
		} else {
			span.SetStatus(codes.Ok, "")
		}

		span.SetAttributes(
			semconv.HTTPStatusCodeKey.Int(c.Response().Status),
			semconv.HTTPResponseContentLengthKey.Int64(c.Response().Size),
		)

		return err
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
	TotalFilesStored int64 `json:"totalFiles"`
	DealsOnChain     int64 `json:"dealsOnChain"`
}

func (s *Server) handlePublicStats(c echo.Context) error {
	val, ok := s.checkCache("public/stats", time.Minute)
	if ok {
		return c.JSON(200, val)
	}

	var stats publicStatsResponse
	if err := s.DB.Model(Content{}).Where("active and not aggregated_in > 0").Select("SUM(size) as total_storage").Scan(&stats).Error; err != nil {
		return err
	}

	if err := s.DB.Model(Content{}).Where("active and not aggregate").Count(&stats.TotalFilesStored).Error; err != nil {
		return err
	}

	if err := s.DB.Model(contentDeal{}).Where("not failed and deal_id > 0").Count(&stats.DealsOnChain).Error; err != nil {
		return err
	}

	s.setCache("public/stats", stats)
	return c.JSON(200, stats)
}

func (s *Server) handleGetBucketDiag(c echo.Context) error {
	return c.JSON(200, s.CM.getStagingZoneSnapshot(c.Request().Context()))
}

func (s *Server) handleGetStagingZoneForUser(c echo.Context, u *User) error {
	return c.JSON(200, s.CM.getStagingZonesForUser(c.Request().Context(), u.ID))
}

func (s *Server) handleUserExportData(c echo.Context, u *User) error {
	export, err := s.exportUserData(u.ID)
	if err != nil {
		return err
	}

	return c.JSON(200, export)
}

func (s *Server) handleNetPeers(c echo.Context) error {
	return c.JSON(200, s.Node.Host.Network().Peers())
}

func (s *Server) handleNetAddrs(c echo.Context) error {
	id := s.Node.Host.ID()
	addrs := s.Node.Host.Addrs()

	return c.JSON(200, map[string]interface{}{
		"id":        id,
		"addresses": addrs,
	})
}

type dealMetricsInfo struct {
	Time              time.Time `json:"time"`
	DealsOnChain      int       `json:"dealsOnChain"`
	DealsOnChainBytes int64     `json:"dealsOnChainBytes"`
	DealsAttempted    int       `json:"dealsAttempted"`
	DealsSealed       int       `json:"dealsSealed"`
	DealsSealedBytes  int64     `json:"dealsSealedBytes"`
	DealsFailed       int       `json:"dealsFailed"`
}

type metricsDealJoin struct {
	CreatedAt        time.Time
	Failed           bool
	FailedAt         time.Time
	DealID           int64
	Size             int64
	TransferStarted  time.Time `json:"transferStarted"`
	TransferFinished time.Time `json:"transferFinished"`

	OnChainAt time.Time `json:"onChainAt"`
	SealedAt  time.Time `json:"sealedAt"`
}

func (s *Server) handleMetricsDealOnChain(c echo.Context) error {
	var deals []*metricsDealJoin
	if err := s.DB.Model(contentDeal{}).
		Joins("left join contents on content_deals.content = contents.id").
		Select("content_deals.failed as failed, failed_at, deal_id, size, transfer_started, transfer_finished, on_chain_at, sealed_at").
		Scan(&deals).Error; err != nil {
		return err
	}

	coll := make(map[time.Time]*dealMetricsInfo)
	onchainbuckets := make(map[time.Time][]*metricsDealJoin)
	attempts := make(map[time.Time][]*metricsDealJoin)
	sealed := make(map[time.Time][]*metricsDealJoin)
	beginning := time.Now().Add(time.Hour * -100000)
	failed := make(map[time.Time][]*metricsDealJoin)

	for _, d := range deals {
		created := d.CreatedAt.Round(time.Hour * 24)
		attempts[created] = append(attempts[created], d)

		if !(d.DealID == 0 || d.Failed) {
			if d.OnChainAt.Before(beginning) {
				d.OnChainAt = time.Time{}
			}

			btime := d.OnChainAt.Round(time.Hour * 24)
			onchainbuckets[btime] = append(onchainbuckets[btime], d)
		}

		if d.SealedAt.After(beginning) {
			sbuck := d.SealedAt.Round(time.Hour * 24)
			sealed[sbuck] = append(sealed[sbuck], d)
		}

		if d.Failed {
			fbuck := d.FailedAt.Round(time.Hour * 24)
			failed[fbuck] = append(failed[fbuck], d)
		}
	}

	for bt, deals := range onchainbuckets {
		dmi := &dealMetricsInfo{
			Time:         bt,
			DealsOnChain: len(deals),
		}
		for _, d := range deals {
			dmi.DealsOnChainBytes += d.Size
		}

		coll[bt] = dmi
	}

	for bt, deals := range attempts {
		dmi, ok := coll[bt]
		if !ok {
			dmi = &dealMetricsInfo{
				Time: bt,
			}
			coll[bt] = dmi
		}

		dmi.DealsAttempted = len(deals)
	}

	for bt, deals := range sealed {
		dmi, ok := coll[bt]
		if !ok {
			dmi = &dealMetricsInfo{
				Time: bt,
			}
			coll[bt] = dmi
		}

		dmi.DealsSealed = len(deals)
		for _, d := range deals {
			dmi.DealsSealedBytes += d.Size
		}
	}

	for bt, deals := range failed {
		dmi, ok := coll[bt]
		if !ok {
			dmi = &dealMetricsInfo{
				Time: bt,
			}
			coll[bt] = dmi
		}

		dmi.DealsFailed = len(deals)
	}

	var out []*dealMetricsInfo
	for _, dmi := range coll {
		out = append(out, dmi)
	}

	sort.Slice(out, func(i, j int) bool {
		return out[i].Time.Before(out[j].Time)
	})

	return c.JSON(200, out)
}

type dealQuery struct {
	DealID    int64
	Contentid uint
	Cid       util.DbCID
	Aggregate bool
}

type dealPairs struct {
	Deals []int64   `json:"deals"`
	Cids  []cid.Cid `json:"cids"`
}

func (s *Server) handleGetAllDealsForUser(c echo.Context, u *User) error {

	begin := time.Now().Add(time.Hour * 24)
	duration := time.Hour * 24

	if beg := c.QueryParam("begin"); beg != "" {
		ts, err := time.Parse("2006-01-02T15:04", beg)
		if err != nil {
			return err
		}
		begin = ts
	}

	if dur := c.QueryParam("duration"); dur != "" {
		dur, err := time.ParseDuration(dur)
		if err != nil {
			return err
		}

		duration = dur
	}

	all := (c.QueryParam("all") != "")

	var deals []dealQuery
	if err := s.DB.Model(contentDeal{}).
		Where("deal_id > 0 AND (? OR (on_chain_at >= ? AND on_chain_at <= ?)) AND user_id = ?", all, begin, begin.Add(duration), u.ID).
		Joins("left join contents on content_deals.content = contents.id").
		Select("deal_id, contents.id as contentid, cid, aggregate").
		Scan(&deals).Error; err != nil {
		return err
	}

	contmap := make(map[uint][]dealQuery)
	for _, d := range deals {
		contmap[d.Contentid] = append(contmap[d.Contentid], d)
	}

	var out []dealPairs
	for cont, deals := range contmap {
		var dp dealPairs
		if deals[0].Aggregate {
			var conts []Content
			if err := s.DB.Model(Content{}).Where("aggregated_in = ?", cont).Select("cid").Scan(&conts).Error; err != nil {
				return err
			}

			for _, c := range conts {
				dp.Cids = append(dp.Cids, c.Cid.CID)
			}
		} else {
			dp.Cids = []cid.Cid{deals[0].Cid.CID}
		}

		for _, d := range deals {
			dp.Deals = append(dp.Deals, d.DealID)
		}
		out = append(out, dp)
	}

	return c.JSON(200, out)
}

type setDealMakingBody struct {
	Enabled bool `json:"enabled"`
}

func (s *Server) handleSetDealMaking(c echo.Context) error {
	var body setDealMakingBody
	if err := c.Bind(&body); err != nil {
		return err
	}

	s.CM.setDealMakingEnabled(body.Enabled)
	return c.JSON(200, map[string]string{})
}

func (s *Server) handleContentHealthCheck(c echo.Context) error {
	ctx := c.Request().Context()
	val, err := strconv.Atoi(c.Param("id"))
	if err != nil {
		return err
	}

	var cont Content
	if err := s.DB.First(&cont, "id = ?", val).Error; err != nil {
		return err
	}

	if cont.Location != "local" {
		return c.JSON(200, map[string]interface{}{
			"error": "requested content was not local to this instance, cannot check health right now",
		})
	}

	var u User
	if err := s.DB.First(&u, "id = ?", cont.UserID).Error; err != nil {
		return err
	}

	var deals []contentDeal
	if err := s.DB.Find(&deals, "content = ?", cont.ID).Error; err != nil {
		return err
	}

	_, rootFetchErr := s.Node.Blockstore.Get(cont.Cid.CID)
	if rootFetchErr != nil {
		log.Errorf("failed to fetch root: %s", rootFetchErr)
	}

	if cont.Aggregate && rootFetchErr != nil {
		// if this is an aggregate and we dont have the root, thats funky, but we can regenerate the root
		var children []Content
		if err := s.DB.Find(&children, "aggregated_in = ?", cont.ID).Error; err != nil {
			return err
		}

		nd, err := s.CM.createAggregate(ctx, children)
		if err != nil {
			return fmt.Errorf("failed to create aggregate: %w", err)
		}

		if nd.Cid() != cont.Cid.CID {
			return fmt.Errorf("recreated aggregate cid does not match one recorded in db: %s != %s", nd.Cid(), cont.Cid.CID)
		}

		if err := s.Node.Blockstore.Put(nd); err != nil {
			return err
		}
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
	}, cont.Cid.CID, cset.Visit, merkledag.Concurrent())

	errstr := ""
	if err != nil {
		errstr = err.Error()
	}

	return c.JSON(200, map[string]interface{}{
		"user":          u.Username,
		"filename":      cont.Name,
		"size":          cont.Size,
		"cid":           cont.Cid.CID,
		"deals":         deals,
		"traverseError": errstr,
		"foundBlocks":   cset.Len(),
	})
}

type initShuttleResponse struct {
	Handle string `json:"handle"`
	Token  string `json:"token"`
}

func (s *Server) handleShuttleInit(c echo.Context) error {
	shuttle := &Shuttle{
		Handle: "SHUTTLE" + uuid.New().String() + "HANDLE",
		Token:  "SECRET" + uuid.New().String() + "SECRET",
		Open:   true,
	}
	if err := s.DB.Create(shuttle).Error; err != nil {
		return err
	}

	return c.JSON(200, &initShuttleResponse{
		Handle: shuttle.Handle,
		Token:  shuttle.Token,
	})
}

type shuttleListResponse struct {
	Handle         string          `json:"handle"`
	Token          string          `json:"token"`
	Online         bool            `json:"online"`
	LastConnection time.Time       `json:"lastConnection"`
	AddrInfo       *peer.AddrInfo  `json:"addrInfo"`
	Address        address.Address `json:"address"`
	Hostname       string          `json:"hostname"`

	StorageStats *shuttleStorageStats `json:"storageStats"`
}

func (s *Server) handleShuttleList(c echo.Context) error {
	var shuttles []Shuttle
	if err := s.DB.Find(&shuttles).Error; err != nil {
		return err
	}

	var out []shuttleListResponse
	for _, d := range shuttles {
		out = append(out, shuttleListResponse{
			Handle:         d.Handle,
			Token:          d.Token,
			LastConnection: d.LastConnection,
			Online:         s.CM.shuttleIsOnline(d.Handle),
			AddrInfo:       s.CM.shuttleAddrInfo(d.Handle),
			Hostname:       s.CM.shuttleHostName(d.Handle),
			StorageStats:   s.CM.shuttleStorageStats(d.Handle),
		})
	}

	return c.JSON(200, out)
}

func (s *Server) handleShuttleConnection(c echo.Context) error {
	auth, err := util.ExtractAuth(c)
	if err != nil {
		return err
	}

	var shuttle Shuttle
	if err := s.DB.First(&shuttle, "token = ?", auth).Error; err != nil {
		return err
	}

	websocket.Handler(func(ws *websocket.Conn) {
		done := make(chan struct{})
		defer close(done)
		defer ws.Close()
		var hello drpc.Hello
		if err := websocket.JSON.Receive(ws, &hello); err != nil {
			log.Errorf("failed to read hello message from client: %s", err)
			return
		}

		cmds, unreg, err := s.CM.registerShuttleConnection(shuttle.Handle, &hello)
		if err != nil {
			log.Errorf("failed to register shuttle: %s", err)
			return
		}
		defer unreg()

		go func() {
			for {
				select {
				case cmd := <-cmds:
					// Write
					err := websocket.JSON.Send(ws, cmd)
					if err != nil {
						log.Errorf("failed to write command to shuttle: %s", err)
						return
					}
				case <-done:
					return
				}
			}
		}()

		for {
			var msg drpc.Message
			if err := websocket.JSON.Receive(ws, &msg); err != nil {
				log.Errorf("failed to read message from shuttle: %s", err)
				return
			}

			go func(msg *drpc.Message) {
				if err := s.CM.processShuttleMessage(shuttle.Handle, msg); err != nil {
					log.Errorf("failed to process message from shuttle: %s", err)
					return
				}
			}(&msg)
		}
	}).ServeHTTP(c.Response(), c.Request())
	return nil
}

type allDealsQuery struct {
	Miner  string
	Cid    util.DbCID
	DealID int64
}

func (s *Server) handleDebugGetAllDeals(c echo.Context) error {
	var out []allDealsQuery
	if err := s.DB.Model(contentDeal{}).Where("deal_id > 0 and not failed").
		Joins("left join contents on content_deals.content = contents.id").
		Select("miner, contents.cid as cid, deal_id").
		Scan(&out).
		Error; err != nil {
		return err
	}
	return c.JSON(200, out)
}

type logLevelBody struct {
	System string `json:"system"`
	Level  string `json:"level"`
}

func (s *Server) handleLogLevel(c echo.Context) error {
	var body logLevelBody
	if err := c.Bind(&body); err != nil {
		return err
	}

	logging.SetLogLevel(body.System, body.Level)

	return c.JSON(200, map[string]interface{}{})
}

func (s *Server) handleStorageFailures(c echo.Context) error {
	limit := 2000
	if limstr := c.QueryParam("limit"); limstr != "" {
		nlim, err := strconv.Atoi(limstr)
		if err != nil {
			return err
		}
		limit = nlim
	}

	q := s.DB.Limit(limit).Order("created_at desc")

	if bef := c.QueryParam("before"); bef != "" {
		beftime, err := time.Parse(time.RFC3339, bef)
		if err != nil {
			return err
		}

		q = q.Where("created_at <= ?", beftime)
	}

	var recs []dfeRecord
	if err := q.Scan(&recs).Error; err != nil {
		return err
	}

	return c.JSON(200, recs)
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

func (s *Server) handleCreateContent(c echo.Context, u *User) error {
	var req createContentBody
	if err := c.Bind(&req); err != nil {
		return err
	}

	content := &Content{
		Cid:         util.DbCID{req.Root},
		Name:        req.Name,
		Active:      false,
		Pinning:     false,
		UserID:      u.ID,
		Replication: defaultReplication,
		Location:    "local",
	}
	if err := s.DB.Create(content).Error; err != nil {
		return err
	}

	return c.JSON(200, createContentResponse{
		ID: content.ID,
	})
}

type claimMinerBody struct {
	Miner address.Address `json:"miner"`
	Claim string          `json:"claim"`
}

func (s *Server) handleUserClaimMiner(c echo.Context, u *User) error {
	ctx := c.Request().Context()

	var cmb claimMinerBody
	if err := c.Bind(&cmb); err != nil {
		return err
	}

	var sm storageMiner
	if err := s.DB.First(&sm, "address = ?", cmb.Miner.String()).Error; err != nil {
		return err
	}

	minfo, err := s.Api.StateMinerInfo(ctx, cmb.Miner, types.EmptyTSK)
	if err != nil {
		return err
	}

	acckey, err := s.Api.StateAccountKey(ctx, minfo.Worker, types.EmptyTSK)
	if err != nil {
		return err
	}

	sigb, err := hex.DecodeString(cmb.Claim)
	if err != nil {
		return err
	}

	if len(sigb) < 2 {
		return &util.HttpError{
			Code:    400,
			Message: util.ERR_INVALID_INPUT,
		}
	}

	sig := &crypto.Signature{
		Type: crypto.SigType(sigb[0]),
		Data: sigb[1:],
	}

	msg := s.msgForMinerClaim(cmb.Miner, u.ID)

	if err := sigs.Verify(sig, acckey, msg); err != nil {
		return err
	}

	if err := s.DB.Model(storageMiner{}).Where("id = ?", sm.ID).UpdateColumn("owner", u.ID).Error; err != nil {
		return err
	}

	return c.JSON(200, map[string]interface{}{
		"success": true,
	})
}

func (s *Server) handleUserGetClaimMinerMsg(c echo.Context, u *User) error {
	m, err := address.NewFromString(c.Param("miner"))
	if err != nil {
		return err
	}

	return c.JSON(200, map[string]string{
		"hexmsg": hex.EncodeToString(s.msgForMinerClaim(m, u.ID)),
	})
}

func (s *Server) msgForMinerClaim(miner address.Address, uid uint) []byte {
	return []byte(fmt.Sprintf("---- user %d owns miner %s ----", uid, miner))
}

type progressResponse struct {
	GoodContents []uint
	InProgress   []uint
	NoDeals      []uint

	TotalTopLevel int64
	TotalPinning  int64
}

type contCheck struct {
	ID       uint
	NumDeals int
}

func (s *Server) handleAdminGetProgress(c echo.Context) error {
	var out progressResponse
	if err := s.DB.Model(Content{}).Where("not aggregated_in > 0 AND (pinning OR active) AND not failed").Count(&out.TotalTopLevel).Error; err != nil {
		return err
	}

	if err := s.DB.Model(Content{}).Where("pinning and not failed").Count(&out.TotalPinning).Error; err != nil {
		return err
	}

	var conts []contCheck
	if err := s.DB.Model(Content{}).Where("not aggregated_in > 0 and active").
		Select("id, (?) as num_deals",
			s.DB.Model(contentDeal{}).
				Where("content = contents.id and deal_id > 0 and not failed").
				Select("count(1)"),
		).Scan(&conts).Error; err != nil {
		return err
	}

	for _, c := range conts {
		if c.NumDeals >= defaultReplication {
			out.GoodContents = append(out.GoodContents, c.ID)
		} else if c.NumDeals > 0 {
			out.InProgress = append(out.InProgress, c.ID)
		} else {
			out.NoDeals = append(out.NoDeals, c.ID)
		}
	}

	return c.JSON(200, out)
}
