package main

import (
	"context"
	"fmt"
	"github.com/application-research/estuary/node"
	"github.com/application-research/estuary/pinner"
	"github.com/application-research/estuary/stagingbs"
	"github.com/application-research/estuary/util"
	"github.com/application-research/estuary/util/gateway"
	"github.com/application-research/filclient"
	"github.com/filecoin-project/go-address"
	datatransfer "github.com/filecoin-project/go-data-transfer"
	"github.com/filecoin-project/lotus/api"
	lru "github.com/hashicorp/golang-lru"
	"github.com/ipfs/go-cid"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	"github.com/ipfs/go-ipfs-provider/batched"
	format "github.com/ipfs/go-ipld-format"
	"github.com/ipld/go-car"
	"github.com/labstack/echo/v4"
	"github.com/libp2p/go-libp2p-core/host"
	"github.com/stretchr/testify/assert"
	"github.com/whyrusleeping/memo"
	"go.opentelemetry.io/otel/trace"
	"gorm.io/gorm"
	"io"
	"sync"
	"testing"
	"time"
)

func TestContentManager_addDatabaseTracking(t *testing.T) {
	type fields struct {
		DB                         *gorm.DB
		Api                        api.Gateway
		FilClient                  *filclient.FilClient
		Provider                   *batched.BatchProvidingSystem
		Node                       *node.Node
		Host                       host.Host
		tracer                     trace.Tracer
		Blockstore                 node.EstuaryBlockstore
		Tracker                    *TrackingBlockstore
		NotifyBlockstore           *node.NotifyBlockstore
		ToCheck                    chan uint
		queueMgr                   *queueManager
		retrLk                     sync.Mutex
		retrievalsInProgress       map[uint]*util.RetrievalProgress
		contentLk                  sync.RWMutex
		contentSizeLimit           int64
		minerLk                    sync.Mutex
		sortedMiners               []address.Address
		rawData                    []*minerDealStats
		lastComputed               time.Time
		bucketLk                   sync.Mutex
		buckets                    map[uint][]*contentStagingZone
		FailDealOnTransferFailure  bool
		dealDisabledLk             sync.Mutex
		isDealMakingDisabled       bool
		contentAddingDisabled      bool
		localContentAddingDisabled bool
		Replication                int
		hostname                   string
		pinJobs                    map[uint]*pinner.PinningOperation
		pinLk                      sync.Mutex
		pinMgr                     *pinner.PinManager
		shuttlesLk                 sync.Mutex
		shuttles                   map[string]*ShuttleConnection
		remoteTransferStatus       *lru.ARCCache
		inflightCids               map[cid.Cid]uint
		inflightCidsLk             sync.Mutex
		VerifiedDeal               bool
	}
	type args struct {
		ctx         context.Context
		u           *User
		dserv       format.NodeGetter
		bs          blockstore.Blockstore
		root        cid.Cid
		fname       string
		replication int
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    *Content
		wantErr assert.ErrorAssertionFunc
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cm := &ContentManager{
				DB:                         tt.fields.DB,
				Api:                        tt.fields.Api,
				FilClient:                  tt.fields.FilClient,
				Provider:                   tt.fields.Provider,
				Node:                       tt.fields.Node,
				Host:                       tt.fields.Host,
				tracer:                     tt.fields.tracer,
				Blockstore:                 tt.fields.Blockstore,
				Tracker:                    tt.fields.Tracker,
				NotifyBlockstore:           tt.fields.NotifyBlockstore,
				ToCheck:                    tt.fields.ToCheck,
				queueMgr:                   tt.fields.queueMgr,
				retrLk:                     tt.fields.retrLk,
				retrievalsInProgress:       tt.fields.retrievalsInProgress,
				contentLk:                  tt.fields.contentLk,
				contentSizeLimit:           tt.fields.contentSizeLimit,
				minerLk:                    tt.fields.minerLk,
				sortedMiners:               tt.fields.sortedMiners,
				rawData:                    tt.fields.rawData,
				lastComputed:               tt.fields.lastComputed,
				bucketLk:                   tt.fields.bucketLk,
				buckets:                    tt.fields.buckets,
				FailDealOnTransferFailure:  tt.fields.FailDealOnTransferFailure,
				dealDisabledLk:             tt.fields.dealDisabledLk,
				isDealMakingDisabled:       tt.fields.isDealMakingDisabled,
				contentAddingDisabled:      tt.fields.contentAddingDisabled,
				localContentAddingDisabled: tt.fields.localContentAddingDisabled,
				Replication:                tt.fields.Replication,
				hostname:                   tt.fields.hostname,
				pinJobs:                    tt.fields.pinJobs,
				pinLk:                      tt.fields.pinLk,
				pinMgr:                     tt.fields.pinMgr,
				shuttlesLk:                 tt.fields.shuttlesLk,
				shuttles:                   tt.fields.shuttles,
				remoteTransferStatus:       tt.fields.remoteTransferStatus,
				inflightCids:               tt.fields.inflightCids,
				inflightCidsLk:             tt.fields.inflightCidsLk,
				VerifiedDeal:               tt.fields.VerifiedDeal,
			}
			got, err := cm.addDatabaseTracking(tt.args.ctx, tt.args.u, tt.args.dserv, tt.args.bs, tt.args.root, tt.args.fname, tt.args.replication)
			if !tt.wantErr(t, err, fmt.Sprintf("addDatabaseTracking(%v, %v, %v, %v, %v, %v, %v)", tt.args.ctx, tt.args.u, tt.args.dserv, tt.args.bs, tt.args.root, tt.args.fname, tt.args.replication)) {
				return
			}
			assert.Equalf(t, tt.want, got, "addDatabaseTracking(%v, %v, %v, %v, %v, %v, %v)", tt.args.ctx, tt.args.u, tt.args.dserv, tt.args.bs, tt.args.root, tt.args.fname, tt.args.replication)
		})
	}
}

func TestContentManager_addDatabaseTrackingToContent(t *testing.T) {
	type fields struct {
		DB                         *gorm.DB
		Api                        api.Gateway
		FilClient                  *filclient.FilClient
		Provider                   *batched.BatchProvidingSystem
		Node                       *node.Node
		Host                       host.Host
		tracer                     trace.Tracer
		Blockstore                 node.EstuaryBlockstore
		Tracker                    *TrackingBlockstore
		NotifyBlockstore           *node.NotifyBlockstore
		ToCheck                    chan uint
		queueMgr                   *queueManager
		retrLk                     sync.Mutex
		retrievalsInProgress       map[uint]*util.RetrievalProgress
		contentLk                  sync.RWMutex
		contentSizeLimit           int64
		minerLk                    sync.Mutex
		sortedMiners               []address.Address
		rawData                    []*minerDealStats
		lastComputed               time.Time
		bucketLk                   sync.Mutex
		buckets                    map[uint][]*contentStagingZone
		FailDealOnTransferFailure  bool
		dealDisabledLk             sync.Mutex
		isDealMakingDisabled       bool
		contentAddingDisabled      bool
		localContentAddingDisabled bool
		Replication                int
		hostname                   string
		pinJobs                    map[uint]*pinner.PinningOperation
		pinLk                      sync.Mutex
		pinMgr                     *pinner.PinManager
		shuttlesLk                 sync.Mutex
		shuttles                   map[string]*ShuttleConnection
		remoteTransferStatus       *lru.ARCCache
		inflightCids               map[cid.Cid]uint
		inflightCidsLk             sync.Mutex
		VerifiedDeal               bool
	}
	type args struct {
		ctx   context.Context
		cont  uint
		dserv format.NodeGetter
		bs    blockstore.Blockstore
		root  cid.Cid
		cb    func(int64)
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr assert.ErrorAssertionFunc
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cm := &ContentManager{
				DB:                         tt.fields.DB,
				Api:                        tt.fields.Api,
				FilClient:                  tt.fields.FilClient,
				Provider:                   tt.fields.Provider,
				Node:                       tt.fields.Node,
				Host:                       tt.fields.Host,
				tracer:                     tt.fields.tracer,
				Blockstore:                 tt.fields.Blockstore,
				Tracker:                    tt.fields.Tracker,
				NotifyBlockstore:           tt.fields.NotifyBlockstore,
				ToCheck:                    tt.fields.ToCheck,
				queueMgr:                   tt.fields.queueMgr,
				retrLk:                     tt.fields.retrLk,
				retrievalsInProgress:       tt.fields.retrievalsInProgress,
				contentLk:                  tt.fields.contentLk,
				contentSizeLimit:           tt.fields.contentSizeLimit,
				minerLk:                    tt.fields.minerLk,
				sortedMiners:               tt.fields.sortedMiners,
				rawData:                    tt.fields.rawData,
				lastComputed:               tt.fields.lastComputed,
				bucketLk:                   tt.fields.bucketLk,
				buckets:                    tt.fields.buckets,
				FailDealOnTransferFailure:  tt.fields.FailDealOnTransferFailure,
				dealDisabledLk:             tt.fields.dealDisabledLk,
				isDealMakingDisabled:       tt.fields.isDealMakingDisabled,
				contentAddingDisabled:      tt.fields.contentAddingDisabled,
				localContentAddingDisabled: tt.fields.localContentAddingDisabled,
				Replication:                tt.fields.Replication,
				hostname:                   tt.fields.hostname,
				pinJobs:                    tt.fields.pinJobs,
				pinLk:                      tt.fields.pinLk,
				pinMgr:                     tt.fields.pinMgr,
				shuttlesLk:                 tt.fields.shuttlesLk,
				shuttles:                   tt.fields.shuttles,
				remoteTransferStatus:       tt.fields.remoteTransferStatus,
				inflightCids:               tt.fields.inflightCids,
				inflightCidsLk:             tt.fields.inflightCidsLk,
				VerifiedDeal:               tt.fields.VerifiedDeal,
			}
			tt.wantErr(t, cm.addDatabaseTrackingToContent(tt.args.ctx, tt.args.cont, tt.args.dserv, tt.args.bs, tt.args.root, tt.args.cb), fmt.Sprintf("addDatabaseTrackingToContent(%v, %v, %v, %v, %v, %v)", tt.args.ctx, tt.args.cont, tt.args.dserv, tt.args.bs, tt.args.root, tt.args.cb))
		})
	}
}

func TestServer_AuthRequired(t *testing.T) {
	type fields struct {
		tracer      trace.Tracer
		Node        *node.Node
		DB          *gorm.DB
		FilClient   *filclient.FilClient
		Api         api.Gateway
		CM          *ContentManager
		StagingMgr  *stagingbs.StagingBSMgr
		gwayHandler *gateway.GatewayHandler
		cacher      *memo.Cacher
	}
	type args struct {
		level int
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   echo.MiddlewareFunc
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Server{
				tracer:      tt.fields.tracer,
				Node:        tt.fields.Node,
				DB:          tt.fields.DB,
				FilClient:   tt.fields.FilClient,
				Api:         tt.fields.Api,
				CM:          tt.fields.CM,
				StagingMgr:  tt.fields.StagingMgr,
				gwayHandler: tt.fields.gwayHandler,
				cacher:      tt.fields.cacher,
			}
			assert.Equalf(t, tt.want, s.AuthRequired(tt.args.level), "AuthRequired(%v)", tt.args.level)
		})
	}
}

func TestServer_ServeAPI(t *testing.T) {
	type fields struct {
		tracer      trace.Tracer
		Node        *node.Node
		DB          *gorm.DB
		FilClient   *filclient.FilClient
		Api         api.Gateway
		CM          *ContentManager
		StagingMgr  *stagingbs.StagingBSMgr
		gwayHandler *gateway.GatewayHandler
		cacher      *memo.Cacher
	}
	type args struct {
		srv      string
		logging  bool
		lsteptok string
		cachedir string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr assert.ErrorAssertionFunc
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Server{
				tracer:      tt.fields.tracer,
				Node:        tt.fields.Node,
				DB:          tt.fields.DB,
				FilClient:   tt.fields.FilClient,
				Api:         tt.fields.Api,
				CM:          tt.fields.CM,
				StagingMgr:  tt.fields.StagingMgr,
				gwayHandler: tt.fields.gwayHandler,
				cacher:      tt.fields.cacher,
			}
			tt.wantErr(t, s.ServeAPI(tt.args.srv, tt.args.logging, tt.args.lsteptok, tt.args.cachedir), fmt.Sprintf("ServeAPI(%v, %v, %v, %v)", tt.args.srv, tt.args.logging, tt.args.lsteptok, tt.args.cachedir))
		})
	}
}

func TestServer_calcSelector(t *testing.T) {
	type fields struct {
		tracer      trace.Tracer
		Node        *node.Node
		DB          *gorm.DB
		FilClient   *filclient.FilClient
		Api         api.Gateway
		CM          *ContentManager
		StagingMgr  *stagingbs.StagingBSMgr
		gwayHandler *gateway.GatewayHandler
		cacher      *memo.Cacher
	}
	type args struct {
		aggregatedIn uint
		contentID    uint
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    string
		wantErr assert.ErrorAssertionFunc
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Server{
				tracer:      tt.fields.tracer,
				Node:        tt.fields.Node,
				DB:          tt.fields.DB,
				FilClient:   tt.fields.FilClient,
				Api:         tt.fields.Api,
				CM:          tt.fields.CM,
				StagingMgr:  tt.fields.StagingMgr,
				gwayHandler: tt.fields.gwayHandler,
				cacher:      tt.fields.cacher,
			}
			got, err := s.calcSelector(tt.args.aggregatedIn, tt.args.contentID)
			if !tt.wantErr(t, err, fmt.Sprintf("calcSelector(%v, %v)", tt.args.aggregatedIn, tt.args.contentID)) {
				return
			}
			assert.Equalf(t, tt.want, got, "calcSelector(%v, %v)", tt.args.aggregatedIn, tt.args.contentID)
		})
	}
}

func TestServer_checkGatewayRedirect(t *testing.T) {
	type fields struct {
		tracer      trace.Tracer
		Node        *node.Node
		DB          *gorm.DB
		FilClient   *filclient.FilClient
		Api         api.Gateway
		CM          *ContentManager
		StagingMgr  *stagingbs.StagingBSMgr
		gwayHandler *gateway.GatewayHandler
		cacher      *memo.Cacher
	}
	type args struct {
		proto string
		cc    cid.Cid
		segs  []string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    string
		wantErr assert.ErrorAssertionFunc
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Server{
				tracer:      tt.fields.tracer,
				Node:        tt.fields.Node,
				DB:          tt.fields.DB,
				FilClient:   tt.fields.FilClient,
				Api:         tt.fields.Api,
				CM:          tt.fields.CM,
				StagingMgr:  tt.fields.StagingMgr,
				gwayHandler: tt.fields.gwayHandler,
				cacher:      tt.fields.cacher,
			}
			got, err := s.checkGatewayRedirect(tt.args.proto, tt.args.cc, tt.args.segs)
			if !tt.wantErr(t, err, fmt.Sprintf("checkGatewayRedirect(%v, %v, %v)", tt.args.proto, tt.args.cc, tt.args.segs)) {
				return
			}
			assert.Equalf(t, tt.want, got, "checkGatewayRedirect(%v, %v, %v)", tt.args.proto, tt.args.cc, tt.args.segs)
		})
	}
}

func TestServer_checkNewMiner(t *testing.T) {
	type fields struct {
		tracer      trace.Tracer
		Node        *node.Node
		DB          *gorm.DB
		FilClient   *filclient.FilClient
		Api         api.Gateway
		CM          *ContentManager
		StagingMgr  *stagingbs.StagingBSMgr
		gwayHandler *gateway.GatewayHandler
		cacher      *memo.Cacher
	}
	type args struct {
		ctx  context.Context
		addr address.Address
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr assert.ErrorAssertionFunc
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Server{
				tracer:      tt.fields.tracer,
				Node:        tt.fields.Node,
				DB:          tt.fields.DB,
				FilClient:   tt.fields.FilClient,
				Api:         tt.fields.Api,
				CM:          tt.fields.CM,
				StagingMgr:  tt.fields.StagingMgr,
				gwayHandler: tt.fields.gwayHandler,
				cacher:      tt.fields.cacher,
			}
			tt.wantErr(t, s.checkNewMiner(tt.args.ctx, tt.args.addr), fmt.Sprintf("checkNewMiner(%v, %v)", tt.args.ctx, tt.args.addr))
		})
	}
}

func TestServer_checkTokenAuth(t *testing.T) {
	type fields struct {
		tracer      trace.Tracer
		Node        *node.Node
		DB          *gorm.DB
		FilClient   *filclient.FilClient
		Api         api.Gateway
		CM          *ContentManager
		StagingMgr  *stagingbs.StagingBSMgr
		gwayHandler *gateway.GatewayHandler
		cacher      *memo.Cacher
	}
	type args struct {
		token string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    *User
		wantErr assert.ErrorAssertionFunc
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Server{
				tracer:      tt.fields.tracer,
				Node:        tt.fields.Node,
				DB:          tt.fields.DB,
				FilClient:   tt.fields.FilClient,
				Api:         tt.fields.Api,
				CM:          tt.fields.CM,
				StagingMgr:  tt.fields.StagingMgr,
				gwayHandler: tt.fields.gwayHandler,
				cacher:      tt.fields.cacher,
			}
			got, err := s.checkTokenAuth(tt.args.token)
			if !tt.wantErr(t, err, fmt.Sprintf("checkTokenAuth(%v)", tt.args.token)) {
				return
			}
			assert.Equalf(t, tt.want, got, "checkTokenAuth(%v)", tt.args.token)
		})
	}
}

func TestServer_computeDealMetrics(t *testing.T) {
	type fields struct {
		tracer      trace.Tracer
		Node        *node.Node
		DB          *gorm.DB
		FilClient   *filclient.FilClient
		Api         api.Gateway
		CM          *ContentManager
		StagingMgr  *stagingbs.StagingBSMgr
		gwayHandler *gateway.GatewayHandler
		cacher      *memo.Cacher
	}
	tests := []struct {
		name    string
		fields  fields
		want    []*dealMetricsInfo
		wantErr assert.ErrorAssertionFunc
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Server{
				tracer:      tt.fields.tracer,
				Node:        tt.fields.Node,
				DB:          tt.fields.DB,
				FilClient:   tt.fields.FilClient,
				Api:         tt.fields.Api,
				CM:          tt.fields.CM,
				StagingMgr:  tt.fields.StagingMgr,
				gwayHandler: tt.fields.gwayHandler,
				cacher:      tt.fields.cacher,
			}
			got, err := s.computeDealMetrics()
			if !tt.wantErr(t, err, fmt.Sprintf("computeDealMetrics()")) {
				return
			}
			assert.Equalf(t, tt.want, got, "computeDealMetrics()")
		})
	}
}

func TestServer_computePublicStats(t *testing.T) {
	type fields struct {
		tracer      trace.Tracer
		Node        *node.Node
		DB          *gorm.DB
		FilClient   *filclient.FilClient
		Api         api.Gateway
		CM          *ContentManager
		StagingMgr  *stagingbs.StagingBSMgr
		gwayHandler *gateway.GatewayHandler
		cacher      *memo.Cacher
	}
	tests := []struct {
		name    string
		fields  fields
		want    *publicStatsResponse
		wantErr assert.ErrorAssertionFunc
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Server{
				tracer:      tt.fields.tracer,
				Node:        tt.fields.Node,
				DB:          tt.fields.DB,
				FilClient:   tt.fields.FilClient,
				Api:         tt.fields.Api,
				CM:          tt.fields.CM,
				StagingMgr:  tt.fields.StagingMgr,
				gwayHandler: tt.fields.gwayHandler,
				cacher:      tt.fields.cacher,
			}
			got, err := s.computePublicStats()
			if !tt.wantErr(t, err, fmt.Sprintf("computePublicStats()")) {
				return
			}
			assert.Equalf(t, tt.want, got, "computePublicStats()")
		})
	}
}

func TestServer_dealStatusByID(t *testing.T) {
	type fields struct {
		tracer      trace.Tracer
		Node        *node.Node
		DB          *gorm.DB
		FilClient   *filclient.FilClient
		Api         api.Gateway
		CM          *ContentManager
		StagingMgr  *stagingbs.StagingBSMgr
		gwayHandler *gateway.GatewayHandler
		cacher      *memo.Cacher
	}
	type args struct {
		ctx    context.Context
		dealid uint
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    *dealStatus
		wantErr assert.ErrorAssertionFunc
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Server{
				tracer:      tt.fields.tracer,
				Node:        tt.fields.Node,
				DB:          tt.fields.DB,
				FilClient:   tt.fields.FilClient,
				Api:         tt.fields.Api,
				CM:          tt.fields.CM,
				StagingMgr:  tt.fields.StagingMgr,
				gwayHandler: tt.fields.gwayHandler,
				cacher:      tt.fields.cacher,
			}
			got, err := s.dealStatusByID(tt.args.ctx, tt.args.dealid)
			if !tt.wantErr(t, err, fmt.Sprintf("dealStatusByID(%v, %v)", tt.args.ctx, tt.args.dealid)) {
				return
			}
			assert.Equalf(t, tt.want, got, "dealStatusByID(%v, %v)", tt.args.ctx, tt.args.dealid)
		})
	}
}

func TestServer_dumpBlockstoreTo(t *testing.T) {
	type fields struct {
		tracer      trace.Tracer
		Node        *node.Node
		DB          *gorm.DB
		FilClient   *filclient.FilClient
		Api         api.Gateway
		CM          *ContentManager
		StagingMgr  *stagingbs.StagingBSMgr
		gwayHandler *gateway.GatewayHandler
		cacher      *memo.Cacher
	}
	type args struct {
		ctx  context.Context
		from blockstore.Blockstore
		to   blockstore.Blockstore
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr assert.ErrorAssertionFunc
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Server{
				tracer:      tt.fields.tracer,
				Node:        tt.fields.Node,
				DB:          tt.fields.DB,
				FilClient:   tt.fields.FilClient,
				Api:         tt.fields.Api,
				CM:          tt.fields.CM,
				StagingMgr:  tt.fields.StagingMgr,
				gwayHandler: tt.fields.gwayHandler,
				cacher:      tt.fields.cacher,
			}
			tt.wantErr(t, s.dumpBlockstoreTo(tt.args.ctx, tt.args.from, tt.args.to), fmt.Sprintf("dumpBlockstoreTo(%v, %v, %v)", tt.args.ctx, tt.args.from, tt.args.to))
		})
	}
}

func TestServer_getMinersOwnedByUser(t *testing.T) {
	type fields struct {
		tracer      trace.Tracer
		Node        *node.Node
		DB          *gorm.DB
		FilClient   *filclient.FilClient
		Api         api.Gateway
		CM          *ContentManager
		StagingMgr  *stagingbs.StagingBSMgr
		gwayHandler *gateway.GatewayHandler
		cacher      *memo.Cacher
	}
	type args struct {
		u *User
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   []string
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Server{
				tracer:      tt.fields.tracer,
				Node:        tt.fields.Node,
				DB:          tt.fields.DB,
				FilClient:   tt.fields.FilClient,
				Api:         tt.fields.Api,
				CM:          tt.fields.CM,
				StagingMgr:  tt.fields.StagingMgr,
				gwayHandler: tt.fields.gwayHandler,
				cacher:      tt.fields.cacher,
			}
			assert.Equalf(t, tt.want, s.getMinersOwnedByUser(tt.args.u), "getMinersOwnedByUser(%v)", tt.args.u)
		})
	}
}

func TestServer_getPreferredUploadEndpoints(t *testing.T) {
	type fields struct {
		tracer      trace.Tracer
		Node        *node.Node
		DB          *gorm.DB
		FilClient   *filclient.FilClient
		Api         api.Gateway
		CM          *ContentManager
		StagingMgr  *stagingbs.StagingBSMgr
		gwayHandler *gateway.GatewayHandler
		cacher      *memo.Cacher
	}
	type args struct {
		u *User
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    []string
		wantErr assert.ErrorAssertionFunc
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Server{
				tracer:      tt.fields.tracer,
				Node:        tt.fields.Node,
				DB:          tt.fields.DB,
				FilClient:   tt.fields.FilClient,
				Api:         tt.fields.Api,
				CM:          tt.fields.CM,
				StagingMgr:  tt.fields.StagingMgr,
				gwayHandler: tt.fields.gwayHandler,
				cacher:      tt.fields.cacher,
			}
			got, err := s.getPreferredUploadEndpoints(tt.args.u)
			if !tt.wantErr(t, err, fmt.Sprintf("getPreferredUploadEndpoints(%v)", tt.args.u)) {
				return
			}
			assert.Equalf(t, tt.want, got, "getPreferredUploadEndpoints(%v)", tt.args.u)
		})
	}
}

func TestServer_handleAdd(t *testing.T) {
	type fields struct {
		tracer      trace.Tracer
		Node        *node.Node
		DB          *gorm.DB
		FilClient   *filclient.FilClient
		Api         api.Gateway
		CM          *ContentManager
		StagingMgr  *stagingbs.StagingBSMgr
		gwayHandler *gateway.GatewayHandler
		cacher      *memo.Cacher
	}
	type args struct {
		c echo.Context
		u *User
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr assert.ErrorAssertionFunc
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Server{
				tracer:      tt.fields.tracer,
				Node:        tt.fields.Node,
				DB:          tt.fields.DB,
				FilClient:   tt.fields.FilClient,
				Api:         tt.fields.Api,
				CM:          tt.fields.CM,
				StagingMgr:  tt.fields.StagingMgr,
				gwayHandler: tt.fields.gwayHandler,
				cacher:      tt.fields.cacher,
			}
			tt.wantErr(t, s.handleAdd(tt.args.c, tt.args.u), fmt.Sprintf("handleAdd(%v, %v)", tt.args.c, tt.args.u))
		})
	}
}

func TestServer_handleAddCar(t *testing.T) {
	type fields struct {
		tracer      trace.Tracer
		Node        *node.Node
		DB          *gorm.DB
		FilClient   *filclient.FilClient
		Api         api.Gateway
		CM          *ContentManager
		StagingMgr  *stagingbs.StagingBSMgr
		gwayHandler *gateway.GatewayHandler
		cacher      *memo.Cacher
	}
	type args struct {
		c echo.Context
		u *User
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr assert.ErrorAssertionFunc
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Server{
				tracer:      tt.fields.tracer,
				Node:        tt.fields.Node,
				DB:          tt.fields.DB,
				FilClient:   tt.fields.FilClient,
				Api:         tt.fields.Api,
				CM:          tt.fields.CM,
				StagingMgr:  tt.fields.StagingMgr,
				gwayHandler: tt.fields.gwayHandler,
				cacher:      tt.fields.cacher,
			}
			tt.wantErr(t, s.handleAddCar(tt.args.c, tt.args.u), fmt.Sprintf("handleAddCar(%v, %v)", tt.args.c, tt.args.u))
		})
	}
}

func TestServer_handleAddContentsToCollection(t *testing.T) {
	type fields struct {
		tracer      trace.Tracer
		Node        *node.Node
		DB          *gorm.DB
		FilClient   *filclient.FilClient
		Api         api.Gateway
		CM          *ContentManager
		StagingMgr  *stagingbs.StagingBSMgr
		gwayHandler *gateway.GatewayHandler
		cacher      *memo.Cacher
	}
	type args struct {
		c echo.Context
		u *User
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr assert.ErrorAssertionFunc
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Server{
				tracer:      tt.fields.tracer,
				Node:        tt.fields.Node,
				DB:          tt.fields.DB,
				FilClient:   tt.fields.FilClient,
				Api:         tt.fields.Api,
				CM:          tt.fields.CM,
				StagingMgr:  tt.fields.StagingMgr,
				gwayHandler: tt.fields.gwayHandler,
				cacher:      tt.fields.cacher,
			}
			tt.wantErr(t, s.handleAddContentsToCollection(tt.args.c, tt.args.u), fmt.Sprintf("handleAddContentsToCollection(%v, %v)", tt.args.c, tt.args.u))
		})
	}
}

func TestServer_handleAddIpfs(t *testing.T) {
	type fields struct {
		tracer      trace.Tracer
		Node        *node.Node
		DB          *gorm.DB
		FilClient   *filclient.FilClient
		Api         api.Gateway
		CM          *ContentManager
		StagingMgr  *stagingbs.StagingBSMgr
		gwayHandler *gateway.GatewayHandler
		cacher      *memo.Cacher
	}
	type args struct {
		c echo.Context
		u *User
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr assert.ErrorAssertionFunc
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Server{
				tracer:      tt.fields.tracer,
				Node:        tt.fields.Node,
				DB:          tt.fields.DB,
				FilClient:   tt.fields.FilClient,
				Api:         tt.fields.Api,
				CM:          tt.fields.CM,
				StagingMgr:  tt.fields.StagingMgr,
				gwayHandler: tt.fields.gwayHandler,
				cacher:      tt.fields.cacher,
			}
			tt.wantErr(t, s.handleAddIpfs(tt.args.c, tt.args.u), fmt.Sprintf("handleAddIpfs(%v, %v)", tt.args.c, tt.args.u))
		})
	}
}

func TestServer_handleAdminAddEscrow(t *testing.T) {
	type fields struct {
		tracer      trace.Tracer
		Node        *node.Node
		DB          *gorm.DB
		FilClient   *filclient.FilClient
		Api         api.Gateway
		CM          *ContentManager
		StagingMgr  *stagingbs.StagingBSMgr
		gwayHandler *gateway.GatewayHandler
		cacher      *memo.Cacher
	}
	type args struct {
		c echo.Context
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr assert.ErrorAssertionFunc
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Server{
				tracer:      tt.fields.tracer,
				Node:        tt.fields.Node,
				DB:          tt.fields.DB,
				FilClient:   tt.fields.FilClient,
				Api:         tt.fields.Api,
				CM:          tt.fields.CM,
				StagingMgr:  tt.fields.StagingMgr,
				gwayHandler: tt.fields.gwayHandler,
				cacher:      tt.fields.cacher,
			}
			tt.wantErr(t, s.handleAdminAddEscrow(tt.args.c), fmt.Sprintf("handleAdminAddEscrow(%v)", tt.args.c))
		})
	}
}

func TestServer_handleAdminAddMiner(t *testing.T) {
	type fields struct {
		tracer      trace.Tracer
		Node        *node.Node
		DB          *gorm.DB
		FilClient   *filclient.FilClient
		Api         api.Gateway
		CM          *ContentManager
		StagingMgr  *stagingbs.StagingBSMgr
		gwayHandler *gateway.GatewayHandler
		cacher      *memo.Cacher
	}
	type args struct {
		c echo.Context
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr assert.ErrorAssertionFunc
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Server{
				tracer:      tt.fields.tracer,
				Node:        tt.fields.Node,
				DB:          tt.fields.DB,
				FilClient:   tt.fields.FilClient,
				Api:         tt.fields.Api,
				CM:          tt.fields.CM,
				StagingMgr:  tt.fields.StagingMgr,
				gwayHandler: tt.fields.gwayHandler,
				cacher:      tt.fields.cacher,
			}
			tt.wantErr(t, s.handleAdminAddMiner(tt.args.c), fmt.Sprintf("handleAdminAddMiner(%v)", tt.args.c))
		})
	}
}

func TestServer_handleAdminBalance(t *testing.T) {
	type fields struct {
		tracer      trace.Tracer
		Node        *node.Node
		DB          *gorm.DB
		FilClient   *filclient.FilClient
		Api         api.Gateway
		CM          *ContentManager
		StagingMgr  *stagingbs.StagingBSMgr
		gwayHandler *gateway.GatewayHandler
		cacher      *memo.Cacher
	}
	type args struct {
		c echo.Context
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr assert.ErrorAssertionFunc
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Server{
				tracer:      tt.fields.tracer,
				Node:        tt.fields.Node,
				DB:          tt.fields.DB,
				FilClient:   tt.fields.FilClient,
				Api:         tt.fields.Api,
				CM:          tt.fields.CM,
				StagingMgr:  tt.fields.StagingMgr,
				gwayHandler: tt.fields.gwayHandler,
				cacher:      tt.fields.cacher,
			}
			tt.wantErr(t, s.handleAdminBalance(tt.args.c), fmt.Sprintf("handleAdminBalance(%v)", tt.args.c))
		})
	}
}

func TestServer_handleAdminBreakAggregate(t *testing.T) {
	type fields struct {
		tracer      trace.Tracer
		Node        *node.Node
		DB          *gorm.DB
		FilClient   *filclient.FilClient
		Api         api.Gateway
		CM          *ContentManager
		StagingMgr  *stagingbs.StagingBSMgr
		gwayHandler *gateway.GatewayHandler
		cacher      *memo.Cacher
	}
	type args struct {
		c echo.Context
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr assert.ErrorAssertionFunc
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Server{
				tracer:      tt.fields.tracer,
				Node:        tt.fields.Node,
				DB:          tt.fields.DB,
				FilClient:   tt.fields.FilClient,
				Api:         tt.fields.Api,
				CM:          tt.fields.CM,
				StagingMgr:  tt.fields.StagingMgr,
				gwayHandler: tt.fields.gwayHandler,
				cacher:      tt.fields.cacher,
			}
			tt.wantErr(t, s.handleAdminBreakAggregate(tt.args.c), fmt.Sprintf("handleAdminBreakAggregate(%v)", tt.args.c))
		})
	}
}

func TestServer_handleAdminCreateInvite(t *testing.T) {
	type fields struct {
		tracer      trace.Tracer
		Node        *node.Node
		DB          *gorm.DB
		FilClient   *filclient.FilClient
		Api         api.Gateway
		CM          *ContentManager
		StagingMgr  *stagingbs.StagingBSMgr
		gwayHandler *gateway.GatewayHandler
		cacher      *memo.Cacher
	}
	type args struct {
		c echo.Context
		u *User
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr assert.ErrorAssertionFunc
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Server{
				tracer:      tt.fields.tracer,
				Node:        tt.fields.Node,
				DB:          tt.fields.DB,
				FilClient:   tt.fields.FilClient,
				Api:         tt.fields.Api,
				CM:          tt.fields.CM,
				StagingMgr:  tt.fields.StagingMgr,
				gwayHandler: tt.fields.gwayHandler,
				cacher:      tt.fields.cacher,
			}
			tt.wantErr(t, s.handleAdminCreateInvite(tt.args.c, tt.args.u), fmt.Sprintf("handleAdminCreateInvite(%v, %v)", tt.args.c, tt.args.u))
		})
	}
}

func TestServer_handleAdminGetInvites(t *testing.T) {
	type fields struct {
		tracer      trace.Tracer
		Node        *node.Node
		DB          *gorm.DB
		FilClient   *filclient.FilClient
		Api         api.Gateway
		CM          *ContentManager
		StagingMgr  *stagingbs.StagingBSMgr
		gwayHandler *gateway.GatewayHandler
		cacher      *memo.Cacher
	}
	type args struct {
		c echo.Context
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr assert.ErrorAssertionFunc
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Server{
				tracer:      tt.fields.tracer,
				Node:        tt.fields.Node,
				DB:          tt.fields.DB,
				FilClient:   tt.fields.FilClient,
				Api:         tt.fields.Api,
				CM:          tt.fields.CM,
				StagingMgr:  tt.fields.StagingMgr,
				gwayHandler: tt.fields.gwayHandler,
				cacher:      tt.fields.cacher,
			}
			tt.wantErr(t, s.handleAdminGetInvites(tt.args.c), fmt.Sprintf("handleAdminGetInvites(%v)", tt.args.c))
		})
	}
}

func TestServer_handleAdminGetMinerStats(t *testing.T) {
	type fields struct {
		tracer      trace.Tracer
		Node        *node.Node
		DB          *gorm.DB
		FilClient   *filclient.FilClient
		Api         api.Gateway
		CM          *ContentManager
		StagingMgr  *stagingbs.StagingBSMgr
		gwayHandler *gateway.GatewayHandler
		cacher      *memo.Cacher
	}
	type args struct {
		c echo.Context
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr assert.ErrorAssertionFunc
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Server{
				tracer:      tt.fields.tracer,
				Node:        tt.fields.Node,
				DB:          tt.fields.DB,
				FilClient:   tt.fields.FilClient,
				Api:         tt.fields.Api,
				CM:          tt.fields.CM,
				StagingMgr:  tt.fields.StagingMgr,
				gwayHandler: tt.fields.gwayHandler,
				cacher:      tt.fields.cacher,
			}
			tt.wantErr(t, s.handleAdminGetMinerStats(tt.args.c), fmt.Sprintf("handleAdminGetMinerStats(%v)", tt.args.c))
		})
	}
}

func TestServer_handleAdminGetMiners(t *testing.T) {
	type fields struct {
		tracer      trace.Tracer
		Node        *node.Node
		DB          *gorm.DB
		FilClient   *filclient.FilClient
		Api         api.Gateway
		CM          *ContentManager
		StagingMgr  *stagingbs.StagingBSMgr
		gwayHandler *gateway.GatewayHandler
		cacher      *memo.Cacher
	}
	type args struct {
		c echo.Context
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr assert.ErrorAssertionFunc
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Server{
				tracer:      tt.fields.tracer,
				Node:        tt.fields.Node,
				DB:          tt.fields.DB,
				FilClient:   tt.fields.FilClient,
				Api:         tt.fields.Api,
				CM:          tt.fields.CM,
				StagingMgr:  tt.fields.StagingMgr,
				gwayHandler: tt.fields.gwayHandler,
				cacher:      tt.fields.cacher,
			}
			tt.wantErr(t, s.handleAdminGetMiners(tt.args.c), fmt.Sprintf("handleAdminGetMiners(%v)", tt.args.c))
		})
	}
}

func TestServer_handleAdminGetProgress(t *testing.T) {
	type fields struct {
		tracer      trace.Tracer
		Node        *node.Node
		DB          *gorm.DB
		FilClient   *filclient.FilClient
		Api         api.Gateway
		CM          *ContentManager
		StagingMgr  *stagingbs.StagingBSMgr
		gwayHandler *gateway.GatewayHandler
		cacher      *memo.Cacher
	}
	type args struct {
		c echo.Context
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr assert.ErrorAssertionFunc
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Server{
				tracer:      tt.fields.tracer,
				Node:        tt.fields.Node,
				DB:          tt.fields.DB,
				FilClient:   tt.fields.FilClient,
				Api:         tt.fields.Api,
				CM:          tt.fields.CM,
				StagingMgr:  tt.fields.StagingMgr,
				gwayHandler: tt.fields.gwayHandler,
				cacher:      tt.fields.cacher,
			}
			tt.wantErr(t, s.handleAdminGetProgress(tt.args.c), fmt.Sprintf("handleAdminGetProgress(%v)", tt.args.c))
		})
	}
}

func TestServer_handleAdminGetStagingZones(t *testing.T) {
	type fields struct {
		tracer      trace.Tracer
		Node        *node.Node
		DB          *gorm.DB
		FilClient   *filclient.FilClient
		Api         api.Gateway
		CM          *ContentManager
		StagingMgr  *stagingbs.StagingBSMgr
		gwayHandler *gateway.GatewayHandler
		cacher      *memo.Cacher
	}
	type args struct {
		c echo.Context
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr assert.ErrorAssertionFunc
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Server{
				tracer:      tt.fields.tracer,
				Node:        tt.fields.Node,
				DB:          tt.fields.DB,
				FilClient:   tt.fields.FilClient,
				Api:         tt.fields.Api,
				CM:          tt.fields.CM,
				StagingMgr:  tt.fields.StagingMgr,
				gwayHandler: tt.fields.gwayHandler,
				cacher:      tt.fields.cacher,
			}
			tt.wantErr(t, s.handleAdminGetStagingZones(tt.args.c), fmt.Sprintf("handleAdminGetStagingZones(%v)", tt.args.c))
		})
	}
}

func TestServer_handleAdminGetUsers(t *testing.T) {
	type fields struct {
		tracer      trace.Tracer
		Node        *node.Node
		DB          *gorm.DB
		FilClient   *filclient.FilClient
		Api         api.Gateway
		CM          *ContentManager
		StagingMgr  *stagingbs.StagingBSMgr
		gwayHandler *gateway.GatewayHandler
		cacher      *memo.Cacher
	}
	type args struct {
		c echo.Context
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr assert.ErrorAssertionFunc
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Server{
				tracer:      tt.fields.tracer,
				Node:        tt.fields.Node,
				DB:          tt.fields.DB,
				FilClient:   tt.fields.FilClient,
				Api:         tt.fields.Api,
				CM:          tt.fields.CM,
				StagingMgr:  tt.fields.StagingMgr,
				gwayHandler: tt.fields.gwayHandler,
				cacher:      tt.fields.cacher,
			}
			tt.wantErr(t, s.handleAdminGetUsers(tt.args.c), fmt.Sprintf("handleAdminGetUsers(%v)", tt.args.c))
		})
	}
}

func TestServer_handleAdminRemoveMiner(t *testing.T) {
	type fields struct {
		tracer      trace.Tracer
		Node        *node.Node
		DB          *gorm.DB
		FilClient   *filclient.FilClient
		Api         api.Gateway
		CM          *ContentManager
		StagingMgr  *stagingbs.StagingBSMgr
		gwayHandler *gateway.GatewayHandler
		cacher      *memo.Cacher
	}
	type args struct {
		c echo.Context
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr assert.ErrorAssertionFunc
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Server{
				tracer:      tt.fields.tracer,
				Node:        tt.fields.Node,
				DB:          tt.fields.DB,
				FilClient:   tt.fields.FilClient,
				Api:         tt.fields.Api,
				CM:          tt.fields.CM,
				StagingMgr:  tt.fields.StagingMgr,
				gwayHandler: tt.fields.gwayHandler,
				cacher:      tt.fields.cacher,
			}
			tt.wantErr(t, s.handleAdminRemoveMiner(tt.args.c), fmt.Sprintf("handleAdminRemoveMiner(%v)", tt.args.c))
		})
	}
}

func TestServer_handleAdminStats(t *testing.T) {
	type fields struct {
		tracer      trace.Tracer
		Node        *node.Node
		DB          *gorm.DB
		FilClient   *filclient.FilClient
		Api         api.Gateway
		CM          *ContentManager
		StagingMgr  *stagingbs.StagingBSMgr
		gwayHandler *gateway.GatewayHandler
		cacher      *memo.Cacher
	}
	type args struct {
		c echo.Context
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr assert.ErrorAssertionFunc
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Server{
				tracer:      tt.fields.tracer,
				Node:        tt.fields.Node,
				DB:          tt.fields.DB,
				FilClient:   tt.fields.FilClient,
				Api:         tt.fields.Api,
				CM:          tt.fields.CM,
				StagingMgr:  tt.fields.StagingMgr,
				gwayHandler: tt.fields.gwayHandler,
				cacher:      tt.fields.cacher,
			}
			tt.wantErr(t, s.handleAdminStats(tt.args.c), fmt.Sprintf("handleAdminStats(%v)", tt.args.c))
		})
	}
}

func TestServer_handleAutoretrieveHeartbeat(t *testing.T) {
	type fields struct {
		tracer      trace.Tracer
		Node        *node.Node
		DB          *gorm.DB
		FilClient   *filclient.FilClient
		Api         api.Gateway
		CM          *ContentManager
		StagingMgr  *stagingbs.StagingBSMgr
		gwayHandler *gateway.GatewayHandler
		cacher      *memo.Cacher
	}
	type args struct {
		c echo.Context
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr assert.ErrorAssertionFunc
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Server{
				tracer:      tt.fields.tracer,
				Node:        tt.fields.Node,
				DB:          tt.fields.DB,
				FilClient:   tt.fields.FilClient,
				Api:         tt.fields.Api,
				CM:          tt.fields.CM,
				StagingMgr:  tt.fields.StagingMgr,
				gwayHandler: tt.fields.gwayHandler,
				cacher:      tt.fields.cacher,
			}
			tt.wantErr(t, s.handleAutoretrieveHeartbeat(tt.args.c), fmt.Sprintf("handleAutoretrieveHeartbeat(%v)", tt.args.c))
		})
	}
}

func TestServer_handleAutoretrieveInit(t *testing.T) {
	type fields struct {
		tracer      trace.Tracer
		Node        *node.Node
		DB          *gorm.DB
		FilClient   *filclient.FilClient
		Api         api.Gateway
		CM          *ContentManager
		StagingMgr  *stagingbs.StagingBSMgr
		gwayHandler *gateway.GatewayHandler
		cacher      *memo.Cacher
	}
	type args struct {
		c echo.Context
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr assert.ErrorAssertionFunc
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Server{
				tracer:      tt.fields.tracer,
				Node:        tt.fields.Node,
				DB:          tt.fields.DB,
				FilClient:   tt.fields.FilClient,
				Api:         tt.fields.Api,
				CM:          tt.fields.CM,
				StagingMgr:  tt.fields.StagingMgr,
				gwayHandler: tt.fields.gwayHandler,
				cacher:      tt.fields.cacher,
			}
			tt.wantErr(t, s.handleAutoretrieveInit(tt.args.c), fmt.Sprintf("handleAutoretrieveInit(%v)", tt.args.c))
		})
	}
}

func TestServer_handleAutoretrieveList(t *testing.T) {
	type fields struct {
		tracer      trace.Tracer
		Node        *node.Node
		DB          *gorm.DB
		FilClient   *filclient.FilClient
		Api         api.Gateway
		CM          *ContentManager
		StagingMgr  *stagingbs.StagingBSMgr
		gwayHandler *gateway.GatewayHandler
		cacher      *memo.Cacher
	}
	type args struct {
		c echo.Context
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr assert.ErrorAssertionFunc
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Server{
				tracer:      tt.fields.tracer,
				Node:        tt.fields.Node,
				DB:          tt.fields.DB,
				FilClient:   tt.fields.FilClient,
				Api:         tt.fields.Api,
				CM:          tt.fields.CM,
				StagingMgr:  tt.fields.StagingMgr,
				gwayHandler: tt.fields.gwayHandler,
				cacher:      tt.fields.cacher,
			}
			tt.wantErr(t, s.handleAutoretrieveList(tt.args.c), fmt.Sprintf("handleAutoretrieveList(%v)", tt.args.c))
		})
	}
}

func TestServer_handleColfsAdd(t *testing.T) {
	type fields struct {
		tracer      trace.Tracer
		Node        *node.Node
		DB          *gorm.DB
		FilClient   *filclient.FilClient
		Api         api.Gateway
		CM          *ContentManager
		StagingMgr  *stagingbs.StagingBSMgr
		gwayHandler *gateway.GatewayHandler
		cacher      *memo.Cacher
	}
	type args struct {
		c echo.Context
		u *User
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr assert.ErrorAssertionFunc
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Server{
				tracer:      tt.fields.tracer,
				Node:        tt.fields.Node,
				DB:          tt.fields.DB,
				FilClient:   tt.fields.FilClient,
				Api:         tt.fields.Api,
				CM:          tt.fields.CM,
				StagingMgr:  tt.fields.StagingMgr,
				gwayHandler: tt.fields.gwayHandler,
				cacher:      tt.fields.cacher,
			}
			tt.wantErr(t, s.handleColfsAdd(tt.args.c, tt.args.u), fmt.Sprintf("handleColfsAdd(%v, %v)", tt.args.c, tt.args.u))
		})
	}
}

func TestServer_handleColfsListDir(t *testing.T) {
	type fields struct {
		tracer      trace.Tracer
		Node        *node.Node
		DB          *gorm.DB
		FilClient   *filclient.FilClient
		Api         api.Gateway
		CM          *ContentManager
		StagingMgr  *stagingbs.StagingBSMgr
		gwayHandler *gateway.GatewayHandler
		cacher      *memo.Cacher
	}
	type args struct {
		c echo.Context
		u *User
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr assert.ErrorAssertionFunc
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Server{
				tracer:      tt.fields.tracer,
				Node:        tt.fields.Node,
				DB:          tt.fields.DB,
				FilClient:   tt.fields.FilClient,
				Api:         tt.fields.Api,
				CM:          tt.fields.CM,
				StagingMgr:  tt.fields.StagingMgr,
				gwayHandler: tt.fields.gwayHandler,
				cacher:      tt.fields.cacher,
			}
			tt.wantErr(t, s.handleColfsListDir(tt.args.c, tt.args.u), fmt.Sprintf("handleColfsListDir(%v, %v)", tt.args.c, tt.args.u))
		})
	}
}

func TestServer_handleCommitCollection(t *testing.T) {
	type fields struct {
		tracer      trace.Tracer
		Node        *node.Node
		DB          *gorm.DB
		FilClient   *filclient.FilClient
		Api         api.Gateway
		CM          *ContentManager
		StagingMgr  *stagingbs.StagingBSMgr
		gwayHandler *gateway.GatewayHandler
		cacher      *memo.Cacher
	}
	type args struct {
		c echo.Context
		u *User
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr assert.ErrorAssertionFunc
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Server{
				tracer:      tt.fields.tracer,
				Node:        tt.fields.Node,
				DB:          tt.fields.DB,
				FilClient:   tt.fields.FilClient,
				Api:         tt.fields.Api,
				CM:          tt.fields.CM,
				StagingMgr:  tt.fields.StagingMgr,
				gwayHandler: tt.fields.gwayHandler,
				cacher:      tt.fields.cacher,
			}
			tt.wantErr(t, s.handleCommitCollection(tt.args.c, tt.args.u), fmt.Sprintf("handleCommitCollection(%v, %v)", tt.args.c, tt.args.u))
		})
	}
}

func TestServer_handleContentHealthCheck(t *testing.T) {
	type fields struct {
		tracer      trace.Tracer
		Node        *node.Node
		DB          *gorm.DB
		FilClient   *filclient.FilClient
		Api         api.Gateway
		CM          *ContentManager
		StagingMgr  *stagingbs.StagingBSMgr
		gwayHandler *gateway.GatewayHandler
		cacher      *memo.Cacher
	}
	type args struct {
		c echo.Context
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr assert.ErrorAssertionFunc
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Server{
				tracer:      tt.fields.tracer,
				Node:        tt.fields.Node,
				DB:          tt.fields.DB,
				FilClient:   tt.fields.FilClient,
				Api:         tt.fields.Api,
				CM:          tt.fields.CM,
				StagingMgr:  tt.fields.StagingMgr,
				gwayHandler: tt.fields.gwayHandler,
				cacher:      tt.fields.cacher,
			}
			tt.wantErr(t, s.handleContentHealthCheck(tt.args.c), fmt.Sprintf("handleContentHealthCheck(%v)", tt.args.c))
		})
	}
}

func TestServer_handleContentHealthCheckByCid(t *testing.T) {
	type fields struct {
		tracer      trace.Tracer
		Node        *node.Node
		DB          *gorm.DB
		FilClient   *filclient.FilClient
		Api         api.Gateway
		CM          *ContentManager
		StagingMgr  *stagingbs.StagingBSMgr
		gwayHandler *gateway.GatewayHandler
		cacher      *memo.Cacher
	}
	type args struct {
		c echo.Context
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr assert.ErrorAssertionFunc
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Server{
				tracer:      tt.fields.tracer,
				Node:        tt.fields.Node,
				DB:          tt.fields.DB,
				FilClient:   tt.fields.FilClient,
				Api:         tt.fields.Api,
				CM:          tt.fields.CM,
				StagingMgr:  tt.fields.StagingMgr,
				gwayHandler: tt.fields.gwayHandler,
				cacher:      tt.fields.cacher,
			}
			tt.wantErr(t, s.handleContentHealthCheckByCid(tt.args.c), fmt.Sprintf("handleContentHealthCheckByCid(%v)", tt.args.c))
		})
	}
}

func TestServer_handleContentStatus(t *testing.T) {
	type fields struct {
		tracer      trace.Tracer
		Node        *node.Node
		DB          *gorm.DB
		FilClient   *filclient.FilClient
		Api         api.Gateway
		CM          *ContentManager
		StagingMgr  *stagingbs.StagingBSMgr
		gwayHandler *gateway.GatewayHandler
		cacher      *memo.Cacher
	}
	type args struct {
		c echo.Context
		u *User
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr assert.ErrorAssertionFunc
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Server{
				tracer:      tt.fields.tracer,
				Node:        tt.fields.Node,
				DB:          tt.fields.DB,
				FilClient:   tt.fields.FilClient,
				Api:         tt.fields.Api,
				CM:          tt.fields.CM,
				StagingMgr:  tt.fields.StagingMgr,
				gwayHandler: tt.fields.gwayHandler,
				cacher:      tt.fields.cacher,
			}
			tt.wantErr(t, s.handleContentStatus(tt.args.c, tt.args.u), fmt.Sprintf("handleContentStatus(%v, %v)", tt.args.c, tt.args.u))
		})
	}
}

func TestServer_handleCreateCollection(t *testing.T) {
	type fields struct {
		tracer      trace.Tracer
		Node        *node.Node
		DB          *gorm.DB
		FilClient   *filclient.FilClient
		Api         api.Gateway
		CM          *ContentManager
		StagingMgr  *stagingbs.StagingBSMgr
		gwayHandler *gateway.GatewayHandler
		cacher      *memo.Cacher
	}
	type args struct {
		c echo.Context
		u *User
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr assert.ErrorAssertionFunc
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Server{
				tracer:      tt.fields.tracer,
				Node:        tt.fields.Node,
				DB:          tt.fields.DB,
				FilClient:   tt.fields.FilClient,
				Api:         tt.fields.Api,
				CM:          tt.fields.CM,
				StagingMgr:  tt.fields.StagingMgr,
				gwayHandler: tt.fields.gwayHandler,
				cacher:      tt.fields.cacher,
			}
			tt.wantErr(t, s.handleCreateCollection(tt.args.c, tt.args.u), fmt.Sprintf("handleCreateCollection(%v, %v)", tt.args.c, tt.args.u))
		})
	}
}

func TestServer_handleCreateContent(t *testing.T) {
	type fields struct {
		tracer      trace.Tracer
		Node        *node.Node
		DB          *gorm.DB
		FilClient   *filclient.FilClient
		Api         api.Gateway
		CM          *ContentManager
		StagingMgr  *stagingbs.StagingBSMgr
		gwayHandler *gateway.GatewayHandler
		cacher      *memo.Cacher
	}
	type args struct {
		c echo.Context
		u *User
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr assert.ErrorAssertionFunc
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Server{
				tracer:      tt.fields.tracer,
				Node:        tt.fields.Node,
				DB:          tt.fields.DB,
				FilClient:   tt.fields.FilClient,
				Api:         tt.fields.Api,
				CM:          tt.fields.CM,
				StagingMgr:  tt.fields.StagingMgr,
				gwayHandler: tt.fields.gwayHandler,
				cacher:      tt.fields.cacher,
			}
			tt.wantErr(t, s.handleCreateContent(tt.args.c, tt.args.u), fmt.Sprintf("handleCreateContent(%v, %v)", tt.args.c, tt.args.u))
		})
	}
}

func TestServer_handleDealStats(t *testing.T) {
	type fields struct {
		tracer      trace.Tracer
		Node        *node.Node
		DB          *gorm.DB
		FilClient   *filclient.FilClient
		Api         api.Gateway
		CM          *ContentManager
		StagingMgr  *stagingbs.StagingBSMgr
		gwayHandler *gateway.GatewayHandler
		cacher      *memo.Cacher
	}
	type args struct {
		c echo.Context
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr assert.ErrorAssertionFunc
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Server{
				tracer:      tt.fields.tracer,
				Node:        tt.fields.Node,
				DB:          tt.fields.DB,
				FilClient:   tt.fields.FilClient,
				Api:         tt.fields.Api,
				CM:          tt.fields.CM,
				StagingMgr:  tt.fields.StagingMgr,
				gwayHandler: tt.fields.gwayHandler,
				cacher:      tt.fields.cacher,
			}
			tt.wantErr(t, s.handleDealStats(tt.args.c), fmt.Sprintf("handleDealStats(%v)", tt.args.c))
		})
	}
}

func TestServer_handleDealStatus(t *testing.T) {
	type fields struct {
		tracer      trace.Tracer
		Node        *node.Node
		DB          *gorm.DB
		FilClient   *filclient.FilClient
		Api         api.Gateway
		CM          *ContentManager
		StagingMgr  *stagingbs.StagingBSMgr
		gwayHandler *gateway.GatewayHandler
		cacher      *memo.Cacher
	}
	type args struct {
		c echo.Context
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr assert.ErrorAssertionFunc
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Server{
				tracer:      tt.fields.tracer,
				Node:        tt.fields.Node,
				DB:          tt.fields.DB,
				FilClient:   tt.fields.FilClient,
				Api:         tt.fields.Api,
				CM:          tt.fields.CM,
				StagingMgr:  tt.fields.StagingMgr,
				gwayHandler: tt.fields.gwayHandler,
				cacher:      tt.fields.cacher,
			}
			tt.wantErr(t, s.handleDealStatus(tt.args.c), fmt.Sprintf("handleDealStatus(%v)", tt.args.c))
		})
	}
}

func TestServer_handleDebugGetAllDeals(t *testing.T) {
	type fields struct {
		tracer      trace.Tracer
		Node        *node.Node
		DB          *gorm.DB
		FilClient   *filclient.FilClient
		Api         api.Gateway
		CM          *ContentManager
		StagingMgr  *stagingbs.StagingBSMgr
		gwayHandler *gateway.GatewayHandler
		cacher      *memo.Cacher
	}
	type args struct {
		c echo.Context
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr assert.ErrorAssertionFunc
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Server{
				tracer:      tt.fields.tracer,
				Node:        tt.fields.Node,
				DB:          tt.fields.DB,
				FilClient:   tt.fields.FilClient,
				Api:         tt.fields.Api,
				CM:          tt.fields.CM,
				StagingMgr:  tt.fields.StagingMgr,
				gwayHandler: tt.fields.gwayHandler,
				cacher:      tt.fields.cacher,
			}
			tt.wantErr(t, s.handleDebugGetAllDeals(tt.args.c), fmt.Sprintf("handleDebugGetAllDeals(%v)", tt.args.c))
		})
	}
}

func TestServer_handleDeleteCollection(t *testing.T) {
	type fields struct {
		tracer      trace.Tracer
		Node        *node.Node
		DB          *gorm.DB
		FilClient   *filclient.FilClient
		Api         api.Gateway
		CM          *ContentManager
		StagingMgr  *stagingbs.StagingBSMgr
		gwayHandler *gateway.GatewayHandler
		cacher      *memo.Cacher
	}
	type args struct {
		c echo.Context
		u *User
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr assert.ErrorAssertionFunc
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Server{
				tracer:      tt.fields.tracer,
				Node:        tt.fields.Node,
				DB:          tt.fields.DB,
				FilClient:   tt.fields.FilClient,
				Api:         tt.fields.Api,
				CM:          tt.fields.CM,
				StagingMgr:  tt.fields.StagingMgr,
				gwayHandler: tt.fields.gwayHandler,
				cacher:      tt.fields.cacher,
			}
			tt.wantErr(t, s.handleDeleteCollection(tt.args.c, tt.args.u), fmt.Sprintf("handleDeleteCollection(%v, %v)", tt.args.c, tt.args.u))
		})
	}
}

func TestServer_handleDiskSpaceCheck(t *testing.T) {
	type fields struct {
		tracer      trace.Tracer
		Node        *node.Node
		DB          *gorm.DB
		FilClient   *filclient.FilClient
		Api         api.Gateway
		CM          *ContentManager
		StagingMgr  *stagingbs.StagingBSMgr
		gwayHandler *gateway.GatewayHandler
		cacher      *memo.Cacher
	}
	type args struct {
		c echo.Context
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr assert.ErrorAssertionFunc
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Server{
				tracer:      tt.fields.tracer,
				Node:        tt.fields.Node,
				DB:          tt.fields.DB,
				FilClient:   tt.fields.FilClient,
				Api:         tt.fields.Api,
				CM:          tt.fields.CM,
				StagingMgr:  tt.fields.StagingMgr,
				gwayHandler: tt.fields.gwayHandler,
				cacher:      tt.fields.cacher,
			}
			tt.wantErr(t, s.handleDiskSpaceCheck(tt.args.c), fmt.Sprintf("handleDiskSpaceCheck(%v)", tt.args.c))
		})
	}
}

func TestServer_handleEnsureReplication(t *testing.T) {
	type fields struct {
		tracer      trace.Tracer
		Node        *node.Node
		DB          *gorm.DB
		FilClient   *filclient.FilClient
		Api         api.Gateway
		CM          *ContentManager
		StagingMgr  *stagingbs.StagingBSMgr
		gwayHandler *gateway.GatewayHandler
		cacher      *memo.Cacher
	}
	type args struct {
		c echo.Context
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr assert.ErrorAssertionFunc
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Server{
				tracer:      tt.fields.tracer,
				Node:        tt.fields.Node,
				DB:          tt.fields.DB,
				FilClient:   tt.fields.FilClient,
				Api:         tt.fields.Api,
				CM:          tt.fields.CM,
				StagingMgr:  tt.fields.StagingMgr,
				gwayHandler: tt.fields.gwayHandler,
				cacher:      tt.fields.cacher,
			}
			tt.wantErr(t, s.handleEnsureReplication(tt.args.c), fmt.Sprintf("handleEnsureReplication(%v)", tt.args.c))
		})
	}
}

func TestServer_handleEstimateDealCost(t *testing.T) {
	type fields struct {
		tracer      trace.Tracer
		Node        *node.Node
		DB          *gorm.DB
		FilClient   *filclient.FilClient
		Api         api.Gateway
		CM          *ContentManager
		StagingMgr  *stagingbs.StagingBSMgr
		gwayHandler *gateway.GatewayHandler
		cacher      *memo.Cacher
	}
	type args struct {
		c echo.Context
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr assert.ErrorAssertionFunc
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Server{
				tracer:      tt.fields.tracer,
				Node:        tt.fields.Node,
				DB:          tt.fields.DB,
				FilClient:   tt.fields.FilClient,
				Api:         tt.fields.Api,
				CM:          tt.fields.CM,
				StagingMgr:  tt.fields.StagingMgr,
				gwayHandler: tt.fields.gwayHandler,
				cacher:      tt.fields.cacher,
			}
			tt.wantErr(t, s.handleEstimateDealCost(tt.args.c), fmt.Sprintf("handleEstimateDealCost(%v)", tt.args.c))
		})
	}
}

func TestServer_handleGateway(t *testing.T) {
	type fields struct {
		tracer      trace.Tracer
		Node        *node.Node
		DB          *gorm.DB
		FilClient   *filclient.FilClient
		Api         api.Gateway
		CM          *ContentManager
		StagingMgr  *stagingbs.StagingBSMgr
		gwayHandler *gateway.GatewayHandler
		cacher      *memo.Cacher
	}
	type args struct {
		c echo.Context
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr assert.ErrorAssertionFunc
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Server{
				tracer:      tt.fields.tracer,
				Node:        tt.fields.Node,
				DB:          tt.fields.DB,
				FilClient:   tt.fields.FilClient,
				Api:         tt.fields.Api,
				CM:          tt.fields.CM,
				StagingMgr:  tt.fields.StagingMgr,
				gwayHandler: tt.fields.gwayHandler,
				cacher:      tt.fields.cacher,
			}
			tt.wantErr(t, s.handleGateway(tt.args.c), fmt.Sprintf("handleGateway(%v)", tt.args.c))
		})
	}
}

func TestServer_handleGetAggregatedForContent(t *testing.T) {
	type fields struct {
		tracer      trace.Tracer
		Node        *node.Node
		DB          *gorm.DB
		FilClient   *filclient.FilClient
		Api         api.Gateway
		CM          *ContentManager
		StagingMgr  *stagingbs.StagingBSMgr
		gwayHandler *gateway.GatewayHandler
		cacher      *memo.Cacher
	}
	type args struct {
		c echo.Context
		u *User
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr assert.ErrorAssertionFunc
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Server{
				tracer:      tt.fields.tracer,
				Node:        tt.fields.Node,
				DB:          tt.fields.DB,
				FilClient:   tt.fields.FilClient,
				Api:         tt.fields.Api,
				CM:          tt.fields.CM,
				StagingMgr:  tt.fields.StagingMgr,
				gwayHandler: tt.fields.gwayHandler,
				cacher:      tt.fields.cacher,
			}
			tt.wantErr(t, s.handleGetAggregatedForContent(tt.args.c, tt.args.u), fmt.Sprintf("handleGetAggregatedForContent(%v, %v)", tt.args.c, tt.args.u))
		})
	}
}

func TestServer_handleGetAllDealsForUser(t *testing.T) {
	type fields struct {
		tracer      trace.Tracer
		Node        *node.Node
		DB          *gorm.DB
		FilClient   *filclient.FilClient
		Api         api.Gateway
		CM          *ContentManager
		StagingMgr  *stagingbs.StagingBSMgr
		gwayHandler *gateway.GatewayHandler
		cacher      *memo.Cacher
	}
	type args struct {
		c echo.Context
		u *User
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr assert.ErrorAssertionFunc
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Server{
				tracer:      tt.fields.tracer,
				Node:        tt.fields.Node,
				DB:          tt.fields.DB,
				FilClient:   tt.fields.FilClient,
				Api:         tt.fields.Api,
				CM:          tt.fields.CM,
				StagingMgr:  tt.fields.StagingMgr,
				gwayHandler: tt.fields.gwayHandler,
				cacher:      tt.fields.cacher,
			}
			tt.wantErr(t, s.handleGetAllDealsForUser(tt.args.c, tt.args.u), fmt.Sprintf("handleGetAllDealsForUser(%v, %v)", tt.args.c, tt.args.u))
		})
	}
}

func TestServer_handleGetBucketDiag(t *testing.T) {
	type fields struct {
		tracer      trace.Tracer
		Node        *node.Node
		DB          *gorm.DB
		FilClient   *filclient.FilClient
		Api         api.Gateway
		CM          *ContentManager
		StagingMgr  *stagingbs.StagingBSMgr
		gwayHandler *gateway.GatewayHandler
		cacher      *memo.Cacher
	}
	type args struct {
		c echo.Context
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr assert.ErrorAssertionFunc
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Server{
				tracer:      tt.fields.tracer,
				Node:        tt.fields.Node,
				DB:          tt.fields.DB,
				FilClient:   tt.fields.FilClient,
				Api:         tt.fields.Api,
				CM:          tt.fields.CM,
				StagingMgr:  tt.fields.StagingMgr,
				gwayHandler: tt.fields.gwayHandler,
				cacher:      tt.fields.cacher,
			}
			tt.wantErr(t, s.handleGetBucketDiag(tt.args.c), fmt.Sprintf("handleGetBucketDiag(%v)", tt.args.c))
		})
	}
}

func TestServer_handleGetCollectionContents(t *testing.T) {
	type fields struct {
		tracer      trace.Tracer
		Node        *node.Node
		DB          *gorm.DB
		FilClient   *filclient.FilClient
		Api         api.Gateway
		CM          *ContentManager
		StagingMgr  *stagingbs.StagingBSMgr
		gwayHandler *gateway.GatewayHandler
		cacher      *memo.Cacher
	}
	type args struct {
		c echo.Context
		u *User
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr assert.ErrorAssertionFunc
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Server{
				tracer:      tt.fields.tracer,
				Node:        tt.fields.Node,
				DB:          tt.fields.DB,
				FilClient:   tt.fields.FilClient,
				Api:         tt.fields.Api,
				CM:          tt.fields.CM,
				StagingMgr:  tt.fields.StagingMgr,
				gwayHandler: tt.fields.gwayHandler,
				cacher:      tt.fields.cacher,
			}
			tt.wantErr(t, s.handleGetCollectionContents(tt.args.c, tt.args.u), fmt.Sprintf("handleGetCollectionContents(%v, %v)", tt.args.c, tt.args.u))
		})
	}
}

func TestServer_handleGetContentBandwidth(t *testing.T) {
	type fields struct {
		tracer      trace.Tracer
		Node        *node.Node
		DB          *gorm.DB
		FilClient   *filclient.FilClient
		Api         api.Gateway
		CM          *ContentManager
		StagingMgr  *stagingbs.StagingBSMgr
		gwayHandler *gateway.GatewayHandler
		cacher      *memo.Cacher
	}
	type args struct {
		c echo.Context
		u *User
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr assert.ErrorAssertionFunc
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Server{
				tracer:      tt.fields.tracer,
				Node:        tt.fields.Node,
				DB:          tt.fields.DB,
				FilClient:   tt.fields.FilClient,
				Api:         tt.fields.Api,
				CM:          tt.fields.CM,
				StagingMgr:  tt.fields.StagingMgr,
				gwayHandler: tt.fields.gwayHandler,
				cacher:      tt.fields.cacher,
			}
			tt.wantErr(t, s.handleGetContentBandwidth(tt.args.c, tt.args.u), fmt.Sprintf("handleGetContentBandwidth(%v, %v)", tt.args.c, tt.args.u))
		})
	}
}

func TestServer_handleGetContentByCid(t *testing.T) {
	type fields struct {
		tracer      trace.Tracer
		Node        *node.Node
		DB          *gorm.DB
		FilClient   *filclient.FilClient
		Api         api.Gateway
		CM          *ContentManager
		StagingMgr  *stagingbs.StagingBSMgr
		gwayHandler *gateway.GatewayHandler
		cacher      *memo.Cacher
	}
	type args struct {
		c echo.Context
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr assert.ErrorAssertionFunc
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Server{
				tracer:      tt.fields.tracer,
				Node:        tt.fields.Node,
				DB:          tt.fields.DB,
				FilClient:   tt.fields.FilClient,
				Api:         tt.fields.Api,
				CM:          tt.fields.CM,
				StagingMgr:  tt.fields.StagingMgr,
				gwayHandler: tt.fields.gwayHandler,
				cacher:      tt.fields.cacher,
			}
			tt.wantErr(t, s.handleGetContentByCid(tt.args.c), fmt.Sprintf("handleGetContentByCid(%v)", tt.args.c))
		})
	}
}

func TestServer_handleGetContentFailures(t *testing.T) {
	type fields struct {
		tracer      trace.Tracer
		Node        *node.Node
		DB          *gorm.DB
		FilClient   *filclient.FilClient
		Api         api.Gateway
		CM          *ContentManager
		StagingMgr  *stagingbs.StagingBSMgr
		gwayHandler *gateway.GatewayHandler
		cacher      *memo.Cacher
	}
	type args struct {
		c echo.Context
		u *User
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr assert.ErrorAssertionFunc
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Server{
				tracer:      tt.fields.tracer,
				Node:        tt.fields.Node,
				DB:          tt.fields.DB,
				FilClient:   tt.fields.FilClient,
				Api:         tt.fields.Api,
				CM:          tt.fields.CM,
				StagingMgr:  tt.fields.StagingMgr,
				gwayHandler: tt.fields.gwayHandler,
				cacher:      tt.fields.cacher,
			}
			tt.wantErr(t, s.handleGetContentFailures(tt.args.c, tt.args.u), fmt.Sprintf("handleGetContentFailures(%v, %v)", tt.args.c, tt.args.u))
		})
	}
}

func TestServer_handleGetDealInfo(t *testing.T) {
	type fields struct {
		tracer      trace.Tracer
		Node        *node.Node
		DB          *gorm.DB
		FilClient   *filclient.FilClient
		Api         api.Gateway
		CM          *ContentManager
		StagingMgr  *stagingbs.StagingBSMgr
		gwayHandler *gateway.GatewayHandler
		cacher      *memo.Cacher
	}
	type args struct {
		c echo.Context
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr assert.ErrorAssertionFunc
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Server{
				tracer:      tt.fields.tracer,
				Node:        tt.fields.Node,
				DB:          tt.fields.DB,
				FilClient:   tt.fields.FilClient,
				Api:         tt.fields.Api,
				CM:          tt.fields.CM,
				StagingMgr:  tt.fields.StagingMgr,
				gwayHandler: tt.fields.gwayHandler,
				cacher:      tt.fields.cacher,
			}
			tt.wantErr(t, s.handleGetDealInfo(tt.args.c), fmt.Sprintf("handleGetDealInfo(%v)", tt.args.c))
		})
	}
}

func TestServer_handleGetDealStatus(t *testing.T) {
	type fields struct {
		tracer      trace.Tracer
		Node        *node.Node
		DB          *gorm.DB
		FilClient   *filclient.FilClient
		Api         api.Gateway
		CM          *ContentManager
		StagingMgr  *stagingbs.StagingBSMgr
		gwayHandler *gateway.GatewayHandler
		cacher      *memo.Cacher
	}
	type args struct {
		c echo.Context
		u *User
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr assert.ErrorAssertionFunc
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Server{
				tracer:      tt.fields.tracer,
				Node:        tt.fields.Node,
				DB:          tt.fields.DB,
				FilClient:   tt.fields.FilClient,
				Api:         tt.fields.Api,
				CM:          tt.fields.CM,
				StagingMgr:  tt.fields.StagingMgr,
				gwayHandler: tt.fields.gwayHandler,
				cacher:      tt.fields.cacher,
			}
			tt.wantErr(t, s.handleGetDealStatus(tt.args.c, tt.args.u), fmt.Sprintf("handleGetDealStatus(%v, %v)", tt.args.c, tt.args.u))
		})
	}
}

func TestServer_handleGetDealStatusByPropCid(t *testing.T) {
	type fields struct {
		tracer      trace.Tracer
		Node        *node.Node
		DB          *gorm.DB
		FilClient   *filclient.FilClient
		Api         api.Gateway
		CM          *ContentManager
		StagingMgr  *stagingbs.StagingBSMgr
		gwayHandler *gateway.GatewayHandler
		cacher      *memo.Cacher
	}
	type args struct {
		c echo.Context
		u *User
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr assert.ErrorAssertionFunc
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Server{
				tracer:      tt.fields.tracer,
				Node:        tt.fields.Node,
				DB:          tt.fields.DB,
				FilClient:   tt.fields.FilClient,
				Api:         tt.fields.Api,
				CM:          tt.fields.CM,
				StagingMgr:  tt.fields.StagingMgr,
				gwayHandler: tt.fields.gwayHandler,
				cacher:      tt.fields.cacher,
			}
			tt.wantErr(t, s.handleGetDealStatusByPropCid(tt.args.c, tt.args.u), fmt.Sprintf("handleGetDealStatusByPropCid(%v, %v)", tt.args.c, tt.args.u))
		})
	}
}

func TestServer_handleGetMinerDeals(t *testing.T) {
	type fields struct {
		tracer      trace.Tracer
		Node        *node.Node
		DB          *gorm.DB
		FilClient   *filclient.FilClient
		Api         api.Gateway
		CM          *ContentManager
		StagingMgr  *stagingbs.StagingBSMgr
		gwayHandler *gateway.GatewayHandler
		cacher      *memo.Cacher
	}
	type args struct {
		c echo.Context
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr assert.ErrorAssertionFunc
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Server{
				tracer:      tt.fields.tracer,
				Node:        tt.fields.Node,
				DB:          tt.fields.DB,
				FilClient:   tt.fields.FilClient,
				Api:         tt.fields.Api,
				CM:          tt.fields.CM,
				StagingMgr:  tt.fields.StagingMgr,
				gwayHandler: tt.fields.gwayHandler,
				cacher:      tt.fields.cacher,
			}
			tt.wantErr(t, s.handleGetMinerDeals(tt.args.c), fmt.Sprintf("handleGetMinerDeals(%v)", tt.args.c))
		})
	}
}

func TestServer_handleGetMinerFailures(t *testing.T) {
	type fields struct {
		tracer      trace.Tracer
		Node        *node.Node
		DB          *gorm.DB
		FilClient   *filclient.FilClient
		Api         api.Gateway
		CM          *ContentManager
		StagingMgr  *stagingbs.StagingBSMgr
		gwayHandler *gateway.GatewayHandler
		cacher      *memo.Cacher
	}
	type args struct {
		c echo.Context
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr assert.ErrorAssertionFunc
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Server{
				tracer:      tt.fields.tracer,
				Node:        tt.fields.Node,
				DB:          tt.fields.DB,
				FilClient:   tt.fields.FilClient,
				Api:         tt.fields.Api,
				CM:          tt.fields.CM,
				StagingMgr:  tt.fields.StagingMgr,
				gwayHandler: tt.fields.gwayHandler,
				cacher:      tt.fields.cacher,
			}
			tt.wantErr(t, s.handleGetMinerFailures(tt.args.c), fmt.Sprintf("handleGetMinerFailures(%v)", tt.args.c))
		})
	}
}

func TestServer_handleGetMinerStats(t *testing.T) {
	type fields struct {
		tracer      trace.Tracer
		Node        *node.Node
		DB          *gorm.DB
		FilClient   *filclient.FilClient
		Api         api.Gateway
		CM          *ContentManager
		StagingMgr  *stagingbs.StagingBSMgr
		gwayHandler *gateway.GatewayHandler
		cacher      *memo.Cacher
	}
	type args struct {
		c echo.Context
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr assert.ErrorAssertionFunc
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Server{
				tracer:      tt.fields.tracer,
				Node:        tt.fields.Node,
				DB:          tt.fields.DB,
				FilClient:   tt.fields.FilClient,
				Api:         tt.fields.Api,
				CM:          tt.fields.CM,
				StagingMgr:  tt.fields.StagingMgr,
				gwayHandler: tt.fields.gwayHandler,
				cacher:      tt.fields.cacher,
			}
			tt.wantErr(t, s.handleGetMinerStats(tt.args.c), fmt.Sprintf("handleGetMinerStats(%v)", tt.args.c))
		})
	}
}

func TestServer_handleGetOffloadingCandidates(t *testing.T) {
	type fields struct {
		tracer      trace.Tracer
		Node        *node.Node
		DB          *gorm.DB
		FilClient   *filclient.FilClient
		Api         api.Gateway
		CM          *ContentManager
		StagingMgr  *stagingbs.StagingBSMgr
		gwayHandler *gateway.GatewayHandler
		cacher      *memo.Cacher
	}
	type args struct {
		c echo.Context
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr assert.ErrorAssertionFunc
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Server{
				tracer:      tt.fields.tracer,
				Node:        tt.fields.Node,
				DB:          tt.fields.DB,
				FilClient:   tt.fields.FilClient,
				Api:         tt.fields.Api,
				CM:          tt.fields.CM,
				StagingMgr:  tt.fields.StagingMgr,
				gwayHandler: tt.fields.gwayHandler,
				cacher:      tt.fields.cacher,
			}
			tt.wantErr(t, s.handleGetOffloadingCandidates(tt.args.c), fmt.Sprintf("handleGetOffloadingCandidates(%v)", tt.args.c))
		})
	}
}

func TestServer_handleGetProposal(t *testing.T) {
	type fields struct {
		tracer      trace.Tracer
		Node        *node.Node
		DB          *gorm.DB
		FilClient   *filclient.FilClient
		Api         api.Gateway
		CM          *ContentManager
		StagingMgr  *stagingbs.StagingBSMgr
		gwayHandler *gateway.GatewayHandler
		cacher      *memo.Cacher
	}
	type args struct {
		c echo.Context
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr assert.ErrorAssertionFunc
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Server{
				tracer:      tt.fields.tracer,
				Node:        tt.fields.Node,
				DB:          tt.fields.DB,
				FilClient:   tt.fields.FilClient,
				Api:         tt.fields.Api,
				CM:          tt.fields.CM,
				StagingMgr:  tt.fields.StagingMgr,
				gwayHandler: tt.fields.gwayHandler,
				cacher:      tt.fields.cacher,
			}
			tt.wantErr(t, s.handleGetProposal(tt.args.c), fmt.Sprintf("handleGetProposal(%v)", tt.args.c))
		})
	}
}

func TestServer_handleGetPublicNodeInfo(t *testing.T) {
	type fields struct {
		tracer      trace.Tracer
		Node        *node.Node
		DB          *gorm.DB
		FilClient   *filclient.FilClient
		Api         api.Gateway
		CM          *ContentManager
		StagingMgr  *stagingbs.StagingBSMgr
		gwayHandler *gateway.GatewayHandler
		cacher      *memo.Cacher
	}
	type args struct {
		c echo.Context
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr assert.ErrorAssertionFunc
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Server{
				tracer:      tt.fields.tracer,
				Node:        tt.fields.Node,
				DB:          tt.fields.DB,
				FilClient:   tt.fields.FilClient,
				Api:         tt.fields.Api,
				CM:          tt.fields.CM,
				StagingMgr:  tt.fields.StagingMgr,
				gwayHandler: tt.fields.gwayHandler,
				cacher:      tt.fields.cacher,
			}
			tt.wantErr(t, s.handleGetPublicNodeInfo(tt.args.c), fmt.Sprintf("handleGetPublicNodeInfo(%v)", tt.args.c))
		})
	}
}

func TestServer_handleGetRetrievalCandidates(t *testing.T) {
	type fields struct {
		tracer      trace.Tracer
		Node        *node.Node
		DB          *gorm.DB
		FilClient   *filclient.FilClient
		Api         api.Gateway
		CM          *ContentManager
		StagingMgr  *stagingbs.StagingBSMgr
		gwayHandler *gateway.GatewayHandler
		cacher      *memo.Cacher
	}
	type args struct {
		c echo.Context
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr assert.ErrorAssertionFunc
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Server{
				tracer:      tt.fields.tracer,
				Node:        tt.fields.Node,
				DB:          tt.fields.DB,
				FilClient:   tt.fields.FilClient,
				Api:         tt.fields.Api,
				CM:          tt.fields.CM,
				StagingMgr:  tt.fields.StagingMgr,
				gwayHandler: tt.fields.gwayHandler,
				cacher:      tt.fields.cacher,
			}
			tt.wantErr(t, s.handleGetRetrievalCandidates(tt.args.c), fmt.Sprintf("handleGetRetrievalCandidates(%v)", tt.args.c))
		})
	}
}

func TestServer_handleGetRetrievalInfo(t *testing.T) {
	type fields struct {
		tracer      trace.Tracer
		Node        *node.Node
		DB          *gorm.DB
		FilClient   *filclient.FilClient
		Api         api.Gateway
		CM          *ContentManager
		StagingMgr  *stagingbs.StagingBSMgr
		gwayHandler *gateway.GatewayHandler
		cacher      *memo.Cacher
	}
	type args struct {
		c echo.Context
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr assert.ErrorAssertionFunc
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Server{
				tracer:      tt.fields.tracer,
				Node:        tt.fields.Node,
				DB:          tt.fields.DB,
				FilClient:   tt.fields.FilClient,
				Api:         tt.fields.Api,
				CM:          tt.fields.CM,
				StagingMgr:  tt.fields.StagingMgr,
				gwayHandler: tt.fields.gwayHandler,
				cacher:      tt.fields.cacher,
			}
			tt.wantErr(t, s.handleGetRetrievalInfo(tt.args.c), fmt.Sprintf("handleGetRetrievalInfo(%v)", tt.args.c))
		})
	}
}

func TestServer_handleGetStagingZoneForUser(t *testing.T) {
	type fields struct {
		tracer      trace.Tracer
		Node        *node.Node
		DB          *gorm.DB
		FilClient   *filclient.FilClient
		Api         api.Gateway
		CM          *ContentManager
		StagingMgr  *stagingbs.StagingBSMgr
		gwayHandler *gateway.GatewayHandler
		cacher      *memo.Cacher
	}
	type args struct {
		c echo.Context
		u *User
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr assert.ErrorAssertionFunc
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Server{
				tracer:      tt.fields.tracer,
				Node:        tt.fields.Node,
				DB:          tt.fields.DB,
				FilClient:   tt.fields.FilClient,
				Api:         tt.fields.Api,
				CM:          tt.fields.CM,
				StagingMgr:  tt.fields.StagingMgr,
				gwayHandler: tt.fields.gwayHandler,
				cacher:      tt.fields.cacher,
			}
			tt.wantErr(t, s.handleGetStagingZoneForUser(tt.args.c, tt.args.u), fmt.Sprintf("handleGetStagingZoneForUser(%v, %v)", tt.args.c, tt.args.u))
		})
	}
}

func TestServer_handleGetUserStats(t *testing.T) {
	type fields struct {
		tracer      trace.Tracer
		Node        *node.Node
		DB          *gorm.DB
		FilClient   *filclient.FilClient
		Api         api.Gateway
		CM          *ContentManager
		StagingMgr  *stagingbs.StagingBSMgr
		gwayHandler *gateway.GatewayHandler
		cacher      *memo.Cacher
	}
	type args struct {
		c echo.Context
		u *User
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr assert.ErrorAssertionFunc
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Server{
				tracer:      tt.fields.tracer,
				Node:        tt.fields.Node,
				DB:          tt.fields.DB,
				FilClient:   tt.fields.FilClient,
				Api:         tt.fields.Api,
				CM:          tt.fields.CM,
				StagingMgr:  tt.fields.StagingMgr,
				gwayHandler: tt.fields.gwayHandler,
				cacher:      tt.fields.cacher,
			}
			tt.wantErr(t, s.handleGetUserStats(tt.args.c, tt.args.u), fmt.Sprintf("handleGetUserStats(%v, %v)", tt.args.c, tt.args.u))
		})
	}
}

func TestServer_handleGetViewer(t *testing.T) {
	type fields struct {
		tracer      trace.Tracer
		Node        *node.Node
		DB          *gorm.DB
		FilClient   *filclient.FilClient
		Api         api.Gateway
		CM          *ContentManager
		StagingMgr  *stagingbs.StagingBSMgr
		gwayHandler *gateway.GatewayHandler
		cacher      *memo.Cacher
	}
	type args struct {
		c echo.Context
		u *User
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr assert.ErrorAssertionFunc
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Server{
				tracer:      tt.fields.tracer,
				Node:        tt.fields.Node,
				DB:          tt.fields.DB,
				FilClient:   tt.fields.FilClient,
				Api:         tt.fields.Api,
				CM:          tt.fields.CM,
				StagingMgr:  tt.fields.StagingMgr,
				gwayHandler: tt.fields.gwayHandler,
				cacher:      tt.fields.cacher,
			}
			tt.wantErr(t, s.handleGetViewer(tt.args.c, tt.args.u), fmt.Sprintf("handleGetViewer(%v, %v)", tt.args.c, tt.args.u))
		})
	}
}

func TestServer_handleHealth(t *testing.T) {
	type fields struct {
		tracer      trace.Tracer
		Node        *node.Node
		DB          *gorm.DB
		FilClient   *filclient.FilClient
		Api         api.Gateway
		CM          *ContentManager
		StagingMgr  *stagingbs.StagingBSMgr
		gwayHandler *gateway.GatewayHandler
		cacher      *memo.Cacher
	}
	type args struct {
		c echo.Context
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr assert.ErrorAssertionFunc
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Server{
				tracer:      tt.fields.tracer,
				Node:        tt.fields.Node,
				DB:          tt.fields.DB,
				FilClient:   tt.fields.FilClient,
				Api:         tt.fields.Api,
				CM:          tt.fields.CM,
				StagingMgr:  tt.fields.StagingMgr,
				gwayHandler: tt.fields.gwayHandler,
				cacher:      tt.fields.cacher,
			}
			tt.wantErr(t, s.handleHealth(tt.args.c), fmt.Sprintf("handleHealth(%v)", tt.args.c))
		})
	}
}

func TestServer_handleListCollections(t *testing.T) {
	type fields struct {
		tracer      trace.Tracer
		Node        *node.Node
		DB          *gorm.DB
		FilClient   *filclient.FilClient
		Api         api.Gateway
		CM          *ContentManager
		StagingMgr  *stagingbs.StagingBSMgr
		gwayHandler *gateway.GatewayHandler
		cacher      *memo.Cacher
	}
	type args struct {
		c echo.Context
		u *User
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr assert.ErrorAssertionFunc
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Server{
				tracer:      tt.fields.tracer,
				Node:        tt.fields.Node,
				DB:          tt.fields.DB,
				FilClient:   tt.fields.FilClient,
				Api:         tt.fields.Api,
				CM:          tt.fields.CM,
				StagingMgr:  tt.fields.StagingMgr,
				gwayHandler: tt.fields.gwayHandler,
				cacher:      tt.fields.cacher,
			}
			tt.wantErr(t, s.handleListCollections(tt.args.c, tt.args.u), fmt.Sprintf("handleListCollections(%v, %v)", tt.args.c, tt.args.u))
		})
	}
}

func TestServer_handleListContent(t *testing.T) {
	type fields struct {
		tracer      trace.Tracer
		Node        *node.Node
		DB          *gorm.DB
		FilClient   *filclient.FilClient
		Api         api.Gateway
		CM          *ContentManager
		StagingMgr  *stagingbs.StagingBSMgr
		gwayHandler *gateway.GatewayHandler
		cacher      *memo.Cacher
	}
	type args struct {
		c echo.Context
		u *User
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr assert.ErrorAssertionFunc
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Server{
				tracer:      tt.fields.tracer,
				Node:        tt.fields.Node,
				DB:          tt.fields.DB,
				FilClient:   tt.fields.FilClient,
				Api:         tt.fields.Api,
				CM:          tt.fields.CM,
				StagingMgr:  tt.fields.StagingMgr,
				gwayHandler: tt.fields.gwayHandler,
				cacher:      tt.fields.cacher,
			}
			tt.wantErr(t, s.handleListContent(tt.args.c, tt.args.u), fmt.Sprintf("handleListContent(%v, %v)", tt.args.c, tt.args.u))
		})
	}
}

func TestServer_handleListContentWithDeals(t *testing.T) {
	type fields struct {
		tracer      trace.Tracer
		Node        *node.Node
		DB          *gorm.DB
		FilClient   *filclient.FilClient
		Api         api.Gateway
		CM          *ContentManager
		StagingMgr  *stagingbs.StagingBSMgr
		gwayHandler *gateway.GatewayHandler
		cacher      *memo.Cacher
	}
	type args struct {
		c echo.Context
		u *User
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr assert.ErrorAssertionFunc
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Server{
				tracer:      tt.fields.tracer,
				Node:        tt.fields.Node,
				DB:          tt.fields.DB,
				FilClient:   tt.fields.FilClient,
				Api:         tt.fields.Api,
				CM:          tt.fields.CM,
				StagingMgr:  tt.fields.StagingMgr,
				gwayHandler: tt.fields.gwayHandler,
				cacher:      tt.fields.cacher,
			}
			tt.wantErr(t, s.handleListContentWithDeals(tt.args.c, tt.args.u), fmt.Sprintf("handleListContentWithDeals(%v, %v)", tt.args.c, tt.args.u))
		})
	}
}

func TestServer_handleLogLevel(t *testing.T) {
	type fields struct {
		tracer      trace.Tracer
		Node        *node.Node
		DB          *gorm.DB
		FilClient   *filclient.FilClient
		Api         api.Gateway
		CM          *ContentManager
		StagingMgr  *stagingbs.StagingBSMgr
		gwayHandler *gateway.GatewayHandler
		cacher      *memo.Cacher
	}
	type args struct {
		c echo.Context
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr assert.ErrorAssertionFunc
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Server{
				tracer:      tt.fields.tracer,
				Node:        tt.fields.Node,
				DB:          tt.fields.DB,
				FilClient:   tt.fields.FilClient,
				Api:         tt.fields.Api,
				CM:          tt.fields.CM,
				StagingMgr:  tt.fields.StagingMgr,
				gwayHandler: tt.fields.gwayHandler,
				cacher:      tt.fields.cacher,
			}
			tt.wantErr(t, s.handleLogLevel(tt.args.c), fmt.Sprintf("handleLogLevel(%v)", tt.args.c))
		})
	}
}

func TestServer_handleLoginUser(t *testing.T) {
	type fields struct {
		tracer      trace.Tracer
		Node        *node.Node
		DB          *gorm.DB
		FilClient   *filclient.FilClient
		Api         api.Gateway
		CM          *ContentManager
		StagingMgr  *stagingbs.StagingBSMgr
		gwayHandler *gateway.GatewayHandler
		cacher      *memo.Cacher
	}
	type args struct {
		c echo.Context
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr assert.ErrorAssertionFunc
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Server{
				tracer:      tt.fields.tracer,
				Node:        tt.fields.Node,
				DB:          tt.fields.DB,
				FilClient:   tt.fields.FilClient,
				Api:         tt.fields.Api,
				CM:          tt.fields.CM,
				StagingMgr:  tt.fields.StagingMgr,
				gwayHandler: tt.fields.gwayHandler,
				cacher:      tt.fields.cacher,
			}
			tt.wantErr(t, s.handleLoginUser(tt.args.c), fmt.Sprintf("handleLoginUser(%v)", tt.args.c))
		})
	}
}

func TestServer_handleMakeDeal(t *testing.T) {
	type fields struct {
		tracer      trace.Tracer
		Node        *node.Node
		DB          *gorm.DB
		FilClient   *filclient.FilClient
		Api         api.Gateway
		CM          *ContentManager
		StagingMgr  *stagingbs.StagingBSMgr
		gwayHandler *gateway.GatewayHandler
		cacher      *memo.Cacher
	}
	type args struct {
		c echo.Context
		u *User
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr assert.ErrorAssertionFunc
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Server{
				tracer:      tt.fields.tracer,
				Node:        tt.fields.Node,
				DB:          tt.fields.DB,
				FilClient:   tt.fields.FilClient,
				Api:         tt.fields.Api,
				CM:          tt.fields.CM,
				StagingMgr:  tt.fields.StagingMgr,
				gwayHandler: tt.fields.gwayHandler,
				cacher:      tt.fields.cacher,
			}
			tt.wantErr(t, s.handleMakeDeal(tt.args.c, tt.args.u), fmt.Sprintf("handleMakeDeal(%v, %v)", tt.args.c, tt.args.u))
		})
	}
}

func TestServer_handleMetricsDealOnChain(t *testing.T) {
	type fields struct {
		tracer      trace.Tracer
		Node        *node.Node
		DB          *gorm.DB
		FilClient   *filclient.FilClient
		Api         api.Gateway
		CM          *ContentManager
		StagingMgr  *stagingbs.StagingBSMgr
		gwayHandler *gateway.GatewayHandler
		cacher      *memo.Cacher
	}
	type args struct {
		c echo.Context
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr assert.ErrorAssertionFunc
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Server{
				tracer:      tt.fields.tracer,
				Node:        tt.fields.Node,
				DB:          tt.fields.DB,
				FilClient:   tt.fields.FilClient,
				Api:         tt.fields.Api,
				CM:          tt.fields.CM,
				StagingMgr:  tt.fields.StagingMgr,
				gwayHandler: tt.fields.gwayHandler,
				cacher:      tt.fields.cacher,
			}
			tt.wantErr(t, s.handleMetricsDealOnChain(tt.args.c), fmt.Sprintf("handleMetricsDealOnChain(%v)", tt.args.c))
		})
	}
}

func TestServer_handleMinerTransferDiagnostics(t *testing.T) {
	type fields struct {
		tracer      trace.Tracer
		Node        *node.Node
		DB          *gorm.DB
		FilClient   *filclient.FilClient
		Api         api.Gateway
		CM          *ContentManager
		StagingMgr  *stagingbs.StagingBSMgr
		gwayHandler *gateway.GatewayHandler
		cacher      *memo.Cacher
	}
	type args struct {
		c echo.Context
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr assert.ErrorAssertionFunc
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Server{
				tracer:      tt.fields.tracer,
				Node:        tt.fields.Node,
				DB:          tt.fields.DB,
				FilClient:   tt.fields.FilClient,
				Api:         tt.fields.Api,
				CM:          tt.fields.CM,
				StagingMgr:  tt.fields.StagingMgr,
				gwayHandler: tt.fields.gwayHandler,
				cacher:      tt.fields.cacher,
			}
			tt.wantErr(t, s.handleMinerTransferDiagnostics(tt.args.c), fmt.Sprintf("handleMinerTransferDiagnostics(%v)", tt.args.c))
		})
	}
}

func TestServer_handleMinersSetInfo(t *testing.T) {
	type fields struct {
		tracer      trace.Tracer
		Node        *node.Node
		DB          *gorm.DB
		FilClient   *filclient.FilClient
		Api         api.Gateway
		CM          *ContentManager
		StagingMgr  *stagingbs.StagingBSMgr
		gwayHandler *gateway.GatewayHandler
		cacher      *memo.Cacher
	}
	type args struct {
		c echo.Context
		u *User
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr assert.ErrorAssertionFunc
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Server{
				tracer:      tt.fields.tracer,
				Node:        tt.fields.Node,
				DB:          tt.fields.DB,
				FilClient:   tt.fields.FilClient,
				Api:         tt.fields.Api,
				CM:          tt.fields.CM,
				StagingMgr:  tt.fields.StagingMgr,
				gwayHandler: tt.fields.gwayHandler,
				cacher:      tt.fields.cacher,
			}
			tt.wantErr(t, s.handleMinersSetInfo(tt.args.c, tt.args.u), fmt.Sprintf("handleMinersSetInfo(%v, %v)", tt.args.c, tt.args.u))
		})
	}
}

func TestServer_handleMoveContent(t *testing.T) {
	type fields struct {
		tracer      trace.Tracer
		Node        *node.Node
		DB          *gorm.DB
		FilClient   *filclient.FilClient
		Api         api.Gateway
		CM          *ContentManager
		StagingMgr  *stagingbs.StagingBSMgr
		gwayHandler *gateway.GatewayHandler
		cacher      *memo.Cacher
	}
	type args struct {
		c echo.Context
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr assert.ErrorAssertionFunc
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Server{
				tracer:      tt.fields.tracer,
				Node:        tt.fields.Node,
				DB:          tt.fields.DB,
				FilClient:   tt.fields.FilClient,
				Api:         tt.fields.Api,
				CM:          tt.fields.CM,
				StagingMgr:  tt.fields.StagingMgr,
				gwayHandler: tt.fields.gwayHandler,
				cacher:      tt.fields.cacher,
			}
			tt.wantErr(t, s.handleMoveContent(tt.args.c), fmt.Sprintf("handleMoveContent(%v)", tt.args.c))
		})
	}
}

func TestServer_handleNetAddrs(t *testing.T) {
	type fields struct {
		tracer      trace.Tracer
		Node        *node.Node
		DB          *gorm.DB
		FilClient   *filclient.FilClient
		Api         api.Gateway
		CM          *ContentManager
		StagingMgr  *stagingbs.StagingBSMgr
		gwayHandler *gateway.GatewayHandler
		cacher      *memo.Cacher
	}
	type args struct {
		c echo.Context
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr assert.ErrorAssertionFunc
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Server{
				tracer:      tt.fields.tracer,
				Node:        tt.fields.Node,
				DB:          tt.fields.DB,
				FilClient:   tt.fields.FilClient,
				Api:         tt.fields.Api,
				CM:          tt.fields.CM,
				StagingMgr:  tt.fields.StagingMgr,
				gwayHandler: tt.fields.gwayHandler,
				cacher:      tt.fields.cacher,
			}
			tt.wantErr(t, s.handleNetAddrs(tt.args.c), fmt.Sprintf("handleNetAddrs(%v)", tt.args.c))
		})
	}
}

func TestServer_handleNetPeers(t *testing.T) {
	type fields struct {
		tracer      trace.Tracer
		Node        *node.Node
		DB          *gorm.DB
		FilClient   *filclient.FilClient
		Api         api.Gateway
		CM          *ContentManager
		StagingMgr  *stagingbs.StagingBSMgr
		gwayHandler *gateway.GatewayHandler
		cacher      *memo.Cacher
	}
	type args struct {
		c echo.Context
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr assert.ErrorAssertionFunc
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Server{
				tracer:      tt.fields.tracer,
				Node:        tt.fields.Node,
				DB:          tt.fields.DB,
				FilClient:   tt.fields.FilClient,
				Api:         tt.fields.Api,
				CM:          tt.fields.CM,
				StagingMgr:  tt.fields.StagingMgr,
				gwayHandler: tt.fields.gwayHandler,
				cacher:      tt.fields.cacher,
			}
			tt.wantErr(t, s.handleNetPeers(tt.args.c), fmt.Sprintf("handleNetPeers(%v)", tt.args.c))
		})
	}
}

func TestServer_handleOffloadContent(t *testing.T) {
	type fields struct {
		tracer      trace.Tracer
		Node        *node.Node
		DB          *gorm.DB
		FilClient   *filclient.FilClient
		Api         api.Gateway
		CM          *ContentManager
		StagingMgr  *stagingbs.StagingBSMgr
		gwayHandler *gateway.GatewayHandler
		cacher      *memo.Cacher
	}
	type args struct {
		c echo.Context
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr assert.ErrorAssertionFunc
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Server{
				tracer:      tt.fields.tracer,
				Node:        tt.fields.Node,
				DB:          tt.fields.DB,
				FilClient:   tt.fields.FilClient,
				Api:         tt.fields.Api,
				CM:          tt.fields.CM,
				StagingMgr:  tt.fields.StagingMgr,
				gwayHandler: tt.fields.gwayHandler,
				cacher:      tt.fields.cacher,
			}
			tt.wantErr(t, s.handleOffloadContent(tt.args.c), fmt.Sprintf("handleOffloadContent(%v)", tt.args.c))
		})
	}
}

func TestServer_handlePublicGetMinerStats(t *testing.T) {
	type fields struct {
		tracer      trace.Tracer
		Node        *node.Node
		DB          *gorm.DB
		FilClient   *filclient.FilClient
		Api         api.Gateway
		CM          *ContentManager
		StagingMgr  *stagingbs.StagingBSMgr
		gwayHandler *gateway.GatewayHandler
		cacher      *memo.Cacher
	}
	type args struct {
		c echo.Context
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr assert.ErrorAssertionFunc
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Server{
				tracer:      tt.fields.tracer,
				Node:        tt.fields.Node,
				DB:          tt.fields.DB,
				FilClient:   tt.fields.FilClient,
				Api:         tt.fields.Api,
				CM:          tt.fields.CM,
				StagingMgr:  tt.fields.StagingMgr,
				gwayHandler: tt.fields.gwayHandler,
				cacher:      tt.fields.cacher,
			}
			tt.wantErr(t, s.handlePublicGetMinerStats(tt.args.c), fmt.Sprintf("handlePublicGetMinerStats(%v)", tt.args.c))
		})
	}
}

func TestServer_handlePublicStats(t *testing.T) {
	type fields struct {
		tracer      trace.Tracer
		Node        *node.Node
		DB          *gorm.DB
		FilClient   *filclient.FilClient
		Api         api.Gateway
		CM          *ContentManager
		StagingMgr  *stagingbs.StagingBSMgr
		gwayHandler *gateway.GatewayHandler
		cacher      *memo.Cacher
	}
	type args struct {
		c echo.Context
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr assert.ErrorAssertionFunc
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Server{
				tracer:      tt.fields.tracer,
				Node:        tt.fields.Node,
				DB:          tt.fields.DB,
				FilClient:   tt.fields.FilClient,
				Api:         tt.fields.Api,
				CM:          tt.fields.CM,
				StagingMgr:  tt.fields.StagingMgr,
				gwayHandler: tt.fields.gwayHandler,
				cacher:      tt.fields.cacher,
			}
			tt.wantErr(t, s.handlePublicStats(tt.args.c), fmt.Sprintf("handlePublicStats(%v)", tt.args.c))
		})
	}
}

func TestServer_handleQueryAsk(t *testing.T) {
	type fields struct {
		tracer      trace.Tracer
		Node        *node.Node
		DB          *gorm.DB
		FilClient   *filclient.FilClient
		Api         api.Gateway
		CM          *ContentManager
		StagingMgr  *stagingbs.StagingBSMgr
		gwayHandler *gateway.GatewayHandler
		cacher      *memo.Cacher
	}
	type args struct {
		c echo.Context
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr assert.ErrorAssertionFunc
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Server{
				tracer:      tt.fields.tracer,
				Node:        tt.fields.Node,
				DB:          tt.fields.DB,
				FilClient:   tt.fields.FilClient,
				Api:         tt.fields.Api,
				CM:          tt.fields.CM,
				StagingMgr:  tt.fields.StagingMgr,
				gwayHandler: tt.fields.gwayHandler,
				cacher:      tt.fields.cacher,
			}
			tt.wantErr(t, s.handleQueryAsk(tt.args.c), fmt.Sprintf("handleQueryAsk(%v)", tt.args.c))
		})
	}
}

func TestServer_handleReadLocalContent(t *testing.T) {
	type fields struct {
		tracer      trace.Tracer
		Node        *node.Node
		DB          *gorm.DB
		FilClient   *filclient.FilClient
		Api         api.Gateway
		CM          *ContentManager
		StagingMgr  *stagingbs.StagingBSMgr
		gwayHandler *gateway.GatewayHandler
		cacher      *memo.Cacher
	}
	type args struct {
		c echo.Context
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr assert.ErrorAssertionFunc
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Server{
				tracer:      tt.fields.tracer,
				Node:        tt.fields.Node,
				DB:          tt.fields.DB,
				FilClient:   tt.fields.FilClient,
				Api:         tt.fields.Api,
				CM:          tt.fields.CM,
				StagingMgr:  tt.fields.StagingMgr,
				gwayHandler: tt.fields.gwayHandler,
				cacher:      tt.fields.cacher,
			}
			tt.wantErr(t, s.handleReadLocalContent(tt.args.c), fmt.Sprintf("handleReadLocalContent(%v)", tt.args.c))
		})
	}
}

func TestServer_handleRefreshContent(t *testing.T) {
	type fields struct {
		tracer      trace.Tracer
		Node        *node.Node
		DB          *gorm.DB
		FilClient   *filclient.FilClient
		Api         api.Gateway
		CM          *ContentManager
		StagingMgr  *stagingbs.StagingBSMgr
		gwayHandler *gateway.GatewayHandler
		cacher      *memo.Cacher
	}
	type args struct {
		c echo.Context
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr assert.ErrorAssertionFunc
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Server{
				tracer:      tt.fields.tracer,
				Node:        tt.fields.Node,
				DB:          tt.fields.DB,
				FilClient:   tt.fields.FilClient,
				Api:         tt.fields.Api,
				CM:          tt.fields.CM,
				StagingMgr:  tt.fields.StagingMgr,
				gwayHandler: tt.fields.gwayHandler,
				cacher:      tt.fields.cacher,
			}
			tt.wantErr(t, s.handleRefreshContent(tt.args.c), fmt.Sprintf("handleRefreshContent(%v)", tt.args.c))
		})
	}
}

func TestServer_handleRegisterUser(t *testing.T) {
	type fields struct {
		tracer      trace.Tracer
		Node        *node.Node
		DB          *gorm.DB
		FilClient   *filclient.FilClient
		Api         api.Gateway
		CM          *ContentManager
		StagingMgr  *stagingbs.StagingBSMgr
		gwayHandler *gateway.GatewayHandler
		cacher      *memo.Cacher
	}
	type args struct {
		c echo.Context
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr assert.ErrorAssertionFunc
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Server{
				tracer:      tt.fields.tracer,
				Node:        tt.fields.Node,
				DB:          tt.fields.DB,
				FilClient:   tt.fields.FilClient,
				Api:         tt.fields.Api,
				CM:          tt.fields.CM,
				StagingMgr:  tt.fields.StagingMgr,
				gwayHandler: tt.fields.gwayHandler,
				cacher:      tt.fields.cacher,
			}
			tt.wantErr(t, s.handleRegisterUser(tt.args.c), fmt.Sprintf("handleRegisterUser(%v)", tt.args.c))
		})
	}
}

func TestServer_handleRetrievalCheck(t *testing.T) {
	type fields struct {
		tracer      trace.Tracer
		Node        *node.Node
		DB          *gorm.DB
		FilClient   *filclient.FilClient
		Api         api.Gateway
		CM          *ContentManager
		StagingMgr  *stagingbs.StagingBSMgr
		gwayHandler *gateway.GatewayHandler
		cacher      *memo.Cacher
	}
	type args struct {
		c echo.Context
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr assert.ErrorAssertionFunc
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Server{
				tracer:      tt.fields.tracer,
				Node:        tt.fields.Node,
				DB:          tt.fields.DB,
				FilClient:   tt.fields.FilClient,
				Api:         tt.fields.Api,
				CM:          tt.fields.CM,
				StagingMgr:  tt.fields.StagingMgr,
				gwayHandler: tt.fields.gwayHandler,
				cacher:      tt.fields.cacher,
			}
			tt.wantErr(t, s.handleRetrievalCheck(tt.args.c), fmt.Sprintf("handleRetrievalCheck(%v)", tt.args.c))
		})
	}
}

func TestServer_handleRunGc(t *testing.T) {
	type fields struct {
		tracer      trace.Tracer
		Node        *node.Node
		DB          *gorm.DB
		FilClient   *filclient.FilClient
		Api         api.Gateway
		CM          *ContentManager
		StagingMgr  *stagingbs.StagingBSMgr
		gwayHandler *gateway.GatewayHandler
		cacher      *memo.Cacher
	}
	type args struct {
		c echo.Context
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr assert.ErrorAssertionFunc
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Server{
				tracer:      tt.fields.tracer,
				Node:        tt.fields.Node,
				DB:          tt.fields.DB,
				FilClient:   tt.fields.FilClient,
				Api:         tt.fields.Api,
				CM:          tt.fields.CM,
				StagingMgr:  tt.fields.StagingMgr,
				gwayHandler: tt.fields.gwayHandler,
				cacher:      tt.fields.cacher,
			}
			tt.wantErr(t, s.handleRunGc(tt.args.c), fmt.Sprintf("handleRunGc(%v)", tt.args.c))
		})
	}
}

func TestServer_handleRunOffloadingCollection(t *testing.T) {
	type fields struct {
		tracer      trace.Tracer
		Node        *node.Node
		DB          *gorm.DB
		FilClient   *filclient.FilClient
		Api         api.Gateway
		CM          *ContentManager
		StagingMgr  *stagingbs.StagingBSMgr
		gwayHandler *gateway.GatewayHandler
		cacher      *memo.Cacher
	}
	type args struct {
		c echo.Context
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr assert.ErrorAssertionFunc
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Server{
				tracer:      tt.fields.tracer,
				Node:        tt.fields.Node,
				DB:          tt.fields.DB,
				FilClient:   tt.fields.FilClient,
				Api:         tt.fields.Api,
				CM:          tt.fields.CM,
				StagingMgr:  tt.fields.StagingMgr,
				gwayHandler: tt.fields.gwayHandler,
				cacher:      tt.fields.cacher,
			}
			tt.wantErr(t, s.handleRunOffloadingCollection(tt.args.c), fmt.Sprintf("handleRunOffloadingCollection(%v)", tt.args.c))
		})
	}
}

func TestServer_handleSetDealMaking(t *testing.T) {
	type fields struct {
		tracer      trace.Tracer
		Node        *node.Node
		DB          *gorm.DB
		FilClient   *filclient.FilClient
		Api         api.Gateway
		CM          *ContentManager
		StagingMgr  *stagingbs.StagingBSMgr
		gwayHandler *gateway.GatewayHandler
		cacher      *memo.Cacher
	}
	type args struct {
		c echo.Context
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr assert.ErrorAssertionFunc
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Server{
				tracer:      tt.fields.tracer,
				Node:        tt.fields.Node,
				DB:          tt.fields.DB,
				FilClient:   tt.fields.FilClient,
				Api:         tt.fields.Api,
				CM:          tt.fields.CM,
				StagingMgr:  tt.fields.StagingMgr,
				gwayHandler: tt.fields.gwayHandler,
				cacher:      tt.fields.cacher,
			}
			tt.wantErr(t, s.handleSetDealMaking(tt.args.c), fmt.Sprintf("handleSetDealMaking(%v)", tt.args.c))
		})
	}
}

func TestServer_handleShuttleConnection(t *testing.T) {
	type fields struct {
		tracer      trace.Tracer
		Node        *node.Node
		DB          *gorm.DB
		FilClient   *filclient.FilClient
		Api         api.Gateway
		CM          *ContentManager
		StagingMgr  *stagingbs.StagingBSMgr
		gwayHandler *gateway.GatewayHandler
		cacher      *memo.Cacher
	}
	type args struct {
		c echo.Context
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr assert.ErrorAssertionFunc
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Server{
				tracer:      tt.fields.tracer,
				Node:        tt.fields.Node,
				DB:          tt.fields.DB,
				FilClient:   tt.fields.FilClient,
				Api:         tt.fields.Api,
				CM:          tt.fields.CM,
				StagingMgr:  tt.fields.StagingMgr,
				gwayHandler: tt.fields.gwayHandler,
				cacher:      tt.fields.cacher,
			}
			tt.wantErr(t, s.handleShuttleConnection(tt.args.c), fmt.Sprintf("handleShuttleConnection(%v)", tt.args.c))
		})
	}
}

func TestServer_handleShuttleCreateContent(t *testing.T) {
	type fields struct {
		tracer      trace.Tracer
		Node        *node.Node
		DB          *gorm.DB
		FilClient   *filclient.FilClient
		Api         api.Gateway
		CM          *ContentManager
		StagingMgr  *stagingbs.StagingBSMgr
		gwayHandler *gateway.GatewayHandler
		cacher      *memo.Cacher
	}
	type args struct {
		c echo.Context
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr assert.ErrorAssertionFunc
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Server{
				tracer:      tt.fields.tracer,
				Node:        tt.fields.Node,
				DB:          tt.fields.DB,
				FilClient:   tt.fields.FilClient,
				Api:         tt.fields.Api,
				CM:          tt.fields.CM,
				StagingMgr:  tt.fields.StagingMgr,
				gwayHandler: tt.fields.gwayHandler,
				cacher:      tt.fields.cacher,
			}
			tt.wantErr(t, s.handleShuttleCreateContent(tt.args.c), fmt.Sprintf("handleShuttleCreateContent(%v)", tt.args.c))
		})
	}
}

func TestServer_handleShuttleInit(t *testing.T) {
	type fields struct {
		tracer      trace.Tracer
		Node        *node.Node
		DB          *gorm.DB
		FilClient   *filclient.FilClient
		Api         api.Gateway
		CM          *ContentManager
		StagingMgr  *stagingbs.StagingBSMgr
		gwayHandler *gateway.GatewayHandler
		cacher      *memo.Cacher
	}
	type args struct {
		c echo.Context
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr assert.ErrorAssertionFunc
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Server{
				tracer:      tt.fields.tracer,
				Node:        tt.fields.Node,
				DB:          tt.fields.DB,
				FilClient:   tt.fields.FilClient,
				Api:         tt.fields.Api,
				CM:          tt.fields.CM,
				StagingMgr:  tt.fields.StagingMgr,
				gwayHandler: tt.fields.gwayHandler,
				cacher:      tt.fields.cacher,
			}
			tt.wantErr(t, s.handleShuttleInit(tt.args.c), fmt.Sprintf("handleShuttleInit(%v)", tt.args.c))
		})
	}
}

func TestServer_handleShuttleList(t *testing.T) {
	type fields struct {
		tracer      trace.Tracer
		Node        *node.Node
		DB          *gorm.DB
		FilClient   *filclient.FilClient
		Api         api.Gateway
		CM          *ContentManager
		StagingMgr  *stagingbs.StagingBSMgr
		gwayHandler *gateway.GatewayHandler
		cacher      *memo.Cacher
	}
	type args struct {
		c echo.Context
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr assert.ErrorAssertionFunc
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Server{
				tracer:      tt.fields.tracer,
				Node:        tt.fields.Node,
				DB:          tt.fields.DB,
				FilClient:   tt.fields.FilClient,
				Api:         tt.fields.Api,
				CM:          tt.fields.CM,
				StagingMgr:  tt.fields.StagingMgr,
				gwayHandler: tt.fields.gwayHandler,
				cacher:      tt.fields.cacher,
			}
			tt.wantErr(t, s.handleShuttleList(tt.args.c), fmt.Sprintf("handleShuttleList(%v)", tt.args.c))
		})
	}
}

func TestServer_handleShuttleRepinAll(t *testing.T) {
	type fields struct {
		tracer      trace.Tracer
		Node        *node.Node
		DB          *gorm.DB
		FilClient   *filclient.FilClient
		Api         api.Gateway
		CM          *ContentManager
		StagingMgr  *stagingbs.StagingBSMgr
		gwayHandler *gateway.GatewayHandler
		cacher      *memo.Cacher
	}
	type args struct {
		c echo.Context
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr assert.ErrorAssertionFunc
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Server{
				tracer:      tt.fields.tracer,
				Node:        tt.fields.Node,
				DB:          tt.fields.DB,
				FilClient:   tt.fields.FilClient,
				Api:         tt.fields.Api,
				CM:          tt.fields.CM,
				StagingMgr:  tt.fields.StagingMgr,
				gwayHandler: tt.fields.gwayHandler,
				cacher:      tt.fields.cacher,
			}
			tt.wantErr(t, s.handleShuttleRepinAll(tt.args.c), fmt.Sprintf("handleShuttleRepinAll(%v)", tt.args.c))
		})
	}
}

func TestServer_handleStats(t *testing.T) {
	type fields struct {
		tracer      trace.Tracer
		Node        *node.Node
		DB          *gorm.DB
		FilClient   *filclient.FilClient
		Api         api.Gateway
		CM          *ContentManager
		StagingMgr  *stagingbs.StagingBSMgr
		gwayHandler *gateway.GatewayHandler
		cacher      *memo.Cacher
	}
	type args struct {
		c echo.Context
		u *User
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr assert.ErrorAssertionFunc
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Server{
				tracer:      tt.fields.tracer,
				Node:        tt.fields.Node,
				DB:          tt.fields.DB,
				FilClient:   tt.fields.FilClient,
				Api:         tt.fields.Api,
				CM:          tt.fields.CM,
				StagingMgr:  tt.fields.StagingMgr,
				gwayHandler: tt.fields.gwayHandler,
				cacher:      tt.fields.cacher,
			}
			tt.wantErr(t, s.handleStats(tt.args.c, tt.args.u), fmt.Sprintf("handleStats(%v, %v)", tt.args.c, tt.args.u))
		})
	}
}

func TestServer_handleStorageFailures(t *testing.T) {
	type fields struct {
		tracer      trace.Tracer
		Node        *node.Node
		DB          *gorm.DB
		FilClient   *filclient.FilClient
		Api         api.Gateway
		CM          *ContentManager
		StagingMgr  *stagingbs.StagingBSMgr
		gwayHandler *gateway.GatewayHandler
		cacher      *memo.Cacher
	}
	type args struct {
		c echo.Context
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr assert.ErrorAssertionFunc
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Server{
				tracer:      tt.fields.tracer,
				Node:        tt.fields.Node,
				DB:          tt.fields.DB,
				FilClient:   tt.fields.FilClient,
				Api:         tt.fields.Api,
				CM:          tt.fields.CM,
				StagingMgr:  tt.fields.StagingMgr,
				gwayHandler: tt.fields.gwayHandler,
				cacher:      tt.fields.cacher,
			}
			tt.wantErr(t, s.handleStorageFailures(tt.args.c), fmt.Sprintf("handleStorageFailures(%v)", tt.args.c))
		})
	}
}

func TestServer_handleSuspendMiner(t *testing.T) {
	type fields struct {
		tracer      trace.Tracer
		Node        *node.Node
		DB          *gorm.DB
		FilClient   *filclient.FilClient
		Api         api.Gateway
		CM          *ContentManager
		StagingMgr  *stagingbs.StagingBSMgr
		gwayHandler *gateway.GatewayHandler
		cacher      *memo.Cacher
	}
	type args struct {
		c echo.Context
		u *User
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr assert.ErrorAssertionFunc
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Server{
				tracer:      tt.fields.tracer,
				Node:        tt.fields.Node,
				DB:          tt.fields.DB,
				FilClient:   tt.fields.FilClient,
				Api:         tt.fields.Api,
				CM:          tt.fields.CM,
				StagingMgr:  tt.fields.StagingMgr,
				gwayHandler: tt.fields.gwayHandler,
				cacher:      tt.fields.cacher,
			}
			tt.wantErr(t, s.handleSuspendMiner(tt.args.c, tt.args.u), fmt.Sprintf("handleSuspendMiner(%v, %v)", tt.args.c, tt.args.u))
		})
	}
}

func TestServer_handleTestError(t *testing.T) {
	type fields struct {
		tracer      trace.Tracer
		Node        *node.Node
		DB          *gorm.DB
		FilClient   *filclient.FilClient
		Api         api.Gateway
		CM          *ContentManager
		StagingMgr  *stagingbs.StagingBSMgr
		gwayHandler *gateway.GatewayHandler
		cacher      *memo.Cacher
	}
	type args struct {
		c echo.Context
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr assert.ErrorAssertionFunc
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Server{
				tracer:      tt.fields.tracer,
				Node:        tt.fields.Node,
				DB:          tt.fields.DB,
				FilClient:   tt.fields.FilClient,
				Api:         tt.fields.Api,
				CM:          tt.fields.CM,
				StagingMgr:  tt.fields.StagingMgr,
				gwayHandler: tt.fields.gwayHandler,
				cacher:      tt.fields.cacher,
			}
			tt.wantErr(t, s.handleTestError(tt.args.c), fmt.Sprintf("handleTestError(%v)", tt.args.c))
		})
	}
}

func TestServer_handleTransferInProgress(t *testing.T) {
	type fields struct {
		tracer      trace.Tracer
		Node        *node.Node
		DB          *gorm.DB
		FilClient   *filclient.FilClient
		Api         api.Gateway
		CM          *ContentManager
		StagingMgr  *stagingbs.StagingBSMgr
		gwayHandler *gateway.GatewayHandler
		cacher      *memo.Cacher
	}
	type args struct {
		c echo.Context
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr assert.ErrorAssertionFunc
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Server{
				tracer:      tt.fields.tracer,
				Node:        tt.fields.Node,
				DB:          tt.fields.DB,
				FilClient:   tt.fields.FilClient,
				Api:         tt.fields.Api,
				CM:          tt.fields.CM,
				StagingMgr:  tt.fields.StagingMgr,
				gwayHandler: tt.fields.gwayHandler,
				cacher:      tt.fields.cacher,
			}
			tt.wantErr(t, s.handleTransferInProgress(tt.args.c), fmt.Sprintf("handleTransferInProgress(%v)", tt.args.c))
		})
	}
}

func TestServer_handleTransferRestart(t *testing.T) {
	type fields struct {
		tracer      trace.Tracer
		Node        *node.Node
		DB          *gorm.DB
		FilClient   *filclient.FilClient
		Api         api.Gateway
		CM          *ContentManager
		StagingMgr  *stagingbs.StagingBSMgr
		gwayHandler *gateway.GatewayHandler
		cacher      *memo.Cacher
	}
	type args struct {
		c echo.Context
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr assert.ErrorAssertionFunc
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Server{
				tracer:      tt.fields.tracer,
				Node:        tt.fields.Node,
				DB:          tt.fields.DB,
				FilClient:   tt.fields.FilClient,
				Api:         tt.fields.Api,
				CM:          tt.fields.CM,
				StagingMgr:  tt.fields.StagingMgr,
				gwayHandler: tt.fields.gwayHandler,
				cacher:      tt.fields.cacher,
			}
			tt.wantErr(t, s.handleTransferRestart(tt.args.c), fmt.Sprintf("handleTransferRestart(%v)", tt.args.c))
		})
	}
}

func TestServer_handleTransferStart(t *testing.T) {
	type fields struct {
		tracer      trace.Tracer
		Node        *node.Node
		DB          *gorm.DB
		FilClient   *filclient.FilClient
		Api         api.Gateway
		CM          *ContentManager
		StagingMgr  *stagingbs.StagingBSMgr
		gwayHandler *gateway.GatewayHandler
		cacher      *memo.Cacher
	}
	type args struct {
		c echo.Context
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr assert.ErrorAssertionFunc
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Server{
				tracer:      tt.fields.tracer,
				Node:        tt.fields.Node,
				DB:          tt.fields.DB,
				FilClient:   tt.fields.FilClient,
				Api:         tt.fields.Api,
				CM:          tt.fields.CM,
				StagingMgr:  tt.fields.StagingMgr,
				gwayHandler: tt.fields.gwayHandler,
				cacher:      tt.fields.cacher,
			}
			tt.wantErr(t, s.handleTransferStart(tt.args.c), fmt.Sprintf("handleTransferStart(%v)", tt.args.c))
		})
	}
}

func TestServer_handleTransferStatus(t *testing.T) {
	type fields struct {
		tracer      trace.Tracer
		Node        *node.Node
		DB          *gorm.DB
		FilClient   *filclient.FilClient
		Api         api.Gateway
		CM          *ContentManager
		StagingMgr  *stagingbs.StagingBSMgr
		gwayHandler *gateway.GatewayHandler
		cacher      *memo.Cacher
	}
	type args struct {
		c echo.Context
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr assert.ErrorAssertionFunc
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Server{
				tracer:      tt.fields.tracer,
				Node:        tt.fields.Node,
				DB:          tt.fields.DB,
				FilClient:   tt.fields.FilClient,
				Api:         tt.fields.Api,
				CM:          tt.fields.CM,
				StagingMgr:  tt.fields.StagingMgr,
				gwayHandler: tt.fields.gwayHandler,
				cacher:      tt.fields.cacher,
			}
			tt.wantErr(t, s.handleTransferStatus(tt.args.c), fmt.Sprintf("handleTransferStatus(%v)", tt.args.c))
		})
	}
}

func TestServer_handleTransferStatusByID(t *testing.T) {
	type fields struct {
		tracer      trace.Tracer
		Node        *node.Node
		DB          *gorm.DB
		FilClient   *filclient.FilClient
		Api         api.Gateway
		CM          *ContentManager
		StagingMgr  *stagingbs.StagingBSMgr
		gwayHandler *gateway.GatewayHandler
		cacher      *memo.Cacher
	}
	type args struct {
		c echo.Context
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr assert.ErrorAssertionFunc
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Server{
				tracer:      tt.fields.tracer,
				Node:        tt.fields.Node,
				DB:          tt.fields.DB,
				FilClient:   tt.fields.FilClient,
				Api:         tt.fields.Api,
				CM:          tt.fields.CM,
				StagingMgr:  tt.fields.StagingMgr,
				gwayHandler: tt.fields.gwayHandler,
				cacher:      tt.fields.cacher,
			}
			tt.wantErr(t, s.handleTransferStatusByID(tt.args.c), fmt.Sprintf("handleTransferStatusByID(%v)", tt.args.c))
		})
	}
}

func TestServer_handleUnsuspendMiner(t *testing.T) {
	type fields struct {
		tracer      trace.Tracer
		Node        *node.Node
		DB          *gorm.DB
		FilClient   *filclient.FilClient
		Api         api.Gateway
		CM          *ContentManager
		StagingMgr  *stagingbs.StagingBSMgr
		gwayHandler *gateway.GatewayHandler
		cacher      *memo.Cacher
	}
	type args struct {
		c echo.Context
		u *User
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr assert.ErrorAssertionFunc
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Server{
				tracer:      tt.fields.tracer,
				Node:        tt.fields.Node,
				DB:          tt.fields.DB,
				FilClient:   tt.fields.FilClient,
				Api:         tt.fields.Api,
				CM:          tt.fields.CM,
				StagingMgr:  tt.fields.StagingMgr,
				gwayHandler: tt.fields.gwayHandler,
				cacher:      tt.fields.cacher,
			}
			tt.wantErr(t, s.handleUnsuspendMiner(tt.args.c, tt.args.u), fmt.Sprintf("handleUnsuspendMiner(%v, %v)", tt.args.c, tt.args.u))
		})
	}
}

func TestServer_handleUserChangeAddress(t *testing.T) {
	type fields struct {
		tracer      trace.Tracer
		Node        *node.Node
		DB          *gorm.DB
		FilClient   *filclient.FilClient
		Api         api.Gateway
		CM          *ContentManager
		StagingMgr  *stagingbs.StagingBSMgr
		gwayHandler *gateway.GatewayHandler
		cacher      *memo.Cacher
	}
	type args struct {
		c echo.Context
		u *User
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr assert.ErrorAssertionFunc
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Server{
				tracer:      tt.fields.tracer,
				Node:        tt.fields.Node,
				DB:          tt.fields.DB,
				FilClient:   tt.fields.FilClient,
				Api:         tt.fields.Api,
				CM:          tt.fields.CM,
				StagingMgr:  tt.fields.StagingMgr,
				gwayHandler: tt.fields.gwayHandler,
				cacher:      tt.fields.cacher,
			}
			tt.wantErr(t, s.handleUserChangeAddress(tt.args.c, tt.args.u), fmt.Sprintf("handleUserChangeAddress(%v, %v)", tt.args.c, tt.args.u))
		})
	}
}

func TestServer_handleUserChangePassword(t *testing.T) {
	type fields struct {
		tracer      trace.Tracer
		Node        *node.Node
		DB          *gorm.DB
		FilClient   *filclient.FilClient
		Api         api.Gateway
		CM          *ContentManager
		StagingMgr  *stagingbs.StagingBSMgr
		gwayHandler *gateway.GatewayHandler
		cacher      *memo.Cacher
	}
	type args struct {
		c echo.Context
		u *User
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr assert.ErrorAssertionFunc
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Server{
				tracer:      tt.fields.tracer,
				Node:        tt.fields.Node,
				DB:          tt.fields.DB,
				FilClient:   tt.fields.FilClient,
				Api:         tt.fields.Api,
				CM:          tt.fields.CM,
				StagingMgr:  tt.fields.StagingMgr,
				gwayHandler: tt.fields.gwayHandler,
				cacher:      tt.fields.cacher,
			}
			tt.wantErr(t, s.handleUserChangePassword(tt.args.c, tt.args.u), fmt.Sprintf("handleUserChangePassword(%v, %v)", tt.args.c, tt.args.u))
		})
	}
}

func TestServer_handleUserClaimMiner(t *testing.T) {
	type fields struct {
		tracer      trace.Tracer
		Node        *node.Node
		DB          *gorm.DB
		FilClient   *filclient.FilClient
		Api         api.Gateway
		CM          *ContentManager
		StagingMgr  *stagingbs.StagingBSMgr
		gwayHandler *gateway.GatewayHandler
		cacher      *memo.Cacher
	}
	type args struct {
		c echo.Context
		u *User
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr assert.ErrorAssertionFunc
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Server{
				tracer:      tt.fields.tracer,
				Node:        tt.fields.Node,
				DB:          tt.fields.DB,
				FilClient:   tt.fields.FilClient,
				Api:         tt.fields.Api,
				CM:          tt.fields.CM,
				StagingMgr:  tt.fields.StagingMgr,
				gwayHandler: tt.fields.gwayHandler,
				cacher:      tt.fields.cacher,
			}
			tt.wantErr(t, s.handleUserClaimMiner(tt.args.c, tt.args.u), fmt.Sprintf("handleUserClaimMiner(%v, %v)", tt.args.c, tt.args.u))
		})
	}
}

func TestServer_handleUserCreateApiKey(t *testing.T) {
	type fields struct {
		tracer      trace.Tracer
		Node        *node.Node
		DB          *gorm.DB
		FilClient   *filclient.FilClient
		Api         api.Gateway
		CM          *ContentManager
		StagingMgr  *stagingbs.StagingBSMgr
		gwayHandler *gateway.GatewayHandler
		cacher      *memo.Cacher
	}
	type args struct {
		c echo.Context
		u *User
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr assert.ErrorAssertionFunc
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Server{
				tracer:      tt.fields.tracer,
				Node:        tt.fields.Node,
				DB:          tt.fields.DB,
				FilClient:   tt.fields.FilClient,
				Api:         tt.fields.Api,
				CM:          tt.fields.CM,
				StagingMgr:  tt.fields.StagingMgr,
				gwayHandler: tt.fields.gwayHandler,
				cacher:      tt.fields.cacher,
			}
			tt.wantErr(t, s.handleUserCreateApiKey(tt.args.c, tt.args.u), fmt.Sprintf("handleUserCreateApiKey(%v, %v)", tt.args.c, tt.args.u))
		})
	}
}

func TestServer_handleUserExportData(t *testing.T) {
	type fields struct {
		tracer      trace.Tracer
		Node        *node.Node
		DB          *gorm.DB
		FilClient   *filclient.FilClient
		Api         api.Gateway
		CM          *ContentManager
		StagingMgr  *stagingbs.StagingBSMgr
		gwayHandler *gateway.GatewayHandler
		cacher      *memo.Cacher
	}
	type args struct {
		c echo.Context
		u *User
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr assert.ErrorAssertionFunc
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Server{
				tracer:      tt.fields.tracer,
				Node:        tt.fields.Node,
				DB:          tt.fields.DB,
				FilClient:   tt.fields.FilClient,
				Api:         tt.fields.Api,
				CM:          tt.fields.CM,
				StagingMgr:  tt.fields.StagingMgr,
				gwayHandler: tt.fields.gwayHandler,
				cacher:      tt.fields.cacher,
			}
			tt.wantErr(t, s.handleUserExportData(tt.args.c, tt.args.u), fmt.Sprintf("handleUserExportData(%v, %v)", tt.args.c, tt.args.u))
		})
	}
}

func TestServer_handleUserGetApiKeys(t *testing.T) {
	type fields struct {
		tracer      trace.Tracer
		Node        *node.Node
		DB          *gorm.DB
		FilClient   *filclient.FilClient
		Api         api.Gateway
		CM          *ContentManager
		StagingMgr  *stagingbs.StagingBSMgr
		gwayHandler *gateway.GatewayHandler
		cacher      *memo.Cacher
	}
	type args struct {
		c echo.Context
		u *User
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr assert.ErrorAssertionFunc
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Server{
				tracer:      tt.fields.tracer,
				Node:        tt.fields.Node,
				DB:          tt.fields.DB,
				FilClient:   tt.fields.FilClient,
				Api:         tt.fields.Api,
				CM:          tt.fields.CM,
				StagingMgr:  tt.fields.StagingMgr,
				gwayHandler: tt.fields.gwayHandler,
				cacher:      tt.fields.cacher,
			}
			tt.wantErr(t, s.handleUserGetApiKeys(tt.args.c, tt.args.u), fmt.Sprintf("handleUserGetApiKeys(%v, %v)", tt.args.c, tt.args.u))
		})
	}
}

func TestServer_handleUserGetClaimMinerMsg(t *testing.T) {
	type fields struct {
		tracer      trace.Tracer
		Node        *node.Node
		DB          *gorm.DB
		FilClient   *filclient.FilClient
		Api         api.Gateway
		CM          *ContentManager
		StagingMgr  *stagingbs.StagingBSMgr
		gwayHandler *gateway.GatewayHandler
		cacher      *memo.Cacher
	}
	type args struct {
		c echo.Context
		u *User
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr assert.ErrorAssertionFunc
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Server{
				tracer:      tt.fields.tracer,
				Node:        tt.fields.Node,
				DB:          tt.fields.DB,
				FilClient:   tt.fields.FilClient,
				Api:         tt.fields.Api,
				CM:          tt.fields.CM,
				StagingMgr:  tt.fields.StagingMgr,
				gwayHandler: tt.fields.gwayHandler,
				cacher:      tt.fields.cacher,
			}
			tt.wantErr(t, s.handleUserGetClaimMinerMsg(tt.args.c, tt.args.u), fmt.Sprintf("handleUserGetClaimMinerMsg(%v, %v)", tt.args.c, tt.args.u))
		})
	}
}

func TestServer_handleUserRevokeApiKey(t *testing.T) {
	type fields struct {
		tracer      trace.Tracer
		Node        *node.Node
		DB          *gorm.DB
		FilClient   *filclient.FilClient
		Api         api.Gateway
		CM          *ContentManager
		StagingMgr  *stagingbs.StagingBSMgr
		gwayHandler *gateway.GatewayHandler
		cacher      *memo.Cacher
	}
	type args struct {
		c echo.Context
		u *User
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr assert.ErrorAssertionFunc
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Server{
				tracer:      tt.fields.tracer,
				Node:        tt.fields.Node,
				DB:          tt.fields.DB,
				FilClient:   tt.fields.FilClient,
				Api:         tt.fields.Api,
				CM:          tt.fields.CM,
				StagingMgr:  tt.fields.StagingMgr,
				gwayHandler: tt.fields.gwayHandler,
				cacher:      tt.fields.cacher,
			}
			tt.wantErr(t, s.handleUserRevokeApiKey(tt.args.c, tt.args.u), fmt.Sprintf("handleUserRevokeApiKey(%v, %v)", tt.args.c, tt.args.u))
		})
	}
}

func TestServer_importFile(t *testing.T) {
	type fields struct {
		tracer      trace.Tracer
		Node        *node.Node
		DB          *gorm.DB
		FilClient   *filclient.FilClient
		Api         api.Gateway
		CM          *ContentManager
		StagingMgr  *stagingbs.StagingBSMgr
		gwayHandler *gateway.GatewayHandler
		cacher      *memo.Cacher
	}
	type args struct {
		ctx   context.Context
		dserv format.DAGService
		fi    io.Reader
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    format.Node
		wantErr assert.ErrorAssertionFunc
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Server{
				tracer:      tt.fields.tracer,
				Node:        tt.fields.Node,
				DB:          tt.fields.DB,
				FilClient:   tt.fields.FilClient,
				Api:         tt.fields.Api,
				CM:          tt.fields.CM,
				StagingMgr:  tt.fields.StagingMgr,
				gwayHandler: tt.fields.gwayHandler,
				cacher:      tt.fields.cacher,
			}
			got, err := s.importFile(tt.args.ctx, tt.args.dserv, tt.args.fi)
			if !tt.wantErr(t, err, fmt.Sprintf("importFile(%v, %v, %v)", tt.args.ctx, tt.args.dserv, tt.args.fi)) {
				return
			}
			assert.Equalf(t, tt.want, got, "importFile(%v, %v, %v)", tt.args.ctx, tt.args.dserv, tt.args.fi)
		})
	}
}

func TestServer_isDupCIDContent(t *testing.T) {
	type fields struct {
		tracer      trace.Tracer
		Node        *node.Node
		DB          *gorm.DB
		FilClient   *filclient.FilClient
		Api         api.Gateway
		CM          *ContentManager
		StagingMgr  *stagingbs.StagingBSMgr
		gwayHandler *gateway.GatewayHandler
		cacher      *memo.Cacher
	}
	type args struct {
		c       echo.Context
		rootCID cid.Cid
		u       *User
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    bool
		wantErr assert.ErrorAssertionFunc
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Server{
				tracer:      tt.fields.tracer,
				Node:        tt.fields.Node,
				DB:          tt.fields.DB,
				FilClient:   tt.fields.FilClient,
				Api:         tt.fields.Api,
				CM:          tt.fields.CM,
				StagingMgr:  tt.fields.StagingMgr,
				gwayHandler: tt.fields.gwayHandler,
				cacher:      tt.fields.cacher,
			}
			got, err := s.isDupCIDContent(tt.args.c, tt.args.rootCID, tt.args.u)
			if !tt.wantErr(t, err, fmt.Sprintf("isDupCIDContent(%v, %v, %v)", tt.args.c, tt.args.rootCID, tt.args.u)) {
				return
			}
			assert.Equalf(t, tt.want, got, "isDupCIDContent(%v, %v, %v)", tt.args.c, tt.args.rootCID, tt.args.u)
		})
	}
}

func TestServer_loadCar(t *testing.T) {
	type fields struct {
		tracer      trace.Tracer
		Node        *node.Node
		DB          *gorm.DB
		FilClient   *filclient.FilClient
		Api         api.Gateway
		CM          *ContentManager
		StagingMgr  *stagingbs.StagingBSMgr
		gwayHandler *gateway.GatewayHandler
		cacher      *memo.Cacher
	}
	type args struct {
		ctx context.Context
		bs  blockstore.Blockstore
		r   io.Reader
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    *car.CarHeader
		wantErr assert.ErrorAssertionFunc
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Server{
				tracer:      tt.fields.tracer,
				Node:        tt.fields.Node,
				DB:          tt.fields.DB,
				FilClient:   tt.fields.FilClient,
				Api:         tt.fields.Api,
				CM:          tt.fields.CM,
				StagingMgr:  tt.fields.StagingMgr,
				gwayHandler: tt.fields.gwayHandler,
				cacher:      tt.fields.cacher,
			}
			got, err := s.loadCar(tt.args.ctx, tt.args.bs, tt.args.r)
			if !tt.wantErr(t, err, fmt.Sprintf("loadCar(%v, %v, %v)", tt.args.ctx, tt.args.bs, tt.args.r)) {
				return
			}
			assert.Equalf(t, tt.want, got, "loadCar(%v, %v, %v)", tt.args.ctx, tt.args.bs, tt.args.r)
		})
	}
}

func TestServer_msgForMinerClaim(t *testing.T) {
	type fields struct {
		tracer      trace.Tracer
		Node        *node.Node
		DB          *gorm.DB
		FilClient   *filclient.FilClient
		Api         api.Gateway
		CM          *ContentManager
		StagingMgr  *stagingbs.StagingBSMgr
		gwayHandler *gateway.GatewayHandler
		cacher      *memo.Cacher
	}
	type args struct {
		miner address.Address
		uid   uint
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   []byte
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Server{
				tracer:      tt.fields.tracer,
				Node:        tt.fields.Node,
				DB:          tt.fields.DB,
				FilClient:   tt.fields.FilClient,
				Api:         tt.fields.Api,
				CM:          tt.fields.CM,
				StagingMgr:  tt.fields.StagingMgr,
				gwayHandler: tt.fields.gwayHandler,
				cacher:      tt.fields.cacher,
			}
			assert.Equalf(t, tt.want, s.msgForMinerClaim(tt.args.miner, tt.args.uid), "msgForMinerClaim(%v, %v)", tt.args.miner, tt.args.uid)
		})
	}
}

func TestServer_newAuthTokenForUser(t *testing.T) {
	type fields struct {
		tracer      trace.Tracer
		Node        *node.Node
		DB          *gorm.DB
		FilClient   *filclient.FilClient
		Api         api.Gateway
		CM          *ContentManager
		StagingMgr  *stagingbs.StagingBSMgr
		gwayHandler *gateway.GatewayHandler
		cacher      *memo.Cacher
	}
	type args struct {
		user   *User
		expiry time.Time
		perms  []string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    *AuthToken
		wantErr assert.ErrorAssertionFunc
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Server{
				tracer:      tt.fields.tracer,
				Node:        tt.fields.Node,
				DB:          tt.fields.DB,
				FilClient:   tt.fields.FilClient,
				Api:         tt.fields.Api,
				CM:          tt.fields.CM,
				StagingMgr:  tt.fields.StagingMgr,
				gwayHandler: tt.fields.gwayHandler,
				cacher:      tt.fields.cacher,
			}
			got, err := s.newAuthTokenForUser(tt.args.user, tt.args.expiry, tt.args.perms)
			if !tt.wantErr(t, err, fmt.Sprintf("newAuthTokenForUser(%v, %v, %v)", tt.args.user, tt.args.expiry, tt.args.perms)) {
				return
			}
			assert.Equalf(t, tt.want, got, "newAuthTokenForUser(%v, %v, %v)", tt.args.user, tt.args.expiry, tt.args.perms)
		})
	}
}

func TestServer_tracingMiddleware(t *testing.T) {
	type fields struct {
		tracer      trace.Tracer
		Node        *node.Node
		DB          *gorm.DB
		FilClient   *filclient.FilClient
		Api         api.Gateway
		CM          *ContentManager
		StagingMgr  *stagingbs.StagingBSMgr
		gwayHandler *gateway.GatewayHandler
		cacher      *memo.Cacher
	}
	type args struct {
		next echo.HandlerFunc
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   echo.HandlerFunc
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Server{
				tracer:      tt.fields.tracer,
				Node:        tt.fields.Node,
				DB:          tt.fields.DB,
				FilClient:   tt.fields.FilClient,
				Api:         tt.fields.Api,
				CM:          tt.fields.CM,
				StagingMgr:  tt.fields.StagingMgr,
				gwayHandler: tt.fields.gwayHandler,
				cacher:      tt.fields.cacher,
			}
			assert.Equalf(t, tt.want, s.tracingMiddleware(tt.args.next), "tracingMiddleware(%v)", tt.args.next)
		})
	}
}

func TestServer_withShuttleAuth(t *testing.T) {
	type fields struct {
		tracer      trace.Tracer
		Node        *node.Node
		DB          *gorm.DB
		FilClient   *filclient.FilClient
		Api         api.Gateway
		CM          *ContentManager
		StagingMgr  *stagingbs.StagingBSMgr
		gwayHandler *gateway.GatewayHandler
		cacher      *memo.Cacher
	}
	tests := []struct {
		name   string
		fields fields
		want   echo.MiddlewareFunc
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Server{
				tracer:      tt.fields.tracer,
				Node:        tt.fields.Node,
				DB:          tt.fields.DB,
				FilClient:   tt.fields.FilClient,
				Api:         tt.fields.Api,
				CM:          tt.fields.CM,
				StagingMgr:  tt.fields.StagingMgr,
				gwayHandler: tt.fields.gwayHandler,
				cacher:      tt.fields.cacher,
			}
			assert.Equalf(t, tt.want, s.withShuttleAuth(), "withShuttleAuth()")
		})
	}
}

func Test_binder_Bind(t *testing.T) {
	type args struct {
		i interface{}
		c echo.Context
	}
	tests := []struct {
		name    string
		args    args
		wantErr assert.ErrorAssertionFunc
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := binder{}
			tt.wantErr(t, b.Bind(tt.args.i, tt.args.c), fmt.Sprintf("Bind(%v, %v)", tt.args.i, tt.args.c))
		})
	}
}

func Test_openApiMiddleware(t *testing.T) {
	type args struct {
		next echo.HandlerFunc
	}
	tests := []struct {
		name string
		args args
		want echo.HandlerFunc
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equalf(t, tt.want, openApiMiddleware(tt.args.next), "openApiMiddleware(%v)", tt.args.next)
		})
	}
}

func Test_parseChanID(t *testing.T) {
	type args struct {
		chanid string
	}
	tests := []struct {
		name    string
		args    args
		want    *datatransfer.ChannelID
		wantErr assert.ErrorAssertionFunc
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseChanID(tt.args.chanid)
			if !tt.wantErr(t, err, fmt.Sprintf("parseChanID(%v)", tt.args.chanid)) {
				return
			}
			assert.Equalf(t, tt.want, got, "parseChanID(%v)", tt.args.chanid)
		})
	}
}

func Test_sanitizePath(t *testing.T) {
	type args struct {
		p string
	}
	tests := []struct {
		name    string
		args    args
		want    string
		wantErr assert.ErrorAssertionFunc
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := sanitizePath(tt.args.p)
			if !tt.wantErr(t, err, fmt.Sprintf("sanitizePath(%v)", tt.args.p)) {
				return
			}
			assert.Equalf(t, tt.want, got, "sanitizePath(%v)", tt.args.p)
		})
	}
}

func Test_serveCpuProfile(t *testing.T) {
	type args struct {
		c echo.Context
	}
	tests := []struct {
		name    string
		args    args
		wantErr assert.ErrorAssertionFunc
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.wantErr(t, serveCpuProfile(tt.args.c), fmt.Sprintf("serveCpuProfile(%v)", tt.args.c))
		})
	}
}

func Test_serveProfile(t *testing.T) {
	type args struct {
		c echo.Context
	}
	tests := []struct {
		name    string
		args    args
		wantErr assert.ErrorAssertionFunc
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.wantErr(t, serveProfile(tt.args.c), fmt.Sprintf("serveProfile(%v)", tt.args.c))
		})
	}
}

func Test_withUser(t *testing.T) {
	type args struct {
		f func(echo.Context, *User) error
	}
	tests := []struct {
		name string
		args args
		want func(echo.Context) error
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equalf(t, tt.want, withUser(tt.args.f), "withUser(%v)", tt.args.f)
		})
	}
}
