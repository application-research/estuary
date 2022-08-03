package util

import (
	"sync"
	"time"

	"github.com/application-research/estuary/drpc"
	"github.com/application-research/estuary/node"
	"github.com/application-research/estuary/pinner"
	"github.com/application-research/estuary/stagingbs"
	"github.com/application-research/estuary/util/gateway"
	"github.com/application-research/filclient"
	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/lotus/api"
	lru "github.com/hashicorp/golang-lru"
	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/whyrusleeping/memo"
	"go.opencensus.io/trace"
	"gorm.io/gorm"
)

type InitShuttleResponse struct {
	Handle string `json:"handle"`
	Token  string `json:"token"`
}

type ShuttleStorageStats struct {
	BlockstoreSize uint64 `json:"blockstoreSize"`
	BlockstoreFree uint64 `json:"blockstoreFree"`
	PinCount       int64  `json:"pinCount"`
	PinQueueLength int64  `json:"pinQueueLength"`
}

type ShuttleListResponse struct {
	Handle         string          `json:"handle"`
	Token          string          `json:"token"`
	Online         bool            `json:"online"`
	LastConnection time.Time       `json:"lastConnection"`
	AddrInfo       *peer.AddrInfo  `json:"addrInfo"`
	Address        address.Address `json:"address"`
	Hostname       string          `json:"hostname"`

	StorageStats *ShuttleStorageStats `json:"storageStats"`
}

type ShuttleCreateContentBody struct {
	ContentCreateBody
	Collections  []string `json:"collections"`
	DagSplitRoot uint     `json:"dagSplitRoot"`
	User         uint     `json:"user"`
}

type Shuttle struct {
	Node       *node.Node
	Api        api.Gateway
	DB         *gorm.DB
	PinMgr     *pinner.PinManager
	Filc       *filclient.FilClient
	StagingMgr *stagingbs.StagingBSMgr

	GwayHandler *gateway.GatewayHandler

	Tracer trace.Tracer

	TcLk             sync.Mutex
	TrackingChannels map[string]*ChanTrack

	SplitLk          sync.Mutex
	SplitsInProgress map[uint]bool

	AddPinLk sync.Mutex

	Outgoing chan *drpc.Message

	Private            bool
	DisableLocalAdding bool
	Dev                bool

	Hostname      string
	EstuaryHost   string
	ShuttleHandle string
	ShuttleToken  string

	CommpMemo *memo.Memoizer

	AuthCache *lru.TwoQueueCache

	RetrLk               sync.Mutex
	RetrievalsInProgress map[uint]*RetrievalProgress

	InflightCids   map[cid.Cid]uint
	InflightCidsLk sync.Mutex
}

type ChanTrack struct {
	Dbid uint
	Last *filclient.ChannelState
}
