package autoretrieve

import (
	"fmt"

	datatransfer "github.com/filecoin-project/go-data-transfer"
	"github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"
	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p-core/crypto"
	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/peer"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/multiformats/go-multiaddr"
)

const (
	// NoPublisher indicates that no announcements are made to the network and all advertisements
	// are only stored locally.
	NoPublisher PublisherKind = ""

	// DataTransferPublisher makes announcements over a gossipsub topic and exposes a
	// datatransfer/graphsync server that allows peers in the network to sync advertisements.
	DataTransferPublisher PublisherKind = "dtsync"

	// HttpPublisher exposes a HTTP server that announces published advertisements and allows peers
	// in the network to sync them over raw HTTP transport.
	HttpPublisher PublisherKind = "http"
)

type (
	// PublisherKind represents the kind of publisher to use in order to announce a new
	// advertisement to the network.
	// See: WithPublisherKind, NoPublisher, DataTransferPublisher, HttpPublisher.
	PublisherKind string

	// Option sets a configuration parameter for the provider engine.
	Option func(*options) error

	options struct {
		ds datastore.Batching
		h  host.Host
		// key is always initialized from the host peerstore.
		// Setting an explicit identity must not be exposed unless it is tightly coupled with the
		// host identity. Otherwise, the signature of advertisement will not match the libp2p host
		// ID.
		key crypto.PrivKey

		provider peer.AddrInfo

		pubKind            PublisherKind
		pubDT              datatransfer.Manager
		pubHttpListenAddr  string
		pubTopicName       string
		pubTopic           *pubsub.Topic
		pubExtraGossipData []byte

		entCacheCap  int
		entChunkSize int
		purgeCache   bool
	}
)

func newOptions(o ...Option) (*options, error) {
	opts := &options{
		pubKind:           NoPublisher,
		pubHttpListenAddr: "0.0.0.0:3104",
		pubTopicName:      "/indexer/ingest/mainnet",
		// Keep 1024 chunks in cache; keeps 256MiB if chunks are 0.25MiB.
		entCacheCap: 1024,
		// Multihashes are 128 bytes so 16384 results in 0.25MiB chunk when full.
		entChunkSize: 16384,
		purgeCache:   false,
	}

	for _, apply := range o {
		if err := apply(opts); err != nil {
			return nil, err
		}
	}

	if opts.ds == nil {
		opts.ds = dssync.MutexWrap(datastore.NewMapDatastore())
	}

	if opts.h == nil {
		h, err := libp2p.New()
		if err != nil {
			return nil, err
		}
		log.Infow("Libp2p host is not configured, but required; created a new host.", "id", h.ID())
		opts.h = h
	}

	// Initialize private key from libp2p host
	opts.key = opts.h.Peerstore().PrivKey(opts.h.ID())
	// Defensively check that host's self private key is indeed set.
	if opts.key == nil {
		return nil, fmt.Errorf("cannot find private key in self peerstore; libp2p host is misconfigured")
	}

	if len(opts.provider.Addrs) == 0 {
		opts.provider.Addrs = opts.h.Addrs()
		log.Infow("Retrieval address not configured; using host listen addresses instead.", "retrievalAddrs", opts.provider.Addrs)
	}
	if opts.provider.ID == "" {
		opts.provider.ID = opts.h.ID()
		log.Infow("Retrieval ID not configured; using host ID instead.", "retrievalID", opts.provider.ID)
	}

	return opts, nil
}

func (o *options) retrievalAddrsAsString() []string {
	var ras []string
	for _, ra := range o.provider.Addrs {
		ras = append(ras, ra.String())
	}
	return ras
}

// WithPurgeCacheOnStart sets whether to clear any cached entries chunks when the provider engine
// starts.
// If unset, cache is rehydrated from previously cached entries stored in datastore if present.
// See: WithDatastore.
func WithPurgeCacheOnStart(p bool) Option {
	return func(o *options) error {
		o.purgeCache = p
		return nil
	}
}

// WithEntriesChunkSize sets the maximum number of multihashes to include in a single entries chunk.
// If unset, the default size of 16384 is used.
//
// See: WithEntriesCacheCapacity, chunker.CachedEntriesChunker
func WithEntriesChunkSize(s int) Option {
	return func(o *options) error {
		o.entChunkSize = s
		return nil
	}
}

// WithEntriesCacheCapacity sets the maximum number of advertisement entries chains to cache.
// If unset, the default capacity of 1024 is used.
//
// The cache is evicted using LRU policy. Note that the capacity dictates the number of complete
// chains that are cached, not individual entry chunks. This means, the maximum storage used by the
// cache is a factor of capacity, chunk size and the length of multihashes in each chunk.
//
// As an example, for 128-bit long multihashes the cache with default capacity of 1024, and default
// chunk size of 16384 can grow up to 256MiB when full.
//
// See: WithEntriesChunkSize, chunker.CachedEntriesChunker.
func WithEntriesCacheCapacity(s int) Option {
	return func(o *options) error {
		o.entCacheCap = s
		return nil
	}
}

// WithPublisherKind sets the kind of publisher used to announce new advertisements.
// If unset, advertisements are only stored locally and no announcements are made.
// See: PublisherKind.
func WithPublisherKind(k PublisherKind) Option {
	return func(o *options) error {
		o.pubKind = k
		return nil
	}
}

// WithHttpPublisherListenAddr sets the net listen address for the HTTP publisher.
// If unset, the default net listen address of '0.0.0.0:3104' is used.
//
// Note that this option only takes effect if the PublisherKind is set to HttpPublisher.
// See: WithPublisherKind.
func WithHttpPublisherListenAddr(addr string) Option {
	return func(o *options) error {
		o.pubHttpListenAddr = addr
		return nil
	}
}

// WithTopicName sets the topic name on which pubsub announcements are published.
// To override the default pubsub configuration, use WithTopic.
//
// Note that this option only takes effect if the PublisherKind is set to DataTransferPublisher.
// See: WithPublisherKind.
func WithTopicName(t string) Option {
	return func(o *options) error {
		o.pubTopicName = t
		return nil
	}
}

// WithTopic sets the pubsub topic on which new advertisements are announced.
// To use the default pubsub configuration with a specific topic name, use WithTopicName. If both
// options are specified, WithTopic takes presence.
//
// Note that this option only takes effect if the PublisherKind is set to DataTransferPublisher.
// See: WithPublisherKind.
func WithTopic(t *pubsub.Topic) Option {
	return func(o *options) error {
		o.pubTopic = t
		return nil
	}
}

// WithDataTransfer sets the instance of datatransfer.Manager to use.
// If unspecified a new instance is created automatically.
//
// Note that this option only takes effect if the PublisherKind is set to DataTransferPublisher.
// See: WithPublisherKind.
func WithDataTransfer(dt datatransfer.Manager) Option {
	return func(o *options) error {
		o.pubDT = dt
		return nil
	}
}

// WithHost specifies the host to which the provider engine belongs.
// If unspecified, a host is created automatically.
// See: libp2p.New.
func WithHost(h host.Host) Option {
	return func(o *options) error {
		o.h = h
		return nil
	}
}

// WithDatastore sets the datastore that is used by the engine to store advertisements.
// If unspecified, an ephemeral in-memory datastore is used.
// See: datastore.NewMapDatastore.
func WithDatastore(ds datastore.Batching) Option {
	return func(o *options) error {
		o.ds = ds
		return nil
	}
}

// WithRetrievalAddrs sets the addresses that specify where to get the content corresponding to an
// indexing advertisement.
// If unspecified, the libp2p host listen addresses are used.
// See: WithHost.
func WithRetrievalAddrs(addr ...multiaddr.Multiaddr) Option {
	return func(o *options) error {
		o.provider.Addrs = addr
		return nil
	}
}

// WithProvider sets the peer and addresses for the provider to put in indexing advertisements.
// This value overrides `WithRetrievalAddrs`
func WithProvider(provider peer.AddrInfo) Option {
	return func(o *options) error {
		o.provider = provider
		return nil
	}
}

// WithExtraGossipData supplies extra data to include in the pubsub announcement.
// Note that this option only takes effect if the PublisherKind is set to DataTransferPublisher.
// See: WithPublisherKind.
func WithExtraGossipData(extraData []byte) Option {
	return func(o *options) error {
		if len(extraData) != 0 {
			// Make copy for safety.
			o.pubExtraGossipData = make([]byte, len(extraData))
			copy(o.pubExtraGossipData, extraData)
		}
		return nil
	}
}
