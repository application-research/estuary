package autoretrieve

import (
	"context"
	"fmt"
	"io"
	"strings"
	"time"

	provider "github.com/filecoin-project/index-provider"
	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p-core/crypto"
	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/peer"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/multiformats/go-multiaddr"
	"github.com/multiformats/go-multihash"
	"gorm.io/gorm"
)

type Autoretrieve struct {
	gorm.Model

	Handle         string `gorm:"unique"`
	Token          string `gorm:"unique"`
	LastConnection time.Time
	PrivateKey     string `gorm:"unique"`
	Addresses      string
}

type EstuaryMhIterator struct {
	contextIDToAr map[string]int // contextIDToAr[contextID] = AR offset for iterator
	contextID     string
	mh            []multihash.Multihash
}

func (m *EstuaryMhIterator) Next() (multihash.Multihash, error) {
	offset := m.contextIDToAr[m.contextID]
	if offset < len(m.mh) {
		hash := m.mh[offset]
		m.contextIDToAr[m.contextID]++
		return hash, nil
	}
	return nil, io.EOF
}

type SimpleEstuaryMhIterator struct {
	offset int
	mh     []multihash.Multihash
}

func (m *SimpleEstuaryMhIterator) Next() (multihash.Multihash, error) {
	if m.offset < len(m.mh) {
		hash := m.mh[m.offset]
		m.offset++
		return hash, nil
	}
	return nil, io.EOF
}

func NewEstuaryMhIterator() (*EstuaryMhIterator, error) {
	return &EstuaryMhIterator{
		contextIDToAr: make(map[string]int, 0),
		mh:            make([]multihash.Multihash, 0),
	}, nil
}

// newIndexProvider creates a new index-provider engine to send announcements to storetheindex
// this needs to keep running continuously because storetheindex
// will come to fetch advertisements "when it feels like it"
func NewTestAutoretrieveEngine(host host.Host, mhIterator *SimpleEstuaryMhIterator) (*AutoretrieveEngine, error) {
	// TODO: remove s *Server, remove topic, indexerMultiaddr, etc.
	topic := "/indexer/ingest/mainnet"
	indexerMultiaddr, err := multiaddr.NewMultiaddr("/ip4/127.0.0.1/tcp/3003/p2p/12D3KooWRF5NwHcX2FMdPZ6JDmYB4VCTdC22VZiHngP5VJDkyvY8") //TODO: need to adjust p2p addr
	if err != nil {
		return nil, err
	}
	indexerAddrinfo, err := peer.AddrInfosFromP2pAddrs(indexerMultiaddr)
	if err != nil {
		return nil, err
	}
	pubG, err := pubsub.NewGossipSub(context.Background(), host,
		pubsub.WithDirectConnectTicks(1),
		pubsub.WithDirectPeers(indexerAddrinfo),
	)
	if err != nil {
		return nil, err
	}
	pubT, err := pubG.Join(topic)
	if err != nil {
		return nil, err
	}

	newEngine, err := New(
		WithTopic(pubT),      // TODO: remove, testing
		WithTopicName(topic), // TODO: remove, testing
		WithHost(host),       // need to be localhost/estuary
		WithPublisherKind(DataTransferPublisher),
		// we need these addresses to be here instead
		// of on the p2p host h because if we add them
		// as ListenAddrs it'll try to start listening locally
		// engine.WithRetrievalAddrs(addrs...),
	)
	if err != nil {
		return nil, err
	}

	newEngine.RegisterMultihashLister(func(ctx context.Context, contextID []byte) (provider.MultihashIterator, error) {
		return mhIterator, nil
	})

	return newEngine, nil
}

// announceNewCIDs publishes an announcement with the CIDs that were added
// between now and lastTickTime (see updateAutoretrieveIndex)
// func (indexProvider *AutoretrieveIndexProvider) AnnounceNewCIDs(ar Autoretrieve) error {

// 	ad, err := indexProvider.buildAdvertisement(ar)
// 	if err != nil {
// 		return err
// 	}

// 	_, latestAd, err := indexProvider.ProviderEngine.GetLatestAdv(context.Background())
// 	if err != nil {
// 		return err
// 	}

// 	if latestAd != nil && ad.Entries == latestAd.Entries {
// 		return fmt.Errorf("advertisement already announced")
// 	}

// 	_, err = indexProvider.ProviderEngine.Publish(context.Background(), ad)
// 	if err != nil {
// 		return err
// 	}

// 	return nil
// }

func (iter *SimpleEstuaryMhIterator) RegisterNewCIDs(newCids []cid.Cid) error {
	if len(newCids) == 0 {
		return fmt.Errorf("No new CIDs to register")
	}

	// add new contents to mhIterator
	for _, c := range newCids {
		// indexProvider.MhIterator.mh = append(indexProvider.MhIterator.mh, content.Cid.CID.Hash())
		iter.mh = append(iter.mh, c.Hash())
	}

	return nil
}

func (iter *EstuaryMhIterator) RegisterNewCIDs(newCids []cid.Cid) error {
	if len(newCids) == 0 {
		return fmt.Errorf("No new CIDs to register")
	}

	// add new contents to mhIterator
	for _, c := range newCids {
		// indexProvider.MhIterator.mh = append(indexProvider.MhIterator.mh, content.Cid.CID.Hash())
		iter.mh = append(iter.mh, c.Hash())
	}

	return nil
}

// buildAdvertisement constructs a schema.Advertisement{} struct
// containing the information from Autoretrieve as well as Estuary
//func (indexProvider *AutoretrieveIndexProvider) buildAdvertisement(ar Autoretrieve) (schema.Advertisement, error) {
//	// build contextID for advertisement
//	// format: "EstuaryAd-" + ID of autoretrieve server
//	contextID, err := getAutoretrieveContextID(ar)
//	if err != nil {
//		return schema.Advertisement{}, err
//	}

//	md := metadata.New(metadata.Bitswap{})
//	mdBytes, err := md.MarshalBinary()
//	if err != nil {
//		return schema.Advertisement{}, err
//	}

//	//chunkSize and capacity are the default values the engine uses: 1024
//	chunkSize := 1024
//	capacity := 1024
//	entriesChuncker, err := chunker.NewCachedEntriesChunker(context.Background(), indexProvider.Datastore, chunkSize, capacity)
//	if err != nil {
//		return schema.Advertisement{}, err
//	}

//	entries, err := entriesChuncker.Chunk(context.Background(), indexProvider.MhIterator)
//	if err != nil {
//		return schema.Advertisement{}, err
//	}

//	// Add the cid/contextID to datastore
//	// this is done by NotifyPut but we're not using it so we have to
//	// manually add it here
//	keyToCidMapPrefix := "map/keyCid/"
//	cidToKeyMapPrefix := "map/cidKey/"
//	if entries == nil {
//		return schema.Advertisement{}, fmt.Errorf("failed to write context id to entries cid mapping: dsEntry is nil")
//	}

//	dsEntry := entries.(cidlink.Link)
//	if dsEntry.Cid.Bytes() == nil {
//		return schema.Advertisement{}, fmt.Errorf("failed to write context id to entries cid mapping: dsEntry.Cid is empty")
//	}

//	err = indexProvider.Datastore.Put(context.Background(), datastore.NewKey(keyToCidMapPrefix+string(contextID)), dsEntry.Cid.Bytes())
//	if err != nil {
//		return schema.Advertisement{}, err
//	}
//	// And the other way around when graphsync ios making a request,
//	// so the lister in the linksystem knows to what contextID we are referring.
//	err = indexProvider.Datastore.Put(context.Background(), datastore.NewKey(cidToKeyMapPrefix+dsEntry.Cid.String()), contextID)
//	if err != nil {
//		return schema.Advertisement{}, fmt.Errorf("failed to write context id to entries cid mapping: %s", err)
//	}

//	// h, err := createFakeAutoretrieveHost(ar)
//	// if err != nil {
//	// 	return schema.Advertisement{}, err
//	// }

//	// splitAutoretrieveAddresses := strings.Split(ar.Addresses, ",")
//	ad := schema.Advertisement{
//		Provider:  indexProvider.Host.ID().String(), // provider is the estuary p2p host
//		Addresses: multiAddrsToString(indexProvider.Host.Addrs()),
//		// Addresses: splitAutoretrieveAddresses, // addresses are the autoretrieve ones
//		Entries:   entries,
//		ContextID: contextID,
//		Metadata:  mdBytes,
//	}

//	// Sign the advertisement using the provider's private key
//	hostPrivkey := indexProvider.Host.Peerstore().PrivKey(indexProvider.Host.ID())
//	if err := ad.Sign(hostPrivkey); err != nil {
//		return schema.Advertisement{}, err
//	}

//	return ad, nil
//}

// getAutoretrieveContextID builds the contextID for a give autoretrieve server
// format: "EstuaryAd-" + ID of autoretrieve server
func GetAutoretrieveContextID(ar Autoretrieve) ([]byte, error) {
	arPrivKey, err := stringToPrivkey(ar.PrivateKey)
	if err != nil {
		return nil, err
	}

	arID, err := peer.IDFromPrivateKey(arPrivKey)
	if err != nil {
		return nil, err
	}
	strArID := arID.String()
	contextID := []byte("EstuaryAd-" + strArID)

	return contextID, nil
}

// createFakeAutoretrieveHost builds a libp2p host object with the
// private key of the autoretrieve server so we can send advertisements
// on behalf of those servers (as if we were them)
// Note: this is a hack, we should create tokens that give partial
// permissions to other players
func createFakeAutoretrieveHost(ar Autoretrieve) (host.Host, error) {
	arPrivKey, err := stringToPrivkey(ar.PrivateKey)
	if err != nil {
		return nil, err
	}

	h, err := libp2p.New(libp2p.Identity(arPrivKey))
	if err != nil {
		return nil, err
	}

	return h, nil
}

func stringToPrivkey(privKeyStr string) (crypto.PrivKey, error) {
	privKeyBytes, err := crypto.ConfigDecodeKey(privKeyStr)
	if err != nil {
		return nil, err
	}

	privKey, err := crypto.UnmarshalPrivateKey(privKeyBytes)
	if err != nil {
		return nil, err
	}

	return privKey, nil
}

func multiAddrsToString(addrs []multiaddr.Multiaddr) []string {
	var rAddrs []string
	for _, addr := range addrs {
		rAddrs = append(rAddrs, addr.String())
	}
	return rAddrs
}

func stringToMultiAddrs(addrStr string) ([]multiaddr.Multiaddr, error) {
	var mAddrs []multiaddr.Multiaddr
	for _, addr := range strings.Split(addrStr, ",") {
		ma, err := multiaddr.NewMultiaddr(addr)
		if err != nil {
			return nil, err
		}
		mAddrs = append(mAddrs, ma)
	}
	return mAddrs, nil
}
