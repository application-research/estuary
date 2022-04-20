package autoretrieve

import (
	"context"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p-core/crypto"
	"github.com/libp2p/go-libp2p-core/peer"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/multiformats/go-multiaddr"
	"github.com/multiformats/go-multihash"
	"gorm.io/gorm"
)

type Autoretrieve struct {
	gorm.Model

	Handle            string `gorm:"unique"`
	Token             string `gorm:"unique"`
	LastConnection    time.Time
	LastAdvertisement time.Time
	PrivateKey        string `gorm:"unique"`
	Addresses         string
}

type HeartbeatAutoretrieveResponse struct {
	Handle            string         `json:"handle"`
	LastConnection    time.Time      `json:"lastConnection"`
	LastAdvertisement time.Time      `json:"lastAdvertisement"`
	AddrInfo          *peer.AddrInfo `json:"addrInfo"`
}

type AutoretrieveListResponse struct {
	Handle            string         `json:"handle"`
	LastConnection    time.Time      `json:"lastConnection"`
	LastAdvertisement time.Time      `json:"lastAdvertisement"`
	AddrInfo          *peer.AddrInfo `json:"addrInfo"`
}

type AutoretrieveInitResponse struct {
	Handle         string         `json:"handle"`
	Token          string         `json:"token"`
	LastConnection time.Time      `json:"lastConnection"`
	AddrInfo       *peer.AddrInfo `json:"addrInfo"`
}

type SimpleEstuaryMhIterator struct {
	offset int
	Mh     []multihash.Multihash
}

func (m *SimpleEstuaryMhIterator) Next() (multihash.Multihash, error) {
	if m.offset < len(m.Mh) {
		hash := m.Mh[m.offset]
		m.offset++
		return hash, nil
	}
	return nil, io.EOF
}

// newIndexProvider creates a new index-provider engine to send announcements to storetheindex
// this needs to keep running continuously because storetheindex
// will come to fetch advertisements "when it feels like it"
func NewAutoretrieveEngine() (*AutoretrieveEngine, error) {
	host, err := libp2p.New()
	if err != nil {
		return nil, err
	}
	topic := "/indexer/ingest/mainnet"
	indexerMultiaddr, err := multiaddr.NewMultiaddr("/ip4/127.0.0.1/tcp/3003/p2p/12D3KooWChQyVH7a3iR3o8kmdYwXiHf2v3tXQWhSCS9j8NbLVQ9o") //TODO: need to adjust p2p addr
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
	)
	if err != nil {
		return nil, err
	}
	return newEngine, nil
}

// ValidateAddresses checks to see if all multiaddresses are valid
// returns empty []string if all multiaddresses are valid strings
// returns a list of all invalid multiaddresses if any is invalid
func validateAddresses(addresses []string) []string {
	var invalidAddresses []string
	for _, addr := range addresses {
		_, err := multiaddr.NewMultiaddr(addr)
		if err != nil {
			invalidAddresses = append(invalidAddresses, addr)
		}
	}
	return invalidAddresses
}

func ValidatePeerInfo(privKeyStr string, addresses []string) (*peer.AddrInfo, error) {
	// check if peerid is correct
	privateKey, err := stringToPrivKey(privKeyStr)
	if err != nil {
		return nil, fmt.Errorf("unable to decode private key: %s", err)
	}
	_, err = peer.IDFromPrivateKey(privateKey)
	if err != nil {
		return nil, fmt.Errorf("invalid peer information: %s", err)
	}

	if len(addresses) == 0 || addresses[0] == "" {
		return nil, fmt.Errorf("no addresses provided")
	}

	// check if multiaddresses formats are correct
	invalidAddrs := validateAddresses(addresses)
	if len(invalidAddrs) != 0 {
		return nil, fmt.Errorf("invalid address(es): %s", strings.Join(invalidAddrs, ", "))
	}

	// any of the multiaddresses of the peer should work to get addrInfo
	// we get the first one
	addrInfo, err := peer.AddrInfoFromString(addresses[0])
	if err != nil {
		return nil, err
	}

	return addrInfo, nil
}

func stringToPrivKey(privKeyStr string) (crypto.PrivKey, error) {
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

// ParseContextID tries to splits the contextID
// Returns the handle for the autoretrieve entry the contextID belongs to
// Returns an empty autoretrieve entry and error otherwise
func ParseContextID(contextID []byte) (string, error) {
	splitContextID := strings.Split(string(contextID), "_")
	if len(splitContextID) != 2 {
		return "", fmt.Errorf("unable to parse contextID: '%s'", string(contextID))
	}
	return splitContextID[0], nil
}
