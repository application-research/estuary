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

	Handle         string `gorm:"unique"`
	Token          string `gorm:"unique"`
	LastConnection time.Time
	PrivateKey     string `gorm:"unique"`
	Addresses      string
}

type SimpleEstuaryMhIterator struct {
	offset int
	Mh     []multihash.Multihash
}

func (m *SimpleEstuaryMhIterator) Next() (multihash.Multihash, error) {
	fmt.Println("i am next whohoooo")
	if m.offset < len(m.Mh) {
		hash := m.Mh[m.offset]
		fmt.Println("returning ", hash)
		m.offset++
		return hash, nil
	}
	return nil, io.EOF
}

// newIndexProvider creates a new index-provider engine to send announcements to storetheindex
// this needs to keep running continuously because storetheindex
// will come to fetch advertisements "when it feels like it"
func NewAutoretrieveEngine() (*AutoretrieveEngine, error) {
	// TODO: remove s *Server, remove topic, indexerMultiaddr, etc.
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

func ParseContextID(contextID []byte) (time.Time, time.Time, error) {
	layout := "2006/01/02T15:04:05.000Z"
	splitContextID := strings.Split(string(contextID), "-")
	if len(splitContextID) != 3 {
		return time.Time{}, time.Time{}, fmt.Errorf("wrong contextID format, expected 'AR-start_date-end_date', got %s", string(contextID))
	}
	startTimeStr := splitContextID[1]
	endTimeStr := splitContextID[2]

	startTime, err := time.Parse(layout, startTimeStr)
	if err != nil {
		return time.Time{}, time.Time{}, fmt.Errorf("could not parse contextID: %s", err)
	}

	endTime, err := time.Parse(layout, endTimeStr)
	if err != nil {
		return time.Time{}, time.Time{}, fmt.Errorf("could not parse contextID: %s", err)
	}
	return startTime, endTime, nil
}

func GenContextID(startTime, endTime time.Time) ([]byte, error) {
	layout := "2006/01/02T15:04:05.000Z"
	startTimeStr := startTime.Format(layout)
	endTimeStr := endTime.Format(layout)
	return []byte("AR-" + startTimeStr + "-" + endTimeStr), nil
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
