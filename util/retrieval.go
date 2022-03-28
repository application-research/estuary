package util

import (
	"fmt"
	"strings"
	"time"

	"github.com/libp2p/go-libp2p-core/crypto"
	"github.com/libp2p/go-libp2p-core/peer"
	ma "github.com/multiformats/go-multiaddr"
	"gorm.io/gorm"
)

type RetrievalFailureRecord struct {
	gorm.Model
	Miner   string `json:"miner"`
	Phase   string `json:"phase"`
	Message string `json:"message"`
	Content uint   `json:"content"`
	Cid     DbCID  `json:"cid"`
}

type RetrievalProgress struct {
	Wait   chan struct{}
	EndErr error
}

type HeartbeatAutoretrieveResponse struct {
	Handle         string         `json:"handle"`
	LastConnection time.Time      `json:"lastConnection"`
	AddrInfo       *peer.AddrInfo `json:"addrInfo"`
}

type AutoretrieveListResponse struct {
	Handle         string         `json:"handle"`
	LastConnection time.Time      `json:"lastConnection"`
	AddrInfo       *peer.AddrInfo `json:"addrInfo"`
}

type AutoretrieveInitResponse struct {
	Handle         string         `json:"handle"`
	Token          string         `json:"token"`
	LastConnection time.Time      `json:"lastConnection"`
	AddrInfo       *peer.AddrInfo `json:"addrInfo"`
}

// ValidateAddresses checks to see if all multiaddresses are valid
// returns empty []string if all multiaddresses are valid strings
// returns a list of all invalid multiaddresses if any is invalid
func validateAddresses(addresses []string) []string {
	var invalidAddresses []string
	for _, addr := range addresses {
		_, err := ma.NewMultiaddr(addr)
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
		return nil, fmt.Errorf("invalid peer information: %s", err)
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

	fmt.Println("test2")
	privKey, err := crypto.UnmarshalPrivateKey(privKeyBytes)
	if err != nil {
		return nil, err
	}
	fmt.Println("test3")

	return privKey, nil
}
