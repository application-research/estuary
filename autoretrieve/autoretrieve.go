package autoretrieve

import (
	"context"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/application-research/estuary/config"
	provider "github.com/filecoin-project/index-provider"
	"github.com/filecoin-project/index-provider/metadata"
	"github.com/google/uuid"
	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p-core/crypto"
	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/peer"
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
	PubKey            string `gorm:"unique"`
	Addresses         string
}

type HeartbeatAutoretrieveResponse struct {
	Handle            string         `json:"handle"`
	LastConnection    time.Time      `json:"lastConnection"`
	LastAdvertisement time.Time      `json:"lastAdvertisement"`
	AddrInfo          *peer.AddrInfo `json:"addrInfo"`
	AdvertiseInterval string         `json:AdvertiseInterval`
}

type AutoretrieveListResponse struct {
	Handle            string         `json:"handle"`
	LastConnection    time.Time      `json:"lastConnection"`
	LastAdvertisement time.Time      `json:"lastAdvertisement"`
	AddrInfo          *peer.AddrInfo `json:"addrInfo"`
}

type AutoretrieveInitResponse struct {
	Handle            string         `json:"handle"`
	Token             string         `json:"token"`
	LastConnection    time.Time      `json:"lastConnection"`
	AddrInfo          *peer.AddrInfo `json:"addrInfo"`
	AdvertiseInterval string         `json:AdvertiseInterval`
}

// EstuaryMhIterator contains objects to query the database
// incrementally, to avoid having all CIDs in memory at once
type EstuaryMhIterator struct {
	// we need contentOffset because one content might have more than one CID
	offset int // offset of the cid related to the current content
	Cids   []cid.Cid
}

func (m *EstuaryMhIterator) Next() (multihash.Multihash, error) {

	if m.offset >= len(m.Cids) {
		return nil, io.EOF
	}

	curCid := m.Cids[m.offset]
	m.offset++ // go to next CID

	return curCid.Hash(), nil
}

// findNewContents takes a time.Time struct `since` and queries all CIDs
// associated to active util.Content entries created after that time
// Returns a list of the cid.CID created after `since`
// Returns an error if failed
func findNewCids(db *gorm.DB, since time.Time) ([]cid.Cid, error) {
	var newCids []cid.Cid
	err := db.Raw("select objects.cid from objects left join obj_refs on objects.id = obj_refs.object where obj_refs.content in (select id from contents where created_at > ?);", since).
		Scan(&newCids).
		Error
	if err != nil {
		return nil, fmt.Errorf("unable to query new CIDs from database: %s", err)
	}

	return newCids, nil
}

// newIndexProvider creates a new index-provider engine to send announcements to storetheindex
// this needs to keep running continuously because storetheindex
// will come to fetch advertisements "when it feels like it"
func NewAutoretrieveEngine(ctx context.Context, cfg *config.Estuary, db *gorm.DB, libp2pHost host.Host) (*AutoretrieveEngine, error) {
	newEngine, err := New(
		WithHost(libp2pHost), // need to be localhost/estuary
		WithPublisherKind(DataTransferPublisher),
		WithDirectAnnounce(cfg.Node.IndexerURL),
	)
	if err != nil {
		return nil, err
	}

	// Create index-provider engine (s.Node.IndexProvider) to send announcements to
	// this needs to keep running continuously because storetheindex
	// will come to fetch for advertisements "when it feels like it"
	newEngine.RegisterMultihashLister(func(ctx context.Context, contextID []byte) (provider.MultihashIterator, error) {

		// TODO: remove these comments
		// contextID = autoretrievehandle_randomnumber
		// arHandle := strings.Split(string(contextID), "_")[0]
		// arHandle := contextID // contextID is the autoretrieve handle

		// get the autoretrieve entry from the database
		var arList []Autoretrieve
		err = db.Find(&arList).Error
		if err != nil {
			return nil, err
		}

		var newestAdvertisement time.Time // this is the oldest time possible (0001-01-01 00:00:00 +0000 UTC)
		for _, ar := range arList {
			if newestAdvertisement.Before(ar.LastAdvertisement) {
				newestAdvertisement = ar.LastAdvertisement
			}
		}

		// find all new content entries since the last time we advertised for this autoretrieve server
		log.Debugf("Querying for new CIDs now (this could take a while)")
		newCids, err := findNewCids(db, newestAdvertisement)
		if err != nil {
			return nil, err
		}

		if len(newCids) == 0 {
			return nil, fmt.Errorf("no new CIDs to announce")
		}

		log.Infof("announcing %d new CIDs", len(newCids))

		return &EstuaryMhIterator{
			Cids: newCids,
		}, nil
	})

	newEngine.context = ctx
	newEngine.TickInterval = time.Duration(cfg.Node.IndexerTickInterval) * time.Minute
	newEngine.db = db

	// start engine
	newEngine.Start(newEngine.context)

	return newEngine, nil
}

func getAddressesWithoutPeerID(addresses string) []string {
	var retrievalAddresses []string
	for _, fullAddr := range strings.Split(addresses, ",") {
		arAddrInfo, err := peer.AddrInfoFromString(fullAddr)
		if err != nil {
			log.Errorf("could not parse multiaddress '%s': %s", fullAddr, err)
			continue
		}
		retrievalAddresses = append(retrievalAddresses, arAddrInfo.Addrs[0].String())
	}

	return retrievalAddresses
}

func updateLastAdvertisements(db *gorm.DB, connectedARs []Autoretrieve) error {
	var allTokens []string
	for _, ar := range connectedARs {
		allTokens = append(allTokens, ar.Token)
	}
	if err := db.Model(&Autoretrieve{}).Where("token IN ?", allTokens).UpdateColumn("last_advertisement", time.Now()).Error; err != nil {
		return fmt.Errorf("unable to update advertisement time on database: %s", err)
	}

	return nil
}

func (arEng *AutoretrieveEngine) announce(connectedARs []Autoretrieve) error {
	// TODO: in the future we need to check if no new CIDs exist but new AR servers are online
	// TODO: then we'd need to send a change advertisement (for that we need to store the contextID, see TODO below)
	retrievalAddresses := []string{}
	for _, ar := range connectedARs {
		arAddresses := getAddressesWithoutPeerID(ar.Addresses)
		retrievalAddresses = append(retrievalAddresses, arAddresses...)
		if len(retrievalAddresses) == 0 {
			return fmt.Errorf("no retrieval addresses for autoretrieve %s, skipping", ar.Handle)
		}
	}

	// TODO: check if this can really be our host's peerID or if it has to be AR's
	providerID := arEng.h.ID().String()
	if providerID == "" {
		return fmt.Errorf("no providerID for arEng, skipping announcement")
	}

	// contextID is just a random contextID for now
	// TODO: in the future we'd store this and refer it when doing changes to advertisements
	newContextID := []byte(uuid.New().String())

	log.Infof("sending announcement to connected autoretrieve servers")
	adCid, err := arEng.NotifyPut(context.Background(), newContextID, providerID, retrievalAddresses, metadata.New(metadata.Bitswap{}))
	if err != nil {
		return fmt.Errorf("could not announce new CIDs: %s", err)
	}

	if err := updateLastAdvertisements(arEng.db, connectedARs); err != nil {
		return err
	}

	log.Infof("finished announcing new CIDs: %s", adCid)
	return nil
}

func (arEng *AutoretrieveEngine) Run() {
	var connectedARs []Autoretrieve
	var lastTickTime time.Time
	var curTime time.Time

	// start ticker
	ticker := time.NewTicker(arEng.TickInterval)
	defer ticker.Stop()

	for {
		curTime = time.Now()
		lastTickTime = curTime.Add(-arEng.TickInterval)

		// Find all autoretrieve servers that are online (that sent heartbeat)
		err := arEng.db.Find(&connectedARs, "last_connection > ?", lastTickTime).Error
		if err != nil {
			log.Errorf("unable to query autoretrieve servers from database: %s", err)
			return
		}

		if len(connectedARs) == 0 {
			log.Infof("no autoretrieve servers online")
			// wait for next tick, or quit
			select {
			case <-ticker.C:
				continue
			case <-arEng.context.Done():
				break
			}
		}

		log.Infof("announcing new CIDs to %d autoretrieve servers", len(connectedARs))

		// announce for all connected ARs
		if err = arEng.announce(connectedARs); err != nil {
			log.Error(err)
		}

		// wait for next tick, or quit
		select {
		case <-ticker.C:
			continue
		case <-arEng.context.Done():
			break
		}
	}
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

func ValidatePeerInfo(pubKeyStr string, addresses []string) (*peer.AddrInfo, error) {
	// check if peerid is correct
	pubKey, err := stringToPubKey(pubKeyStr)
	if err != nil {
		return nil, fmt.Errorf("unable to decode public key: %s", err)
	}
	_, err = peer.IDFromPublicKey(pubKey)
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

func stringToPubKey(pubKeyStr string) (crypto.PubKey, error) {
	pubKeyBytes, err := crypto.ConfigDecodeKey(pubKeyStr)
	if err != nil {
		return nil, err
	}

	pubKey, err := crypto.UnmarshalPublicKey(pubKeyBytes)
	if err != nil {
		return nil, err
	}

	return pubKey, nil
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
