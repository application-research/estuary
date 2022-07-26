package autoretrieve

import (
	"context"
	"fmt"
	"io"
	"math/rand"
	"strconv"
	"strings"
	"time"

	"github.com/application-research/estuary/config"
	provider "github.com/filecoin-project/index-provider"
	"github.com/filecoin-project/index-provider/metadata"
	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p-core/crypto"
	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/multiformats/go-multiaddr"
	"github.com/multiformats/go-multihash"
	"gorm.io/gorm"
)

var ChunksInMemoryAtOnce = 1200 // query max of 6000 advertisement chunks at a time (do not overflow RAM usage)

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
	offset                 int // offset of the cid related to the current content
	db                     *gorm.DB
	startAdvertisementDate time.Time

	cids        []cid.Cid
	queryLimit  int
	queryOffset int
}

func (m *EstuaryMhIterator) Next() (multihash.Multihash, error) {

	// finished publishing last batch of CIDs, get more from database
	if m.offset >= len(m.cids) {
		log.Debugf("Querying for new CIDs (this could take a while): limit %d offset %d", m.queryLimit, m.queryOffset)
		newCids, err := findNewCids(m.db, m.startAdvertisementDate, m.queryLimit, m.queryOffset)
		if err != nil {
			return nil, err
		}

		if len(newCids) == 0 {
			log.Debugf("Iterator finished")
			log.Debugf("iterator: %+v", m)
			return nil, io.EOF
		}

		m.queryOffset += m.queryLimit
		m.offset = 0

		m.cids = newCids
	}

	curCid := m.cids[m.offset]
	m.offset++ // go to next CID

	return curCid.Hash(), nil
}

// findNewContents takes a time.Time struct `lastAdvertisement` and queries all CIDs
// associated to active util.Content entries created after that time
// Returns a list of the cid.CID created after `lastAdvertisement`
// Returns an error if failed
func findNewCids(db *gorm.DB, lastAdvertisement time.Time, limit int, offset int) ([]cid.Cid, error) {
	var newCids []cid.Cid
	err := db.Raw("select objects.cid from objects left join obj_refs on objects.id = obj_refs.object where obj_refs.content in (select id from contents where created_at > ?) limit ? offset ?;", lastAdvertisement, limit, offset).
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

		// contextID = autoretrievehandle_randomnumber
		arHandle := strings.Split(string(contextID), "_")[0]
		// arHandle := contextID // contextID is the autoretrieve handle

		// get the autoretrieve entry from the database
		var ar Autoretrieve
		err = db.Find(&ar, "handle = ?", arHandle).Error
		if err != nil {
			return nil, err
		}

		return &EstuaryMhIterator{
			queryLimit: newEngine.entChunkSize * ChunksInMemoryAtOnce, // this is how many CIDs we have in memory at once

			db:                     db,
			startAdvertisementDate: ar.LastAdvertisement,
		}, nil
	})

	newEngine.context = ctx
	newEngine.TickInterval = time.Duration(cfg.Node.IndexerTickInterval) * time.Minute
	newEngine.db = db

	// start engine
	newEngine.Start(newEngine.context)

	return newEngine, nil
}

func (arEng *AutoretrieveEngine) announceForAR(ar Autoretrieve) error {
	// TODO: every contextID needs to be unique but we can't use more than 64 chars for it. Make this better (fix the code in RegisterMultihashLister when we change this)
	newContextID := []byte(ar.Handle + "_" + strconv.Itoa(rand.Intn(100000)))

	retrievalAddresses := []string{}
	providerID := ""
	for _, fullAddr := range strings.Split(ar.Addresses, ",") {
		arAddrInfo, err := peer.AddrInfoFromString(fullAddr)
		if err != nil {
			log.Errorf("could not parse multiaddress '%s': %s", fullAddr, err)
			continue
		}
		providerID = arAddrInfo.ID.String()
		retrievalAddresses = append(retrievalAddresses, arAddrInfo.Addrs[0].String())
	}
	if providerID == "" {
		return fmt.Errorf("no providerID for autoretrieve %s, skipping", ar.Handle)
	}
	if len(retrievalAddresses) == 0 {
		return fmt.Errorf("no retrieval addresses for autoretrieve %s, skipping", ar.Handle)
	}

	// see if there are any new CIDs to announce (calling NotifyPut with no new CIDs will result in a panic)
	newCids, err := findNewCids(arEng.db, ar.LastAdvertisement, 1, 0)
	if err != nil {
		return err
	}
	if len(newCids) == 0 {
		log.Warnf("no new CIDs to announce, skipping")
		return nil
	}

	log.Infof("sending announcement to %s", ar.Handle)
	adCid, err := arEng.NotifyPut(context.Background(), newContextID, providerID, retrievalAddresses, metadata.New(metadata.Bitswap{}))
	if err != nil {
		return fmt.Errorf("could not announce new CIDs: %s", err)
	}

	// update lastAdvertisement time on database
	if err := arEng.db.Model(&Autoretrieve{}).Where("token = ?", ar.Token).UpdateColumn("last_advertisement", time.Now()).Error; err != nil {
		return fmt.Errorf("unable to update advertisement time on database: %s", err)
	}

	log.Infof("announced new CIDs: %s", adCid)
	return nil
}

func (arEng *AutoretrieveEngine) Run() {
	var autoretrieves []Autoretrieve
	var lastTickTime time.Time
	var curTime time.Time

	// start ticker
	ticker := time.NewTicker(arEng.TickInterval)
	defer ticker.Stop()

	for {
		curTime = time.Now()
		lastTickTime = curTime.Add(-arEng.TickInterval)
		// Find all autoretrieve servers that are online (that sent heartbeat)
		err := arEng.db.Find(&autoretrieves, "last_connection > ?", lastTickTime).Error
		if err != nil {
			log.Errorf("unable to query autoretrieve servers from database: %s", err)
			return
		}
		if len(autoretrieves) == 0 {
			log.Infof("no autoretrieve servers online")
			// wait for next tick, or quit
			select {
			case <-ticker.C:
				continue
			case <-arEng.context.Done():
				break
			}
		}

		log.Infof("announcing new CIDs to %d autoretrieve servers", len(autoretrieves))
		// send announcement with new CIDs for each autoretrieve server
		for _, ar := range autoretrieves {
			if err = arEng.announceForAR(ar); err != nil {
				log.Error(err)
			}
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
