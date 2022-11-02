package autoretrieve

import (
	"context"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/application-research/estuary/config"
	"github.com/application-research/estuary/constants"
	datatransfer "github.com/filecoin-project/go-data-transfer"
	provider "github.com/filecoin-project/index-provider"
	"github.com/filecoin-project/index-provider/engine"
	"github.com/filecoin-project/index-provider/metadata"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	logging "github.com/ipfs/go-log/v2"
	"github.com/libp2p/go-libp2p-core/crypto"
	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/multiformats/go-multiaddr"
	"github.com/multiformats/go-multihash"
	"gorm.io/gorm"
)

var arLog = logging.Logger("autoretrieve")

// Engine is an implementation of the core reference provider interface
type AutoretrieveEngine struct {
	RawEngine    *engine.Engine
	Context      context.Context
	TickInterval time.Duration
	Db           *gorm.DB
}

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
func findNewCids(db *gorm.DB, lastAdvertisement time.Time) ([]cid.Cid, error) {
	var newCids []cid.Cid
	err := db.Raw(constants.QueryNewCIDs, lastAdvertisement).
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
func NewAutoretrieveEngine(ctx context.Context, cfg *config.Estuary, db *gorm.DB, libp2pHost host.Host, ds datastore.Batching, dtMgr datatransfer.Manager) (*AutoretrieveEngine, error) {
	newEngine := &AutoretrieveEngine{}
	newRawEngine, err := engine.New(
		engine.WithHost(libp2pHost), // need to be localhost/estuary
		engine.WithPublisherKind(engine.DataTransferPublisher),
		engine.WithDirectAnnounce(cfg.Node.IndexerURL),
		engine.WithDatastore(ds),
		engine.WithDataTransfer(dtMgr),
	)
	if err != nil {
		return nil, err
	}

	// Create index-provider engine (s.Node.IndexProvider) to send announcements to
	// this needs to keep running continuously because storetheindex
	// will come to fetch for advertisements "when it feels like it"
	newRawEngine.RegisterMultihashLister(func(ctx context.Context, provider peer.ID, contextID []byte) (provider.MultihashIterator, error) {

		// on advertiseForAR we set the contextID to LastAdvertisement
		// so that we know that we need to advertise all CIDs uploaded after lastAdvertisement
		lastAdvertisementForAR, err := time.Parse(time.RFC3339, string(contextID))
		if err != nil {
			return nil, fmt.Errorf("could not parse contextID: %v", err)
		}

		// find all new content entries since the last time we advertised for this autoretrieve server
		arLog.Debugf("Querying for new CIDs now (this could take a while)")
		newCids, err := findNewCids(db, lastAdvertisementForAR)
		if err != nil {
			return nil, err
		}
		arLog.Debugf("CIDS:", newCids)

		if len(newCids) == 0 {
			return nil, fmt.Errorf("no new CIDs to announce")
		}

		arLog.Infof("announcing %d new CIDs", len(newCids))

		return &EstuaryMhIterator{
			Cids: newCids,
		}, nil
	})

	newEngine.Context = ctx
	newEngine.TickInterval = time.Duration(cfg.Node.IndexerTickInterval) * time.Minute
	newEngine.Db = db
	newEngine.RawEngine = newRawEngine

	// start engine
	if err := newRawEngine.Start(newEngine.Context); err != nil {
		return nil, err
	}

	return newEngine, nil
}

// Advertise on behalf an autoretrieve server
func (arEng *AutoretrieveEngine) advertiseForAR(ar Autoretrieve) error {
	// This is the time of the last advertisement because we parse it in RegisterMultihashLister
	// to know which CIDs to advertise (we advertise all CIDs uploaded after LastAdvertisement)
	newContextID := []byte(ar.LastAdvertisement.Format(time.RFC3339))

	retrievalAddresses := []multiaddr.Multiaddr{}
	providerID := peer.ID("")
	for _, fullAddr := range strings.Split(ar.Addresses, ",") {
		arAddrInfo, err := peer.AddrInfoFromString(fullAddr)
		if err != nil {
			arLog.Errorf("could not parse multiaddress '%s': %s", fullAddr, err)
			continue
		}
		providerID = arAddrInfo.ID
		retrievalAddresses = append(retrievalAddresses, arAddrInfo.Addrs[0])
	}
	if providerID == "" {
		return fmt.Errorf("no providerID for autoretrieve %s, skipping", ar.Handle)
	}
	if len(retrievalAddresses) == 0 {
		return fmt.Errorf("no retrieval addresses for autoretrieve %s, skipping", ar.Handle)
	}

	arAddrInfo := peer.AddrInfo{ID: providerID, Addrs: retrievalAddresses}

	arLog.Debugf("sending announcement to %s", ar.Handle)
	adCid, err := arEng.RawEngine.NotifyPut(context.Background(), &arAddrInfo, newContextID, metadata.New(metadata.Bitswap{}))
	if err != nil {
		return fmt.Errorf("could not announce new CIDs: %s", err)
	}

	// update lastAdvertisement time on database
	if err := arEng.Db.Model(&Autoretrieve{}).Where("token = ?", ar.Token).UpdateColumn("last_advertisement", time.Now()).Error; err != nil {
		return fmt.Errorf("unable to update advertisement time on database: %s", err)
	}

	arLog.Infof("announced new CIDs: %s", adCid)
	return nil
}

func (arEng *AutoretrieveEngine) Run() {
	var autoretrieves []Autoretrieve
	var lastTickTime time.Time
	var curTime time.Time

	arLog.Infof("starting autoretrieves engine")

	// start ticker
	ticker := time.NewTicker(arEng.TickInterval)
	defer ticker.Stop()

	for {
		curTime = time.Now()
		lastTickTime = curTime.Add(-arEng.TickInterval)

		// Find all autoretrieve servers that are online (that sent heartbeat after lastTickTime)
		err := arEng.Db.Find(&autoretrieves, "last_connection > ?", lastTickTime).Error
		if err != nil {
			arLog.Errorf("unable to query autoretrieve servers from database: %s", err)
			return
		}
		arLog.Infof("%d autoretrieve servers online", len(autoretrieves))
		if len(autoretrieves) == 0 {
			// wait for next tick, or quit
			select {
			case <-ticker.C:
				continue
			case <-arEng.Context.Done():
				break
			}
		}

		arLog.Infof("announcing new CIDs to %d autoretrieve servers", len(autoretrieves))
		// send announcement with new CIDs for each autoretrieve server
		for _, ar := range autoretrieves {
			if err = arEng.advertiseForAR(ar); err != nil {
				arLog.Error(err)
			}
		}

		// wait for next tick, or quit
		select {
		case <-ticker.C:
			continue
		case <-arEng.Context.Done():
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
