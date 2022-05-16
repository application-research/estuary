package autoretrieve

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/application-research/estuary/util"
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

// EstuaryMhIterator contains objects to query the database
// incrementally, to avoid having all CIDs in memory at once
type EstuaryMhIterator struct {
	Cursor *sql.Rows
	Db     *gorm.DB
}

func (m *EstuaryMhIterator) Next() (multihash.Multihash, error) {
	var nextCid cid.Cid
	if m.Cursor.Next() {
		m.Db.ScanRows(m.Cursor, &nextCid)
		return nextCid.Hash(), nil
	}
	return nil, io.EOF
}

// newIndexProvider creates a new index-provider engine to send announcements to storetheindex
// this needs to keep running continuously because storetheindex
// will come to fetch advertisements "when it feels like it"
func NewAutoretrieveEngine(ctx context.Context, tickInterval time.Duration, db *gorm.DB, libp2pHost host.Host) (*AutoretrieveEngine, error) {
	newEngine, err := New(
		WithHost(libp2pHost), // need to be localhost/estuary
		WithPublisherKind(DataTransferPublisher),
	)
	if err != nil {
		return nil, err
	}

	// Create index-provider engine (s.Node.IndexProvider) to send announcements to
	// this needs to keep running continuously because storetheindex
	// will come to fetch for advertisements "when it feels like it"
	newEngine.RegisterMultihashLister(func(ctx context.Context, contextID []byte) (provider.MultihashIterator, error) {

		arHandle := contextID // contextID is the autoretrieve handle
		if err != nil {
			return nil, err
		}

		var ar Autoretrieve
		// get the autoretrieve entry from the database
		err = db.Find(&ar, "handle = ?", arHandle).Error
		if err != nil {
			return nil, err
		}

		// find all new multihashes since the last time we advertised for this autoretrieve server
		// joins three tables (contents, obj_refs and objects) to query all CIDs (from objects)
		// that are active (from contents)
		// TODO: figure out where to close the Rows object. Maybe do an autoretrieve close function and call `defer ArEngine.Close()` in main
		rows, err := db.Model(&util.Content{}).
			Joins("LEFT JOIN (select * from obj_refs left join objects on obj_refs.object = objects.id) all_objs ON contents.id = all_objs.content").
			Where("contents.active = true AND all_objs.cid IS NOT NULL").
			Where("contents.created_at >= ?", ar.LastAdvertisement).
			Select("all_objs.cid AS cid").
			Rows()
		if err != nil {
			return nil, err
		}

		return &EstuaryMhIterator{
			Cursor: rows,
			Db:     db,
		}, nil
	})

	newEngine.context = ctx
	newEngine.tickInterval = tickInterval
	newEngine.db = db

	// start engine
	newEngine.Start(newEngine.context)

	return newEngine, nil
}

func (arEng *AutoretrieveEngine) announceForAR(ar Autoretrieve) error {
	newContextID := []byte(ar.Handle)

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

	// see if we need to make a new announcement for this AR server (if there were new CIDs since last advertisement)
	var newContentsCount int64
	err := arEng.db.Model(&util.Content{}).
		Joins("LEFT JOIN (select * from obj_refs left join objects on obj_refs.object = objects.id) all_objs ON contents.id = all_objs.content").
		Where("contents.active = true AND all_objs.cid IS NOT NULL").
		Where("contents.created_at >= ?", ar.LastAdvertisement).
		Select("all_objs.cid").
		Count(&newContentsCount).
		Error
	if err != nil {
		return fmt.Errorf("unable to query new CIDs from database: %s", err)
	}
	if newContentsCount == 0 {
		log.Debugf("no new CIDs to announce, skipping")
		return nil
	}

	log.Debugf("found %d new CIDs, announcing", newContentsCount)

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
	ticker := time.NewTicker(arEng.tickInterval)
	defer ticker.Stop()

	for {
		curTime = time.Now()
		lastTickTime = curTime.Add(-arEng.tickInterval)
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
