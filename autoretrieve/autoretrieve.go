package autoretrieve

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/application-research/estuary/util"
	provider "github.com/filecoin-project/index-provider"
	"github.com/filecoin-project/index-provider/engine"
	"github.com/filecoin-project/index-provider/metadata"
	"github.com/ipfs/go-cid"
	logging "github.com/ipfs/go-log/v2"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/multiformats/go-multihash"
	"gorm.io/gorm"
)

var log = logging.Logger("estuary")

type Autoretrieve struct {
	gorm.Model

	Handle            string `gorm:"unique"`
	Token             string `gorm:"unique"`
	LastConnection    time.Time
	LastAdvertisement time.Time
	PubKey            string `gorm:"unique"`
	Addresses         string
}

func (autoretrieve *Autoretrieve) AddrInfo() (*peer.AddrInfo, error) {
	addresses := strings.Split(autoretrieve.Addresses, ",")
	addrInfo, err := peer.AddrInfoFromString(addresses[0])
	if err != nil {
		return nil, err
	}

	return addrInfo, nil
}

type PublishedContents struct {
	gorm.Model

	ContentID          uint `gorm:"unique"`
	AutoretrieveHandle string
}

type HeartbeatAutoretrieveResponse struct {
	Handle            string         `json:"handle"`
	LastConnection    time.Time      `json:"lastConnection"`
	LastAdvertisement time.Time      `json:"lastAdvertisement"`
	AddrInfo          *peer.AddrInfo `json:"addrInfo"`
	AdvertiseInterval string         `json:"advertiseInterval"`
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
	AdvertiseInterval string         `json:"advertiseInterval"`
}

type Provider struct {
	engine *engine.Engine
	db     *gorm.DB
}

type Iterator struct {
	mhs       []multihash.Multihash
	index     uint
	contentID uint
}

func NewIterator(db *gorm.DB, contentID uint) (*Iterator, error) {

	// Read CID strings for this content ID
	var cidStrings []string
	if err := db.Raw(`SELECT cid FROM objects WHERE id IN (SELECT object FROM obj_refs WHERE content IN (SELECT id FROM contents WHERE contents.id = ?))`, contentID).Scan(&cidStrings).Error; err != nil {
		return nil, err
	}
	if len(cidStrings) == 0 {
		return nil, fmt.Errorf("no multihashes for this content")
	}
	log.Infof("Creating iterator for %d (%d MHs)", contentID, len(cidStrings))

	// Parse CID strings and extract multihashes
	var mhs []multihash.Multihash
	for _, cidString := range cidStrings {
		_, cid, err := cid.CidFromBytes([]byte(cidString))
		if err != nil {
			return nil, fmt.Errorf("failed to parse cid string '%s': %v", cidString, err)
		}

		mhs = append(mhs, cid.Hash())

		log.Infof("Multihash: %s", cid.Hash())
	}

	return &Iterator{
		mhs:       mhs,
		contentID: contentID,
	}, nil
}

func (iter *Iterator) Next() (multihash.Multihash, error) {
	if iter.index == uint(len(iter.mhs)) {
		return nil, io.EOF
	}

	mh := iter.mhs[iter.index]

	iter.index++

	return mh, nil
}

func NewProvider(db *gorm.DB) (*Provider, error) {
	eng, err := engine.New(engine.WithPublisherKind(engine.DataTransferPublisher), engine.WithDirectAnnounce("http://127.0.0.1:3001"))
	if err != nil {
		return nil, fmt.Errorf("failed to init engine: %v", err)
	}

	eng.RegisterMultihashLister(func(
		ctx context.Context,
		peer peer.ID,
		contextID []byte,
	) (provider.MultihashIterator, error) {
		iter, err := NewIterator(db, readContextID(contextID))
		if err != nil {
			return nil, err
		}

		return iter, nil
	})

	return &Provider{
		engine: eng,
		db:     db,
	}, nil
}

func (provider *Provider) Run(ctx context.Context) error {
	if err := provider.engine.Start(ctx); err != nil {
		return err
	}

	go func() {
		ctx := context.Background()

		var contentIDs []uint
		if err := provider.db.Model(&util.Content{}).Select("id").Find(&contentIDs).Error; err != nil {
			log.Errorf("No good: %v", err)
		}

		log.Infof("Got content IDs")

		var autoretrieves []Autoretrieve
		if err := provider.db.Find(&autoretrieves).Error; err != nil {
			log.Errorf("Failed to get autoretrieves: %v", err)
			return
		}

		// TODO: unnecessary
		if len(autoretrieves) == 0 {
			log.Errorf("No autoretrieves registered")
			return
		}

		addrInfo, err := autoretrieves[0].AddrInfo()
		if err != nil {
			log.Errorf("Failed to get autoretrieve address info: %v", err)
			return
		}

		for _, contentID := range contentIDs {
			log.Infof("(CONTENT ID)==> %d", contentID)
			adCid, err := provider.engine.NotifyPut(ctx, addrInfo, makeContextID(contentID), metadata.New(metadata.Bitswap{}))
			if err != nil {
				log.Errorf("Failed to publish: %v", err)
				continue
			}

			log.Infof("Published advertisement CID: %s", adCid)
		}
	}()

	return nil
}

func (provider *Provider) Stop() error {
	return provider.engine.Shutdown()
}

// Content ID to context ID
func makeContextID(contentID uint) []byte {
	contextID := make([]byte, 4)
	binary.BigEndian.PutUint32(contextID, uint32(contentID))
	return contextID
}

// Context ID to content ID
func readContextID(contextID []byte) uint {
	return uint(binary.BigEndian.Uint32(contextID))
}
