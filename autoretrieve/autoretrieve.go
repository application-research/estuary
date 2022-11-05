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

// A batch that has been published for a specific autoretrieve
type PublishedBatch struct {
	gorm.Model

	FirstContentID     uint `gorm:"unique"`
	Count              uint
	AutoretrieveHandle string
}

func (PublishedBatch) TableName() string { return "published_batches" }

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
	engine            *engine.Engine
	db                *gorm.DB
	advertiseInterval time.Duration
	batchSize         uint
}

type Iterator struct {
	mhs            []multihash.Multihash
	index          uint
	firstContentID uint
	count          uint
}

func NewIterator(db *gorm.DB, firstContentID uint, count uint) (*Iterator, error) {

	// Read CID strings for this content ID
	var cidStrings []string
	if err := db.Raw(
		"SELECT objects.cid FROM objects LEFT JOIN obj_refs ON objects.id = obj_refs.object WHERE obj_refs.content BETWEEN ? AND ?",
		firstContentID,
		firstContentID+count,
	).Scan(&cidStrings).Error; err != nil {
		return nil, err
	}

	if len(cidStrings) == 0 {
		return nil, fmt.Errorf("no multihashes for this content")
	}

	log.Infof(
		"Creating iterator for content IDs %d to %d (%d MHs)",
		firstContentID,
		firstContentID+count,
		len(cidStrings),
	)

	// Parse CID strings and extract multihashes
	var mhs []multihash.Multihash
	for _, cidString := range cidStrings {
		_, cid, err := cid.CidFromBytes([]byte(cidString))
		if err != nil {
			log.Warnf("Failed to parse cid string '%s': %v", cidString, err)
			continue
		}

		mhs = append(mhs, cid.Hash())
	}

	return &Iterator{
		mhs:            mhs,
		firstContentID: firstContentID,
		count:          count,
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

func NewProvider(db *gorm.DB, advertiseInterval time.Duration, indexerURL string) (*Provider, error) {
	eng, err := engine.New(engine.WithPublisherKind(engine.DataTransferPublisher), engine.WithDirectAnnounce(indexerURL))
	if err != nil {
		return nil, fmt.Errorf("failed to init engine: %v", err)
	}

	eng.RegisterMultihashLister(func(
		ctx context.Context,
		peer peer.ID,
		contextID []byte,
	) (provider.MultihashIterator, error) {
		firstContentID, count := readContextID(contextID)
		log.Infof("Received pull request (first content ID: %d, count: %d)", firstContentID, count)
		iter, err := NewIterator(db, firstContentID, count)
		if err != nil {
			return nil, err
		}

		return iter, nil
	})

	return &Provider{
		engine:            eng,
		db:                db,
		advertiseInterval: advertiseInterval,
		batchSize:         25000,
	}, nil
}

func (provider *Provider) Run(ctx context.Context) error {
	if err := provider.engine.Start(ctx); err != nil {
		return err
	}

	// time.Tick will drop ticks to make up for slow advertisements
	log.Infof("Starting autoretrieve update loop every %s", provider.advertiseInterval)
	ticker := time.NewTicker(provider.advertiseInterval)
	for ; true; <-ticker.C {
		if ctx.Err() != nil {
			ticker.Stop()
			break
		}

		log.Infof("Starting autoretrieve advertisement tick")

		ctx := context.Background()

		// Find the highest current content ID for later
		var lastContent util.Content
		if err := provider.db.Last(&lastContent).Error; err != nil {
			log.Infof("Failed to get last provider content ID: %v", err)
			continue
		}

		// TODO: make sure last advertised is within threshold
		var onlineAutoretrieves []Autoretrieve
		if err := provider.db.Find(&onlineAutoretrieves).Error; err != nil {
			log.Errorf("Failed to get autoretrieves: %v", err)
			continue
		}

		// For each currently available autoretrieve...
		for _, autoretrieve := range onlineAutoretrieves {

			addrInfo, err := autoretrieve.AddrInfo()
			if err != nil {
				log.Errorf("Failed to get autoretrieve address info: %v", err)
				continue
			}

			// For each batch that should be advertised...
			for firstContentID := uint(0); firstContentID <= lastContent.ID; firstContentID += provider.batchSize {

				// Find the amount of contents in this batch (likely less than
				// the batch size if this is the last batch)
				count := provider.batchSize
				remaining := lastContent.ID - firstContentID
				if remaining < count {
					count = remaining
				}

				log := log.With("first_content_id", firstContentID, "count", count)

				// Search for an entry (this array will have either 0 or 1
				// elements)
				var publishedBatches []PublishedBatch
				if err := provider.db.Where("autoretrieve_handle = ? AND first_content_id = ?", autoretrieve.Handle, firstContentID).Find(&publishedBatches).Error; err != nil {
					log.Errorf("Failed to get published contents: %v", err)
					continue
				}

				// And check if it's...

				// 1. fully advertised: do nothing
				if len(publishedBatches) != 0 && publishedBatches[0].Count == provider.batchSize {
					log.Errorf("Skipping already advertised batch")
					continue
				}

				// In other cases, we will definitely notify put, so do it first
				adCid, err := provider.engine.NotifyPut(
					ctx,
					addrInfo,
					// The batch size should always be the same unless the
					// config changes
					makeContextID(firstContentID, provider.batchSize),
					metadata.New(metadata.Bitswap{}),
				)
				if err != nil {
					log.Errorf("Failed to publish batch: %v", err)
					continue
				}

				log.Infof("Published ad CID: %s", adCid)

				// 2. not advertised: now that notify put is called, create DB
				// entry, continue
				if len(publishedBatches) == 0 {
					log.Infof("Published new batch")
					if err := provider.db.Create(&PublishedBatch{
						FirstContentID:     firstContentID,
						AutoretrieveHandle: autoretrieve.Handle,
						Count:              count,
					}).Error; err != nil {
						log.Errorf("Failed to write batch to database")
					}
					continue
				}

				// 3. incompletely advertised: now that notify put is called,
				// update DB entry, continue
				publishedBatch := publishedBatches[0]
				if publishedBatch.Count != count {
					log.Infof("Updated incomplete batch")
					publishedBatch.Count = count
					if err := provider.db.Save(&publishedBatch).Error; err != nil {
						log.Errorf("Failed to update batch in database")
					}
					continue
				}
			}
		}
	}

	return nil
}

func (provider *Provider) Stop() error {
	return provider.engine.Shutdown()
}

// Content ID to context ID
func makeContextID(firstContentID uint, count uint) []byte {
	contextID := make([]byte, 8)
	binary.BigEndian.PutUint32(contextID[0:4], uint32(firstContentID))
	binary.BigEndian.PutUint32(contextID[4:8], uint32(count))
	return contextID
}

// Context ID to content ID
func readContextID(contextID []byte) (uint, uint) {
	return uint(binary.BigEndian.Uint32(contextID[0:4])),
		uint(binary.BigEndian.Uint32(contextID[4:8]))
}
