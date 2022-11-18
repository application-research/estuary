package autoretrieve

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/application-research/estuary/constants"
	"github.com/application-research/estuary/util"
	providerpkg "github.com/filecoin-project/index-provider"
	"github.com/filecoin-project/index-provider/engine"
	"github.com/filecoin-project/index-provider/metadata"
	"github.com/ipfs/go-cid"
	logging "github.com/ipfs/go-log/v2"
	"github.com/libp2p/go-libp2p-core/crypto"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/multiformats/go-multiaddr"
	"github.com/multiformats/go-multihash"
	"gorm.io/gorm"
)

var log = logging.Logger("autoretrieve")

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
	addrStrings := strings.Split(autoretrieve.Addresses, ",")

	pubKeyBytes, err := crypto.ConfigDecodeKey(autoretrieve.PubKey)
	if err != nil {
		return nil, err
	}
	pubKey, err := crypto.UnmarshalPublicKey(pubKeyBytes)
	if err != nil {
		return nil, err
	}
	peerID, err := peer.IDFromPublicKey(pubKey)
	if err != nil {
		return nil, err
	}

	var addrs []multiaddr.Multiaddr
	var invalidAddrStrings []string
	for _, addrString := range addrStrings {
		addr, err := multiaddr.NewMultiaddr(addrString)
		if err != nil {
			invalidAddrStrings = append(invalidAddrStrings, addrString)
			continue
		}
		addrs = append(addrs, addr)
	}
	if len(invalidAddrStrings) != 0 {
		return nil, fmt.Errorf("got invalid addresses: %#v", invalidAddrStrings)
	}

	addrInfo := peer.AddrInfo{
		ID:    peerID,
		Addrs: addrs,
	}

	return &addrInfo, nil
}

// A batch that has been published for a specific autoretrieve
type PublishedBatch struct {
	gorm.Model

	FirstContentID     uint
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
	engine                *engine.Engine
	db                    *gorm.DB
	advertisementInterval time.Duration
	advertiseOffline      bool
	batchSize             uint
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
	// NOTE(@elijaharita 2022-12-11): CIDs are often empty in the database for
	// some reason, so I just count the amount that are empty and put one print
	// statement for them at the end to avoid thousands of lines of log spam.
	emptyCount := 0
	for _, cidString := range cidStrings {
		if cidString == "" {
			emptyCount++
			continue
		}

		_, cid, err := cid.CidFromBytes([]byte(cidString))
		if err != nil {
			log.Warnf("Failed to parse CID string '%s': %v", cidString, err)
			continue
		}

		mhs = append(mhs, cid.Hash())
	}

	if emptyCount != 0 {
		log.Warnf("Skipped %d empty CIDs", emptyCount)
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

func NewProvider(db *gorm.DB, advertisementInterval time.Duration, indexerURL string, advertiseOffline bool) (*Provider, error) {
	eng, err := engine.New(engine.WithPublisherKind(engine.DataTransferPublisher), engine.WithDirectAnnounce(indexerURL))
	if err != nil {
		return nil, fmt.Errorf("failed to init engine: %v", err)
	}

	eng.RegisterMultihashLister(func(
		ctx context.Context,
		peer peer.ID,
		contextID []byte,
	) (providerpkg.MultihashIterator, error) {
		log := log.Named("lister")

		params, err := readContextID(contextID)
		if err != nil {
			return nil, err
		}

		log = log.With(
			"first_content_id", params.firstContentID,
			"count", params.count,
			"indexer_peer_id", params.provider,
		)

		log.Infof(
			"Received pull request (peer ID: %s, first content ID: %d, count: %d)",
			params.provider,
			params.firstContentID,
			params.count,
		)
		iter, err := NewIterator(db, params.firstContentID, params.count)
		if err != nil {
			return nil, err
		}

		return iter, nil
	})

	return &Provider{
		engine:                eng,
		db:                    db,
		advertisementInterval: advertisementInterval,
		advertiseOffline:      advertiseOffline,
		batchSize:             constants.AutoretrieveProviderBatchSize,
	}, nil
}

func (provider *Provider) Run(ctx context.Context) error {
	log := log.Named("loop")

	if err := provider.engine.Start(ctx); err != nil {
		return err
	}

	// time.Tick will drop ticks to make up for slow advertisements
	log.Infof("Starting autoretrieve advertisement loop every %s", provider.advertisementInterval)
	ticker := time.NewTicker(provider.advertisementInterval)
	for ; true; <-ticker.C {
		if ctx.Err() != nil {
			ticker.Stop()
			break
		}

		log.Infof("Starting autoretrieve advertisement tick")

		// Find the highest current content ID for later
		var lastContent util.Content
		if err := provider.db.Last(&lastContent).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				log.Infof("Failed to get last provider content ID: %v", err)
				continue
			} else {
				log.Warnf("No contents to advertise")
				continue
			}
		}

		var autoretrieves []Autoretrieve
		if err := provider.db.Find(&autoretrieves).Error; err != nil {
			log.Errorf("Failed to get autoretrieves: %v", err)
			continue
		}

		// For each registered autoretrieve...
		for _, autoretrieve := range autoretrieves {
			log := log.With("autoretrieve_handle", autoretrieve.Handle)

			// Make sure it is online (if offline checking isn't disabled)
			if !provider.advertiseOffline {
				if time.Since(autoretrieve.LastConnection) > provider.advertisementInterval {
					log.Debugf("Skipping offline autoretrieve")
					continue
				}
			}

			// Get address info for later
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
				// elements depending on whether an advertisement was found)
				var publishedBatches []PublishedBatch
				if err := provider.db.Where(
					"autoretrieve_handle = ? AND first_content_id = ?",
					autoretrieve.Handle,
					firstContentID,
				).Find(&publishedBatches).Error; err != nil {
					log.Errorf("Failed to get published contents: %v", err)
					continue
				}

				// And check if it's...

				// 1. fully advertised, or no changes: do nothing
				if len(publishedBatches) != 0 && publishedBatches[0].Count == count {
					log.Debugf("Skipping already advertised batch")
					continue
				}

				// The batch size should always be the same unless the
				// config changes
				contextID, err := makeContextID(contextParams{
					provider:       addrInfo.ID,
					firstContentID: firstContentID,
					count:          provider.batchSize,
				})
				if err != nil {
					log.Errorf("Failed to make context ID: %v", err)
					continue
				}

				// 2. not advertised: notify put, create DB entry, continue
				if len(publishedBatches) == 0 {
					adCid, err := provider.engine.NotifyPut(
						ctx,
						addrInfo,
						contextID,
						metadata.New(metadata.Bitswap{}),
					)
					if err != nil {
						// If there was an error, check whether already
						// advertised
						if errors.Is(err, providerpkg.ErrAlreadyAdvertised) {
							// If so, try deleting it first...
							log.Warnf("Batch was unexpectedly already advertised, removing old batch")
							if _, err := provider.engine.NotifyRemove(ctx, addrInfo.ID, contextID); err != nil {
								log.Errorf("Failed to remove unexpected existing advertisement: %v", err)
							}

							// ...and then re-advertise
							_adCid, err := provider.engine.NotifyPut(
								ctx,
								addrInfo,
								contextID,
								metadata.New(metadata.Bitswap{}),
							)
							if err != nil {
								log.Errorf("Failed to publish batch after deleting unexpected existing advertisement: %v", err)
								continue
							}

							adCid = _adCid
						} else {
							// Otherwise, fail out
							log.Errorf("Failed to publish batch: %v", err)
							continue
						}
					}

					log.Infof("Published new batch with advertisement CID %s", adCid)
					if err := provider.db.Create(&PublishedBatch{
						FirstContentID:     firstContentID,
						AutoretrieveHandle: autoretrieve.Handle,
						Count:              count,
					}).Error; err != nil {
						log.Errorf("Failed to write batch to database: %v", err)
					}
					continue
				}

				// 3. incompletely advertised: delete and then notify put,
				// update DB entry, continue
				publishedBatch := publishedBatches[0]
				if publishedBatch.Count != count {
					oldAdCid, err := provider.engine.NotifyRemove(
						ctx,
						addrInfo.ID,
						contextID,
					)
					if err != nil {
						log.Warnf("Failed to remove batch (going to re-publish anyway): %v", err)
					}
					log.Infof("Removed old advertisement")

					adCid, err := provider.engine.NotifyPut(
						ctx,
						addrInfo,
						contextID,
						metadata.New(metadata.Bitswap{}),
					)
					if err != nil {
						log.Errorf("Failed to publish batch: %v", err)
						continue
					}

					log.Infof("Updated incomplete batch with new ad CID %s (previously %s)", adCid, oldAdCid)
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

type contextParams struct {
	provider       peer.ID
	firstContentID uint
	count          uint
}

// Content ID to context ID
func makeContextID(params contextParams) ([]byte, error) {
	contextID := make([]byte, 8)
	binary.BigEndian.PutUint32(contextID[0:4], uint32(params.firstContentID))
	binary.BigEndian.PutUint32(contextID[4:8], uint32(params.count))

	peerIDBytes, err := params.provider.MarshalBinary()
	if err != nil {
		return nil, fmt.Errorf("failed to write context peer ID: %v", err)
	}
	contextID = append(contextID, peerIDBytes...)
	return contextID, nil
}

// Context ID to content ID
func readContextID(contextID []byte) (contextParams, error) {
	peerID, err := peer.IDFromBytes(contextID[8:])
	if err != nil {
		return contextParams{}, fmt.Errorf("failed to read context peer ID: %v", err)
	}

	return contextParams{
		provider:       peerID,
		firstContentID: uint(binary.BigEndian.Uint32(contextID[0:4])),
		count:          uint(binary.BigEndian.Uint32(contextID[4:8])),
	}, nil
}
