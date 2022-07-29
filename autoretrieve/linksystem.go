package autoretrieve

import (
	"bytes"
	"errors"
	"io"

	provider "github.com/filecoin-project/index-provider"
	"github.com/filecoin-project/storetheindex/api/v0/ingest/schema"
	"github.com/ipfs/go-datastore"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/codec/dagjson"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/node/basicnode"
)

var errNoEntries = errors.New("no entries; see schema.NoEntries")

// Creates the main engine linksystem.
func (e *AutoretrieveEngine) mkLinkSystem() ipld.LinkSystem {
	lsys := cidlink.DefaultLinkSystem()
	lsys.StorageReadOpener = func(lctx ipld.LinkContext, lnk ipld.Link) (io.Reader, error) {
		// If link corresponds to schema.NoEntries return error immediately.
		if lnk == schema.NoEntries {
			return nil, errNoEntries
		}

		ctx := lctx.Ctx
		c := lnk.(cidlink.Link).Cid
		log.Debugf("Triggered ReadOpener from engine's linksystem with cid (%s)", c)

		// Get the node from main datastore. If it is in the
		// main datastore it means it is an advertisement.
		val, err := e.ds.Get(ctx, datastore.NewKey(c.String()))
		if err != nil && err != datastore.ErrNotFound {
			log.Errorf("Error getting object from datastore in linksystem: %s", err)
			return nil, err
		}

		// If data was retrieved from the datastore, this may be an advertisement.
		if len(val) != 0 {
			// Decode the node to check its type to see if it is an Advertisement.
			n, err := decodeIPLDNode(bytes.NewBuffer(val))
			if err != nil {
				log.Errorf("Could not decode IPLD node for potential advertisement: %s", err)
				return nil, err
			}
			// If this was an advertisement, then return it.
			if isAdvertisement(n) {
				log.Infow("Retrieved advertisement from datastore", "cid", c, "size", len(val))
				return bytes.NewBuffer(val), nil
			}
			log.Infow("Retrieved non-advertisement object from datastore", "cid", c, "size", len(val))
		}

		// Not an advertisement, so this means we are receiving ingestion data.

		// If no lister registered return error
		if e.mhLister == nil {
			log.Error("No multihash lister has been registered in engine")
			return nil, provider.ErrNoMultihashLister
		}

		log.Debugw("Checking cache for data", "cid", c)

		// Check if the key is already cached.
		b, err := e.entriesChunker.GetRawCachedChunk(ctx, lnk)
		if err != nil {
			log.Errorf("Error fetching cached list for Cid (%s): %s", c, err)
			return nil, err
		}

		// If we don't have the link, generate the linked list of entries in
		// cache so it is ready to serve for this and future ingestion.
		//
		// The reason for caching this is because the indexer requests each
		// chunk entry, and a specific subset of entries cannot be read from a
		// car.  So all entry chunks are kept in cache to serve to the indexer.
		// The cache uses the entry chunk CID as a key that maps to the entry
		// chunk data.
		if b == nil {
			log.Infow("Entry for CID is not cached, generating chunks", "cid", c)
			// If the link is not found, it means that the root link of the list has
			// not been generated and we need to get the relationship between the cid
			// received and the contextID so the lister knows how to
			// regenerate the list of CIDs.
			key, err := e.getCidKeyMap(ctx, c)
			if err != nil {
				log.Errorf("Error fetching relationship between CID and contextID: %s", err)
				return nil, err
			}

			// Get the car iterator needed to create the entry chunks.
			// Normally for removal this is not needed since the indexer
			// deletes all indexes for the contextID in the removal
			// advertisement.  Only if the removal had no contextID would the
			// indexer ask for entry chunks to remove.
			mhIter, err := e.mhLister(ctx, key)
			if err != nil {
				return nil, err
			}

			// Store the linked list entries in cache as we generate them.  We
			// use the cache linksystem that stores entries in an in-memory
			// datastore.
			_, err = e.entriesChunker.Chunk(ctx, mhIter)
			if err != nil {
				log.Errorf("Error generating linked list from multihash lister: %s", err)
				return nil, err
			}
		} else {
			log.Infow("Found cache entry for CID", "cid", c)
		}

		// Return the linked list node.
		val, err = e.entriesChunker.GetRawCachedChunk(ctx, lnk)
		if err != nil {
			log.Errorf("Error fetching cached list for CID (%s): %s", c, err)
			return nil, err
		}

		// If no value was populated it means that nothing was found
		// in the multiple datastores.
		if len(val) == 0 {
			log.Errorf("No object found in linksystem for CID (%s)", c)
			return nil, datastore.ErrNotFound
		}

		return bytes.NewBuffer(val), nil
	}
	lsys.StorageWriteOpener = func(lctx ipld.LinkContext) (io.Writer, ipld.BlockWriteCommitter, error) {
		buf := bytes.NewBuffer(nil)
		return buf, func(lnk ipld.Link) error {
			c := lnk.(cidlink.Link).Cid
			return e.ds.Put(lctx.Ctx, datastore.NewKey(c.String()), buf.Bytes())
		}, nil
	}
	return lsys
}

// vanillaLinkSystem plainly loads and stores from engine datastore.
//
// This is used to plainly load and store links without the complex
// logic of the main linksystem. This is mainly used to retrieve
// stored advertisements through the link from the main blockstore.
func (e *AutoretrieveEngine) vanillaLinkSystem() ipld.LinkSystem {
	lsys := cidlink.DefaultLinkSystem()
	lsys.StorageReadOpener = func(lctx ipld.LinkContext, lnk ipld.Link) (io.Reader, error) {
		c := lnk.(cidlink.Link).Cid
		val, err := e.ds.Get(lctx.Ctx, datastore.NewKey(c.String()))
		if err != nil {
			return nil, err
		}
		return bytes.NewBuffer(val), nil
	}
	lsys.StorageWriteOpener = func(lctx ipld.LinkContext) (io.Writer, ipld.BlockWriteCommitter, error) {
		buf := bytes.NewBuffer(nil)
		return buf, func(lnk ipld.Link) error {
			c := lnk.(cidlink.Link).Cid
			return e.ds.Put(lctx.Ctx, datastore.NewKey(c.String()), buf.Bytes())
		}, nil
	}
	return lsys
}

// decodeIPLDNode reads the content of the given reader fully as an IPLD node.
func decodeIPLDNode(r io.Reader) (ipld.Node, error) {
	nb := basicnode.Prototype.Any.NewBuilder()
	err := dagjson.Decode(nb, r)
	if err != nil {
		return nil, err
	}
	return nb.Build(), nil
}

// isAdvertisement loosely checks if an IPLD node is an advertisement or an index.
// This is done simply by checking if `Signature` filed is present.
func isAdvertisement(n ipld.Node) bool {
	indexID, _ := n.LookupByString("Signature")
	return indexID != nil
}
