package bitswap

import (
	"context"
	"sync"

	"github.com/application-research/estuary/cmd/autoretrieve/blocks"
	"github.com/application-research/estuary/cmd/autoretrieve/filecoin"
	"github.com/ipfs/go-bitswap/message"
	bitswap_message_pb "github.com/ipfs/go-bitswap/message/pb"
	"github.com/ipfs/go-bitswap/network"
	"github.com/ipfs/go-datastore"
	"github.com/ipfs/go-log/v2"
	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/peer"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	"github.com/libp2p/go-libp2p-kad-dht/fullrt"
)

var logger = log.Logger("autoretrieve")

const (
	wantTypeHave  = bitswap_message_pb.Message_Wantlist_Have
	wantTypeBlock = bitswap_message_pb.Message_Wantlist_Block
)

type ProviderConfig struct {
	DataDir string
}

type Provider struct {
	config        ProviderConfig
	network       network.BitSwapNetwork
	blockManager  *blocks.Manager
	retriever     *filecoin.Retriever
	blockQueues   map[peer.ID]BlockQueue
	blockQueuesLk sync.Mutex
}

type BlockQueue struct {
	wantHaveChan  chan blocks.Block
	wantBlockChan chan blocks.Block
}

func NewProvider(
	config ProviderConfig,
	host host.Host,
	datastore datastore.Batching,
	blockManager *blocks.Manager,
	retriever *filecoin.Retriever,
) (*Provider, error) {

	fullRT, err := fullrt.NewFullRT(host, dht.DefaultPrefix, fullrt.DHTOption(
		dht.Datastore(datastore),
		dht.BucketSize(20),
		dht.BootstrapPeers(dht.GetDefaultBootstrapPeerAddrInfos()...),
	))
	if err != nil {
		return nil, err
	}

	provider := &Provider{
		config:       config,
		network:      network.NewFromIpfsHost(host, fullRT),
		blockManager: blockManager,
		retriever:    retriever,
		blockQueues:  make(map[peer.ID]BlockQueue),
	}

	provider.network.SetDelegate(provider)

	return provider, nil
}

// Upon receiving a message, provider will iterate over the requested CIDs. For
// each CID, it'll check if it's present in the blockstore. If it is, it will
// respond with that block, and if it isn't, it'll start a retrieval and
// register the block to be sent later.
func (provider *Provider) ReceiveMessage(ctx context.Context, sender peer.ID, incoming message.BitSwapMessage) {

	// For each item in the incoming wantlist, we either need to say whether we
	// have the block or not, or send the actual block.

entryLoop:
	for _, entry := range incoming.Wantlist() {

		// Only respond to WANT_HAVE and WANT_BLOCK
		if entry.WantType != wantTypeHave && entry.WantType != wantTypeBlock {
			continue
		}

		// First we check the local blockstore
		block, err := provider.blockManager.Get(entry.Cid)

		switch {
		case err == nil:
			// If we did have the block locally, just respond with that
			provider.respondPositive(sender, entry, block)
		default:
			// Otherwise, we will need to ask for it...
			if err := provider.retriever.Request(entry.Cid); err != nil {
				// If request failed, it means there's no way we'll be able to
				// get that block at the moment, so respond with DONT_HAVE
				provider.respondNegative(sender, entry)
				continue entryLoop
			}

			var queue chan blocks.Block

			provider.blockQueuesLk.Lock()
			switch entry.WantType {
			case wantTypeHave:
				queue = provider.blockQueues[sender].wantHaveChan
			case wantTypeBlock:
				queue = provider.blockQueues[sender].wantBlockChan
			}
			provider.blockQueuesLk.Unlock()

			provider.blockManager.GetAwait(entry.Cid, queue)
		}
	}
}

func (provider *Provider) ReceiveError(err error) {
	logger.Errorf("Error receiving bitswap message: %v", err)
}

func (provider *Provider) PeerConnected(peer peer.ID) {
	blockQueue := BlockQueue{
		wantHaveChan:  make(chan blocks.Block, 10),
		wantBlockChan: make(chan blocks.Block, 10),
	}

	// When a peer connects, create a queue for it
	provider.blockQueuesLk.Lock()
	provider.blockQueues[peer] = blockQueue
	provider.blockQueuesLk.Unlock()

	// And start a send routine each for the WANT_BLOCKs and WANT_HAVEs

	go func() {
		for block := range blockQueue.wantBlockChan {
			msg := message.New(false)
			msg.AddBlock(block)
			if err := provider.network.SendMessage(context.Background(), peer, msg); err != nil {
				logger.Errorf("Could not send block %s to %s: %v", block.Cid(), peer, err)
			}
		}
	}()

	go func() {
		for block := range blockQueue.wantHaveChan {
			msg := message.New(false)
			msg.AddHave(block.Cid())
			if err := provider.network.SendMessage(context.Background(), peer, msg); err != nil {
				logger.Errorf("Could not send HAVE %s to %s: %v", block.Cid(), peer, err)
			}
		}
	}()
}

func (provider *Provider) PeerDisconnected(peer peer.ID) {
	// On disconnect, destroy the peer's queue
	provider.blockQueuesLk.Lock()
	blockQueue := provider.blockQueues[peer]
	close(blockQueue.wantBlockChan)
	close(blockQueue.wantHaveChan)
	delete(provider.blockQueues, peer)
	provider.blockQueuesLk.Unlock()
}

// Sends either a HAVE or a block to a peer, depending on whether the peer
// requested a WANT_HAVE or WANT_BLOCK.
func (provider *Provider) respondPositive(peer peer.ID, entry message.Entry, block blocks.Block) {
	msg := message.New(false)

	switch entry.WantType {
	case wantTypeHave:
		// If WANT_HAVE, say we have the block
		msg.AddHave(entry.Cid)
	case wantTypeBlock:
		// If WANT_BLOCK, make sure block is not nil, and add the block to the
		// response
		if block == nil {
			logger.Errorf("Cannot respond to WANT_BLOCK from %s with nil block", peer)
			return
		}
		msg.AddBlock(block)
	default:
		logger.Errorf("Cannot respond to %s with unknown want type %v", peer, entry.WantType)
		return
	}

	if err := provider.network.SendMessage(context.Background(), peer, msg); err != nil {
		logger.Errorf("Failed to respond to %s: %v", peer, err)
	}
}

func (provider *Provider) respondNegative(peer peer.ID, entry message.Entry) {
	msg := message.New(false)

	msg.AddDontHave(entry.Cid)

	if err := provider.network.SendMessage(context.Background(), peer, msg); err != nil {
		logger.Errorf("Failed to respond to %s: %v", peer, err)
	}
}
