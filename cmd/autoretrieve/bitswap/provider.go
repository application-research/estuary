package bitswap

import (
	"context"
	"sync"

	"github.com/application-research/estuary/cmd/autoretrieve/blocks"
	"github.com/application-research/estuary/cmd/autoretrieve/filecoin"
	"github.com/ipfs/go-bitswap/message"
	bitswap_message_pb "github.com/ipfs/go-bitswap/message/pb"
	"github.com/ipfs/go-bitswap/network"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	"github.com/ipfs/go-log/v2"
	"github.com/ipfs/go-peertaskqueue"
	"github.com/ipfs/go-peertaskqueue/peertask"
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

type taskQueueTopic uint

const (
	topicSendBlock taskQueueTopic = iota
	topicSendHave
	topicSendDontHave
)

const targetMessageSize = 16384

type ProviderConfig struct {
	MaxSendWorkers uint
}

type Provider struct {
	config            ProviderConfig
	network           network.BitSwapNetwork
	blockManager      *blocks.Manager
	retriever         *filecoin.Retriever
	taskQueue         *peertaskqueue.PeerTaskQueue
	sendWorkerCount   uint
	sendWorkerCountLk sync.Mutex
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
		config:          config,
		network:         network.NewFromIpfsHost(host, fullRT),
		blockManager:    blockManager,
		retriever:       retriever,
		taskQueue:       peertaskqueue.New(),
		sendWorkerCount: 0,
	}

	provider.network.SetDelegate(provider)

	return provider, nil
}

func (provider *Provider) startSend() {
	provider.sendWorkerCountLk.Lock()
	defer provider.sendWorkerCountLk.Unlock()

	if provider.sendWorkerCount < provider.config.MaxSendWorkers {
		provider.sendWorkerCount++
		go func() {
			for {
				peer, tasks, _ := provider.taskQueue.PopTasks(targetMessageSize)

				if len(tasks) == 0 {
					break
				}

				msg := message.New(false)

				for _, task := range tasks {
					switch task.Topic {
					case topicSendBlock:
						msg.AddBlock(task.Data.(blocks.Block))
					case topicSendHave:
						msg.AddHave(task.Data.(cid.Cid))
					case topicSendDontHave:
						msg.AddDontHave(task.Data.(cid.Cid))
					}
				}

				if err := provider.network.SendMessage(context.Background(), peer, msg); err != nil {
					logger.Errorf("Could not send bitswap message to %s: %v", peer, err)
				}
			}

			provider.sendWorkerCountLk.Lock()
			provider.sendWorkerCount--
			provider.sendWorkerCountLk.Unlock()
		}()
	}
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
			switch entry.WantType {
			case wantTypeHave:
				provider.taskQueue.PushTasks(sender, peertask.Task{
					Topic:    topicSendHave,
					Priority: 0,
					Work:     block.Cid().ByteLen(),
					Data:     block.Cid(),
				})
			case wantTypeBlock:
				provider.taskQueue.PushTasks(sender, peertask.Task{
					Topic:    topicSendBlock,
					Priority: 0,
					Work:     len(block.RawData()),
					Data:     block,
				})
			}
		default:
			// Otherwise, we will need to ask for it...
			if err := provider.retriever.Request(entry.Cid); err != nil {
				// If request failed, it means there's no way we'll be able to
				// get that block at the moment, so respond with DONT_HAVE
				provider.taskQueue.PushTasks(sender, peertask.Task{
					Topic:    topicSendDontHave,
					Priority: 0,
					Work:     entry.Cid.ByteLen(),
					Data:     entry.Cid,
				})
				continue entryLoop
			}

			// Queue the block to be sent once we get it
			var callback func(blocks.Block)
			switch entry.WantType {
			case wantTypeHave:
				callback = func(block blocks.Block) {
					provider.taskQueue.PushTasks(sender, peertask.Task{
						Topic:    topicSendHave,
						Priority: 0,
						Work:     block.Cid().ByteLen(),
						Data:     block.Cid(),
					})
				}
			case wantTypeBlock:
				callback = func(block blocks.Block) {
					provider.taskQueue.PushTasks(sender, peertask.Task{
						Topic:    topicSendBlock,
						Priority: 0,
						Work:     len(block.RawData()),
						Data:     block,
					})
				}
			}
			if err := provider.blockManager.GetAwait(entry.Cid, callback); err != nil {
				logger.Errorf("Error waiting for block: %v", err)
			}
		}
	}

	provider.startSend()
}

func (provider *Provider) ReceiveError(err error) {
	logger.Errorf("Error receiving bitswap message: %v", err)
}

func (provider *Provider) PeerConnected(peer peer.ID) {}

func (provider *Provider) PeerDisconnected(peer peer.ID) {}

// Sends either a HAVE or a block to a peer, depending on whether the peer
// requested a WANT_HAVE or WANT_BLOCK.
// func (provider *Provider) respondPositive(peer peer.ID, entry message.Entry, block blocks.Block) {
// 	msg := message.New(false)

// 	switch entry.WantType {
// 	case wantTypeHave:
// 		// If WANT_HAVE, say we have the block
// 		msg.AddHave(entry.Cid)
// 	case wantTypeBlock:
// 		// If WANT_BLOCK, make sure block is not nil, and add the block to the
// 		// response
// 		if block == nil {
// 			logger.Errorf("Cannot respond to WANT_BLOCK from %s with nil block", peer)
// 			return
// 		}
// 		msg.AddBlock(block)
// 	default:
// 		logger.Errorf("Cannot respond to %s with unknown want type %v", peer, entry.WantType)
// 		return
// 	}

// 	if err := provider.network.SendMessage(context.Background(), peer, msg); err != nil {
// 		logger.Errorf("Failed to respond to %s: %v", peer, err)
// 	}
// }

// func (provider *Provider) respondNegative(peer peer.ID, entry message.Entry) {
// 	msg := message.New(false)

// 	msg.AddDontHave(entry.Cid)

// 	if err := provider.network.SendMessage(context.Background(), peer, msg); err != nil {
// 		logger.Errorf("Failed to respond to %s: %v", peer, err)
// 	}
// }
