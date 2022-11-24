package config

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/application-research/filclient"
	boostcar "github.com/filecoin-project/boost/car"
	"github.com/filecoin-project/boost/transport/httptransport"
	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-data-transfer/channelmonitor"
	"github.com/filecoin-project/lotus/api"
	"github.com/filecoin-project/lotus/chain/wallet"
	"github.com/ipfs/go-datastore"
	gsimpl "github.com/ipfs/go-graphsync/impl"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	"github.com/libp2p/go-libp2p/core/host"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/facebookgo/atomicfile"
)

var ErrNotInitialized = errors.New("node not initialized, please run configure")

// encode configuration with JSON
func encode(cfg interface{}, w io.Writer) error {
	// need to prettyprint, hence MarshalIndent, instead of Encoder
	buf, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		return err
	}
	_, err = w.Write(buf)
	return err
}

func load(cfg interface{}, filename string) (err error) {
	f, err := os.Open(filepath.Clean(filename))
	if err != nil {
		if os.IsNotExist(err) {
			err = ErrNotInitialized
		}
		return err
	}

	defer func() {
		if errC := f.Close(); errC != nil {
			err = errC
		}
	}()

	if err := json.NewDecoder(f).Decode(cfg); err != nil {
		return fmt.Errorf("failure to decode config: %s", err)
	}
	return err
}

// save writes the config from `cfg` into `filename`.
func save(cfg interface{}, filename string) error {
	err := os.MkdirAll(filepath.Dir(filename), 0750)
	if err != nil {
		return err
	}

	f, err := atomicfile.New(filename, 0600)
	if err != nil {
		return err
	}
	defer f.Close()

	return encode(cfg, f)
}

const maxTraversalLinks = 32 * (1 << 20)

func FilclientConfig(h host.Host, api api.Gateway, w *wallet.LocalWallet, addr address.Address, bs blockstore.Blockstore, ds datastore.Batching, ddir string, throttleLimit uint, opts ...func(*filclient.Config)) *filclient.Config {
	cfg := &filclient.Config{
		Host:       h,
		Api:        api,
		Wallet:     w,
		Addr:       addr,
		Blockstore: bs,
		Datastore:  ds,
		DataDir:    ddir,
		GraphsyncOpts: []gsimpl.Option{
			gsimpl.MaxInProgressIncomingRequests(200),
			gsimpl.MaxInProgressOutgoingRequests(200),
			gsimpl.MaxMemoryResponder(8 << 30),
			gsimpl.MaxMemoryPerPeerResponder(32 << 20),
			gsimpl.MaxInProgressIncomingRequestsPerPeer(20),
			gsimpl.MessageSendRetries(2),
			gsimpl.SendMessageTimeout(2 * time.Minute),
			gsimpl.MaxLinksPerIncomingRequests(maxTraversalLinks),
			gsimpl.MaxLinksPerOutgoingRequests(maxTraversalLinks),
		},
		ChannelMonitorConfig: channelmonitor.Config{

			AcceptTimeout:          time.Hour * 24,
			RestartDebounce:        time.Second * 10,
			RestartBackoff:         time.Second * 20,
			MaxConsecutiveRestarts: 15,
			//RestartAckTimeout:      time.Second * 30,
			CompleteTimeout: time.Minute * 40,

			// Called when a restart completes successfully
			//OnRestartComplete func(id datatransfer.ChannelID)
		},
		Lp2pDTConfig: filclient.Lp2pDTConfig{
			Server: httptransport.ServerConfig{
				// Keep the cache around for one minute after a request
				// finishes in case the connection bounced in the middle
				// of a transfer, or there is a request for the same payload
				// soon after
				BlockInfoCacheManager: boostcar.NewDelayedUnrefBICM(time.Minute),
				ThrottleLimit:         throttleLimit,
			},
			// Wait up to 24 hours for the transfer to complete (including
			// after a connection bounce) before erroring out the deal
			TransferTimeout: 24 * time.Hour,
		},
	}

	for _, opt := range opts {
		opt(cfg)
	}
	return cfg
}

var ErrEmptyPath = errors.New("node not initialized, please run configure")
