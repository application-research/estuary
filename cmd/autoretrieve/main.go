package main

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"time"

	"github.com/application-research/estuary/cmd/autoretrieve/bitswap"
	"github.com/application-research/estuary/cmd/autoretrieve/blocks"
	"github.com/application-research/estuary/cmd/autoretrieve/filecoin"
	"github.com/application-research/estuary/cmd/autoretrieve/metrics"
	"github.com/application-research/filclient"
	"github.com/application-research/filclient/keystore"
	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/lotus/chain/wallet"
	lcli "github.com/filecoin-project/lotus/cli"
	leveldb "github.com/ipfs/go-ds-leveldb"
	"github.com/ipfs/go-log/v2"
	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p-core/crypto"
	"github.com/libp2p/go-libp2p-core/host"
	"github.com/multiformats/go-multiaddr"
	"github.com/urfave/cli/v2"
)

var logger = log.Logger("autoretrieve")

const minerBlacklistFilename = "blacklist.txt"
const datastoreSubdir = "datastore"
const walletSubdir = "wallet"

func main() {
	log.SetLogLevel("autoretrieve", "DEBUG")

	app := cli.NewApp()

	app.Flags = []cli.Flag{
		&cli.StringFlag{
			Name:    "datadir",
			Value:   "./estuary-ar",
			EnvVars: []string{"ESTUARY_AR_DATADIR"},
		},
		&cli.DurationFlag{
			Name:  "timeout",
			Value: 60 * time.Second,
			Usage: "Time to wait on a hanging retrieval before moving on, using a Go ParseDuration(...) string, e.g. 60s, 2m",
		},
		&cli.StringFlag{
			Name:  "endpoint",
			Value: "https://api.estuary.tech/retrieval-candidates",
		},
		&cli.IntFlag{
			Name:  "max-send-workers",
			Value: 4,
			Usage: "Max bitswap message sender worker thread count",
		},
	}

	app.Action = run

	app.Commands = []*cli.Command{
		{
			Name:   "check-blacklist",
			Action: cmdCheckBlacklist,
		},
	}

	ctx := contextWithInterruptCancel()
	if err := app.RunContext(ctx, os.Args); err != nil {
		logger.Fatalf("%w", err)
	}
}

// Creates a context that will get cancelled when the user presses Ctrl+C or
// otherwise triggers an interrupt signal.
func contextWithInterruptCancel() context.Context {
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		ch := make(chan os.Signal, 1)
		signal.Notify(ch, os.Interrupt)

		<-ch

		signal.Ignore(os.Interrupt)
		fmt.Printf("Interrupt detected, gracefully exiting... (interrupt again to force termination)\n")
		cancel()
	}()

	return ctx
}

// Main command entry point.
func run(cctx *cli.Context) error {
	dataDir := cctx.String("datadir")
	endpoint := cctx.String("endpoint")
	timeout := cctx.Duration("timeout")
	maxSendWorkers := cctx.Int("max-send-workers")

	metrics.GoMetricsInjectPrometheus()
	metricsInst := metrics.NewPrometheus(cctx.Context, metrics.NewBasic(&metrics.Noop{}, logger))

	// Load miner blacklist
	minerBlacklist, err := readMinerBlacklist(dataDir)
	if err != nil {
		return err
	}

	// Initialize P2P host
	host, err := initHost(cctx.Context, dataDir, multiaddr.StringCast("/ip4/0.0.0.0/tcp/6746"))
	if err != nil {
		return err
	}

	// Open Lotus API
	api, closer, err := lcli.GetGatewayAPI(cctx)
	if err != nil {
		return err
	}
	defer closer()

	// Initialize blockstore manager
	blockManager, err := blocks.NewManager(blocks.ManagerConfig{
		DataDir: dataDir,
	})
	if err != nil {
		return err
	}

	// Open datastore
	datastore, err := leveldb.NewDatastore(filepath.Join(dataDir, datastoreSubdir), nil)
	if err != nil {
		return err
	}

	// Set up FilClient

	keystore, err := keystore.OpenOrInitKeystore(filepath.Join(dataDir, walletSubdir))
	if err != nil {
		logger.Errorf("Keystore initialization failed: %v", err)
		return nil
	}

	wallet, err := wallet.NewWallet(keystore)
	if err != nil {
		logger.Errorf("Wallet initialization failed: %v", err)
	}

	walletAddr, err := wallet.GetDefault()
	if err != nil {
		walletAddr = address.Undef
	}
	metricsInst.RecordWallet(metrics.WalletInfo{
		Err:  err,
		Addr: walletAddr,
	})

	filClient, err := filclient.NewClient(host, api, wallet, walletAddr, blockManager, datastore, dataDir)
	if err != nil {
		logger.Errorf("FilClient initialization failed: %v", err)
	}
	// Initialize Filecoin retriever
	retriever, err := filecoin.NewRetriever(filecoin.RetrieverConfig{
		MinerBlacklist:   minerBlacklist,
		RetrievalTimeout: timeout,
		Metrics:          metricsInst,
	}, filClient, endpoint, host, api, datastore, blockManager)
	if err != nil {
		return err
	}

	// Initialize Bitswap provider
	_, err = bitswap.NewProvider(bitswap.ProviderConfig{
		MaxSendWorkers: uint(maxSendWorkers),
	}, host, datastore, blockManager, retriever)
	if err != nil {
		return err
	}

	<-cctx.Context.Done()

	return nil
}

func cmdCheckBlacklist(cctx *cli.Context) error {
	minerBlacklist, err := readMinerBlacklist(cctx.String("datadir"))
	if err != nil {
		return err
	}

	if len(minerBlacklist) == 0 {
		fmt.Printf("No blacklisted miners were found\n")
		return nil
	}

	for miner := range minerBlacklist {
		fmt.Printf("%s\n", miner)
	}

	return nil
}

func initHost(ctx context.Context, dataDir string, listenAddrs ...multiaddr.Multiaddr) (host.Host, error) {
	var peerkey crypto.PrivKey
	keyPath := filepath.Join(dataDir, "peerkey")
	keyFile, err := os.ReadFile(keyPath)
	if err != nil {
		if !os.IsNotExist(err) {
			return nil, err
		}

		logger.Infof("Generating new peer key...")

		key, _, err := crypto.GenerateEd25519Key(rand.Reader)
		if err != nil {
			return nil, err
		}
		peerkey = key

		data, err := crypto.MarshalPrivateKey(key)
		if err != nil {
			return nil, err
		}

		if err := os.WriteFile(keyPath, data, 0600); err != nil {
			return nil, err
		}
	} else {
		key, err := crypto.UnmarshalPrivateKey(keyFile)
		if err != nil {
			return nil, err
		}

		peerkey = key
	}

	if peerkey == nil {
		panic("sanity check: peer key is uninitialized")
	}

	host, err := libp2p.New(ctx, libp2p.ListenAddrs(listenAddrs...), libp2p.Identity(peerkey))
	if err != nil {
		return nil, err
	}

	return host, nil
}

func readMinerBlacklist(dataDir string) (map[address.Address]bool, error) {
	bytes, err := os.ReadFile(filepath.Join(dataDir, minerBlacklistFilename))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}

		return nil, err
	}

	strs := strings.Split(string(bytes), "\n")

	var blacklistArr []address.Address
	for lineNum, str := range strs {
		str = strings.TrimSpace(str)

		if str == "" {
			continue
		}

		miner, err := address.NewFromString(str)
		if err != nil {
			logger.Warnf("Skipping unparseable entry \"%v\" at line %v: %v", str, lineNum, err)
			continue
		}

		blacklistArr = append(blacklistArr, miner)
	}

	blacklist := make(map[address.Address]bool)
	for _, miner := range blacklistArr {
		blacklist[miner] = true
	}

	return blacklist, nil
}
