package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math/rand"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"text/tabwriter"
	"time"

	"github.com/cheggaaa/pb/v3"
	"github.com/dustin/go-humanize"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/ipfs/go-bitswap"
	bsnet "github.com/ipfs/go-bitswap/network"
	"github.com/ipfs/go-blockservice"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-cidutil"
	"github.com/ipfs/go-datastore"
	dsync "github.com/ipfs/go-datastore/sync"
	"github.com/ipfs/go-filestore"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	chunker "github.com/ipfs/go-ipfs-chunker"
	cbor "github.com/ipfs/go-ipld-cbor"
	format "github.com/ipfs/go-ipld-format"
	ipld "github.com/ipfs/go-ipld-format"
	"github.com/ipfs/go-merkledag"
	metri "github.com/ipfs/go-metrics-interface"
	"github.com/ipfs/go-unixfs"
	"github.com/ipfs/go-unixfs/importer/balanced"
	ihelper "github.com/ipfs/go-unixfs/importer/helpers"
	"github.com/libp2p/go-libp2p"
	connmgr "github.com/libp2p/go-libp2p-connmgr"
	"github.com/libp2p/go-libp2p-core/host"
	metrics "github.com/libp2p/go-libp2p-core/metrics"
	"github.com/libp2p/go-libp2p-core/network"
	"github.com/libp2p/go-libp2p-core/peer"
	rhelp "github.com/libp2p/go-libp2p-routing-helpers"
	"github.com/mitchellh/go-homedir"
	"github.com/multiformats/go-multiaddr"
	mh "github.com/multiformats/go-multihash"
	"github.com/spf13/viper"
	cli "github.com/urfave/cli/v2"
	"golang.org/x/xerrors"

	"github.com/application-research/estuary/pinner/types"
	dagsplit "github.com/application-research/estuary/util/dagsplit"
)

func main() {
	app := cli.NewApp()

	app.Name = "barge"
	app.Commands = []*cli.Command{
		configCmd,
		loginCmd,
		plumbCmd,
		collectionsCmd,
		initCmd,
		bargeAddCmd,
		bargeStatusCmd,
		bargeSyncCmd,
		bargeCheckCmd,
		bargeShareCmd,
	}
	app.Flags = []cli.Flag{
		&cli.BoolFlag{
			Name:  "debug",
			Usage: "enable debug logging",
		},
	}
	app.Before = func(cctx *cli.Context) error {
		if err := loadConfig(); err != nil {
			return err
		}
		return nil
	}

	if err := app.Run(os.Args); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

type EstuaryConfig struct {
	Token          string `json:"token"`
	Host           string `json:"host"`
	PrimaryShuttle string `json:"primaryShuttle"`
}

type Config struct {
	Estuary EstuaryConfig `json:"estuary"`
}

func loadConfig() error {
	bargeDir, err := homedir.Expand("~/.barge")
	if err != nil {
		return err
	}

	if err := os.MkdirAll(bargeDir, 0775); err != nil {
		return err
	}

	viper.SetConfigName("config")
	viper.SetConfigType("json")
	viper.AddConfigPath("$HOME/.barge")

	if err := viper.ReadInConfig(); err != nil {
		if _, ok := err.(viper.ConfigFileNotFoundError); ok {
			return viper.WriteConfigAs(filepath.Join(bargeDir, "config"))
		} else {
			fmt.Printf("read err: %#v\n", err)
			return err
		}
	}
	return nil
}

var configCmd = &cli.Command{
	Name: "config",
	Subcommands: []*cli.Command{
		configSetCmd,
		configShowCmd,
	},
}

var configSetCmd = &cli.Command{
	Name: "set",
	Action: func(cctx *cli.Context) error {
		if cctx.Args().Len() != 2 {
			return fmt.Errorf("must pass two arguments: key and value")
		}

		viper.Set(cctx.Args().Get(0), cctx.Args().Get(1))

		if err := viper.WriteConfig(); err != nil {
			return fmt.Errorf("failed to write config file: %w", err)
		}
		return nil
	},
}

var configShowCmd = &cli.Command{
	Name: "show",
	Action: func(cctx *cli.Context) error {
		var m map[string]interface{}
		if err := viper.Unmarshal(&m); err != nil {
			return err
		}

		b, err := json.MarshalIndent(m, "  ", "")
		if err != nil {
			return err
		}

		fmt.Println(string(b))
		return nil
	},
}

var loginCmd = &cli.Command{
	Name: "login",
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:  "host",
			Value: "https://api.estuary.tech",
		},
	},
	Action: func(cctx *cli.Context) error {
		if !cctx.Args().Present() {
			return fmt.Errorf("must specify api token")
		}

		tok := cctx.Args().First()

		ec := &EstClient{
			Host: cctx.String("host"),
			Tok:  tok,
		}

		vresp, err := ec.Viewer(cctx.Context)
		if err != nil {
			return err
		}

		fmt.Println("logging in as user: ", vresp.Username)

		if len(vresp.Settings.UploadEndpoints) > 0 {
			sh := vresp.Settings.UploadEndpoints[0]
			u, err := url.Parse(sh)
			if err != nil {
				return err
			}

			u.Path = ""
			u.RawQuery = ""
			u.Fragment = ""

			fmt.Printf("selecting %s as our primary shuttle\n", u.String())

			viper.Set("estuary.primaryShuttle", u.String())
		}

		viper.Set("estuary.token", tok)
		viper.Set("estuary.host", ec.Host)

		return viper.WriteConfig()
	},
}

var plumbCmd = &cli.Command{
	Name:   "plumb",
	Hidden: true,
	Usage:  "low level plumbing commands",
	Subcommands: []*cli.Command{
		plumbPutFileCmd,
		plumbPutCarCmd,
		plumbSplitAddFileCmd,
		plumbPutDirCmd,
	},
}

func loadClient(cctx *cli.Context) (*EstClient, error) {
	tok, ok := viper.Get("estuary.token").(string)
	if !ok || tok == "" {
		return nil, fmt.Errorf("no token set in barge config")
	}

	host, ok := viper.Get("estuary.host").(string)
	if !ok || host == "" {
		return nil, fmt.Errorf("no host set in barge config")
	}

	shuttle, ok := viper.Get("estuary.primaryShuttle").(string)
	if !ok || shuttle == "" {
		return nil, fmt.Errorf("no primaryShuttle set in barge config")
	}

	return &EstClient{
		Host:       host,
		Tok:        tok,
		Shuttle:    shuttle,
		LogTimings: cctx.Bool("debug"),
	}, nil
}

var plumbPutFileCmd = &cli.Command{
	Name: "put-file",
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:  "name",
			Usage: "specify alternate name for file to be added with",
		},
	},
	Action: func(cctx *cli.Context) error {
		if !cctx.Args().Present() {
			return fmt.Errorf("must specify filename to upload")
		}

		c, err := loadClient(cctx)
		if err != nil {
			return err
		}

		f := cctx.Args().First()
		fname := filepath.Base(f)
		if oname := cctx.String("name"); oname != "" {
			fname = oname
		}

		resp, err := c.AddFile(f, fname)
		if err != nil {
			return err
		}

		fmt.Println(resp.Cid)
		return nil
	},
}

var plumbPutDirCmd = &cli.Command{
	Name: "put-dir",
	Action: func(cctx *cli.Context) error {
		ctx := cctx.Context
		client, err := loadClient(cctx)
		if err != nil {
			return err
		}

		ds := dsync.MutexWrap(datastore.NewMapDatastore())
		fsm := filestore.NewFileManager(ds, "/")

		bs := blockstore.NewBlockstore(ds)

		fsm.AllowFiles = true
		fstore := filestore.NewFilestore(bs, fsm)

		fname := cctx.Args().First()

		dnd, err := addDirectory(ctx, fstore, fname)
		if err != nil {
			return err
		}

		fmt.Println("imported directory: ", dnd.Cid())

		return doAddPin(ctx, fstore, client, dnd.Cid(), fname)
	},
}

func addDirectory(ctx context.Context, fstore *filestore.Filestore, dir string) (*merkledag.ProtoNode, error) {
	dirents, err := ioutil.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	progCb := func(int64) {}

	dirnode := unixfs.EmptyDirNode()
	for _, d := range dirents {
		name := filepath.Join(dir, d.Name())
		if d.IsDir() {
			dirn, err := addDirectory(ctx, fstore, name)
			if err != nil {
				return nil, err
			}
			if err := dirnode.AddNodeLink(d.Name(), dirn); err != nil {
				return nil, err
			}
		} else {
			fcid, size, err := filestoreAdd(fstore, name, progCb)
			if err != nil {
				return nil, err
			}

			if err := dirnode.AddRawLink(d.Name(), &format.Link{
				Size: size,
				Cid:  fcid,
			}); err != nil {
				return nil, err
			}
		}
	}

	return dirnode, nil
}

func doAddPin(ctx context.Context, bstore blockstore.Blockstore, client *EstClient, root cid.Cid, fname string) error {
	pc, err := setupBitswap(ctx, bstore)
	if err != nil {
		return err
	}

	h := pc.host

	var addrs []string
	for _, a := range h.Addrs() {
		addrs = append(addrs, fmt.Sprintf("%s/p2p/%s", a, h.ID()))
	}
	fmt.Println("addresses: ", addrs)

	basename := filepath.Base(fname)

	st, err := client.PinAdd(ctx, root, basename, addrs, nil)
	if err != nil {
		return xerrors.Errorf("failed to pin %s to estuary: %w", root, err)
	}

	if err := connectToDelegates(ctx, h, st.Delegates); err != nil {
		fmt.Println("failed to connect to pin delegates: ", err)
	}

	pins := []string{st.RequestID}
	for range time.Tick(time.Second * 2) {
		var pinning, queued, pinned, failed int
		for _, p := range pins {
			status, err := client.PinStatus(ctx, p)
			if err != nil {
				fmt.Println("error getting pin status: ", err)
				continue
			}

			switch status.Status {
			case types.PinningStatusPinned:
				pinned++
			case types.PinningStatusFailed:
				failed++
			case types.PinningStatusPinning:
				pinning++
			case types.PinningStatusQueued:
				queued++
			}

			if err := connectToDelegates(ctx, h, status.Delegates); err != nil {
				fmt.Println("failed to connect to pin delegates: ", err)
			}
		}

		st := pc.bwc.GetBandwidthForProtocol("/ipfs/bitswap/1.2.0")
		fmt.Printf("pinned: %d, pinning: %d, queued: %d, failed: %d, xfer rate: %s/s (num conns: %d)\n", pinned, pinning, queued, failed, humanize.Bytes(uint64(st.RateOut)), len(h.Network().Conns()))
		if failed+pinned >= len(pins) {
			break
		}
	}

	fmt.Println("finished pinning: ", root)

	return nil

}

var plumbPutCarCmd = &cli.Command{
	Name: "put-car",
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:  "name",
			Usage: "specify alternate name for file to be added with",
		},
	},
	Action: func(cctx *cli.Context) error {
		if !cctx.Args().Present() {
			return fmt.Errorf("must specify car file to upload")
		}

		c, err := loadClient(cctx)
		if err != nil {
			return err
		}

		c.DoProgress = true

		f := cctx.Args().First()
		fname := filepath.Base(f)
		if oname := cctx.String("name"); oname != "" {
			fname = oname
		}

		resp, err := c.AddCar(f, fname)
		if err != nil {
			return err
		}

		fmt.Println(resp.Cid)
		return nil
	},
}

func listCollections(cctx *cli.Context) error {
	c, err := loadClient(cctx)
	if err != nil {
		return err
	}

	cols, err := c.CollectionsList(cctx.Context)
	if err != nil {
		return err
	}

	w := tabwriter.NewWriter(os.Stdout, 4, 4, 2, ' ', 0)
	for _, c := range cols {
		fmt.Fprintf(w, "%s\t%s\t%s\n", c.Name, c.UUID, c.CreatedAt)
	}
	return w.Flush()
}

var collectionsCmd = &cli.Command{
	Name: "collections",
	Subcommands: []*cli.Command{
		collectionsCreateCmd,
		collectionsLsDirCmd,
	},
	Action: listCollections,
}

var collectionsCreateCmd = &cli.Command{
	Name: "create",
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:     "name",
			Required: true,
		},
		&cli.StringFlag{
			Name: "description",
		},
	},
	Action: func(cctx *cli.Context) error {
		c, err := loadClient(cctx)
		if err != nil {
			return err
		}

		col, err := c.CollectionsCreate(cctx.Context, cctx.String("name"), cctx.String("description"))
		if err != nil {
			return err
		}

		fmt.Println("new collection created")
		fmt.Println(col.Name)
		fmt.Println(col.UUID)

		return nil
	},
}

var collectionsLsDirCmd = &cli.Command{
	Name:  "ls",
	Flags: []cli.Flag{},
	Action: func(cctx *cli.Context) error {
		c, err := loadClient(cctx)
		if err != nil {
			return err
		}

		if cctx.Args().Len() < 2 {
			return fmt.Errorf("must specify collection ID and path to list")
		}

		col := cctx.Args().Get(0)
		path := cctx.Args().Get(1)

		ents, err := c.CollectionsListDir(cctx.Context, col, path)
		if err != nil {
			return err
		}

		for _, e := range ents {
			if e.Dir {
				fmt.Println(e.Name + "/")
			} else {
				fmt.Println(e.Name)
			}
		}

		return nil
	},
}

func importFile(dserv ipld.DAGService, fi io.Reader) (ipld.Node, error) {
	prefix, err := merkledag.PrefixForCidVersion(1)
	if err != nil {
		return nil, err
	}
	prefix.MhType = mh.SHA2_256

	spl := chunker.NewSizeSplitter(fi, 1024*1024)
	dbp := ihelper.DagBuilderParams{
		Maxlinks:  1024,
		RawLeaves: true,

		CidBuilder: cidutil.InlineBuilder{
			Builder: prefix,
			Limit:   32,
		},

		Dagserv: dserv,
		NoCopy:  true,
	}

	db, err := dbp.New(spl)
	if err != nil {
		return nil, err
	}

	return balanced.Layout(db)
}

type filestoreFile struct {
	*os.File
	absPath string
	st      os.FileInfo
	cb      func(int64)
}

func (ff *filestoreFile) AbsPath() string {
	return ff.absPath
}

func (ff *filestoreFile) Size() (int64, error) {
	finfo, err := ff.File.Stat()
	if err != nil {
		return 0, err
	}

	return finfo.Size(), nil
}

func (ff *filestoreFile) Stat() os.FileInfo {
	return ff.st
}

func (ff *filestoreFile) Read(b []byte) (int, error) {
	n, err := ff.File.Read(b)
	ff.cb(int64(n))
	return n, err
}

func newFF(fpath string, cb func(int64)) (*filestoreFile, error) {
	fi, err := os.Open(fpath)
	if err != nil {
		return nil, err
	}

	absp, err := filepath.Abs(fpath)
	if err != nil {
		return nil, err
	}

	st, err := fi.Stat()
	if err != nil {
		return nil, err
	}

	return &filestoreFile{
		File:    fi,
		absPath: absp,
		st:      st,
		cb:      cb,
	}, nil
}

var plumbSplitAddFileCmd = &cli.Command{
	Name: "split-add",
	Flags: []cli.Flag{
		&cli.Uint64Flag{
			Name:  "chunk",
			Value: uint64(abi.PaddedPieceSize(16 << 30).Unpadded()),
		},
		&cli.BoolFlag{
			Name: "no-pin-only-split",
		},
	},
	Action: func(cctx *cli.Context) error {
		ctx := cctx.Context
		client, err := loadClient(cctx)
		if err != nil {
			return err
		}

		ds := dsync.MutexWrap(datastore.NewMapDatastore())
		fsm := filestore.NewFileManager(ds, "/")

		bs := blockstore.NewBlockstore(ds)

		fsm.AllowFiles = true
		fstore := filestore.NewFilestore(bs, fsm)
		cst := cbor.NewCborStore(fstore)

		fname := cctx.Args().First()

		progcb := func(int64) {}
		fcid, _, err := filestoreAdd(fstore, fname, progcb)
		if err != nil {
			return err
		}

		fmt.Println("imported file: ", fcid)

		dserv := merkledag.NewDAGService(blockservice.New(fstore, nil))
		builder := dagsplit.NewBuilder(dserv, cctx.Uint64("chunk"), 0)

		if err := builder.Pack(ctx, fcid); err != nil {
			return err
		}

		for i, box := range builder.Boxes() {
			cc, err := cst.Put(ctx, box)
			if err != nil {
				return err
			}

			tsize := 0
			/* old way, maybe wrong?
			if err := merkledag.Walk(ctx, dserv.GetLinks, cc, func(c cid.Cid) bool {
				size, err := fstore.GetSize(c)
				if err != nil {
					panic(err)
				}

				tsize += size
				return true
			}); err != nil {
				return err
			}
			*/
			cset := cid.NewSet()
			if err := merkledag.Walk(ctx, func(ctx context.Context, c cid.Cid) ([]*ipld.Link, error) {
				node, err := dserv.Get(ctx, c)
				if err != nil {
					return nil, err
				}

				tsize += len(node.RawData())

				return node.Links(), nil
			}, cc, cset.Visit); err != nil {
				return err
			}
			fmt.Printf("%d: %s %d\n", i, cc, tsize)
		}

		if cctx.Bool("no-pin-only-split") {
			return nil
		}
		/*
			if err := builder.Add(cctx.Context, nd.Cid()); err != nil {
				return err
			}
		*/

		pc, err := setupBitswap(ctx, fstore)
		if err != nil {
			return err
		}

		h := pc.host

		var addrs []string
		for _, a := range h.Addrs() {
			addrs = append(addrs, fmt.Sprintf("%s/p2p/%s", a, h.ID()))
		}
		fmt.Println("addresses: ", addrs)

		basename := filepath.Base(fname)

		var pins []string
		var cids []cid.Cid
		for i, box := range builder.Boxes() {
			cc, err := cst.Put(ctx, box)
			if err != nil {
				return err
			}

			cids = append(cids, cc)
			fmt.Println("box: ", i, cc)

			st, err := client.PinAdd(ctx, cc, fmt.Sprintf("%s-%d", basename, i), addrs, nil)
			if err != nil {
				return xerrors.Errorf("failed to pin box %d to estuary: %w", i, err)
			}

			if err := connectToDelegates(ctx, h, st.Delegates); err != nil {
				fmt.Println("failed to connect to pin delegates: ", err)
			}

			pins = append(pins, st.RequestID)
		}

		for range time.Tick(time.Second * 2) {
			var pinning, queued, pinned, failed int
			for _, p := range pins {
				status, err := client.PinStatus(ctx, p)
				if err != nil {
					fmt.Println("error getting pin status: ", err)
					continue
				}

				switch status.Status {
				case types.PinningStatusPinned:
					pinned++
				case types.PinningStatusFailed:
					failed++
				case types.PinningStatusPinning:
					pinning++
				case types.PinningStatusQueued:
					queued++
				}

				if err := connectToDelegates(ctx, h, status.Delegates); err != nil {
					fmt.Println("failed to connect to pin delegates: ", err)
				}
			}

			fmt.Printf("pinned: %d, pinning: %d, queued: %d, failed: %d (num conns: %d)\n", pinned, pinning, queued, failed, len(h.Network().Conns()))
			if failed+pinned >= len(pins) {
				break
			}
		}

		fmt.Println("finished pinning: ", fcid)

		return nil
	},
}

func filestoreAdd(fstore *filestore.Filestore, fpath string, progcb func(int64)) (cid.Cid, uint64, error) {
	ff, err := newFF(fpath, progcb)
	if err != nil {
		return cid.Undef, 0, err
	}
	defer ff.Close()

	dserv := merkledag.NewDAGService(blockservice.New(fstore, nil))
	nd, err := importFile(dserv, ff)
	if err != nil {
		return cid.Undef, 0, err
	}

	size, err := nd.Size()
	if err != nil {
		return cid.Undef, 0, err
	}

	return nd.Cid(), size, nil
}

func connectToDelegates(ctx context.Context, h host.Host, delegates []string) error {
	peers := make(map[peer.ID][]multiaddr.Multiaddr)
	for _, d := range delegates {
		ai, err := peer.AddrInfoFromString(d)
		if err != nil {
			return err
		}

		peers[ai.ID] = append(peers[ai.ID], ai.Addrs...)
	}

	for p, addrs := range peers {
		h.Peerstore().AddAddrs(p, addrs, time.Hour)

		if h.Network().Connectedness(p) != network.Connected {
			if err := h.Connect(ctx, peer.AddrInfo{
				ID: p,
			}); err != nil {
				return err
			}

			h.ConnManager().Protect(p, "pinning")
		}
	}

	return nil
}

type pinclient struct {
	host    host.Host
	bitswap *bitswap.Bitswap
	bwc     metrics.Reporter
}

func setupBitswap(ctx context.Context, bstore blockstore.Blockstore) (*pinclient, error) {
	cmgr, err := connmgr.NewConnManager(2000, 3000)
	if err != nil {
		return nil, err
	}

	bwc := metrics.NewBandwidthCounter()
	h, err := libp2p.New(
		libp2p.ListenAddrStrings("/ip4/0.0.0.0/tcp/0"),
		libp2p.NATPortMap(),
		libp2p.ConnectionManager(cmgr),
		//libp2p.Identity(peerkey),
		libp2p.BandwidthReporter(bwc),
		libp2p.DefaultTransports,
	)
	if err != nil {
		return nil, err
	}

	bsnet := bsnet.NewFromIpfsHost(h, rhelp.Null{})
	bsctx := metri.CtxScope(ctx, "barge.exch")

	bswap := bitswap.New(bsctx, bsnet, bstore,
		bitswap.EngineBlockstoreWorkerCount(600),
		bitswap.TaskWorkerCount(600),
		bitswap.MaxOutstandingBytesPerPeer(10<<20),
	)

	return &pinclient{
		host:    h,
		bitswap: bswap.(*bitswap.Bitswap),
		bwc:     bwc,
	}, nil
}

var bargeAddCmd = &cli.Command{
	Name: "add",
	Flags: []cli.Flag{
		&cli.BoolFlag{
			Name: "progress",
		},
	},
	Action: func(cctx *cli.Context) error {
		r, err := openRepo(cctx)
		if err != nil {
			return err
		}

		progress := cctx.Bool("progress")

		var paths []string
		// TODO: this expansion could be done in parallel to speed things up on large directories
		for _, f := range cctx.Args().Slice() {
			matches, err := filepath.Glob(f)
			if err != nil {
				return err
			}

			for _, m := range matches {
				// TODO: reuse these stats...
				st, err := os.Stat(m)
				if err != nil {
					return err
				}

				if st.IsDir() {
					sub, err := expandDirectory(m)
					if err != nil {
						return err
					}
					// expand!
					paths = append(paths, sub...)
				} else {
					paths = append(paths, m)
				}
			}
		}

		progcb := func(int64) {}
		incrTotal := func(int64) {}
		finish := func() {}

		if progress {
			bar := pb.New64(0)

			bar.Set(pb.Bytes, true)
			bar.SetTemplate(pb.Full)
			bar.Start()

			progcb = func(amt int64) {
				bar.Add64(amt)
			}

			var total int64
			var totlk sync.Mutex

			incrTotal = func(amt int64) {
				totlk.Lock()
				total += amt
				bar.SetTotal(total)
				totlk.Unlock()
			}

			finish = func() {
				bar.Finish()
			}

		}

		type addJob struct {
			Path  string
			Found []File
			Stat  os.FileInfo
		}

		type updateJob struct {
			Path  string
			Found []File
			Stat  os.FileInfo
			Cid   cid.Cid
		}

		tocheck := make(chan string, 1)
		tobuffer := make(chan *addJob, 128)
		toadd := make(chan *addJob)
		toupdate := make(chan updateJob, 128)

		go func() {
			defer close(tocheck)
			for _, f := range paths {
				tocheck <- f
			}
		}()

		go func() {
			defer close(tobuffer)
			for p := range tocheck {
				st, err := os.Stat(p)
				if err != nil {
					fmt.Println(err)
					return
				}

				incrTotal(st.Size())

				var found []File
				if err := r.DB.Find(&found, "path = ?", p).Error; err != nil {
					fmt.Println(err)
					return
				}

				if len(found) > 0 {
					existing := found[0]

					// have it already... check if its changed
					if st.ModTime().Equal(existing.Mtime) {
						// mtime the same, assume its the same file...
						continue
					}
				}

				tobuffer <- &addJob{
					Path:  p,
					Found: found,
					Stat:  st,
				}
			}
		}()

		go func() {
			defer close(toadd)
			var next *addJob
			var buffer []*addJob
			var out chan *addJob
			var inputDone bool

			for {
				select {
				case aj, ok := <-tobuffer:
					if !ok {
						inputDone = true
						if next == nil && len(buffer) == 0 {
							return
						}
						continue
					}
					if out == nil {
						next = aj
						out = toadd
					} else {
						buffer = append(buffer, aj)
					}
				case out <- next:
					if len(buffer) > 0 {
						next = buffer[0]
						buffer = buffer[1:]
					} else {
						out = nil
						next = nil
						if inputDone {
							return
						}
					}
				}
			}
		}()

		var wg sync.WaitGroup
		for i := 0; i < 10; i++ {
			wg.Add(1)

			go func() {
				defer wg.Done()
				for aj := range toadd {
					fcid, _, err := filestoreAdd(r.Filestore, aj.Path, progcb)
					if err != nil {
						fmt.Println(err)
						return
					}

					toupdate <- updateJob{
						Path:  aj.Path,
						Found: aj.Found,
						Cid:   fcid,
						Stat:  aj.Stat,
					}
				}
			}()
		}

		go func() {
			wg.Wait()
			close(toupdate)
		}()

		var batchCreates []*File
		for uj := range toupdate {
			if len(uj.Found) > 0 {
				existing := uj.Found[0]
				if existing.Cid != uj.Cid.String() {
					if err := r.DB.Model(File{}).Where("id = ?", existing.ID).UpdateColumns(map[string]interface{}{
						"cid":   uj.Cid.String(),
						"mtime": uj.Stat.ModTime(),
					}).Error; err != nil {
						return err
					}
				}

				continue
			}

			abs, err := filepath.Abs(uj.Path)
			if err != nil {
				return err
			}

			rel, err := filepath.Rel(r.Dir, abs)
			if err != nil {
				return err
			}

			batchCreates = append(batchCreates, &File{
				Path:  rel,
				Cid:   uj.Cid.String(),
				Mtime: uj.Stat.ModTime(),
			})

			if len(batchCreates) > 200 {
				if err := r.DB.CreateInBatches(batchCreates, 100).Error; err != nil {
					return err
				}
				batchCreates = nil
			}
		}

		if err := r.DB.CreateInBatches(batchCreates, 100).Error; err != nil {
			return err
		}

		finish()

		return nil
	},
}

func expandDirectory(dir string) ([]string, error) {
	dirents, err := ioutil.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	var out []string
	for _, ent := range dirents {
		if strings.HasPrefix(ent.Name(), ".") {
			continue
		}

		if ent.IsDir() {
			sub, err := expandDirectory(filepath.Join(dir, ent.Name()))
			if err != nil {
				return nil, err
			}

			for _, s := range sub {
				out = append(out, s)
			}
		} else {
			out = append(out, filepath.Join(dir, ent.Name()))
		}
	}

	return out, nil
}

func maybeChanged(f File) (bool, string, error) {
	st, err := os.Stat(f.Path)
	if err != nil {
		if os.IsNotExist(err) {
			return true, "deleted", nil
		}
		return false, "", err
	}

	if f.Mtime.Equal(st.ModTime()) {
		return false, "", nil
	}

	return true, "modified", nil
}

var bargeStatusCmd = &cli.Command{
	Name: "status",
	Action: func(cctx *cli.Context) error {
		r, err := openRepo(cctx)
		if err != nil {
			return err
		}

		var allfiles []File
		if err := r.DB.Order("path asc").Find(&allfiles).Error; err != nil {
			return err
		}

		fmt.Println("Changes not yet staged:")

		var unpinned []File
		for _, f := range allfiles {
			ch, reason, err := maybeChanged(f)
			if err != nil {
				return err
			}

			var pins []Pin
			if err := r.DB.Find(&pins, "file = ?", f.ID).Error; err != nil {
				return err
			}

			if !ch {
				if len(pins) > 0 {
					pin := pins[0]

					if pin.Status == types.PinningStatusPinned {
						// unchanged and pinned, no need to print anything
						continue
					}
				}

				unpinned = append(unpinned, f)
				continue
			}

			fmt.Printf("\t%s: %s\n", reason, f.Path)
		}

		if len(unpinned) > 0 {
			fmt.Println()
			fmt.Println("Unpinned files:")
			for _, f := range unpinned {
				fmt.Printf("\t%s\n", f.Path)
			}
		}

		return nil
	},
}

type fileWithPin struct {
	FileID uint
	PinID  uint

	Cid       string
	Path      string
	Status    types.PinningStatus
	RequestID string
}

var bargeSyncCmd = &cli.Command{
	Name: "sync",
	Flags: []cli.Flag{
		&cli.BoolFlag{
			Name: "recover",
		},
		&cli.Int64Flag{
			Name: "new-pin-limit",
		},
	},
	Action: func(cctx *cli.Context) error {
		ctx := cctx.Context
		r, err := openRepo(cctx)
		if err != nil {
			return err
		}

		c, err := loadClient(cctx)
		if err != nil {
			return err
		}

		coluuid := r.Cfg.GetString("collection.uuid")
		if coluuid == "" {
			return fmt.Errorf("barge repo does not have a collection set")
		}

		/*
			var files []File
			if err := r.DB.Find(&files).Error; err != nil {
				return err
			}
		*/

		var filespins []fileWithPin
		if err := r.DB.Model(File{}).Joins("left join pins on pins.file = files.id AND pins.cid = files.cid").Select("files.id as file_id, pins.id as pin_id, path, status, request_id, files.cid as cid").Scan(&filespins).Error; err != nil {
			return err
		}

		pc, err := setupBitswap(ctx, r.Filestore)
		if err != nil {
			return err
		}

		h := pc.host

		var addrs []string
		for _, a := range h.Addrs() {
			addrs = append(addrs, fmt.Sprintf("%s/p2p/%s", a, h.ID()))
		}

		var pinComplete []fileWithPin
		var needsNewPin []fileWithPin
		var inProgress []*Pin
		var checkProgress []fileWithPin
		for _, f := range filespins {
			if f.PinID == 0 {
				needsNewPin = append(needsNewPin, f)
				continue
			}

			if f.Status == types.PinningStatusPinned {
				// TODO: add flag to allow a forced rechecking
				continue
			}

			checkProgress = append(checkProgress, f)
		}

		batchSize := 500
		fmt.Printf("need to check progress of %d pins\n", len(checkProgress))
		for i := 0; i < len(checkProgress); i += batchSize {
			log.Printf("getting pin statuses: %d / %d\n", i, len(checkProgress))
			end := i + batchSize
			if end > len(checkProgress) {
				end = len(checkProgress)
			}

			var reqids []string
			for _, p := range checkProgress[i:end] {
				reqids = append(reqids, p.RequestID)
			}

			resp, err := c.PinStatuses(ctx, reqids)
			if err != nil {
				return fmt.Errorf("failed to recheck pin statuses: %w", err)
			}

			for _, fp := range checkProgress[i:end] {
				st, ok := resp[fp.RequestID]
				if !ok {
					return fmt.Errorf("did not get status back for requestid %s", fp.RequestID)
				}

				switch st.Status {
				case types.PinningStatusPinned:
					pinComplete = append(pinComplete, fp)
					if err := r.DB.Model(Pin{}).Where("id = ?", fp.PinID).UpdateColumn("status", st.Status).Error; err != nil {
						return err
					}
				case types.PinningStatusFailed:
					needsNewPin = append(needsNewPin, fp)
					if err := r.DB.Delete(Pin{ID: fp.PinID}).Error; err != nil {
						return err
					}
				default:
					// pin is technically in progress? do nothing for now
					inProgress = append(inProgress, &Pin{
						ID:        fp.PinID,
						File:      fp.FileID,
						Status:    fp.Status,
						RequestID: fp.RequestID,
					})
				}
			}
		}

		if cctx.Bool("recover") {
			fmt.Println("recovery requested, searching for pins on estuary not tracked locally...")
			for i, nnp := range needsNewPin {
				fmt.Printf("                                \r")
				fmt.Printf("[%d / %d]\r", i, len(needsNewPin))
				// TODO: can batch this
				st, err := c.PinStatusByCid(ctx, []string{nnp.Cid})
				if err != nil {
					fmt.Println("failed to get pin status: ", err)
					continue
				}

				pin, ok := st[nnp.Cid]
				if !ok {
					continue
				}

				if pin.Status == types.PinningStatusFailed {
					// dont bother recording
					continue
				}

				if err := r.DB.Create(&Pin{
					File:      nnp.FileID,
					Cid:       nnp.Cid,
					RequestID: pin.RequestID,
					Status:    pin.Status,
				}).Error; err != nil {
					return err
				}
			}

			return nil
		}

		fmt.Printf("need to make %d new pins\n", len(needsNewPin))
		if lim := cctx.Int64("new-pin-limit"); lim > 0 {
			if int64(len(needsNewPin)) > lim {
				needsNewPin = needsNewPin[:lim]
				fmt.Printf("only making %d for now...\n", lim)
			}
		}

		var dplk sync.Mutex
		var donePins int
		var wg sync.WaitGroup
		newpins := make([]*Pin, len(needsNewPin))
		errs := make([]error, len(needsNewPin))
		sema := make(chan struct{}, 20)
		var delegates []string
		for i := range needsNewPin {
			wg.Add(1)
			go func(ix int) {
				defer wg.Done()

				f := needsNewPin[ix]

				fcid, err := cid.Decode(f.Cid)
				if err != nil {
					errs[ix] = err
					return
				}

				sema <- struct{}{}
				defer func() {
					<-sema
				}()

				resp, err := c.PinAdd(ctx, fcid, filepath.Base(f.Path), addrs, map[string]interface{}{
					"coluuid": coluuid,
					"colpath": "/" + f.Path,
				})
				if err != nil {
					errs[ix] = err
					return
				}

				dplk.Lock()
				delegates = append(delegates, resp.Delegates...)
				donePins++
				fmt.Printf("                                                 \r")
				fmt.Printf("creating new pins %d/%d", donePins, len(needsNewPin))
				dplk.Unlock()

				p := &Pin{
					File:      f.FileID,
					Cid:       fcid.String(),
					RequestID: resp.RequestID,
					Status:    resp.Status,
				}

				newpins[ix] = p
			}(i)
		}
		wg.Wait()

		if err := connectToDelegates(ctx, h, delegates); err != nil {
			fmt.Fprintf(os.Stderr, "failed to connect to deletegates for new pin: %s\n", err)
		}

		var tocreate []*Pin
		for _, p := range newpins {
			if p != nil {
				tocreate = append(tocreate, p)
				inProgress = append(inProgress, p)
			}
		}

		if len(tocreate) > 0 {
			if err := r.DB.CreateInBatches(tocreate, 100).Error; err != nil {
				return err
			}
		}

		for _, err := range errs {
			if err != nil {
				return err
			}
		}

		fmt.Println()
		fmt.Println("transferring data...")

		complete := make(map[string]bool)
		failed := make(map[string]bool)
		for range time.Tick(time.Second * 2) {

		loopstart:
			var tocheck []string
			for _, p := range inProgress {
				if complete[p.RequestID] || failed[p.RequestID] {
					continue
				}

				tocheck = append(tocheck, p.RequestID)

				if len(tocheck) >= 300 {
					break
				}
			}

			// if we have a lot of pins still to check, start randomly selecting some to look at
			if len(inProgress)-(len(complete)+len(failed)) > batchSize*2 {
				for i := 0; i < 200; i++ {
					p := inProgress[rand.Intn(len(inProgress))]
					if complete[p.RequestID] || failed[p.RequestID] {
						continue
					}

					tocheck = append(tocheck, p.RequestID)
				}
			}

			statuses, err := c.PinStatuses(ctx, tocheck)
			if err != nil {
				return fmt.Errorf("failed to check pin statuses: %w", err)
			}

			var newdone int
			for _, req := range tocheck {
				status, ok := statuses[req]
				if !ok {
					fmt.Printf("didnt get expected pin status back in request: %s\n", req)
					continue
				}

				switch status.Status {
				case types.PinningStatusPinned:
					newdone++
					complete[req] = true
					if err := r.DB.Model(Pin{}).Where("request_id = ?", req).UpdateColumn("status", types.PinningStatusPinned).Error; err != nil {
						return err
					}
				case types.PinningStatusFailed:
					newdone++
					failed[req] = true
					if err := r.DB.Model(Pin{}).Where("request_id = ?", req).Delete(Pin{}).Error; err != nil {
						return err
					}
				default:
				}

				if err := connectToDelegates(ctx, h, status.Delegates); err != nil {
					fmt.Println("failed to connect to pin delegates: ", err)
				}
			}

			st := pc.bwc.GetBandwidthForProtocol("/ipfs/bitswap/1.2.0")
			fmt.Printf("pinned: %d, pinning: %d, failed: %d, xfer rate: %s/s (connections: %d)\n", len(complete), len(inProgress)-(len(complete)+len(failed)), len(failed), humanize.Bytes(uint64(st.RateOut)), len(h.Network().Conns()))

			if len(failed)+len(complete) >= len(inProgress) {
				break
			}

			// dont wait if we get a high enough proportion of new info
			if newdone > 100 {
				goto loopstart
			}
		}

		return nil

	},
}

var bargeCheckCmd = &cli.Command{
	Name: "check",
	Action: func(cctx *cli.Context) error {
		r, err := openRepo(cctx)
		if err != nil {
			return err
		}

		for _, path := range cctx.Args().Slice() {
			var file File
			if err := r.DB.First(&file, "path = ?", path).Error; err != nil {
				return err
			}

			fcid, err := cid.Decode(file.Cid)
			if err != nil {
				return err
			}

			ctx := context.TODO()
			lres := filestore.Verify(ctx, r.Filestore, fcid)
			fmt.Println(lres.Status.String())
			fmt.Println(lres.ErrorMsg)
		}

		return nil
	},
}

var bargeShareCmd = &cli.Command{
	Name: "share",
	Action: func(cctx *cli.Context) error {
		r, err := openRepo(cctx)
		if err != nil {
			return err
		}

		pc, err := setupBitswap(cctx.Context, r.Filestore)
		if err != nil {
			return err
		}

		h := pc.host

		for _, a := range h.Addrs() {
			fmt.Printf("%s/p2p/%s\n", a, h.ID())
		}

		select {}

		return nil
	},
}
