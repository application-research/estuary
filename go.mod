module github.com/application-research/estuary

go 1.25.0

require (
	contrib.go.opencensus.io/exporter/prometheus v0.4.2
	github.com/application-research/filclient v0.5.0-rc1
	github.com/application-research/goque v2.1.0+incompatible
	github.com/cenkalti/backoff/v4 v4.3.0
	github.com/cheggaaa/pb/v3 v3.1.7
	github.com/docker/go-units v0.5.0
	github.com/dustin/go-humanize v1.0.1 // indirect
	github.com/facebookgo/atomicfile v0.0.0-20151019160806-2de1f203e7d5
	github.com/filecoin-project/boost v1.7.5
	github.com/filecoin-project/go-address v1.2.0
	github.com/filecoin-project/go-bs-lmdb v1.0.6-0.20211215050109-9e2b984c988e
	github.com/filecoin-project/go-cbor-util v0.0.2
	github.com/filecoin-project/go-data-transfer v1.15.4-boost
	github.com/filecoin-project/go-fil-commcid v0.2.0 // indirect
	github.com/filecoin-project/go-fil-markets v1.28.3
	github.com/filecoin-project/go-jsonrpc v0.7.1
	github.com/filecoin-project/go-legs v0.4.9 // indirect
	github.com/filecoin-project/go-padreader v0.0.1
	github.com/filecoin-project/go-state-types v0.16.0
	github.com/filecoin-project/index-provider v0.8.3-0.20220816114513-ecf5e9c2ae97
	github.com/filecoin-project/lotus v1.32.0-rc3
	github.com/filecoin-project/specs-actors/v6 v6.0.2
	github.com/filecoin-project/storetheindex v0.4.18 // indirect
	github.com/gabriel-vasile/mimetype v1.4.8
	github.com/go-openapi/swag v0.21.1 // indirect
	github.com/google/uuid v1.6.0
	github.com/hashicorp/golang-lru v1.0.2
	github.com/ipfs/go-bitswap v0.12.0
	github.com/ipfs/go-block-format v0.2.0
	github.com/ipfs/go-blockservice v0.5.2
	github.com/ipfs/go-cid v0.6.0
	github.com/ipfs/go-cidutil v0.1.0 // indirect
	github.com/ipfs/go-datastore v0.8.2
	github.com/ipfs/go-ds-leveldb v0.5.2
	github.com/ipfs/go-fetcher v1.6.2
	github.com/ipfs/go-graphsync v0.17.0
	github.com/ipfs/go-ipfs v0.13.1
	github.com/ipfs/go-ipfs-blockstore v1.3.1
	github.com/ipfs/go-ipfs-chunker v0.0.6
	github.com/ipfs/go-ipfs-exchange-interface v0.2.1
	github.com/ipfs/go-ipfs-exchange-offline v0.3.1
	github.com/ipfs/go-ipld-cbor v0.2.0
	github.com/ipfs/go-ipld-format v0.6.0
	github.com/ipfs/go-log/v2 v2.5.1
	github.com/ipfs/go-merkledag v0.11.0
	github.com/ipfs/go-metrics-interface v0.3.0
	github.com/ipfs/go-path v0.3.2
	github.com/ipfs/go-unixfs v0.4.6
	github.com/ipfs/go-unixfsnode v1.10.0
	github.com/ipld/go-car v0.6.2
	github.com/ipld/go-codec-dagpb v1.7.0
	github.com/ipld/go-ipld-prime v0.22.0
	github.com/jinzhu/gorm v1.9.16
	github.com/labstack/echo/v4 v4.13.3
	github.com/labstack/gommon v0.4.2
	github.com/libp2p/go-libp2p v0.41.0
	github.com/libp2p/go-libp2p-kad-dht v0.30.2
	github.com/libp2p/go-libp2p-pubsub v0.13.0 // indirect
	github.com/libp2p/go-libp2p-record v0.3.1
	github.com/libp2p/go-libp2p-routing-helpers v0.7.5
	github.com/mitchellh/go-homedir v1.1.0
	github.com/multiformats/go-multiaddr v0.15.0
	github.com/multiformats/go-multihash v0.2.3
	github.com/paskal/golang-lru v0.6.0
	github.com/pkg/errors v0.9.1
	github.com/prometheus/client_golang v1.22.0
	github.com/stretchr/testify v1.11.1
	github.com/swaggo/echo-swagger v1.4.1
	github.com/swaggo/swag v1.16.4
	github.com/syndtr/goleveldb v1.0.1-0.20210819022825-2ae1ddf74ef7
	github.com/urfave/cli/v2 v2.27.6
	github.com/whyrusleeping/cbor-gen v0.3.1
	github.com/whyrusleeping/memo v0.0.0-20211124220851-3b94446416a3
	go.opencensus.io v0.24.0
	go.opentelemetry.io/otel v1.43.0
	go.opentelemetry.io/otel/exporters/jaeger v1.17.0
	go.opentelemetry.io/otel/sdk v1.43.0
	go.opentelemetry.io/otel/trace v1.43.0
	go.uber.org/zap v1.28.0
	golang.org/x/crypto v0.48.0
	golang.org/x/net v0.49.0
	golang.org/x/sys v0.42.0
	golang.org/x/xerrors v0.0.0-20240903120638-7835f813f4da
	gorm.io/driver/postgres v1.5.11
	gorm.io/driver/sqlite v1.6.0
	gorm.io/gorm v1.31.1
)

require (
	github.com/application-research/go-bs-autobatch v0.1.0
	github.com/ipfs/go-ds-flatfs v0.5.5
	github.com/ipfs/go-ipfs-provider v0.8.2
	github.com/ipfs/go-metrics-prometheus v0.1.0
	github.com/nsqio/go-nsq v1.1.0
	github.com/vmihailenco/msgpack/v5 v5.4.1
	github.com/whyrusleeping/go-bs-measure v0.0.0-20220728174732-077cd42cf89d
	golang.org/x/time v0.11.0
)

require (
	github.com/BurntSushi/toml v1.4.0 // indirect
	github.com/GeertJohan/go.incremental v1.0.0 // indirect
	github.com/GeertJohan/go.rice v1.0.3 // indirect
	github.com/Jorropo/jsync v1.0.1 // indirect
	github.com/Kubuxu/imtui v0.0.0-20210401140320-41663d68d0fa // indirect
	github.com/KyleBanks/depth v1.2.1 // indirect
	github.com/PuerkitoBio/purell v1.1.1 // indirect
	github.com/PuerkitoBio/urlesc v0.0.0-20170810143723-de5bf2ad4578 // indirect
	github.com/StackExchange/wmi v1.2.1 // indirect
	github.com/VividCortex/ewma v1.2.0 // indirect
	github.com/acarl005/stripansi v0.0.0-20180116102854-5a71ef0e047d // indirect
	github.com/akavel/rsrc v0.8.0 // indirect
	github.com/alecthomas/units v0.0.0-20240927000941-0f3dac36c52b // indirect
	github.com/alexbrainman/goissue34681 v0.0.0-20191006012335-3fc7a47baff5 // indirect
	github.com/bahlo/generic-list-go v0.2.0 // indirect
	github.com/beeker1121/goque v2.1.0+incompatible // indirect
	github.com/benbjohnson/clock v1.3.5 // indirect
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/bep/debounce v1.2.1 // indirect
	github.com/buger/jsonparser v1.1.2 // indirect
	github.com/cenkalti/backoff v2.2.1+incompatible // indirect
	github.com/cespare/xxhash v1.1.0 // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/chzyer/readline v1.5.1 // indirect
	github.com/containerd/cgroups v1.1.0 // indirect
	github.com/coreos/go-systemd/v22 v22.5.0 // indirect
	github.com/cpuguy83/go-md2man/v2 v2.0.6 // indirect
	github.com/crackcomm/go-gitignore v0.0.0-20241020182519-7843d2ba8fdf // indirect
	github.com/cskr/pubsub v1.0.2 // indirect
	github.com/daaku/go.zipexe v1.0.2 // indirect
	github.com/davecgh/go-spew v1.1.2-0.20180830191138-d8f796af33cc // indirect
	github.com/davidlazar/go-crypto v0.0.0-20200604182044-b73af7476f6c // indirect
	github.com/decred/dcrd/dcrec/secp256k1/v4 v4.4.0 // indirect
	github.com/dgraph-io/badger/v2 v2.2007.4 // indirect
	github.com/dgraph-io/ristretto v0.1.1 // indirect
	github.com/dgryski/go-farm v0.0.0-20200201041132-a6ae2369ad13 // indirect
	github.com/elastic/gosigar v0.14.3 // indirect
	github.com/fatih/color v1.18.0 // indirect
	github.com/filecoin-project/boost-gfm v1.26.6 // indirect
	github.com/filecoin-project/boost-graphsync v0.13.6 // indirect
	github.com/filecoin-project/filecoin-ffi v1.31.0 // indirect
	github.com/filecoin-project/go-amt-ipld/v2 v2.1.0 // indirect
	github.com/filecoin-project/go-amt-ipld/v3 v3.1.0 // indirect
	github.com/filecoin-project/go-amt-ipld/v4 v4.4.0 // indirect
	github.com/filecoin-project/go-bitfield v0.2.4 // indirect
	github.com/filecoin-project/go-clock v0.1.0 // indirect
	github.com/filecoin-project/go-commp-utils v0.1.4 // indirect
	github.com/filecoin-project/go-commp-utils/v2 v2.1.0 // indirect
	github.com/filecoin-project/go-crypto v0.1.0 // indirect
	github.com/filecoin-project/go-data-transfer/v2 v2.0.0-rc7 // indirect
	github.com/filecoin-project/go-ds-versioning v0.1.2 // indirect
	github.com/filecoin-project/go-f3 v0.8.9 // indirect
	github.com/filecoin-project/go-fil-commp-hashhash v0.2.0 // indirect
	github.com/filecoin-project/go-hamt-ipld v0.1.5 // indirect
	github.com/filecoin-project/go-hamt-ipld/v2 v2.0.0 // indirect
	github.com/filecoin-project/go-hamt-ipld/v3 v3.4.0 // indirect
	github.com/filecoin-project/go-paramfetch v0.0.4 // indirect
	github.com/filecoin-project/go-statemachine v1.0.3 // indirect
	github.com/filecoin-project/go-statestore v0.2.0 // indirect
	github.com/filecoin-project/pubsub v1.0.0 // indirect
	github.com/filecoin-project/specs-actors v0.9.15 // indirect
	github.com/filecoin-project/specs-actors/v2 v2.3.6 // indirect
	github.com/filecoin-project/specs-actors/v3 v3.1.2 // indirect
	github.com/filecoin-project/specs-actors/v4 v4.0.2 // indirect
	github.com/filecoin-project/specs-actors/v5 v5.0.6 // indirect
	github.com/filecoin-project/specs-actors/v7 v7.0.1 // indirect
	github.com/filecoin-project/specs-actors/v8 v8.0.1 // indirect
	github.com/flynn/noise v1.1.0 // indirect
	github.com/francoispqt/gojay v1.2.13 // indirect
	github.com/gammazero/deque v1.0.0 // indirect
	github.com/gbrlsnchs/jwt/v3 v3.0.1 // indirect
	github.com/gdamore/encoding v1.0.0 // indirect
	github.com/gdamore/tcell/v2 v2.2.0 // indirect
	github.com/ghodss/yaml v1.0.0 // indirect
	github.com/go-kit/log v0.2.1 // indirect
	github.com/go-logfmt/logfmt v0.6.0 // indirect
	github.com/go-logr/logr v1.4.3 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/go-ole/go-ole v1.2.5 // indirect
	github.com/go-openapi/jsonpointer v0.19.5 // indirect
	github.com/go-openapi/jsonreference v0.19.6 // indirect
	github.com/go-openapi/spec v0.20.4 // indirect
	github.com/godbus/dbus/v5 v5.1.0 // indirect
	github.com/gogo/protobuf v1.3.2 // indirect
	github.com/golang/glog v1.2.5 // indirect
	github.com/golang/groupcache v0.0.0-20210331224755-41bb18bfe9da // indirect
	github.com/golang/mock v1.6.0 // indirect
	github.com/golang/protobuf v1.5.4 // indirect
	github.com/golang/snappy v0.0.4 // indirect
	github.com/google/go-cmp v0.7.0 // indirect
	github.com/google/gopacket v1.1.19 // indirect
	github.com/gorilla/websocket v1.5.3 // indirect
	github.com/graph-gophers/graphql-go v1.3.0 // indirect
	github.com/hako/durafmt v0.0.0-20200710122514-c0fb7b4da026 // indirect
	github.com/hannahhoward/cbor-gen-for v0.0.0-20230214144701-5d17c9d5243c // indirect
	github.com/hannahhoward/go-pubsub v1.0.0 // indirect
	github.com/hashicorp/errwrap v1.1.0 // indirect
	github.com/hashicorp/go-multierror v1.1.1 // indirect
	github.com/hashicorp/golang-lru/arc/v2 v2.0.7 // indirect
	github.com/hashicorp/golang-lru/v2 v2.0.7 // indirect
	github.com/huin/goupnp v1.3.0 // indirect
	github.com/icza/backscanner v0.0.0-20210726202459-ac2ffc679f94 // indirect
	github.com/invopop/jsonschema v0.12.0 // indirect
	github.com/ipfs/bbloom v0.0.4 // indirect
	github.com/ipfs/boxo v0.28.0 // indirect
	github.com/ipfs/go-bitfield v1.1.0 // indirect
	github.com/ipfs/go-ds-measure v0.2.0 // indirect
	github.com/ipfs/go-fs-lock v0.0.7 // indirect
	github.com/ipfs/go-ipfs-blocksutil v0.0.1 // indirect
	github.com/ipfs/go-ipfs-delay v0.0.1 // indirect
	github.com/ipfs/go-ipfs-ds-help v1.1.1 // indirect
	github.com/ipfs/go-ipfs-files v0.3.0 // indirect
	github.com/ipfs/go-ipfs-posinfo v0.0.1 // indirect
	github.com/ipfs/go-ipfs-pq v0.0.3 // indirect
	github.com/ipfs/go-ipfs-util v0.0.3 // indirect
	github.com/ipfs/go-ipld-legacy v0.2.1 // indirect
	github.com/ipfs/go-libipfs v0.7.0 // indirect
	github.com/ipfs/go-log v1.0.5 // indirect
	github.com/ipfs/go-peertaskqueue v0.8.2 // indirect
	github.com/ipfs/go-verifcid v0.0.3 // indirect
	github.com/ipld/go-car/v2 v2.14.2 // indirect
	github.com/ipld/go-ipld-adl-hamt v0.0.0-20230103232215-ec18ad32db9b // indirect
	github.com/ipni/go-libipni v0.0.8 // indirect
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20240606120523-5a60cdf6a761 // indirect
	github.com/jackc/pgx/v5 v5.9.2 // indirect
	github.com/jackc/puddle/v2 v2.2.2 // indirect
	github.com/jackpal/go-nat-pmp v1.0.2 // indirect
	github.com/jbenet/go-random v0.0.0-20190219211222-123a90aedc0c // indirect
	github.com/jbenet/go-temp-err-catcher v0.1.0 // indirect
	github.com/jbenet/goprocess v0.1.4 // indirect
	github.com/jessevdk/go-flags v1.4.0 // indirect
	github.com/jinzhu/inflection v1.0.0 // indirect
	github.com/jinzhu/now v1.1.5 // indirect
	github.com/josharian/intern v1.0.0 // indirect
	github.com/jpillora/backoff v1.0.0 // indirect
	github.com/kelseyhightower/envconfig v1.4.0 // indirect
	github.com/klauspost/compress v1.18.0 // indirect
	github.com/klauspost/cpuid/v2 v2.2.10 // indirect
	github.com/koron/go-ssdp v0.0.5 // indirect
	github.com/ledgerwatch/lmdb-go v1.18.2 // indirect
	github.com/lib/pq v1.10.4 // indirect
	github.com/libp2p/go-buffer-pool v0.1.0 // indirect
	github.com/libp2p/go-cidranger v1.1.0 // indirect
	github.com/libp2p/go-flow-metrics v0.2.0 // indirect
	github.com/libp2p/go-libp2p-asn-util v0.4.1 // indirect
	github.com/libp2p/go-libp2p-core v0.20.1 // indirect
	github.com/libp2p/go-libp2p-gostream v0.6.0 // indirect
	github.com/libp2p/go-libp2p-http v0.5.0 // indirect
	github.com/libp2p/go-libp2p-kbucket v0.6.5 // indirect
	github.com/libp2p/go-libp2p-peerstore v0.8.0 // indirect
	github.com/libp2p/go-libp2p-xor v0.1.0 // indirect
	github.com/libp2p/go-msgio v0.3.0 // indirect
	github.com/libp2p/go-netroute v0.2.2 // indirect
	github.com/libp2p/go-reuseport v0.4.0 // indirect
	github.com/libp2p/go-yamux/v5 v5.0.0 // indirect
	github.com/lucasb-eyer/go-colorful v1.2.0 // indirect
	github.com/magefile/mage v1.9.0 // indirect
	github.com/mailru/easyjson v0.7.7 // indirect
	github.com/manifoldco/promptui v0.9.0 // indirect
	github.com/marten-seemann/tcp v0.0.0-20210406111302-dfbc87cc63fd // indirect
	github.com/mattn/go-colorable v0.1.14 // indirect
	github.com/mattn/go-isatty v0.0.20 // indirect
	github.com/mattn/go-runewidth v0.0.16 // indirect
	github.com/mattn/go-sqlite3 v1.14.22 // indirect
	github.com/miekg/dns v1.1.63 // indirect
	github.com/mikioh/tcpinfo v0.0.0-20190314235526-30a79bb1804b // indirect
	github.com/mikioh/tcpopt v0.0.0-20190314235656-172688c1accc // indirect
	github.com/minio/blake2b-simd v0.0.0-20160723061019-3f5f724cb5b1 // indirect
	github.com/minio/sha256-simd v1.0.1 // indirect
	github.com/mr-tron/base58 v1.2.0 // indirect
	github.com/multiformats/go-base32 v0.1.0 // indirect
	github.com/multiformats/go-base36 v0.2.0 // indirect
	github.com/multiformats/go-multiaddr-dns v0.4.1 // indirect
	github.com/multiformats/go-multiaddr-fmt v0.1.0 // indirect
	github.com/multiformats/go-multibase v0.2.0 // indirect
	github.com/multiformats/go-multicodec v0.10.0 // indirect
	github.com/multiformats/go-multistream v0.6.0 // indirect
	github.com/multiformats/go-varint v0.1.0 // indirect
	github.com/munnerz/goautoneg v0.0.0-20191010083416-a7dc8b61c822 // indirect
	github.com/nkovacs/streamquote v1.0.0 // indirect
	github.com/onsi/gomega v1.36.2 // indirect
	github.com/opencontainers/runtime-spec v1.2.0 // indirect
	github.com/opentracing/opentracing-go v1.2.0 // indirect
	github.com/pbnjay/memory v0.0.0-20210728143218-7b4eea64cf58 // indirect
	github.com/petar/GoLLRB v0.0.0-20210522233825-ae3b015fd3e9 // indirect
	github.com/pion/datachannel v1.5.10 // indirect
	github.com/pion/dtls/v2 v2.2.12 // indirect
	github.com/pion/dtls/v3 v3.1.1 // indirect
	github.com/pion/ice/v4 v4.0.6 // indirect
	github.com/pion/interceptor v0.1.39 // indirect
	github.com/pion/logging v0.2.4 // indirect
	github.com/pion/mdns/v2 v2.0.7 // indirect
	github.com/pion/randutil v0.1.0 // indirect
	github.com/pion/rtcp v1.2.15 // indirect
	github.com/pion/rtp v1.8.18 // indirect
	github.com/pion/sctp v1.8.36 // indirect
	github.com/pion/sdp/v3 v3.0.10 // indirect
	github.com/pion/srtp/v3 v3.0.4 // indirect
	github.com/pion/stun v0.6.1 // indirect
	github.com/pion/stun/v3 v3.0.0 // indirect
	github.com/pion/transport/v2 v2.2.10 // indirect
	github.com/pion/transport/v3 v3.0.7 // indirect
	github.com/pion/transport/v4 v4.0.1 // indirect
	github.com/pion/turn/v4 v4.0.0 // indirect
	github.com/pion/webrtc/v4 v4.0.10 // indirect
	github.com/pmezard/go-difflib v1.0.1-0.20181226105442-5d4384ee4fb2 // indirect
	github.com/polydawn/refmt v0.89.1-0.20231129105047-37766d95467a // indirect
	github.com/prometheus/client_model v0.6.1 // indirect
	github.com/prometheus/common v0.62.0 // indirect
	github.com/prometheus/procfs v0.15.1 // indirect
	github.com/prometheus/statsd_exporter v0.23.0 // indirect
	github.com/puzpuzpuz/xsync/v2 v2.4.0 // indirect
	github.com/quic-go/qpack v0.5.1 // indirect
	github.com/quic-go/quic-go v0.54.1 // indirect
	github.com/quic-go/webtransport-go v0.8.1-0.20241018022711-4ac2c9250e66 // indirect
	github.com/raulk/clock v1.1.0 // indirect
	github.com/raulk/go-watchdog v1.3.0 // indirect
	github.com/rivo/uniseg v0.4.7 // indirect
	github.com/russross/blackfriday/v2 v2.1.0 // indirect
	github.com/shirou/gopsutil v3.21.4-0.20210419000835-c7a38de76ee5+incompatible // indirect
	github.com/spaolacci/murmur3 v1.1.0 // indirect
	github.com/swaggo/files/v2 v2.0.0 // indirect
	github.com/tklauser/go-sysconf v0.3.5 // indirect
	github.com/tklauser/numcpus v0.2.2 // indirect
	github.com/twmb/murmur3 v1.1.6 // indirect
	github.com/valyala/bytebufferpool v1.0.0 // indirect
	github.com/valyala/fasttemplate v1.2.2 // indirect
	github.com/vmihailenco/tagparser/v2 v2.0.0 // indirect
	github.com/whyrusleeping/base32 v0.0.0-20170828182744-c30ac30633cc // indirect
	github.com/whyrusleeping/bencher v0.0.0-20190829221104-bb6607aa8bba // indirect
	github.com/whyrusleeping/cbor v0.0.0-20171005072247-63513f603b11 // indirect
	github.com/whyrusleeping/chunker v0.0.0-20181014151217-fe64bd25879f // indirect
	github.com/whyrusleeping/go-keyspace v0.0.0-20160322163242-5b898ac5add1 // indirect
	github.com/wk8/go-ordered-map/v2 v2.1.8 // indirect
	github.com/wlynxg/anet v0.0.5 // indirect
	github.com/xrash/smetrics v0.0.0-20240521201337-686a1a2994c1 // indirect
	github.com/zondax/hid v0.9.2 // indirect
	github.com/zondax/ledger-filecoin-go v0.11.1 // indirect
	github.com/zondax/ledger-go v0.14.3 // indirect
	gitlab.com/yawning/secp256k1-voi v0.0.0-20230925100816-f2616030848b // indirect
	gitlab.com/yawning/tuplehash v0.0.0-20230713102510-df83abbf9a02 // indirect
	go.opentelemetry.io/auto/sdk v1.2.1 // indirect
	go.opentelemetry.io/otel/exporters/prometheus v0.50.0 // indirect
	go.opentelemetry.io/otel/metric v1.43.0 // indirect
	go.opentelemetry.io/otel/sdk/metric v1.43.0 // indirect
	go.uber.org/atomic v1.11.0 // indirect
	go.uber.org/dig v1.18.0 // indirect
	go.uber.org/fx v1.23.0 // indirect
	go.uber.org/mock v0.5.0 // indirect
	go.uber.org/multierr v1.11.0 // indirect
	go4.org v0.0.0-20230225012048-214862532bf5 // indirect
	golang.org/x/exp v0.0.0-20250218142911-aa4b98e5adaa // indirect
	golang.org/x/mod v0.32.0 // indirect
	golang.org/x/sync v0.19.0 // indirect
	golang.org/x/telemetry v0.0.0-20260109210033-bd525da824e2 // indirect
	golang.org/x/term v0.40.0 // indirect
	golang.org/x/text v0.34.0 // indirect
	golang.org/x/tools v0.41.0 // indirect
	gonum.org/v1/gonum v0.16.0 // indirect
	google.golang.org/genproto v0.0.0-20230410155749-daa745c078e1 // indirect
	google.golang.org/grpc v1.79.3 // indirect
	google.golang.org/protobuf v1.36.10 // indirect
	gopkg.in/cheggaaa/pb.v1 v1.0.28 // indirect
	gopkg.in/yaml.v2 v2.4.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
	lukechampine.com/blake3 v1.4.0 // indirect
)

//replace github.com/libp2p/go-libp2p-yamux => github.com/libp2p/go-libp2p-yamux v0.5.1

replace github.com/raulk/go-bs-tests => github.com/whyrusleeping/go-bs-tests v0.1.0

replace github.com/filecoin-project/filecoin-ffi => ./extern/filecoin-ffi

replace github.com/filecoin-project/go-bs-lmdb => github.com/gmelodie/go-bs-lmdb v0.1.0
