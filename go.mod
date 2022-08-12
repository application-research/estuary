module github.com/application-research/estuary

go 1.17

require (
	contrib.go.opencensus.io/exporter/prometheus v0.4.0
	github.com/application-research/filclient v0.0.0-20220622165741-3ca6a3f3bc7a
	github.com/application-research/go-bs-autobatch v0.0.0-20211215020302-c4c0b68ef402
	github.com/cenkalti/backoff/v4 v4.1.2
	github.com/cheggaaa/pb/v3 v3.0.8
	github.com/docker/go-units v0.4.0
	github.com/dustin/go-humanize v1.0.0 // indirect
	github.com/facebookgo/atomicfile v0.0.0-20151019160806-2de1f203e7d5
	github.com/filecoin-project/boost v0.0.0-20220310131759-cd38741e464d
	github.com/filecoin-project/go-address v0.0.6
	github.com/filecoin-project/go-bs-lmdb v1.0.6-0.20211215050109-9e2b984c988e
	github.com/filecoin-project/go-cbor-util v0.0.1
	github.com/filecoin-project/go-data-transfer v1.15.1
	github.com/filecoin-project/go-fil-commcid v0.1.0 // indirect
	github.com/filecoin-project/go-fil-markets v1.20.1
	github.com/filecoin-project/go-jsonrpc v0.1.5
	github.com/filecoin-project/go-padreader v0.0.1
	github.com/filecoin-project/go-state-types v0.1.3
	github.com/filecoin-project/index-provider v0.6.1
	github.com/filecoin-project/lotus v1.15.1
	github.com/filecoin-project/specs-actors/v6 v6.0.1
	github.com/filecoin-project/storetheindex v0.4.1
	github.com/go-openapi/swag v0.21.1 // indirect
	github.com/google/uuid v1.3.0
	github.com/hashicorp/golang-lru v0.5.5-0.20210104140557-80c98217689d
	github.com/influxdata/influxdb-client-go/v2 v2.5.1
	github.com/ipfs/go-bitswap v0.5.2-0.20211214021705-dbfc6a1d986e
	github.com/ipfs/go-block-format v0.0.3
	github.com/ipfs/go-blockservice v0.2.1
	github.com/ipfs/go-cid v0.1.0
	github.com/ipfs/go-cidutil v0.0.2
	github.com/ipfs/go-datastore v0.5.1
	github.com/ipfs/go-ds-flatfs v0.5.1
	github.com/ipfs/go-ds-leveldb v0.5.0
	github.com/ipfs/go-fetcher v1.6.1
	github.com/ipfs/go-graphsync v0.13.1
	github.com/ipfs/go-ipfs-blockstore v1.1.2
	github.com/ipfs/go-ipfs-chunker v0.0.5
	github.com/ipfs/go-ipfs-exchange-interface v0.1.0
	github.com/ipfs/go-ipfs-exchange-offline v0.1.1
	github.com/ipfs/go-ipfs-provider v0.7.1
	github.com/ipfs/go-ipld-cbor v0.0.6
	github.com/ipfs/go-ipld-format v0.2.0
	github.com/ipfs/go-log/v2 v2.5.1
	github.com/ipfs/go-merkledag v0.5.1
	github.com/ipfs/go-metrics-interface v0.0.1
	github.com/ipfs/go-metrics-prometheus v0.0.2
	github.com/ipfs/go-path v0.2.1
	github.com/ipfs/go-unixfs v0.3.1
	github.com/ipfs/go-unixfsnode v1.4.0
	github.com/ipld/go-car v0.4.0
	github.com/ipld/go-codec-dagpb v1.4.0
	github.com/ipld/go-ipld-prime v0.16.0
	github.com/jinzhu/gorm v1.9.16
	github.com/labstack/echo/v4 v4.6.1
	github.com/libp2p/go-libp2p v0.18.0
	github.com/libp2p/go-libp2p-connmgr v0.3.1
	github.com/libp2p/go-libp2p-core v0.14.0
	github.com/libp2p/go-libp2p-kad-dht v0.15.0
	github.com/libp2p/go-libp2p-record v0.1.3
	github.com/libp2p/go-libp2p-resource-manager v0.1.5
	github.com/libp2p/go-libp2p-routing-helpers v0.2.3
	github.com/mitchellh/go-homedir v1.1.0
	github.com/multiformats/go-multiaddr v0.5.0
	github.com/multiformats/go-multihash v0.2.0
	github.com/prometheus/client_golang v1.11.0
	github.com/stretchr/testify v1.7.2
	github.com/swaggo/echo-swagger v1.3.0
	github.com/swaggo/swag v1.7.9
	github.com/urfave/cli/v2 v2.10.2
	github.com/whyrusleeping/cbor-gen v0.0.0-20220302191723-37c43cae8e14
	github.com/whyrusleeping/go-bs-measure v0.0.0-20211215015044-d56d1cad3b9e
	github.com/whyrusleeping/memo v0.0.0-20211124220851-3b94446416a3
	go.opencensus.io v0.23.0
	go.opentelemetry.io/otel v1.3.0
	go.opentelemetry.io/otel/exporters/jaeger v1.2.0
	go.opentelemetry.io/otel/sdk v1.2.0
	go.opentelemetry.io/otel/trace v1.3.0
	golang.org/x/crypto v0.0.0-20220722155217-630584e8d5aa // indirect
	golang.org/x/net v0.0.0-20220607020251-c690dde0001d
	golang.org/x/sys v0.0.0-20220804214406-8e32c043e418
	golang.org/x/xerrors v0.0.0-20220517211312-f3a8303e98df
	gorm.io/driver/postgres v1.1.2
	gorm.io/driver/sqlite v1.1.5
	gorm.io/gorm v1.21.15
)

require (
	github.com/ipfs/go-ipfs v0.11.0
	github.com/pkg/errors v0.9.1
)

require (
	github.com/BurntSushi/toml v1.1.0 // indirect
	github.com/DataDog/zstd v1.4.1 // indirect
	github.com/GeertJohan/go.incremental v1.0.0 // indirect
	github.com/GeertJohan/go.rice v1.0.2 // indirect
	github.com/Gurpartap/async v0.0.0-20180927173644-4f7f499dd9ee // indirect
	github.com/Kubuxu/imtui v0.0.0-20210401140320-41663d68d0fa // indirect
	github.com/KyleBanks/depth v1.2.1 // indirect
	github.com/PuerkitoBio/purell v1.1.1 // indirect
	github.com/PuerkitoBio/urlesc v0.0.0-20170810143723-de5bf2ad4578 // indirect
	github.com/StackExchange/wmi v1.2.1 // indirect
	github.com/Stebalien/go-bitfield v0.0.1 // indirect
	github.com/VividCortex/ewma v1.1.1 // indirect
	github.com/acarl005/stripansi v0.0.0-20180116102854-5a71ef0e047d // indirect
	github.com/akavel/rsrc v0.8.0 // indirect
	github.com/alecthomas/units v0.0.0-20210927113745-59d0afb8317a // indirect
	github.com/alexbrainman/goissue34681 v0.0.0-20191006012335-3fc7a47baff5 // indirect
	github.com/benbjohnson/clock v1.2.0 // indirect
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/bep/debounce v1.2.0 // indirect
	github.com/btcsuite/btcd v0.22.0-beta // indirect
	github.com/btcsuite/btcd/btcec/v2 v2.2.0 // indirect
	github.com/buger/goterm v1.0.3 // indirect
	github.com/cenkalti/backoff v2.2.1+incompatible // indirect
	github.com/cespare/xxhash v1.1.0 // indirect
	github.com/cespare/xxhash/v2 v2.1.2 // indirect
	github.com/cheekybits/genny v1.0.0 // indirect
	github.com/chzyer/readline v0.0.0-20180603132655-2972be24d48e // indirect
	github.com/cilium/ebpf v0.2.0 // indirect
	github.com/containerd/cgroups v0.0.0-20201119153540-4cbc285b3327 // indirect
	github.com/coreos/go-systemd/v22 v22.3.2 // indirect
	github.com/cpuguy83/go-md2man/v2 v2.0.2 // indirect
	github.com/crackcomm/go-gitignore v0.0.0-20170627025303-887ab5e44cc3 // indirect
	github.com/cskr/pubsub v1.0.2 // indirect
	github.com/daaku/go.zipexe v1.0.0 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/davidlazar/go-crypto v0.0.0-20200604182044-b73af7476f6c // indirect
	github.com/dchest/uniuri v0.0.0-20200228104902-7aecb25e1fe5 // indirect
	github.com/decred/dcrd/dcrec/secp256k1/v4 v4.0.1 // indirect
	github.com/deepmap/oapi-codegen v1.8.2 // indirect
	github.com/detailyang/go-fallocate v0.0.0-20180908115635-432fa640bd2e // indirect
	github.com/dgraph-io/badger/v2 v2.2007.3 // indirect
	github.com/dgraph-io/ristretto v0.1.0 // indirect
	github.com/dgryski/go-farm v0.0.0-20190423205320-6a90982ecee2 // indirect
	github.com/elastic/go-sysinfo v1.7.0 // indirect
	github.com/elastic/go-windows v1.0.0 // indirect
	github.com/elastic/gosigar v0.14.1 // indirect
	github.com/ethereum/go-ethereum v1.10.21 // indirect
	github.com/fatih/color v1.13.0 // indirect
	github.com/filecoin-project/dagstore v0.5.2 // indirect
	github.com/filecoin-project/filecoin-ffi v0.30.4-0.20200910194244-f640612a1a1f // indirect
	github.com/filecoin-project/go-amt-ipld/v2 v2.1.0 // indirect
	github.com/filecoin-project/go-amt-ipld/v3 v3.1.0 // indirect
	github.com/filecoin-project/go-amt-ipld/v4 v4.0.0 // indirect
	github.com/filecoin-project/go-bitfield v0.2.4 // indirect
	github.com/filecoin-project/go-commp-utils v0.1.3 // indirect
	github.com/filecoin-project/go-crypto v0.0.1 // indirect
	github.com/filecoin-project/go-ds-versioning v0.1.1 // indirect
	github.com/filecoin-project/go-fil-commp-hashhash v0.1.0 // indirect
	github.com/filecoin-project/go-hamt-ipld v0.1.5 // indirect
	github.com/filecoin-project/go-hamt-ipld/v2 v2.0.0 // indirect
	github.com/filecoin-project/go-hamt-ipld/v3 v3.1.0 // indirect
	github.com/filecoin-project/go-legs v0.3.11 // indirect
	github.com/filecoin-project/go-paramfetch v0.0.4 // indirect
	github.com/filecoin-project/go-statemachine v1.0.2 // indirect
	github.com/filecoin-project/go-statestore v0.2.0 // indirect
	github.com/filecoin-project/specs-actors v0.9.14 // indirect
	github.com/filecoin-project/specs-actors/v2 v2.3.6 // indirect
	github.com/filecoin-project/specs-actors/v3 v3.1.1 // indirect
	github.com/filecoin-project/specs-actors/v4 v4.0.1 // indirect
	github.com/filecoin-project/specs-actors/v5 v5.0.4 // indirect
	github.com/filecoin-project/specs-actors/v7 v7.0.0 // indirect
	github.com/filecoin-project/specs-storage v0.2.0 // indirect
	github.com/flynn/noise v1.0.0 // indirect
	github.com/francoispqt/gojay v1.2.13 // indirect
	github.com/fsnotify/fsnotify v1.5.1 // indirect
	github.com/gbrlsnchs/jwt/v3 v3.0.1 // indirect
	github.com/gdamore/encoding v1.0.0 // indirect
	github.com/gdamore/tcell/v2 v2.2.0 // indirect
	github.com/go-kit/log v0.2.0 // indirect
	github.com/go-logfmt/logfmt v0.5.1 // indirect
	github.com/go-logr/logr v1.2.1 // indirect
	github.com/go-logr/stdr v1.2.0 // indirect
	github.com/go-ole/go-ole v1.2.5 // indirect
	github.com/go-openapi/jsonpointer v0.19.5 // indirect
	github.com/go-openapi/jsonreference v0.19.6 // indirect
	github.com/go-openapi/spec v0.20.4 // indirect
	github.com/go-task/slim-sprig v0.0.0-20210107165309-348f09dbbbc0 // indirect
	github.com/godbus/dbus/v5 v5.0.4 // indirect
	github.com/gogo/protobuf v1.3.2 // indirect
	github.com/golang-jwt/jwt v3.2.2+incompatible // indirect
	github.com/golang/glog v1.0.0 // indirect
	github.com/golang/groupcache v0.0.0-20210331224755-41bb18bfe9da // indirect
	github.com/golang/mock v1.6.0 // indirect
	github.com/golang/protobuf v1.5.2 // indirect
	github.com/golang/snappy v0.0.4 // indirect
	github.com/google/gopacket v1.1.19 // indirect
	github.com/gorilla/mux v1.8.0 // indirect
	github.com/gorilla/websocket v1.4.2 // indirect
	github.com/graph-gophers/graphql-go v1.3.0 // indirect
	github.com/hako/durafmt v0.0.0-20200710122514-c0fb7b4da026 // indirect
	github.com/hannahhoward/cbor-gen-for v0.0.0-20200817222906-ea96cece81f1 // indirect
	github.com/hannahhoward/go-pubsub v0.0.0-20200423002714-8d62886cc36e // indirect
	github.com/hashicorp/errwrap v1.0.0 // indirect
	github.com/hashicorp/go-multierror v1.1.1 // indirect
	github.com/huin/goupnp v1.0.3 // indirect
	github.com/icza/backscanner v0.0.0-20210726202459-ac2ffc679f94 // indirect
	github.com/influxdata/line-protocol v0.0.0-20210311194329-9aa0e372d097 // indirect
	github.com/ipfs/bbloom v0.0.4 // indirect
	github.com/ipfs/go-bitfield v1.0.0 // indirect
	github.com/ipfs/go-ds-badger2 v0.1.2 // indirect
	github.com/ipfs/go-ds-measure v0.2.0 // indirect
	github.com/ipfs/go-fs-lock v0.0.7 // indirect
	github.com/ipfs/go-ipfs-blocksutil v0.0.1 // indirect
	github.com/ipfs/go-ipfs-cmds v0.6.0 // indirect
	github.com/ipfs/go-ipfs-delay v0.0.1 // indirect
	github.com/ipfs/go-ipfs-ds-help v1.1.0 // indirect
	github.com/ipfs/go-ipfs-files v0.1.1 // indirect
	github.com/ipfs/go-ipfs-http-client v0.0.6 // indirect
	github.com/ipfs/go-ipfs-posinfo v0.0.1 // indirect
	github.com/ipfs/go-ipfs-pq v0.0.2 // indirect
	github.com/ipfs/go-ipfs-util v0.0.2 // indirect
	github.com/ipfs/go-ipld-legacy v0.1.1 // indirect
	github.com/ipfs/go-ipns v0.1.2 // indirect
	github.com/ipfs/go-log v1.0.5 // indirect
	github.com/ipfs/go-peertaskqueue v0.7.1 // indirect
	github.com/ipfs/go-verifcid v0.0.1 // indirect
	github.com/ipfs/interface-go-ipfs-core v0.5.2 // indirect
	github.com/ipld/go-car/v2 v2.1.2-0.20220124154420-9c7956a6eb9d // indirect
	github.com/ipld/go-ipld-selector-text-lite v0.0.1 // indirect
	github.com/ipsn/go-secp256k1 v0.0.0-20180726113642-9d62b9f0bc52 // indirect
	github.com/jackc/chunkreader/v2 v2.0.1 // indirect
	github.com/jackc/pgconn v1.10.0 // indirect
	github.com/jackc/pgio v1.0.0 // indirect
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgproto3/v2 v2.1.1 // indirect
	github.com/jackc/pgservicefile v0.0.0-20200714003250-2b9c44734f2b // indirect
	github.com/jackc/pgtype v1.8.1 // indirect
	github.com/jackc/pgx/v4 v4.13.0 // indirect
	github.com/jackpal/go-nat-pmp v1.0.2 // indirect
	github.com/jbenet/go-random v0.0.0-20190219211222-123a90aedc0c // indirect
	github.com/jbenet/go-temp-err-catcher v0.1.0 // indirect
	github.com/jbenet/goprocess v0.1.4 // indirect
	github.com/jessevdk/go-flags v1.4.0 // indirect
	github.com/jinzhu/inflection v1.0.0 // indirect
	github.com/jinzhu/now v1.1.2 // indirect
	github.com/joeshaw/multierror v0.0.0-20140124173710-69b34d4ec901 // indirect
	github.com/josharian/intern v1.0.0 // indirect
	github.com/jpillora/backoff v1.0.0 // indirect
	github.com/kelseyhightower/envconfig v1.4.0 // indirect
	github.com/klauspost/compress v1.13.6 // indirect
	github.com/klauspost/cpuid/v2 v2.0.9 // indirect
	github.com/koron/go-ssdp v0.0.2 // indirect
	github.com/labstack/gommon v0.3.1 // indirect
	github.com/ledgerwatch/lmdb-go v1.18.2 // indirect
	github.com/lib/pq v1.10.2 // indirect
	github.com/libp2p/go-buffer-pool v0.0.2 // indirect
	github.com/libp2p/go-cidranger v1.1.0 // indirect
	github.com/libp2p/go-conn-security-multistream v0.3.0 // indirect
	github.com/libp2p/go-eventbus v0.2.1 // indirect
	github.com/libp2p/go-flow-metrics v0.0.3 // indirect
	github.com/libp2p/go-libp2p-asn-util v0.1.0 // indirect
	github.com/libp2p/go-libp2p-blankhost v0.3.0 // indirect
	github.com/libp2p/go-libp2p-discovery v0.6.0 // indirect
	github.com/libp2p/go-libp2p-gostream v0.3.2-0.20220309102559-3d4abe2a19ac // indirect
	github.com/libp2p/go-libp2p-http v0.2.1 // indirect
	github.com/libp2p/go-libp2p-kbucket v0.4.7 // indirect
	github.com/libp2p/go-libp2p-loggables v0.1.0 // indirect
	github.com/libp2p/go-libp2p-mplex v0.6.0 // indirect
	github.com/libp2p/go-libp2p-nat v0.1.0 // indirect
	github.com/libp2p/go-libp2p-noise v0.3.0 // indirect
	github.com/libp2p/go-libp2p-peerstore v0.6.0 // indirect
	github.com/libp2p/go-libp2p-pnet v0.2.0 // indirect
	github.com/libp2p/go-libp2p-pubsub v0.6.1 // indirect
	github.com/libp2p/go-libp2p-quic-transport v0.16.1 // indirect
	github.com/libp2p/go-libp2p-swarm v0.10.2 // indirect
	github.com/libp2p/go-libp2p-tls v0.3.1 // indirect
	github.com/libp2p/go-libp2p-transport-upgrader v0.7.1 // indirect
	github.com/libp2p/go-libp2p-xor v0.0.0-20210714161855-5c005aca55db // indirect
	github.com/libp2p/go-libp2p-yamux v0.8.2 // indirect
	github.com/libp2p/go-mplex v0.6.0 // indirect
	github.com/libp2p/go-msgio v0.1.0 // indirect
	github.com/libp2p/go-nat v0.1.0 // indirect
	github.com/libp2p/go-netroute v0.2.0 // indirect
	github.com/libp2p/go-openssl v0.0.7 // indirect
	github.com/libp2p/go-reuseport v0.1.0 // indirect
	github.com/libp2p/go-reuseport-transport v0.1.0 // indirect
	github.com/libp2p/go-stream-muxer-multistream v0.4.0 // indirect
	github.com/libp2p/go-tcp-transport v0.5.1 // indirect
	github.com/libp2p/go-ws-transport v0.6.0 // indirect
	github.com/libp2p/go-yamux/v3 v3.0.2 // indirect
	github.com/lucas-clemente/quic-go v0.25.0 // indirect
	github.com/lucasb-eyer/go-colorful v1.0.3 // indirect
	github.com/magefile/mage v1.9.0 // indirect
	github.com/mailru/easyjson v0.7.7 // indirect
	github.com/marten-seemann/qtls-go1-16 v0.1.4 // indirect
	github.com/marten-seemann/qtls-go1-17 v0.1.0 // indirect
	github.com/marten-seemann/qtls-go1-18 v0.1.0-beta.1 // indirect
	github.com/marten-seemann/tcp v0.0.0-20210406111302-dfbc87cc63fd // indirect
	github.com/mattn/go-colorable v0.1.12 // indirect
	github.com/mattn/go-isatty v0.0.14 // indirect
	github.com/mattn/go-runewidth v0.0.12 // indirect
	github.com/mattn/go-sqlite3 v1.14.9 // indirect
	github.com/matttproud/golang_protobuf_extensions v1.0.1 // indirect
	github.com/miekg/dns v1.1.43 // indirect
	github.com/mikioh/tcpinfo v0.0.0-20190314235526-30a79bb1804b // indirect
	github.com/mikioh/tcpopt v0.0.0-20190314235656-172688c1accc // indirect
	github.com/minio/blake2b-simd v0.0.0-20160723061019-3f5f724cb5b1 // indirect
	github.com/minio/sha256-simd v1.0.0 // indirect
	github.com/mr-tron/base58 v1.2.0 // indirect
	github.com/multiformats/go-base32 v0.0.4 // indirect
	github.com/multiformats/go-base36 v0.1.0 // indirect
	github.com/multiformats/go-multiaddr-dns v0.3.1 // indirect
	github.com/multiformats/go-multiaddr-fmt v0.1.0 // indirect
	github.com/multiformats/go-multibase v0.0.3 // indirect
	github.com/multiformats/go-multicodec v0.4.1 // indirect
	github.com/multiformats/go-multistream v0.2.2 // indirect
	github.com/multiformats/go-varint v0.0.6 // indirect
	github.com/nkovacs/streamquote v1.0.0 // indirect
	github.com/nxadm/tail v1.4.8 // indirect
	github.com/onsi/ginkgo v1.16.4 // indirect
	github.com/opencontainers/runtime-spec v1.0.2 // indirect
	github.com/opentracing/opentracing-go v1.2.0 // indirect
	github.com/pbnjay/memory v0.0.0-20210728143218-7b4eea64cf58 // indirect
	github.com/petar/GoLLRB v0.0.0-20210522233825-ae3b015fd3e9 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/polydawn/refmt v0.0.0-20201211092308-30ac6d18308e // indirect
	github.com/prometheus/client_model v0.2.0 // indirect
	github.com/prometheus/common v0.30.0 // indirect
	github.com/prometheus/procfs v0.7.3 // indirect
	github.com/prometheus/statsd_exporter v0.21.0 // indirect
	github.com/raulk/clock v1.1.0 // indirect
	github.com/raulk/go-watchdog v1.2.0 // indirect
	github.com/relvacode/iso8601 v1.1.0 // indirect
	github.com/remyoudompheng/bigfft v0.0.0-20200410134404-eec4a21b6bb0 // indirect
	github.com/rivo/uniseg v0.2.0 // indirect
	github.com/rs/cors v1.7.0 // indirect
	github.com/russross/blackfriday/v2 v2.1.0 // indirect
	github.com/shirou/gopsutil v3.21.4-0.20210419000835-c7a38de76ee5+incompatible // indirect
	github.com/shurcooL/sanitized_anchor_name v1.0.0 // indirect
	github.com/sirupsen/logrus v1.8.1 // indirect
	github.com/spacemonkeygo/spacelog v0.0.0-20180420211403-2296661a0572 // indirect
	github.com/spaolacci/murmur3 v1.1.0 // indirect
	github.com/spruceid/siwe-go v0.2.0 // indirect
	github.com/swaggo/files v0.0.0-20210815190702-a29dd2bc99b2 // indirect
	github.com/syndtr/goleveldb v1.0.1-0.20210819022825-2ae1ddf74ef7 // indirect
	github.com/tj/go-spin v1.1.0 // indirect
	github.com/valyala/bytebufferpool v1.0.0 // indirect
	github.com/valyala/fasttemplate v1.2.1 // indirect
	github.com/whyrusleeping/base32 v0.0.0-20170828182744-c30ac30633cc // indirect
	github.com/whyrusleeping/bencher v0.0.0-20190829221104-bb6607aa8bba // indirect
	github.com/whyrusleeping/cbor v0.0.0-20171005072247-63513f603b11 // indirect
	github.com/whyrusleeping/chunker v0.0.0-20181014151217-fe64bd25879f // indirect
	github.com/whyrusleeping/go-keyspace v0.0.0-20160322163242-5b898ac5add1 // indirect
	github.com/whyrusleeping/ledger-filecoin-go v0.9.1-0.20201010031517-c3dcc1bddce4 // indirect
	github.com/whyrusleeping/multiaddr-filter v0.0.0-20160516205228-e903e4adabd7 // indirect
	github.com/whyrusleeping/pubsub v0.0.0-20190708150250-92bcb0691325 // indirect
	github.com/whyrusleeping/timecache v0.0.0-20160911033111-cfcb2f1abfee // indirect
	github.com/xlab/c-for-go v0.0.0-20201112171043-ea6dce5809cb // indirect
	github.com/xlab/pkgconfig v0.0.0-20170226114623-cea12a0fd245 // indirect
	github.com/xrash/smetrics v0.0.0-20201216005158-039620a65673 // indirect
	github.com/zondax/hid v0.9.0 // indirect
	github.com/zondax/ledger-go v0.12.1 // indirect
	go.uber.org/atomic v1.9.0 // indirect
	go.uber.org/dig v1.12.0 // indirect
	go.uber.org/fx v1.15.0 // indirect
	go.uber.org/multierr v1.7.0 // indirect
	go.uber.org/zap v1.19.1 // indirect
	go4.org v0.0.0-20200411211856-f5505b9728dd // indirect
	golang.org/x/exp v0.0.0-20220426173459-3bcf042a4bf5 // indirect
	golang.org/x/mod v0.6.0-dev.0.20211013180041-c96bc1413d57 // indirect
	golang.org/x/sync v0.0.0-20210220032951-036812b2e83c // indirect
	golang.org/x/term v0.0.0-20210927222741-03fcf44c2211 // indirect
	golang.org/x/text v0.3.7 // indirect
	golang.org/x/time v0.0.0-20210723032227-1f47c861a9ac // indirect
	golang.org/x/tools v0.1.9 // indirect
	google.golang.org/genproto v0.0.0-20210917145530-b395a37504d4 // indirect
	google.golang.org/grpc v1.40.0 // indirect
	google.golang.org/protobuf v1.27.1 // indirect
	gopkg.in/cheggaaa/pb.v1 v1.0.28 // indirect
	gopkg.in/tomb.v1 v1.0.0-20141024135613-dd632973f1e7 // indirect
	gopkg.in/yaml.v2 v2.4.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
	howett.net/plist v0.0.0-20181124034731-591f970eefbb // indirect
	lukechampine.com/blake3 v1.1.7 // indirect
	modernc.org/cc v1.0.0 // indirect
	modernc.org/golex v1.0.1 // indirect
	modernc.org/mathutil v1.1.1 // indirect
	modernc.org/strutil v1.1.0 // indirect
	modernc.org/xc v1.0.0 // indirect
)

//replace github.com/libp2p/go-libp2p-yamux => github.com/libp2p/go-libp2p-yamux v0.5.1

replace github.com/raulk/go-bs-tests => github.com/whyrusleeping/go-bs-tests v0.1.0

replace github.com/filecoin-project/filecoin-ffi => ./extern/filecoin-ffi
