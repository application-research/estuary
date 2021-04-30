module github.com/whyrusleeping/estuary

go 1.15

require (
	github.com/cheggaaa/pb/v3 v3.0.6
	github.com/filecoin-project/filecoin-ffi v0.30.4-0.20200910194244-f640612a1a1f
	github.com/filecoin-project/go-address v0.0.5
	github.com/filecoin-project/go-bs-lmdb v1.0.3
	github.com/filecoin-project/go-cbor-util v0.0.0-20191219014500-08c40a1e63a2
	github.com/filecoin-project/go-commp-utils v0.1.1-0.20210427191551-70bf140d31c7
	github.com/filecoin-project/go-data-transfer v1.4.3
	github.com/filecoin-project/go-fil-markets v1.2.5
	github.com/filecoin-project/go-padreader v0.0.0-20200903213702-ed5fae088b20
	github.com/filecoin-project/go-state-types v0.1.0
	github.com/filecoin-project/lotus v1.5.3-rc1.0.20210315003656-a54c6bfb057d
	github.com/filecoin-project/specs-actors v0.9.13
	github.com/google/uuid v1.1.4
	github.com/influxdata/influxdb-client-go/v2 v2.2.2
	github.com/ipfs/go-bitswap v0.3.3
	github.com/ipfs/go-block-format v0.0.3
	github.com/ipfs/go-blockservice v0.1.4
	github.com/ipfs/go-cid v0.0.7
	github.com/ipfs/go-datastore v0.4.5
	github.com/ipfs/go-ds-leveldb v0.4.2
	github.com/ipfs/go-graphsync v0.6.0
	github.com/ipfs/go-ipfs-blockstore v1.0.3
	github.com/ipfs/go-ipfs-chunker v0.0.5
	github.com/ipfs/go-ipfs-exchange-offline v0.0.1
	github.com/ipfs/go-ipld-format v0.2.0
	github.com/ipfs/go-log v1.0.4
	github.com/ipfs/go-merkledag v0.3.2
	github.com/ipfs/go-unixfs v0.2.4
	github.com/labstack/echo/v4 v4.2.0
	github.com/libp2p/go-libp2p v0.13.0
	github.com/libp2p/go-libp2p-connmgr v0.2.4
	github.com/libp2p/go-libp2p-core v0.8.0
	github.com/libp2p/go-libp2p-crypto v0.1.0
	github.com/libp2p/go-libp2p-kad-dht v0.11.1
	github.com/libp2p/go-libp2p-protocol v0.1.0
	github.com/libp2p/go-libp2p-routing-helpers v0.2.3
	github.com/lightstep/otel-launcher-go v0.18.0
	github.com/minio/sha256-simd v1.0.0 // indirect
	github.com/multiformats/go-multiaddr v0.3.1
	github.com/urfave/cli/v2 v2.3.0
	github.com/whyrusleeping/base32 v0.0.0-20170828182744-c30ac30633cc
	go.opentelemetry.io/otel v0.18.0
	go.opentelemetry.io/otel/trace v0.18.0
	golang.org/x/sys v0.0.0-20210123111255-9b0068b26619
	golang.org/x/xerrors v0.0.0-20200804184101-5ec99f83aff1
	gorm.io/driver/postgres v1.0.8
	gorm.io/driver/sqlite v1.1.4
	gorm.io/gorm v1.20.12
)

replace github.com/filecoin-project/filecoin-ffi => ../lotus/extern/filecoin-ffi

replace github.com/filecoin-project/lotus => ../lotus
