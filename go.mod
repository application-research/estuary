module github.com/application-research/estuary

go 1.16

require (
	contrib.go.opencensus.io/exporter/prometheus v0.4.0
	github.com/application-research/filclient v0.0.0-20220202010800-86ee27425384
	github.com/application-research/go-bs-autobatch v0.0.0-20211215020302-c4c0b68ef402
	github.com/cenkalti/backoff/v4 v4.1.2 // indirect
	github.com/cheggaaa/pb/v3 v3.0.8
	github.com/docker/go-units v0.4.0
	github.com/dustin/go-humanize v1.0.0
	github.com/filecoin-project/go-address v0.0.6
	github.com/filecoin-project/go-bs-lmdb v1.0.6-0.20211215050109-9e2b984c988e
	github.com/filecoin-project/go-cbor-util v0.0.1
	github.com/filecoin-project/go-data-transfer v1.14.0
	github.com/filecoin-project/go-fil-commcid v0.1.0
	github.com/filecoin-project/go-fil-markets v1.19.0
	github.com/filecoin-project/go-jsonrpc v0.1.5
	github.com/filecoin-project/go-padreader v0.0.1 // indirect
	github.com/filecoin-project/go-state-types v0.1.3
	github.com/filecoin-project/lotus v1.13.3-0.20220126152212-3e6c482229fb
	github.com/filecoin-project/specs-actors/v6 v6.0.1
	github.com/google/uuid v1.3.0
	github.com/hashicorp/golang-lru v0.5.4
	github.com/influxdata/influxdb-client-go/v2 v2.5.1
	github.com/ipfs/go-bitswap v0.5.2-0.20211214021705-dbfc6a1d986e
	github.com/ipfs/go-block-format v0.0.3
	github.com/ipfs/go-blockservice v0.2.1
	github.com/ipfs/go-cid v0.1.0
	github.com/ipfs/go-cidutil v0.0.2
	github.com/ipfs/go-datastore v0.5.1
	github.com/ipfs/go-ds-flatfs v0.5.1
	github.com/ipfs/go-ds-leveldb v0.5.0
	github.com/ipfs/go-filestore v1.1.0
	github.com/ipfs/go-graphsync v0.12.0
	github.com/ipfs/go-ipfs-blockstore v1.1.2
	github.com/ipfs/go-ipfs-chunker v0.0.5
	github.com/ipfs/go-ipfs-exchange-interface v0.1.0
	github.com/ipfs/go-ipfs-exchange-offline v0.1.1
	github.com/ipfs/go-ipfs-provider v0.7.1
	github.com/ipfs/go-ipld-cbor v0.0.6
	github.com/ipfs/go-ipld-format v0.2.0
	github.com/ipfs/go-log/v2 v2.5.0
	github.com/ipfs/go-merkledag v0.5.1
	github.com/ipfs/go-metrics-interface v0.0.1
	github.com/ipfs/go-metrics-prometheus v0.0.2
	github.com/ipfs/go-path v0.0.7
	github.com/ipfs/go-unixfs v0.3.1
	github.com/ipld/go-car v0.3.3
	github.com/ipld/go-ipld-prime v0.14.4
	github.com/jinzhu/gorm v1.9.16
	github.com/labstack/echo/v4 v4.6.1
	github.com/libp2p/go-libp2p v0.18.0-rc5
	github.com/libp2p/go-libp2p-connmgr v0.3.1
	github.com/libp2p/go-libp2p-core v0.14.0
	github.com/libp2p/go-libp2p-kad-dht v0.15.0
	github.com/libp2p/go-libp2p-record v0.1.3
	github.com/libp2p/go-libp2p-resource-manager v0.1.4
	github.com/libp2p/go-libp2p-routing-helpers v0.2.3
	github.com/mitchellh/go-homedir v1.1.0
	github.com/multiformats/go-multiaddr v0.5.0
	github.com/multiformats/go-multihash v0.1.0
	github.com/prometheus/client_golang v1.11.0
	github.com/spf13/viper v1.9.0
	github.com/stretchr/testify v1.7.0
	github.com/urfave/cli/v2 v2.3.0
	github.com/whyrusleeping/cbor-gen v0.0.0-20210713220151-be142a5ae1a8
	github.com/whyrusleeping/go-bs-measure v0.0.0-20211215015044-d56d1cad3b9e
	github.com/whyrusleeping/memo v0.0.0-20211124220851-3b94446416a3
	go.opencensus.io v0.23.0
	go.opentelemetry.io/otel v1.3.0
	go.opentelemetry.io/otel/exporters/jaeger v1.2.0
	go.opentelemetry.io/otel/sdk v1.2.0
	go.opentelemetry.io/otel/trace v1.3.0
	go.uber.org/fx v1.9.0
	golang.org/x/net v0.0.0-20211112202133-69e39bad7dc2
	golang.org/x/sys v0.0.0-20211209171907-798191bca915
	golang.org/x/xerrors v0.0.0-20200804184101-5ec99f83aff1
	gorm.io/driver/postgres v1.1.2
	gorm.io/driver/sqlite v1.1.5
	gorm.io/gorm v1.21.15
)

//replace github.com/libp2p/go-libp2p-yamux => github.com/libp2p/go-libp2p-yamux v0.5.1

replace github.com/raulk/go-bs-tests => github.com/whyrusleeping/go-bs-tests v0.1.0

replace github.com/filecoin-project/filecoin-ffi => ./extern/filecoin-ffi
