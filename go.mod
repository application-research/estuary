module github.com/application-research/estuary

go 1.16

require (
	github.com/application-research/filclient v0.0.0-20211018220950-391b76d3409d
	github.com/application-research/go-bs-autobatch v0.0.0-20210811233935-cb8cf8232026
	github.com/cenkalti/backoff/v4 v4.1.1
	github.com/cheggaaa/pb/v3 v3.0.8
	github.com/docker/go-units v0.4.0
	github.com/dustin/go-humanize v1.0.0 // indirect
	github.com/filecoin-project/go-address v0.0.6
	github.com/filecoin-project/go-amt-ipld/v2 v2.1.1-0.20201006184820-924ee87a1349 // indirect
	github.com/filecoin-project/go-bs-lmdb v1.0.5
	github.com/filecoin-project/go-cbor-util v0.0.1
	github.com/filecoin-project/go-data-transfer v1.11.3
	github.com/filecoin-project/go-fil-markets v1.13.1
	github.com/filecoin-project/go-padreader v0.0.1
	github.com/filecoin-project/go-state-types v0.1.1-0.20210915140513-d354ccf10379
	github.com/filecoin-project/lotus v1.13.0-rc2.0.20211008131552-c8042540d5a4
	github.com/filecoin-project/specs-actors v0.9.14
	github.com/google/uuid v1.3.0
	github.com/hashicorp/golang-lru v0.5.4
	github.com/influxdata/influxdb-client-go/v2 v2.5.1
	github.com/ipfs/go-bitswap v0.4.0
	github.com/ipfs/go-block-format v0.0.3
	github.com/ipfs/go-blockservice v0.1.7
	github.com/ipfs/go-cid v0.1.0
	github.com/ipfs/go-cidutil v0.0.2
	github.com/ipfs/go-datastore v0.4.6
	github.com/ipfs/go-ds-flatfs v0.4.5
	github.com/ipfs/go-ds-leveldb v0.4.2
	github.com/ipfs/go-filestore v1.0.0
	github.com/ipfs/go-graphsync v0.10.4
	github.com/ipfs/go-ipfs-blockstore v1.0.5-0.20210802214209-c56038684c45
	github.com/ipfs/go-ipfs-chunker v0.0.5
	github.com/ipfs/go-ipfs-exchange-interface v0.0.1
	github.com/ipfs/go-ipfs-exchange-offline v0.0.1
	github.com/ipfs/go-ipfs-pinner v0.1.2
	github.com/ipfs/go-ipfs-provider v0.6.1
	github.com/ipfs/go-ipld-cbor v0.0.5
	github.com/ipfs/go-ipld-format v0.2.0
	github.com/ipfs/go-log/v2 v2.3.0
	github.com/ipfs/go-merkledag v0.4.1
	github.com/ipfs/go-metrics-interface v0.0.1
	github.com/ipfs/go-metrics-prometheus v0.0.2
	github.com/ipfs/go-unixfs v0.2.6
	github.com/ipld/go-car v0.3.2-0.20211001225732-32d0d9933823
	github.com/ipld/go-ipld-prime v0.12.3
	github.com/jinzhu/gorm v1.9.16
	github.com/labstack/echo/v4 v4.6.1
	github.com/libp2p/go-libp2p v0.15.1
	github.com/libp2p/go-libp2p-connmgr v0.2.4
	github.com/libp2p/go-libp2p-core v0.9.0
	github.com/libp2p/go-libp2p-kad-dht v0.13.1
	github.com/libp2p/go-libp2p-peerstore v0.3.0 // indirect
	github.com/libp2p/go-libp2p-quic-transport v0.12.0
	github.com/libp2p/go-libp2p-record v0.1.3
	github.com/libp2p/go-libp2p-routing-helpers v0.2.3
	github.com/lightstep/otel-launcher-go v0.20.0
	github.com/mitchellh/go-homedir v1.1.0
	github.com/multiformats/go-multiaddr v0.4.1
	github.com/multiformats/go-multihash v0.0.16
	github.com/prometheus/client_golang v1.11.0
	github.com/spf13/viper v1.9.0
	github.com/stretchr/testify v1.7.0
	github.com/urfave/cli/v2 v2.3.0
	github.com/whyrusleeping/cbor-gen v0.0.0-20210713220151-be142a5ae1a8
	github.com/whyrusleeping/go-bs-measure v0.0.0-20210916193332-eb273f9dbd52
	github.com/whyrusleeping/memo v0.0.0-20210914173559-2bd9268e6264
	go.opentelemetry.io/otel v0.20.0
	go.opentelemetry.io/otel/trace v0.20.0
	golang.org/x/net v0.0.0-20211007125505-59d4e928ea9d
	golang.org/x/sys v0.0.0-20211007075335-d3039528d8ac
	golang.org/x/xerrors v0.0.0-20200804184101-5ec99f83aff1
	gorm.io/driver/postgres v1.1.2
	gorm.io/driver/sqlite v1.1.5
	gorm.io/gorm v1.21.15
)

replace github.com/libp2p/go-libp2p-yamux => github.com/libp2p/go-libp2p-yamux v0.5.1

replace github.com/filecoin-project/filecoin-ffi => ./extern/filecoin-ffi
