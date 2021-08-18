module github.com/application-research/estuary

go 1.16

require (
	github.com/application-research/filclient v0.0.0-20210805233950-62d1f1f89174
	github.com/application-research/go-bs-autobatch v0.0.0-20210811233935-cb8cf8232026
	github.com/cheggaaa/pb/v3 v3.0.6
	github.com/docker/go-units v0.4.0
	github.com/filecoin-project/go-address v0.0.5
	github.com/filecoin-project/go-bs-lmdb v1.0.5
	github.com/filecoin-project/go-cbor-util v0.0.0-20191219014500-08c40a1e63a2
	github.com/filecoin-project/go-data-transfer v1.7.4
	github.com/filecoin-project/go-fil-markets v1.6.2
	github.com/filecoin-project/go-padreader v0.0.0-20210723183308-812a16dc01b1
	github.com/filecoin-project/go-state-types v0.1.1-0.20210810190654-139e0e79e69e
	github.com/filecoin-project/lotus v1.10.1
	github.com/filecoin-project/specs-actors v0.9.14
	github.com/google/uuid v1.2.0
	github.com/hashicorp/golang-lru v0.5.4
	github.com/influxdata/influxdb-client-go/v2 v2.2.2
	github.com/ipfs/go-bitswap v0.3.3
	github.com/ipfs/go-block-format v0.0.3
	github.com/ipfs/go-blockservice v0.1.4
	github.com/ipfs/go-cid v0.0.7
	github.com/ipfs/go-datastore v0.4.5
	github.com/ipfs/go-ds-flatfs v0.4.5
	github.com/ipfs/go-ds-leveldb v0.4.2
	github.com/ipfs/go-ipfs-blockstore v1.0.5-0.20210802214209-c56038684c45
	github.com/ipfs/go-ipfs-chunker v0.0.5
	github.com/ipfs/go-ipfs-exchange-interface v0.0.1
	github.com/ipfs/go-ipfs-exchange-offline v0.0.1
	github.com/ipfs/go-ipfs-pinner v0.1.2
	github.com/ipfs/go-ipfs-provider v0.5.1
	github.com/ipfs/go-ipld-format v0.2.0
	github.com/ipfs/go-log v1.0.5
	github.com/ipfs/go-merkledag v0.3.2
	github.com/ipfs/go-metrics-interface v0.0.1
	github.com/ipfs/go-metrics-prometheus v0.0.2
	github.com/ipfs/go-unixfs v0.2.4
	github.com/ipld/go-car v0.1.1-0.20201119040415-11b6074b6d4d
	github.com/labstack/echo/v4 v4.2.0
	github.com/libp2p/go-libp2p v0.14.2
	github.com/libp2p/go-libp2p-connmgr v0.2.4
	github.com/libp2p/go-libp2p-core v0.8.6
	github.com/libp2p/go-libp2p-crypto v0.1.0
	github.com/libp2p/go-libp2p-kad-dht v0.12.2
	github.com/libp2p/go-libp2p-quic-transport v0.11.2
	github.com/libp2p/go-libp2p-routing-helpers v0.2.3
	github.com/lightstep/otel-launcher-go v0.18.0
	github.com/mitchellh/go-homedir v1.1.0
	github.com/multiformats/go-multiaddr v0.3.3
	github.com/prometheus/client_golang v1.10.0
	github.com/spf13/viper v1.7.1
	github.com/stretchr/testify v1.7.0
	github.com/urfave/cli/v2 v2.3.0
	github.com/whyrusleeping/cbor-gen v0.0.0-20210219115102-f37d292932f2
	github.com/whyrusleeping/go-bs-measure v0.0.0-20210707212153-630d0432b1a7
	github.com/whyrusleeping/memo v0.0.0-20210319212142-d69afb686d15
	go.opentelemetry.io/otel v0.18.0
	go.opentelemetry.io/otel/trace v0.18.0
	golang.org/x/crypto v0.0.0-20210506145944-38f3c27a63bf
	golang.org/x/net v0.0.0-20210428140749-89ef3d95e781
	golang.org/x/sys v0.0.0-20210511113859-b0526f3d8744
	golang.org/x/xerrors v0.0.0-20200804184101-5ec99f83aff1
	gorm.io/driver/postgres v1.0.8
	gorm.io/driver/sqlite v1.1.4
	gorm.io/gorm v1.20.12
)

replace github.com/libp2p/go-libp2p-yamux => github.com/libp2p/go-libp2p-yamux v0.5.1

replace github.com/filecoin-project/lotus => ../lotus

replace github.com/filecoin-project/filecoin-ffi => ../lotus/extern/filecoin-ffi

replace github.com/application-research/estuary => ./
