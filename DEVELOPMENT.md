# Getting Started Developing With Estuary

This document aims to help you get started with running a local Estuary Service for development.

## Estuary Node

### Initialize & Start Estuary

First, run `estuary setup` and record the token. This token is required when communicating with the Estuary API endpoint.

```bash
$ ./estuary setup
Auth Token: ESTb43c2f9c-9832-498a-8300-35d9c4b8c16eARY
```

Next, set the `FULLNODE_API_INFO` environment variable to a synced lotus node. This can either be a local Lotus node, or a public endpoint, e.g. `wss://api.chain.love`

```bash
$ export FULLNODE_API_INFO=wss://api.chain.love
```

Start your Estuary Node (Note: if you see error messages like `too many open files`, increase the number of open files allow `ulimit -n 10000`)

```bash
$ ./estuary
Wallet address is:  <your_estuary_address_printed_here>
2021-09-16T13:32:48.463-0700    INFO    dt-impl impl/impl.go:145        start data-transfer module
/ip4/192.168.1.235/tcp/6744/p2p/12D3KooWEb5wbWf3KcExdLUMp4x9NqzyaMcBaYbxZ9V6RUNwvpX8
/ip4/127.0.0.1/tcp/6744/p2p/12D3KooWEb5wbWf3KcExdLUMp4x9NqzyaMcBaYbxZ9V6RUNwvpX8
2021-09-16T13:32:48.464-0700    INFO    estuary estuary/replication.go:687      queueing all content for checking: 0

   ____    __
  / __/___/ /  ___
 / _// __/ _ \/ _ \
/___/\__/_//_/\___/ v4.5.0
High performance, minimalist Go web framework
https://echo.labstack.com
____________________________________O/_______
                                    O\
⇨ http server started on [::]:3004
```

### Initialize & Start a Shuttle

Estuary stores data on IPFS before replicating it to the Filecoin Network. When you store data using Estuary, that data will first go to an `estuary-shuttle` *node* that utilizes IPFS as *hot-storage* before replication to the Filecoin Network begins.

First, initialize a shuttle node using the Estuary API endpoint `/admin/shuttle/init`

```bash
$ curl -H "Authorization: Bearer ESTb43c2f9c-9832-498a-8300-35d9c4b8c16eARY" -X POST localhost:3004/admin/shuttle/init
{"handle":"SHUTTLE4e8b1770-326c-4c95-9976-7cc1ee12244bHANDLE","token":"SECRET7528ab25-1266-4fa4-86cf-719a43bbcb4fSECRET"}
```

Using the output from the above command, start a shuttle node in development mode

```bash
$ ./estuary-shuttle --dev --estuary-api=localhost:3004 --auth-token=SECRET7528ab25-1266-4fa4-86cf-719a43bbcb4fSECRET --handle=SHUTTLE4e8b1770-326c-4c95-9976-7cc1ee12244bHANDLE
Wallet address is:  <your_estuary-shuttle_address_printed_here>
2021-09-16T14:47:54.353-0700    INFO    dt-impl impl/impl.go:145        start data-transfer module
2021-09-16T14:47:54.416-0700    INFO    shuttle estuary-shuttle/main.go:1060    refreshing 0 pins

   ____    __
  / __/___/ /  ___
 / _// __/ _ \/ _ \
/___/\__/_//_/\___/ v4.5.0
High performance, minimalist Go web framework
https://echo.labstack.com
____________________________________O/_______
                                    O\
⇨ http server started on [::]:3005
2021-09-16T14:47:54.416-0700    INFO    shuttle estuary-shuttle/rpc.go:54       sending rpc message: ShuttleUpdate
2021-09-16T14:47:54.417-0700    INFO    shuttle estuary-shuttle/main.go:466     connecting to primary estuary node
2021-09-16T14:47:54.417-0700    INFO    shuttle estuary-shuttle/main.go:519     sending hello   {"hostname": "", "address": "<your_estuary-shuttle_address_printed_here>", "pid": "12D3KooWHag4gY8fQkjQ8Rgs5v6Fb4TpP3LXm8xvhVX3GsVKNwUW"}

```

The above commands can be repeated to create more shuttle nodes.

### API

At this point you may begin adding files to estuary via the API described in the [documentation](https://docs.estuary.tech/api-content-add), or using the golang client [creek](https://github.com/iand/creek).

## Prometheus opencensus flags

The following flags can be set to enable the prometheus views
- `ENABLE_RCMGR_BLOCK_PROTO_PEER_VIEW` enable this only if it's required. Setting this to true will enable the `RcmgrBlockProtoPeerView` which can affect the prometheus performance. To enable set this to `true`.