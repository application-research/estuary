# Estuary

> An experimental ipfs node

## Building

Requirements:

- go (1.15 or higher)
- [jq](https://stedolan.github.io/jq/)
- [hwloc](https://www.open-mpi.org/projects/hwloc/)
- opencl

1. Run `make clean all` inside the estuary directory

## Running your own node

To run locally in a 'dev' environment, first run:

```sh
./estuary setup --username=<uname> --password=<pword>
```

Save the auth token that this outputs, you will need it for interacting with
and controlling the node.

NOTE: if you want to use a different database than a sqlite instance stored in your local directory, you will need to configure that with the `--database` flag, like so: `./estuary setup --username=<uname> --password=<pword> --database=XXXXX`

Once you have the setup complete, choose an appropriate directory for estuary to keep its data, and use it as your datadir flag when running estuary.
You will also need to tell estuary where it can access a lotus gateway api, we recommend using:

```sh
export FULLNODE_API_INFO=wss://api.chain.love
```

Then run:

```sh
./estuary --datadir=/path/to/storage --database=IF-YOU-NEED-THIS --logging
```

NOTE: Estuary makes only verified deals by default and this requires the wallet address to have datacap(see https://verify.glif.io/). To make deals without datacap, it will require the wallet to have FIL, and the run command will need the `--verified-deal` option set to `false`.

```sh
./estuary --datadir=/path/to/storage --database=IF-YOU-NEED-THIS --logging --verified-deal=false
```

## Running as daemon with Systemd

The Makefile has a target that will install a generic but workable systemd service for estuary.

Run `make install-estuary-service` on the machine you wish to run estuary on.

Make sure to follow the instructions output by the `make` command as configuration is required before the service can run succesfully.

## Running estuary using docker

- View the guidelines on how to run estuary using docker [here](https://github.com/application-research/estuary-docker).

## Using Estuary

The first thing you'll likely want to do with Estuary is upload content. To upload your first file, use the `/content/add`
endpoint:

```
curl -X POST http://localhost:3004/content/add -H "Authorization: Bearer REPLACE_ME_WITH_API_KEY" -H "Accept: application/json" -H "Content-Type: multipart/form-data" -F "data=@PATH_TO_FILE_BUT_REMEMBER_THE_@_SYMBOL_IS_REQUIRED"
```

You can verify this worked with the `/content/list` endpoint:

```
curl -X GET -H "Authorization: Bearer REPLACE_ME_WITH_API_KEY" http://localhost:3004/content/list
```

You may find the API documentation at [docs.estuary.tech](https://docs.estuary.tech/) useful as you explore Estuary's capabilities.

### Sealing a Deal

Estuary will automatically make a deal with Filecoin miners after 8 hours. If you upload more than 3.57 GiB of data
it will make the deal sooner.

To keep tabs on the status of your uploaded content and Filecoin deals, you can use [estuary-www](https://github.com/application-research/estuary-www).
Clone the `estuary-www` repository and run:

```
npm install
npm run dev
```

And then head to [localhost:4444/staging](http://localhost:4444/staging) to see the status of your deal.


## Contributing
See [`CONTRIBUTING.md`](CONTRIBUTING.md) for contributing and development instructions.

## Troubleshooting
Make sure to install all dependencies as indicated above. Here are a few issues that one can encounter while building estuary

### Guide for: `route ip+net: netlinkrib: too many open files`
#### Error
If you get the following error:

```sh
/ERROR basichost basic/basic_host.go:328 failed to resolve local interface addresses {"error": "route ip+net: netlinkrib: too many open files"}
```

It is because you do not have enough open file handles available.

#### Solution
Update this with the following command:

```sh
ulimit -n 10000
```
### Guide for: Missing `hwloc` on M1 Macs
The Portable Hardware Locality (hwloc) software package provides a portable abstraction of the hierarchical structure of current architectures, including NUMA memory nodes, sockets, shared caches, cores, and simultaneous multi-threading (across OS, versions, architectures, etc.).

`lhwloc` is used by libp2p-core. Estuary uses libp2p for the majority of its features including network communication, pinning, replication and resource manager.

#### Error
```
`ld: library not found for -lhwloc`
```

#### Solution
For M1 Macs, here's the following steps needed
- Step 1: `brew install go bzr jq pkg-config rustup hwloc` - Uninstall rust as it would clash with rustup in case you have installed.
- Step 2: export LIBRARY_PATH=/opt/homebrew/lib
- Step 3: Follow the steps as per the docs.

### Guide for: `cannot find -lfilcrypto collect2`
Related issue [here](https://github.com/application-research/estuary/issues/71)

#### Error
When trying to build estuary in an ARM machine, it returns an error

```
# github.com/filecoin-project/filecoin-ffi/generated /usr/bin/ld: skipping incompatible extern/filecoin-ffi/generated/../libfilcrypto.a when searching for -lfilcrypto /usr/bin/ld: skipping incompatible extern/filecoin-ffi/generated/../libfilcrypto.a when searching for -lfilcrypto /usr/bin/ld: skipping incompatible extern/filecoin-ffi/generated/../libfilcrypto.a when searching for -lfilcrypto /usr/bin/ld: skipping incompatible extern/filecoin-ffi/generated/../libfilcrypto.a when searching for -lfilcrypto /usr/bin/ld: skipping incompatible extern/filecoin-ffi/generated/../libfilcrypto.a when searching for -lfilcrypto /usr/bin/ld: cannot find -lfilcrypto collect2: error: ld returned 1 exit status make: *** [Makefile:67: estuary] Error 2
```

#### Solution
Related solution [here](https://github.com/filecoin-project/lotus/issues/1779#issuecomment-629932097)

```
RUSTFLAGS="-C target-cpu=native -g" FFI_BUILD_FROM_SOURCE=1 make clean deps bench
```
