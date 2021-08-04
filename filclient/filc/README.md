# filc

filc is a simple command line interface for `filclient`. It's great for basic storage/retrieval deals, checking wallet balance, and testing.

## Setup

### Lotus Connection
filc currently needs a connection to a synced Lotus node to function. By default, filc will attempt to connect to a node hosted at `localhost`. If you don't have a self-hosted Lotus node, an alternative address can be specified by setting the environment variable `FULLNODE_API_INFO` (you'll probably want `FULLNODE_API_INFO=wss://api.chain.love`).

### Wallet Setup
Currently, filc will automatically generate a wallet address for you on first run. If you already have a wallet you'd like to use, you can grab the corresponding file starting with `O5` from your existing wallet folder (e.g. `~/.lotus/keystore/`) and place it into `~/.filc/wallet/`.