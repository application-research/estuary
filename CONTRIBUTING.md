# Contributing to Estuary

This document aims to help you get started with running a local Estuary Service for development.

Estuary welcomes contributions to our [open source projects on Github](https://github.com/application-research).

Please refer to each project's style and contribution guidelines for submitting patches and additions. In general, we follow the "fork-and-pull" Git workflow.

 1. **Fork** the repo on GitHub
 2. **Clone** the project to your own machine
 3. **Commit** changes to your own branch
 4. **Push** your work back up to your fork
 5. Submit a **Pull request** so that we can review your changes

NOTE: Be sure to merge the latest from "upstream" before making a pull request!
## Issues

Feel free to submit issues and enhancement requests.

Please use [Estuary Support](https://docs.estuary.tech/feedback) to report specific bugs and errors.

## Development

### Initialize & Start Estuary

First, clone Estuary and build it with all the required dependencies
```bash
git clone https://github.com/application-research/estuary.git
cd estuary
make clean all
```

Then run `./estuary setup --username=<uname> --password=<pword>` to initialize the database as well as the access token. **Make sure to save this token as it'll be used to do almost all API calls**.
```bash
$ ./estuary setup --username=<uname> --password=<pword>
Auth Token: <your_auth_token>
```

Next, set the `FULLNODE_API_INFO` environment variable to a synced lotus node. This can either be a local Lotus node, or a public endpoint, e.g. `wss://api.chain.love`
```bash
$ export FULLNODE_API_INFO=wss://api.chain.love
```

Start your Estuary Node (Note: if you see error messages like `too many open files`, increase the number of open files allow `ulimit -n 10000`)
```bash
$ ./estuary --logging
```

if the node is started successfully, you will receive the following output:
```
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

Build the shuttle binary
```bash
$ make estuary-shuttle
```

Then, initialize a shuttle node. You can do this by using the admin UI at `<host>/admin/shuttle`, by posting to the Estuary API endpoint `/admin/shuttle/init` 
```bash
$ curl -H "Authorization: Bearer REPLACE_ME_WITH_API_KEY" -X POST localhost:3004/admin/shuttle/init
{"handle":"<your_handle_printed_here>", "token":"<your_auth_token_printed_here>"}
```

or by using the CLI 
```bash 
$ ./estuary shuttle-init
{"handle":"<your_handle_printed_here>", "token":"<your_auth_token_printed_here>"}
```

Using the output from the above command, start a shuttle node in development mode: 
```bash
$ ./estuary-shuttle --dev --estuary-api=localhost:3004 --auth-token=AUTH_TOKEN --handle=SHUTTLE_HANDLE --logging --host=localhost:3005
```
output
```
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

NB: The above commands can be repeated to create and run more shuttle nodes. 
To view a list of shuttle nodes that have already been initialized, make a GET request to the `admin/shuttle/list` API endpoint 
```bash
$ curl -H "Authorization: Bearer AUTH" -X GET localhost:3004/admin/shuttle/list 
```

### API

At this point you may begin adding files to estuary via the API described in the [documentation](https://docs.estuary.tech/api-content-add), or using the golang client [creek](https://github.com/iand/creek).

## Copyright and Licensing
-----------------------

Most Estuary open source projects are licensed under the [Apache 2.0 license](http://www.apache.org/licenses/LICENSE-2.0).

To view the license, go [here](LICENSE.md)

## Building

Estuary is an open source experimental IPFS node and is open for anyone to propose changes and modify. 

### API Changes
We use swagger to document all of our endpoints and we require developers to annotate the function that complies with the echo-swagger specification. 

- [echo-swagger](https://github.com/swaggo/echo-swagger)
- Add comments to your API source code, See [Declarative Comments Format](https://github.com/swaggo/swag#declarative-comments-format).
