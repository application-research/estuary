# Estuary Chaos Tests

Collection of shell scripts to perform several estuary API chaos tests

Here are the available test scripts
- Adding large content on a list of shuttles.
- Downloading large content from different IPFS gateways.

## Before running
Make sure data/config/run.config is setup with the right keys.

## Running

From the estuary root folder
```shell
make chaos-tests
```

From this folder
```shell
./run-chaos-tests.sh
```

Run specific Test case
```shell
./run-chaos-tests.sh download-large-files
```

## Generated Reports
All test runs log output goes to the data/logs folder.