# Estuary Regression Tests

Collection of shell scripts to perform several estuary API regression tests

## Before running
Make sure data/config/run.config is setup with the right keys.

## Running

From the estuary root folder
```shell
make regression-tests
```

From this folder
```shell
./run-regression-tests.sh
```

## Generated Reports
All test runs log output goes to the data/logs folder.