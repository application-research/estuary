# Running Benchest

To run benchest, you must compile it directly:

```sh
make benchest
```

You also need to have your `ESTUARY_TOKEN` set as an environment variable.

```sh
# Do not use token below, it is fake
ESTUARY_TOKEN=EST30386b0b-a0b4-4220-8a28-02d5b0d9ff1aARY
```

If you  get an error like the following, please use the recommended host with the `--host xxx` flag.

```sh
error body:  map[details:this estuary instance has disabled adding new content, please redirect your request to one of the following endpoints: [xxx, yyy] error:ERR_CONTENT_ADDING_DISABLED]
```
