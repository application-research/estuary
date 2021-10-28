# auto retrieve

Serve filecoin data to bitswap clients using retrieval info queried from Estuary.

## setup

Pass `--datadir` or set `ESTUARY_AR_DATADIR` to set estuary auto retrieve's data directory.

To blacklist miners for auto retrieve downloads, create `blacklist.txt` in the data directory and populate it with a newline-separated listed of miner strings:

    f01000
    f02000
    [...]