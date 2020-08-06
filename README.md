# Testground test plans for ipfs-restore

This repo is based off of [testground test plans from ipfs](https://github.com/ipfs/test-plans/tree/master/bitswap-tuning) and contains test plans for ipfs, reed-solomon and alpha entanglement codes stored on Bitswap nodes. To run a test case, use

```bash
testground run single --plan=bitswap-restore --testcase=ipfs --builder=exec:go --runner=local:exec
```

## Test Cases

There are currently three test cases available:
* ipfs
* reed-solomon
* entanglement

Some test parameters applicable to all the test cases are:
* `unavailable_count` - the number nodes that other nodes can not reach / with data loss. Default is 0
* `file_size` - the size of the file in bytes. The default is 4194304
* `timeout_secs` - time out until test records failure in seconds. Default is 300 seconds

To set the params, use the `test-params` flag like so:

```bash
testground run single --plan=bitswap-restore --testcase=reed-solomon --test-param unavailable_count=1 --instances=5 --builder=docker:go --runner=local:docker
```

### IPFS Test Case

Some test params specific to the ipfs test case is:
* `replication_factor` - the number of times the whole file is duplicated. This broken into `unixfs` data blocks distributed amoung instances. Default is full replication will all node containing a full copy of all data blocks
