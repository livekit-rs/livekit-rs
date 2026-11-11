# Owned Sentinel + verified TLS Cluster campaign

Build the current `lk-compat` library test executable under coordinator resource control. Then run:

```sh
/usr/bin/python3 tests/interop/redis-sentinel-cluster-tls/run-owned.py \
  --test-binary /absolute/path/to/target/debug/deps/lk_compat-TEST_EXECUTABLE \
  --output /absolute/path/to/new/evidence/directory
```

The runner never builds or cleans Cargo. Redis server, Redis CLI and OpenSSL can be selected with explicit executable options. It refuses an existing output directory and disabled Python assertions.

It owns two fixtures:

- Master, replica and Sentinel. Before promotion, the test reads a fresh key directly from the owned replica at `LIVEKIT_SENTINEL_TEST_REPLICA_ADDR`. It then checks that Sentinel promoted that replica, waits for the event epoch, proves the command pool migrates on its first read while the retained old socket remains readable, and verifies subscription discontinuity, ACKed restoration and new-master publication.
- Three-master TLS Cluster with numeric dial addresses and a DNS-only leaf certificate. Client certificates are mandatory. The Rust test routes multiple slot keys, forces actual ASK and MOVED responses, verifies successful redirected reads, restores the migrated slot, and rejects wrong name, missing client identity and untrusted CA.

Every test invocation must execute exactly one named ignored test and exit zero. Receipts keep binary, runner, source-observation, actual config and public certificate hashes, exit codes, logs and process cleanup results. Source observations do not prove what built a binary; pair the receipt with the coordinator's source-bound build manifest. A RUNNING/interrupted/partial receipt cannot be PASS. Private keys and actual temporary config directories are removed after only owned child processes have terminated.

This campaign checks the Rust production behavior. Fixed-Go differential and final-source global regression remain separate requirements. A wrong-name seed failure alone does not prove negative validation of a differently certified discovered/redirected peer; retain that additional adversarial fixture in the coverage ledger until it is run.

The additional adversarial campaign uses a separately named output directory:

```sh
/usr/bin/python3 tests/interop/redis-sentinel-cluster-tls/run-bad-peer.py \
  --test-binary /absolute/path/to/target/debug/deps/lk_compat-TEST_EXECUTABLE \
  --output /absolute/path/to/new/bad-peer-evidence/directory
```

It gives node 1 a certificate signed by the same trusted CA but containing only `wrong-peer.fixture.test`. Fixture administration uses that actual verified name. The application retains `redis.fixture.test` and must reject a discovered-node read and actual ASK/MOVED targets, while a source-owned key and all fixture admin connections stay usable. The runner configures the cluster through individually verified connections instead of using `redis-cli --cluster create`, whose one SNI cannot verify both different server names. It also repeats the Sentinel campaign; each receipt still requires exactly its two named tests. Neither new campaign has been executed by the implementation subagent.
