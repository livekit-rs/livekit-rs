# Empty-master Redis Cluster wire faults

This test passes two `redis.sentinel_addresses` and an empty
`redis.sentinel_master_name` through the official Go and Rust configuration
entry points. The client discovers a third slot owner from `CLUSTER SLOTS`.
The fixture speaks a controlled subset of Redis over real loopback TCP and TLS.
The separate three-primary Redis tests in this directory cover the live Redis
topology.

| Profile | Failure or positive control |
| --- | --- |
| `write_short` | A constrained receive window and withheld reads make the 16 MiB SET physically partial. The selected peer must see payload bytes followed by client EOF without completing the SET. A new SET and EXISTS on the same slot verify recovery. |
| `write_long` | After a bounded pause, the peer drains and validates the full SET. The client receives OK and reads the same key with EXISTS. |
| `tls_short` | The discovered peer consumes an actual TLS ClientHello, sends no handshake response and observes client EOF on the configured short dial budget. |
| `tls_long` | The same discovered-peer route can negotiate TLS after a delay exceeding the short dial budget. The client uses the fixture CA and localhost name. The peer observes localhost SNI and serves SET and EXISTS. |

The short write profile disables the read timer. Every profile checks fixture
socket retirement and test-process exit. No listener uses a preassigned port.
The fixture creates no Redis processes and changes no system trust settings.

From the server source, compile the Rust test with an owned Cargo target:

```sh
cargo test --offline --locked -p livecloud --lib redis_tls::tests:: --no-run
python3 tests/interop/redis-scoped/run-cluster-wire-fault.py \
  --kind rust --binary /absolute/path/to/livecloud-test-binary --output /tmp/new-rust-receipt
```

Run the Go control from the pinned LiveKit source with its pinned Go
toolchain already on PATH:

```sh
GOTOOLCHAIN=local GOPROXY=off go build -mod=readonly \
  -o /tmp/go-cluster-wire-fault /absolute/path/to/original-go-cluster-wire-fault.go
python3 /absolute/path/to/run-cluster-wire-fault.py \
  --kind go --binary /tmp/go-cluster-wire-fault --output /tmp/new-go-receipt
```

Use a fresh output directory for each execution. Each receipt records the
profile arguments, exact test binary digest, exit status and wire observations.
