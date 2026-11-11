# Redis Cluster hard-stop command control

This control uses the fixed LiveKit Go Redis constructor and the Rust
Standalone Cluster endpoint. It separates server-side command routing
from the original worker and PSRPC paths. The original worker campaigns
exercise the complete public API and media lifecycle separately.

Each profile starts its own three-primary, three-replica plaintext
Cluster. The driver writes four fixture fields and waits for a replica
acknowledgement on the same connection. It warms both command clients,
kills the primary of the exact service key, and waits for automatic
election and slot agreement on every survivor. The already-warm clients
then issue the command well before Go's default 60-second slot-state
refresh interval. Fresh clients on the survivors provide a positive
control. For cleanup, the driver also reads the actual field state on
the promoted node.

| Profile | Command and key | Slot |
| --- | --- | --- |
| Egress metadata | `HGET egress` | 11881 |
| Ingress metadata | `HGET ingress` | 13597 |
| Room cleanup | `HDEL rooms` | 4321 |

The driver accepts a successful command or a refused connection to the
stopped primary as an observed warm-client outcome, and records whether
the outcomes matched. Fresh commands, real state, process exits and port
release are hard assertions.

Run from the repository root with a fresh output directory. Check that
the Go checkout is v1.13.10 at `e5655ac20267fa5005dccaf3f0e3bfc3ea688c07`.

```sh
root="$PWD"
out=/absolute/path/to/new-fixture-root
(
  cd references/livekit-stable
  GOTOOLCHAIN=go1.26.0 go build -mod=readonly -o "$out/go-observer" \
    "$root/livekit-rs-server/tests/interop/redis-hard-stop/go-observer.go"
)
(
  cd livekit-rs-server
  CARGO_TARGET_DIR="$out/target" cargo build --locked -p lk-compat \
    --example redis_cluster_hard_stop_observer
)
python3 livekit-rs-server/tests/interop/redis-hard-stop/run.py \
  --go "$out/go-observer" --rust "$out/target/debug/examples/redis_cluster_hard_stop_observer" \
  --output "$out/results"
```
