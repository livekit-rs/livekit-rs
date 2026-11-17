# cli-list-nodes-cross-binary-registry — paired Go/Rust registry lifecycle (SPEC-0077 C39)

Names the ledger leaf `CLI:list-nodes.CrossBinaryNodeRegistryLifecycle` in every
BATCH.json `items`. Over one literal `127.0.0.1` Redis address (the harness launches
and owns its own loopback `redis-server`, base-port 22100 band, `--save '' --appendonly
no`; the user's Redis is never touched), it proves the cross-process registry lifecycle
between the locked official Go server and the Rust candidate:

1. `go-register-rust-list` — official registers, the Rust `list-nodes` CLI lists exactly
   that id SERVING; graceful stop → both CLIs empty, keyspace empty.
2. `rust-register-go-list` — the reverse direction.
3. `both-live-cross-cli` — one Go + one Rust server: both CLIs see exactly both ids;
   stopping one revokes only its own record in BOTH views.
4. `drain-shutting-down-witness` — Redis MONITOR decodes the exact HSET protobuf bytes
   (sdscatrepr unescape): both sides show HSET(SERVING) … HSET(SHUTTING_DOWN) … HDEL
   in order. The no-participant drain window is milliseconds, so a keyspace poll cannot
   witness it deterministically.
5. `restart-fresh-identity` — same-port stop + fresh start: new id, keyspace exactly the
   new record, no resurrection.
6. `dead-node-pruned-by-peer-start` — SIGKILL residue aged past the 5s availability
   window is HDELed by the PEER binary's server-startup sweep, in both directions; the
   CLI read path itself is shown never to prune.

Connections while running and CLI stat-cell values are recorded per side, never
compared (owned by `RUNTIME:Redis.ServerStartConnectionChurn` /
`RUNTIME:Redis.NodeStatsSampling`). Timing quantities (`drain_seconds`, MONITOR
sequence lengths) are recorded, not compared.

The foundation (wire format, RESP Store/Monitor, process/config/log helpers) is copied
verbatim from `../cli-redis-node-registration-lifecycle/run-paired.py` (c31); only
ITEMS/REGION, the vectors and main differ. The harness hashes both binaries and itself
at start and end and FAILS on any identity change.

## Run

```sh
python3 run-paired.py \
  --official /tmp/parity-oracle/livekit-server-go \
  --rust /Users/zoujiaqing/projects/livekit-rs/target/release/livecloud \
  --output /tmp/c39-run          # must not exist yet
# single vector (gating): add --only <vector-name>  -> PASS_SELECTED_VECTORS_ONLY
```

Requires `redis-server`/`redis-cli` on PATH. Full run expects
`PASS_CLI_LIST_NODES_CROSS_BINARY_REGISTRY_PAIRED`, 6 cases, `mismatches=[]`,
`complete_campaign=true`, exit 0. Acceptance receipts:
livekit-docs `server/evidence/2026-11-17/spec0077-c39-cross-binary-registry/`.