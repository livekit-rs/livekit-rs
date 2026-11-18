# C41 — `list-nodes` message-bus second Redis connection (paired)

Closes the ledger leaf **`list-nodes.PSRPCMessageBusSecondRedisConnection`**
(SPEC-0076 workline B). This is a NEW, independent harness; it does not modify the
historical `../cli-list-nodes-redis-tls/tap-connections.py`, which recorded the
pre-C41 divergence (official 2 connections, Rust 1) and is kept as the historical
record.

## What the official binary does

`list-nodes` → `service.InitializeRouter` → `getMessageBus(rc)` → `redisbus.New(rc)`
(psrpc v0.8.2 `redisbus.go:63-83`):

* `redisbus.New` calls `ps: rc.Subscribe(ctx)` with **no channels** (`redisbus.go:68`)
  and spawns `go r.readWorker()` (`redisbus.go:80`).
* `readWorker`'s first `r.ps.ReceiveMessage(ctx)` (`redisbus.go:152-155`) goes through
  go-redis `PubSub.conn` → `newConn` → the SAME `initConn` handshake the command pool
  uses, eagerly dialing a **dedicated pubsub connection** — the official binary's
  SECOND Redis connection for one successful run.
* `list-nodes` runs `InitializeRouter` + `ListNodes` (HVALS) but **never**
  `router.Start()`, so the keepalive worker's `SubscribePing` never runs and **no
  channel is ever subscribed**. The read connection carries only the `initConn`
  handshake (`hello` / `client maint_notifications` / `client setinfo LIB-NAME` /
  `client setinfo LIB-VER`), never a command and never a `subscribe`.

> Narrative correction (C41): the leaf and the C08 receipt previously attributed the
> second connection to "upstream's psrpc keepalive subscriber". The source shows it
> is the message-bus `readWorker`'s eager **empty**-pubsub dial, not a keepalive
> `SUBSCRIBE`. The keepalive subscribe only happens on `router.Start()`, which the
> read-only CLI never calls. The closure asserts the second connection is init-only
> and command/subscribe-free, which is the faithful contract.

## What the Rust port now does

`crates/livecloud/src/cli_redis_retry.rs::spawn_message_bus` spawns a detached
message-bus read worker that dials the second connection through
`Session::open_message_bus` (reusing the exact `initConn` handshake) and then blocks
in `Wire::read_push` receiving pushes. A `oneshot` signal lets `read_nodes` await the
dial before HVALS, so the connection is established before the node table renders and
the process exits — deterministic on the CLI's current-thread runtime. The worker is
aborted at runtime drop (process exit), the same cutoff that ends the official
goroutine. A failed bus dial is logged-and-dropped, never propagated, exactly as
`readWorker`'s retry loop is independent of `ListNodes`.

## What this harness proves (paired, same owned Redis, both binaries hashed)

Two profiles, N runs per side (default 3):

* **plaintext** — a RESP tap records every client write on every connection. Both
  connections establish deterministically, so the downstream tap count is authoritative.
* **TLS** — a TLS-terminating relay forwards the decrypted stream to a second RESP
  tap fronting the same plain Redis. The **authoritative TLS connection count is the
  relay's TLS-attempt count** (the same layer `../cli-list-nodes-redis-tls/run-paired.py`
  compares), not the downstream established count: the official bus connection's
  handshake is reset by process exit before it forwards, so the downstream tap sees
  only the command connection on the official side. The relay still records both TLS
  attempts (`tls_records == 2`, `plaintext_records == 0`).

Per run, both sides must satisfy:

1. **connection count == 2** — plaintext at the downstream tap; TLS at the relay
   attempt layer — and cross-side parity `2 == 2` on every run;
2. exactly **one command connection** (runs `ping`/`hvals`), deterministic on both
   sides and profiles; its **full RESP trace is byte-identical** across sides;
3. the bus connection is **init-only**: opens with `hello`, every command is a
   **prefix** of the command connection's init handshake, and it never sends
   `ping` / `hvals` / `subscribe`.

The official bus connection is cut at a **racy** point during process exit, so its
trace is asserted as a prefix, not an exact equality, and on TLS it may be absent
downstream (the relay confirms the attempt). The Rust bus connection completes the
full init deterministically (the `oneshot` await) and must establish downstream on
both profiles. These timing artifacts are recorded per run (`tls_outcomes`,
`official_bus_cut_runs`), not hidden.

### Two real gaps surfaced and fixed by this wire-level comparison

* **`endpoint_type` TLS suffix** (`crates/livecloud/src/cli_redis_session.rs`): the
  `CLIENT MAINT_NOTIFICATIONS ON moving-endpoint-type` suffix was hardcoded to `-ip`.
  go-redis `options.go:581` calls `DetectEndpointType(opt.Addr, opt.TLSConfig != nil)`
  and `maintnotifications/config.go:418-480` yields `internal-fqdn` under TLS (FQDN
  preserved for certificate validation) vs `internal-ip` without. The fix passes
  `self.pool.options.tls.is_some()`. This is a **wire-only** divergence — invisible to
  any stdout differential — pinned by
  `cli_redis_session_tests.rs::endpoint_type_suffix_follows_tls_like_go_redis_detect`
  and by the TLS command-trace equality here (`internal-fqdn` on both sides).
* **RESP chunk-splitting in the tap**: `conn_trace` originally parsed each `recv()`
  chunk independently, so a command split across two chunks was dropped (the tail
  chunk no longer starts with `*`). This made the official bus trace spuriously lose
  a mid-stream command and fail the prefix check. `conn_trace` now parses the
  concatenated client-write stream as one continuous RESP sequence, and
  `command_traces` cleanly drops a trailing frame cut mid-header/mid-bulk by process
  exit. Stress-tested at 6 runs/side with zero spurious failures.

## Falsifiability (positive controls)

Built in an isolated `CARGO_TARGET_DIR` so the accepted candidate identity is
preserved; source edits reverted after each run. Recorded in `MUTANTS.json` with
each mutant's binary hash and FAIL receipt:

* **mutant-a-count** — drop the bus dial (`spawn_message_bus` call removed) → Rust
  dials 1 → `expected 2 Redis connections, 1` → FAIL (exit 1);
* **mutant-b-init-only** — bus connection sends `ping` → two command connections →
  `expected exactly one command connection, 2` → FAIL (exit 1).

A `subscribe` on the bus connection would likewise trip the command/subscribe-free
assertion (`FORBIDDEN_BUS_TOKENS`). The component test
`cli_redis_retry_tests.rs::message_bus_read_connection_is_init_only` proves the same
contract at the unit level against a mock Unix Redis peer.

## Scope / residual

Covered: the **simple-client** `list-nodes` surface (plaintext + TLS), which is where
the C08 divergence was measured. **Not covered:** sentinel/cluster `list-nodes`,
which dial through the redis-rs endpoint path (`read_nodes_endpoint`), a separate
consumer whose message-bus wiring remains open. The exact cut point of the official
bus connection during process exit is racy and is asserted as a prefix, not equality.

## Run

```
python3 tap-message-bus.py \
  --official /tmp/parity-oracle/livekit-server-go \
  --rust /abs/target/release/livekit-server \
  --base-port 21749 --output DIR [--runs 3]
```

Starts/stops its own loopback `redis-server`, generates throwaway openssl certs in a
scratch temp dir (never committed), hashes both binaries, and writes `BATCH.json`.
Exit code is preserved (0 = PASS, 1 = FAIL); no pipe masks it.