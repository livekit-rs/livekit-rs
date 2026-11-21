# interrupt-before-running — paired SIGINT-in-startup-window sweep

`run-paired.py` is the named harness for SPEC-0077 ledger item **[1146]
`RUNTIME:Server.InterruptBeforeRunningRace`**. It launches the **real MAIN** of the locked Go
oracle and of the Rust candidate against a private `redis-server`, sends `SIGINT` six times at
fixed offsets into upstream's own startup window, and records what each side does. The certifying
receipt is `BATCH.json` in `--output`.

The two sides are **not** compared for equality. The candidate not reproducing the stall is the
finding, and whether to mirror an upstream hang is a product decision, not a compatibility one.

## The window being probed (locked Go, `pkg/service/server.go`)

| line | statement | why it matters here |
|---|---|---|
| `:222` | `router.RegisterNode()` | the own-node `HSET nodes` that a stalled run must leave behind |
| `:248-270` | `net.Listen` for http/prom/debug | the port is bound before anything flips a start flag |
| `:309` | `logger.Infow("starting LiveKit server")` | the harness anchor, 34 statements before the sleep |
| `:326-332` | `httpGroup.Go(httpServer.Serve)` | requests are accepted during the window |
| `:343` | `time.Sleep(100 * time.Millisecond)` | the window |
| `:345` | `s.running.Store(true)` | the flag `Stop()` gate-keeps on |
| `:347` | `<-s.doneChan` | where a stalled `Start()` waits forever |
| `:380` | `router.Drain()` | runs even on a too-early stop |
| `:390-392` | `if !s.running.Swap(false) { return }` | **the early return** |
| `:394-395` | `router.Stop()` / `close(s.doneChan)` | skipped, so the `HDEL` and the unblock never happen |

`healthCheck` (`:451-464`) answers 406 until the node stats are <4s fresh, so **HTTP 200 is not a
usable anchor**: it can arrive ~1s after the flag was already set, which would make every
"inside the window" vector actually measure the outside. Both binaries print the readiness line, so
the delay is timed from it and the *achieved* offset is what gets recorded and classified.

The candidate has no equivalent gate: `Signals::new()` registers SIGINT/SIGTERM/SIGQUIT before any
listener accepts
([server.rs:705](file:///Users/zoujiaqing/projects/livekit-rs/crates/livecloud/src/server.rs#L705),
after the node `HSET` at
[server.rs:712](file:///Users/zoujiaqing/projects/livekit-rs/crates/livecloud/src/server.rs#L712)
and before the readiness line at
[server.rs:812](file:///Users/zoujiaqing/projects/livekit-rs/crates/livecloud/src/server.rs#L812)),
and the shutdown read is the last await of `run()`
([server.rs:965](file:///Users/zoujiaqing/projects/livekit-rs/crates/livecloud/src/server.rs#L965)),
so there is no "start completed" flag for an early signal to be rejected by.

## Delays and per-side assertions

Six offsets × both binaries, each vector starting from an empty `nodes` hash and identical config
bytes (ports excepted):

| offset | upstream state at `SIGINT` | asserted, official | asserted, candidate |
|---|---|---|---|
| 0.00 / 0.03 / 0.08 s | inside the `:343` sleep, `running == false` | stalls to the 20s cap, `exit == -9`, ≥1 own `nodes` record left behind, **and still answers `GET /` while hung** | never stalls |
| 0.12 / 0.20 / 0.50 s | flag set at `:345` | exits 0 within 1s, `nodes` entry removed | exits 0 within 1s, `nodes` entry removed |

Common to every vector, both sides: the process did not die before the interrupt
(`exited_early == false`), exactly one `nodes` field — its own — was present **at the anchor**
(`own_entries_while_running == 1`, sampled before the signal so "left behind"/"removed" cannot be
trivially true about a hash that was never written), and whatever is still in the hash after the
process is gone is a subset of the field names registered during that same vector. The candidate
must additionally log `exit requested, shutting down` at every offset. Aggregate coverage asserts
that at least one official vector stalled (and only inside-window ones) and at least one exited
gracefully (and only outside-window ones), keyed on the achieved offsets.

## Falsifiability

- **Classifier control**: a `python3 -c` child that sets `signal.signal(SIGINT, SIG_IGN)` must be
  classified `sigkilled` with `exit == -9`, `stop_seconds >= cap - 0.5` and **zero** shutdown lines.
  A stop path that silently never waits therefore cannot produce a green sweep, and `sigkilled` is a
  measurement rather than a default.
- **Timing gate**: an inside-window vector whose achieved offset drifted to ≥0.10s fails with
  "re-run rather than re-label". A vector that never measured the window is not allowed to be
  counted as a graceful reference.
- Every claim above is an absolute per-side assertion; deleting one removes evidence rather than
  turning a failure green.

## Component tests

`mod signal_registration_tests` in
[crates/livecloud/src/server.rs](file:///Users/zoujiaqing/projects/livekit-rs/crates/livecloud/src/server.rs#L1713)
holds the same-window behaviour at unit level: `sigint_is_registered_before_the_first_poll`,
`sigterm_and_sigquit_raised_before_the_first_poll_are_also_captured` and
`interrupt_during_the_startup_window_still_reaches_the_shutdown_wait` (raise, then 100ms of startup
work, then the shutdown read must still return `interrupt` instead of blocking). They serialize on
a module `Mutex` because `libc::raise` is process-wide and tokio's signal registry broadcasts one
arrival to every listener of that kind, so concurrent tests would otherwise steal each other's
signal; the awaited reads carry timeouts so a lost signal fails instead of hanging the suite.

## Run

```sh
cargo build --release --bin livecloud
cd references/livekit-stable && GOTOOLCHAIN=go1.26.0 GOPROXY=off GOWORK=off \
  go build -mod=readonly -buildvcs=false -o /tmp/ibr-build/livekit-server ./cmd/server && cd -

python3 tests/interop/interrupt-before-running/run-paired.py \
  --official /tmp/ibr-build/livekit-server \
  --rust     target/release/livecloud \
  --output   /tmp/ibr-build/acceptance \
  --base-port 21900
```

The harness pins the oracle identity (`livekit-server version 1.13.10`, sha256
`4bc2378e636740b29093061036249ffc9ebbef91f4ece1de12177c8dfee1e7dc`) and refuses to start on a busy
port. Exit code is 0 only when all six vectors and the control pass; `BATCH.json` is written
incrementally, so a FAIL keeps the vectors measured up to that point.

## Not covered here

- The graceful-stop surfaces of the start-path items themselves — the other `cli-redis-*`
  harnesses settle 0.5s before interrupting for exactly this reason.
- Second-signal *force* semantics (`RUNTIME:Server.ForceShutdownOnSecondSignal`) and the
  agent-tunnel drain (`server.go:350`, `:402-416`).
- Windows/colima signal delivery, and any host other than the one recorded in the receipt: the
  offsets are wall-clock timings, so the achieved values in `BATCH.json` are part of the claim.
- Ledger level closure. A PASS receipt is what upgrades
  `official_differential_lifecycle` for [1146]; until it is bound to the evidence slot that level
  stays PARTIAL, and `accepted_scope` keeps the item open by design — it exists so a future
  signal-handling change is written against **both** behaviours and so the `-9` recorded here is
  never read as an unexplained parity failure.
