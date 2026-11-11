# Config wiring-boundary startup differential

Real-binary paired check for the four validations upstream performs while
**wiring services**, not while loading config. Companion to
`tests/oracles/config-observer` (which covers the config-load surface only).

## Why this exists

`cmd/server/main.go:249-305` runs `getConfig` → `ValidateKeys` →
`LoadTURNSecrets` → `NewLocalNode` → `InitializeServer`. The selector, the
WebRTC IP filter and the embedded TURN server are constructed inside
`InitializeServer` (`wire_gen.go:64`, `:148` → `roommanager.go:126`, `:157`), so
a bad `node_selector.kind`, `rtc.ips.includes` CIDR or `turn` port set is a
**startup** refusal, not a config-load refusal. Construction errors are printed
with `fmt.Println(err)` and exit 1 (`main.go:190-192`) — bare upstream text on
stdout, with no `unsupported configuration` wrapper.

`lk_config::validate_server_wiring` mirrors that boundary and
`livecloud::server::run` calls it after the node record is built and before
services are wired, in upstream's construction order.

## Rows

* 5 refusals — `unsupported node selector`, `current region cannot be blank`,
  `unknown lat and lon for the current region`, `invalid TURN ports`,
  `invalid CIDR address: nope`. Asserted: exit code 1 **and** byte-identical
  stdout on both binaries.
* 4 starts — `cpuload`, `sysload`, `random`, `regionaware` with a matching
  region. Asserted: HTTP reachable, survived the grace period, exit 0, no
  SIGKILL, on both binaries.

## Shutdown calibration

The start rows settle 2 s after the port first accepts before sending SIGTERM.
This is measured, not arbitrary. Signalling at the first successful connect
exercises an upstream readiness race rather than startup acceptance:

* `server.go:309` logs `starting LiveKit server`, then `:343` sleeps 100 ms and
  only then `:345` sets `s.running`. The HTTP listener is already accepting.
* `Stop()` early-returns on `!s.running.Swap(false)` (`server.go:390`) without
  closing `doneChan`, so `Start()` blocks on `<-s.doneChan` (`:347`) forever.
* `main.go:313` loops `for i := range 2`, so a **second** signal calls
  `Stop(force=true)` and releases it.

The `early_signal_window` section reproduces this with no settle and records it.
Go's result there is characterization only; the Rust result **is** asserted
(the signal must be caught, logged as `exit requested, shutting down`, and exit
0 — never taken at the default disposition). Rust registers all three handlers
before any listener binds, matching `signal.Notify` at `main.go:310` preceding
`server.Start()` at `:321`; `server::signal_registration_tests` covers the eager
SIGINT registration.

## Environment note

`rtc.tcp_port: 0` is injected into every generated config. The default ICE TCP
port 7881 may be held by an unrelated long-running server on the test host; a
collision there is an environment artifact, not a config-surface result. The
injection merges into an existing `rtc:` block rather than duplicating the key.

## Run

```sh
python3 tests/interop/config-wiring-startup/run.py \
  --official /path/to/livekit-server-go \
  --rust target/debug/livecloud \
  --output /path/to/evidence/dir
```

The output directory must not exist. `results.json` records both binary
SHA-256 identities, `binary_unchanged`, `driven_items`, every row's exit code,
stdout/stderr and full server log, and the early-signal characterization. The
process exit code is 0 only on `PASS`; it is never filtered through a pipe.