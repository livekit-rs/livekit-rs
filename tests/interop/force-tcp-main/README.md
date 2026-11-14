# C13 — `rtc.force_tcp` launched-MAIN consumer (paired Go / Rust)

SPEC-0077 workline C13. Proves the **launched MAIN server** consumes `rtc.force_tcp`,
not merely that the YAML/flag parses. Closes three ledger items together:

- `YAML:rtc.force_tcp`
- `generated_flags:--rtc.force_tcp`
- `CONFIG_ENABLED_CAPABILITY:rtc.force_tcp`

The `force-tcp-cli-flag` vector drives `force_tcp` through the argv flag `--rtc.force_tcp=true`
on a config that **omits** the YAML leaf, so `generated_flags:--rtc.force_tcp` is proven to
reach the launched MAIN on the **same candidate** — no cross-candidate stitching with the
earlier C10 flag→leaf projection proof.

## What is asserted

`force_tcp` forces TCP-only ICE media: the server binds a passive ICE-TCP listener on
`rtc.tcp_port` and binds **no** UDP media socket, even when `rtc.udp_port` is set. With
`force_tcp` on and `tcp_port` unset/0 the server refuses to start.

Upstream oracle (locked v1.13.10, `e5655ac2`):

- `mediatransportutil/pkg/rtcconfig/webrtc_config.go:123` skips UDP network types and the
  UDP mux when `ForceTCP`; `:180` adds TCP only when `TCPPort != 0`; `:201-202` returns
  `errors.New("TCP is forced but not configured")` when no network type remains.
- `livekit-server/pkg/service/roommanager.go:126-128` returns that error **unwrapped**;
  `cmd/server/main.go:190-192` prints `fmt.Println(err)` to **stdout** and exits 1.
- `livekit-server/pkg/service/server.go:287-296` logs `rtc.portTCP` (when `TCPPort!=0`),
  then `rtc.portUDP={Start,End}` when `!ForceTCP && UDPPort.Valid()`, else
  `rtc.portICERange=[start,end]`.

Rust consumer (production):

- `crates/livecloud/src/server.rs:332-335` — `force_tcp && tcp_port==0` →
  `ForcedTcpWithoutPort` (`server.rs:34`, display text byte-identical to upstream), printed
  by `crates/livecloud/src/lib.rs:193` `println!("{e}")` to stdout, exit 1.
- `crates/livecloud/src/interfaces.rs:343` — `udp_enabled = !Disabled && !force_tcp`.
- `crates/livecloud/src/interfaces.rs:267,275` — UDP mux ports / standalone STUN port dropped
  under `force_tcp`.
- `crates/rtc-runtime/src/net/mod.rs:363,396` — UDP muxes bound only when `udp_enabled`; a TCP
  wildcard listener bound only when `tcp_port` is `Some`.
- `crates/livecloud/src/server.rs:1154-1180` — startup-log port keys, field-for-field through
  the zap console encoder (`crates/livecloud/src/zap.rs`).

## Observables (each compared Go-vs-Rust on identical config bytes / argv / ENV)

1. **Startup refusal** — the exact stdout sentence `TCP is forced but not configured`, exit
   code 1, HTTP never ready. Full-line byte parity.
2. **Socket topology** — `lsof` scoped to the owned MAIN PID: is the configured TCP media port
   LISTENing, is the configured UDP media port bound. `force_tcp` must keep TCP and drop UDP.
3. **Startup-log port keys** — `rtc.portTCP` / `rtc.portUDP` / `rtc.portICERange` parsed from
   the zap `starting LiveKit server` context object.
4. **Selected ICE candidate pair** — `probe.mjs` connects the locked official browser SDK
   (livekit-client 2.23.0), publishes a synthetic oscillator audio track, and reads the
   *selected* candidate pair from `RTCPeerConnection.getStats()`: local `protocol` (udp/tcp)
   and remote `port`. This is the negotiated media transport, not a parse of offered
   candidates. Run on the three vectors that pin the negotiated protocol:
   `force-tcp-with-port` and `force-tcp-cli-flag` (→ tcp) and `default-udp` (→ udp).

## Vectors

| id | config | expect |
|----|--------|--------|
| `force-tcp-no-port` | `force_tcp:true, tcp_port:0, udp_port:U` | startup refusal sentence, exit 1, no HTTP |
| `force-tcp-with-port` | `force_tcp:true, tcp_port:T, udp_port:U` | TCP-only listeners, `portTCP`+`portICERange`, browser selects **tcp**/T |
| `force-tcp-cli-flag` | config omits `force_tcp` (default false), argv `--rtc.force_tcp=true`, `tcp_port:T, udp_port:U` | TCP-only listeners, `portTCP`+`portICERange`, browser selects **tcp**/T — driven by the **CLI flag**, proving `generated_flags:--rtc.force_tcp` reaches MAIN |
| `default-udp` | `force_tcp:false, tcp_port:0, udp_port:U` | UDP listener, `portUDP`, browser selects **udp**/U |
| `force-tcp-false-with-tcp-port` | `force_tcp:false, tcp_port:T, udp_port:U` | **both** listeners, `portTCP`+`portUDP` |

Each side must independently satisfy the vector contract **and** match the other side's
comparable shape; equal-but-wrong does not pass. Running vectors must also exit cleanly on the
first SIGINT.

## Falsifiability (positive control)

The operator mutates the force_tcp UDP-drop consumer
(`interfaces.rs:343` drop `&& !rtc.force_tcp`; `interfaces.rs:267` drop `rtc.force_tcp ||`),
rebuilds, and re-runs `force-tcp-with-port`: the UDP media port then binds and the browser
selects UDP, so the vector FAILS. The source is restored and the clean candidate re-run PASSes.
This proves the listener/candidate gates are not vacuous. The mutated and clean binary SHA-256
are both recorded in the evidence.

## Run

```bash
python3 run-paired.py \
  --official /tmp/c13-build/livekit-server-go \
  --rust   target/release/livecloud \
  --browser "$HOME/Library/Caches/ms-playwright/chromium-1248/chrome-mac-arm64/Google Chrome for Testing.app/Contents/MacOS/Google Chrome for Testing" \
  --interop tests/interop \
  --output /tmp/c13-run
```

`BATCH.json` carries explicit top-level `go_binary_sha256` / `rust_binary_sha256` (the paired
classifier), per-vector `expect` / `go_shape` / `rust_shape`, and the full per-side observations
and logs. No JWT or certificate is persisted; the browser child exit is observed for real.