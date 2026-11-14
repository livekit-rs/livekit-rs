# SPEC-0077 C33 — `rtc.use_stun_port_as_ice` launched-MAIN paired Go/Rust differential

This harness proves that the **launched MAIN server** (not an in-process component)
consumes `rtc.use_stun_port_as_ice` identically to the locked official LiveKit Server
Go oracle. It closes the residual that the C32 media-port harness explicitly left
open (`tests/interop/rtc-media-ports/run-paired.py`: "the standalone STUN mux port
(use_stun_port_as_ice) ... remain open").

## Leaf

- `YAML:rtc.use_stun_port_as_ice` (bool, default `false`).

## What the setting does (upstream contract)

Pinned `mediatransportutil-f234b534b095` `pkg/rtcconfig/webrtc_config.go`:

```go
:123  if !rtcConf.ForceTCP {                              // UDP block guard
:127    if rtcConf.ICEPortRangeStart != 0 && rtcConf.ICEPortRangeEnd != 0 {
          s.SetEphemeralUDPPortRange(start, end)          // dedicated range, FIRST
:131    } else if rtcConf.UDPPort.Valid() {               // udp_port mux mode
          muxes, _ := transport.CreateUDPMuxesFromPorts(ports, opts...)
:162      if rtcConf.UseStunPortAsICE {
:163        standalonePortMuxes, _ = transport.CreateUDPMuxesFromPorts([]int{3478}, opts...)
          }
:169      udpMux = transport.NewMultiPortsUDPMux(muxes, standalonePortMuxes)
          s.SetICEUDPMux(udpMux)
        }
      }
```

So `use_stun_port_as_ice=true` adds a **standalone UDP mux on the fixed STUN port
3478** alongside the configured `udp_port` mux, and the daemon gathers a host ICE
candidate on 3478. It is created **only** inside `!ForceTCP` AND mux mode
(`UDPPort.Valid()`, i.e. not the dedicated-range branch). In range mode or under
`force_tcp` the flag is gated off and no 3478 candidate appears.

## Rust mirror (complete launched-MAIN consumer chain)

- config projection: `crates/lk-config/src/normalize.rs:589` — `use_stun_port_as_ice`
  passthrough into the server config.
- standalone STUN mux selection: `crates/livecloud/src/interfaces.rs:274-277`
  `standalone_stun_mux_port(rtc) = (!rtc.force_tcp && rtc.use_stun_port_as_ice &&
  matches!(rtc.udp, UdpMode::Mux { .. })).then_some(3478)` — the SAME `!force_tcp` +
  mux gating as Go `:123`/`:131`/`:162`.
- server assembly: `crates/livecloud/src/server.rs:339` passes
  `standalone_stun_mux_port` into `:336 start_with_configured_ports(...)`.
- socket bind + advertise: `crates/rtc-runtime/src/net/mod.rs:365` iterates two mux
  lists `[(ports, false), (standalone_port, true)]`; `:384-390` pushes the standalone
  3478 bind into `shared_candidates`; `:477` `shared_candidates` is advertised via
  `open_transport`. The 3478 candidate is gathered exactly like Go's
  `standalonePortMuxes`.

## Discriminator (load-bearing, empirically grounded on BOTH sides)

The observer drives an authenticated WHIP Create + DELETE with a filter-less Pion
offer (`webrtc.NewPeerConnection(Configuration{})` — no IP/interface filter, no port
preference), so every recorded host candidate port originates from the daemon's
bound media sockets. With `node_ip=127.0.0.1` + `use_external_ip=false` the Go
NAT1To1 catch-all (`webrtc_config.go:111-116`) masks candidate **IPs** to 127.0.0.1
but **not ports**, so the port is the surviving observable.

The fixed STUN port **3478 is NOT reserved** by the observer (the daemon binds it);
the configured mux port is reserved in the LOW band **40000-40999**, decisively below
the macOS ephemeral floor 49152. The observer verifies 3478 is free before launch
and released after shutdown. The derived discriminator is:

```
stun_3478_present = (3478 in udp_host_ports)
```

| profile | udp mode | use_stun_port_as_ice | force_tcp | expected UDP host ports | stun_3478_present |
|---|---|---|---|---|---|
| `stun-on-mux`       | mux (`udp_port=U`)   | true  | false | `{U, 3478}` | **true** |
| `stun-off-mux`      | mux (`udp_port=U`)   | false | false | `{U}`       | false |
| `stun-on-range`     | range `[A,B]`        | true  | false | subset of `[A,B]`, no 3478 | false |
| `stun-on-force-tcp` | ephemeral            | true  | true  | `[]` (UDP suppressed) | false |

The three gating profiles carry no 3478 on either side, so they **survive** an
accept-but-ignore mutant while `stun-on-mux` **fails** — specific, not indiscriminate.

## Normalization (scope limit)

The dedicated-range ports and the host-candidate **count** are nondeterministic per
launch (cyclic random start, per-interface gathering), so they are **not** compared
byte-equal cross-side. The harness compares only DERIVED deterministic semantics:
`stun_3478_present`, `mux_port_present` (configured mux port in the UDP host ports, or
`None` when no mux port is set), `udp_in_configured_range`, `udp_empty`,
`tcp_port_honored`, the transport catalog, the candidate-type catalog, the WHIP
statuses, the daemon exit code, port release (including 3478) and binary immutability.
Raw configured values and per-side port lists are recorded in `execution.json` for
transparency but are not cross-compared. No comparison field is deleted; these are
explicit scope bounds. This harness proves the `use_stun_port_as_ice` standalone-mux
selection consumer surface only — not the daemon's full ICE surface.

## Files

- `observe.go` — self-contained Pion WHIP observer (separate copy from the C32
  observer; adds `--use-stun-port-as-ice`, names that single leaf as the driven item,
  keeps the C32 observer identity unchanged for anti-splicing).
- `run-paired.py` — wraps the observer per side per profile, normalizes, asserts the
  per-side contract, compares cross-side, emits `BATCH.json` + `execution.json`,
  exits 0 iff every profile and control passes.

## Reproduce

Build the immutable binaries (from the repo root):

```sh
# Rust acceptance (default target dir; reproduces the C32 acceptance identity)
cargo build --release -p livecloud --bin livecloud

# Locked Go oracle (v1.13.10 e5655ac2; pinned mediatransportutil-f234b534b095)
cd references/livekit-stable
GOTOOLCHAIN=go1.26.0 GOPROXY=off GOWORK=off go build -mod=readonly \
  -o /tmp/c33-build/livekit-server-go ./cmd/server

# C33 observer (same module context / toolchain)
GOTOOLCHAIN=go1.26.0 GOPROXY=off GOWORK=off go build -mod=readonly \
  -o /tmp/c33-build/stun-observer \
  /Users/zoujiaqing/projects/livekit-rs/tests/interop/rtc-stun-port-as-ice/observe.go
```

Run the paired differential (fresh output dir; 3478 is shared so runs are sequential):

```sh
python3 tests/interop/rtc-stun-port-as-ice/run-paired.py \
  --go /tmp/c33-build/livekit-server-go \
  --rust /Users/zoujiaqing/projects/livekit-rs/target/release/livecloud \
  --observer /tmp/c33-build/stun-observer \
  --output /tmp/c33-run
```

Expected: `PASS_LAUNCHED_MAIN_STUN_PORT_AS_ICE`, 4 profiles, 12 controls, empty
`failed_profiles`/`failed_controls`, `input_identity_stable=true`, exit 0.

## Falsifiability (positive control)

An accept-but-ignore mutant — `standalone_stun_mux_port` shadows
`use_stun_port_as_ice` to `false` so 3478 is never bound regardless of config — is
built in an isolated `CARGO_TARGET_DIR` and run on the SAME harness/observer/Go
oracle. It must FAIL `stun-on-mux` (3478 absent on Rust, present on Go) and the
`stun_port_added_when_enabled_rust` control, while the three gating profiles and the
remaining controls survive. The mutant receipt is nested under `positive-control/`
and is **not** cited as acceptance evidence. The mutation is then reverted
byte-identical and the acceptance binary rebuilt to confirm it reproduces the
protected identity.