# SPEC-0077 C32 — launched-MAIN media-port family paired differential

**Batch:** C32 (pure verification — no production source change)
**Leaves:** `YAML:rtc.udp_port`, `YAML:rtc.tcp_port`, `YAML:rtc.port_range_start`, `YAML:rtc.port_range_end`
**Verdict:** `PASS_LAUNCHED_MAIN_MEDIA_PORTS` — 5/5 profiles match cross-side, 12/12 cross-profile controls true.

This harness proves the **launched-MAIN** server genuinely consumes the media-port
keys (`rtc.udp_port` mux, `rtc.port_range_start/end` dedicated range, `rtc.tcp_port`
TCP mux) when it binds ICE media sockets and gathers host candidates, on both the
locked official Go oracle and the Rust release candidate, with a falsifiable
accept-but-ignore positive control.

## Consumers under test

| side | anchor | semantics |
| --- | --- | --- |
| Go (locked v1.13.10, pinned `mediatransportutil-f234b534b095`) | `rtcconfig/webrtc_config.go:127-130` | `if ICEPortRangeStart != 0 && ICEPortRangeEnd != 0 { SetEphemeralUDPPortRange(start,end) }` — dedicated range, checked **first** (takes precedence). |
| Go | `rtcconfig/webrtc_config.go:131-171` | `else if UDPPort.Valid() { CreateUDPMuxesFromPorts → SetICEUDPMux }` — `udp_port` mux. |
| Go | `rtcconfig/webrtc_config.go:180-185` | `if TCPPort != 0 { net.ListenTCP("tcp", &TCPAddr{Port}) }` — one wildcard TCP mux listener. |
| Rust projection + precedence | `crates/lk-config/src/normalize.rs:442-460` | `if port_range_start != 0 && port_range_end != 0 { UdpMode::Range } else if udp_port.valid() { UdpMode::Mux } else { Disabled }` — the **same** range-over-mux precedence. |
| Rust net_config | `crates/livecloud/src/interfaces.rs:335-348` | `Mux → (udp_port=ports.first(), port_range=(0,0))`; `Range → (None,(start,end))`; `udp_enabled = !Disabled && !force_tcp`; `tcp_port = rtc.tcp_port`. |
| Rust mux rotation | `interfaces.rs:260-271` (`mux_ports_for_cpu`), `:279` (`startup_mux_ports`) | `force_tcp \|\| !Mux → empty`; else `take(cpus)`. |
| Rust assembly | `crates/livecloud/src/server.rs:330,332,333,336-343` | `net_config` → `tcp_port=(rtc.tcp_port!=0).then_some(..)` → `force_tcp` guard → `start_with_configured_ports`. |
| Rust binds | `crates/rtc-runtime/src/net/mod.rs:344-347,363-394,396-400,658/662` | `cfg.udp_port=ports.first().or(standalone)`; UDP mux binds (advertised port = `bound.port()`); one wildcard TCP `listen(port)`; `bind_dedicated_host → udp::bind_in_range(host.bind, cfg.port_range)`. |
| Rust range fallback | `crates/rtc-runtime/src/net/udp.rs:33-39` | `bind_in_range((0,0)) → bind(ip:0)` OS-ephemeral (the mutant's fallback path). |

## Method

The self-contained [`observe.go`](observe.go) is a thin Pion WHIP driver (a
**separate copy** from the C31 `rtc-loopback-candidate` observer, adding the
media-port flags and naming the four port leaves as the driven items, so the C31
observer identity is unchanged — anti-splicing). For each profile it:

- reserves the daemon HTTP port plus the configured media ports in a **LOW band
  40000-40999** (decisively below the macOS ephemeral range 49152-65535), records
  the reserved values, then releases them so the daemon can bind;
- writes the media-port keys into the daemon YAML with `node_ip=127.0.0.1`,
  `use_external_ip=false`, `enable_loopback_candidate=true`, `use_stun_port_as_ice=false`;
- launches one immutable daemon and drives an authenticated WHIP **Create** (`201`)
  then **DELETE** (`200`) with a Pion offer that configures **no port preference and
  no interface filter** (`webrtc.Configuration{}`);
- extracts the **host** ICE candidate ports per transport from the SDP answer
  (`a=candidate:… typ host`, fields[2]=transport, fields[5]=port) plus the
  transport/type catalogs;
- settles 1 s after DELETE before SIGTERM, checks exclusive port rebinding (HTTP +
  configured TCP + configured UDP mux), and hashes the daemon before/after (anti-tamper).

Because the Pion driver has no port preference and no interface filter, **every
recorded host candidate port originates from the daemon's bound media sockets** and
depends on the media-port keys being consumed — the observable is mutant-sensitive.

### Discriminator (empirically grounded, load-bearing)

With `node_ip=127.0.0.1` + `use_external_ip=false` the Go NAT1To1 catch-all
(`webrtc_config.go:111-116`) masks candidate **IPs** to `127.0.0.1` but **not ports**,
so the port is the surviving observable. The configured ports are reserved in the
low band 40000-40999, decisively below the macOS ephemeral floor 49152, so an
accept-but-ignore mutant that falls back to OS-ephemeral ports lands **outside** the
configured band — the port-selection axis is robust, not probabilistic.

### The five profiles

| Profile | Stimulus (`udp-mode` / `tcp-mode` / `force_tcp`) | Port-selection observation |
| --- | --- | --- |
| `udp-mux` | mux / on / false | sole UDP host port **== U** (exact mux); TCP host port == T; transports `[tcp,udp]` |
| `udp-range` | range / on / false | every UDP host port **∈ [A,B]** (dedicated cyclic search); TCP == T; `[tcp,udp]` |
| `range-precedence` | precedence / on / false | UDP host ports ∈ [A,B] **and mux U unused** (range wins); TCP == T; `[tcp,udp]` |
| `tcp-only` | ephemeral / on / **true** | **zero** UDP host ports; sole TCP host port == T; transports `[tcp]` |
| `ephemeral-baseline` | ephemeral / **zero** / false | UDP host ports OS-ephemeral (outside low band); **zero** TCP host ports (`tcp_port=0`); `[udp]` |

Every profile also requires WHIP `201`/`200`, daemon exit `0`, exclusive rebinding of
the daemon HTTP + configured media ports, and binary immutability. The observer
strips inherited `OTEL_`/`SSL_CERT_` env. `tcp-mode=on` reserves an explicit low TCP
port so the shared-host default `7881` is never touched; `tcp-mode=zero` writes an
explicit `tcp_port=0`. The low media band is shared, so the ten observer invocations
run **sequentially**. The media-port keys never refuse startup here, so there is
**no refusal family**.

### Normalization (scope limit)

The dedicated-range ports and the host-candidate **count** are nondeterministic per
launch (cyclic random start, per-interface gathering), so the harness does **not**
compare them byte-equal cross-side. Instead it compares **derived, deterministic
port-selection semantics**:

- `tcp_port_honored` — TCP host ports == `[configured tcp_port]`, or == `[]` when `tcp_port=0`;
- `udp_mux_exact` — UDP host ports == `[configured udp_port]` when a mux port is set (else `None`);
- `udp_in_configured_range` — non-empty and every UDP host port within `[start,end]`;
- `udp_mux_port_unused` — the configured mux port is absent from the UDP host ports;
- `udp_empty`, `udp_all_ephemeral` (non-empty and every UDP host port above the low band);
- the transport catalog, candidate-type catalog, WHIP statuses, daemon exit code,
  port release and binary immutability.

The raw configured values and observed port lists are recorded per side for
transparency but are **not** cross-compared as byte-equal (per-side reservations /
nondeterministic). No comparison field is deleted; these are explicit scope bounds.
The harness does **not** claim the daemon's full ICE surface is identical — only the
media-port selection consumer this harness drives.

Each profile is asserted against a Go-oracle-derived contract on **each side
independently** (`contract_ok`), then compared cross-side (`cross_equal`), so the
differential is non-vacuous. The 6 `cross_profile_controls` per side (12 total) add
discriminating checks: mux exactness, range containment + precedence, TCP mux port
honored across all configured profiles, force_tcp UDP suppression, the ephemeral
baseline (no configured port), and WHIP/exit/release/integrity.

## Falsifiability (positive control)

Rust binds the configured media ports in `net/mod.rs:start_with_configured_ports`.
The mutant **ignores every configured media port** — shadowing `ports=&[]`,
`standalone_port=None`, `tcp_port=Some(0)`, `cfg.port_range=(0,0)` at the function
entry (the prohibited **accept-but-ignore** failure mode) — built in an independent
target dir (`/tmp/c32-mutant-target`) so the clean candidate is never overwritten.
`udp_enabled`/`force_tcp` are untouched, so UDP suppression still works; only the
port VALUES fall back to OS-ephemeral.

- Mutant harness exit **1**, status **FAIL**, failing exactly the four configured
  profiles (`udp-mux`, `udp-range`, `range-precedence`, `tcp-only`): their UDP/TCP
  host ports fall outside the configured band, flipping `udp_mux_exact`,
  `udp_in_configured_range`, `tcp_port_honored` and the transport catalog.
- The `ephemeral-baseline` profile still **matches** (it carries no configured port,
  so the mutant's ephemeral fallback equals it) — specific, not indiscriminate.
- The Rust discriminating controls flip false (`mux_udp_port_exact_rust`,
  `range_containment_and_precedence_rust`, `tcp_mux_port_honored_rust`); the other
  Rust controls stay true (`tcp_only_suppresses_udp_rust`,
  `ephemeral_baseline_no_config_rust`, `whip_exit_release_integrity_rust`); all Go
  controls stay true.
- `net/mod.rs` restored byte-identical after the mutant; the acceptance target dir
  rebuild reproduces the protected acceptance binary `d1dad0fc`; the clean acceptance
  PASSes (5 profiles, 12 controls) on the **same harness** and **same observer**. The
  mutant FAIL receipt is nested under `positive-control/mutant/` and is **not** cited
  as acceptance evidence.

## Run

```bash
# build the observer with the pinned toolchain (from the fixed Go module)
cd references/livekit-stable
GOTOOLCHAIN=go1.26.0 GOPROXY=off GOWORK=off go build -mod=readonly \
  -o /tmp/c32-build/port-observer \
  /Users/zoujiaqing/projects/livekit-rs/tests/interop/rtc-media-ports/observe.go

# run the paired differential (all paths absolute; --output must not pre-exist)
python3 /Users/zoujiaqing/projects/livekit-rs/tests/interop/rtc-media-ports/run-paired.py \
  --go       /tmp/c32-build/livekit-server-go \
  --rust     /Users/zoujiaqing/projects/livekit-rs/target/release/livecloud \
  --observer /tmp/c32-build/port-observer \
  --output   <fresh-dir>
```

## Out of scope (honest residual)

The srflx/relay candidate surface, multi-port mux rotation across >1 configured port
and CPU-count truncation, the standalone STUN mux port (`use_stun_port_as_ice`),
public NAT / NAT1To1 rewrite profiles, other platforms, ICE restart / dedicated-host
refresh and exhaustion recovery (separately witnessed in the `port_range` historical
evidence), and the generated `--rtc.udp_port` / `--rtc.tcp_port` /
`--rtc.port_range_*` flags are **not** claimed by this batch. These residuals stay
recorded in the ledger `accepted_scope` (marked `remains open`) and are not absorbed
into this batch's PASS. This harness proves the **launched-MAIN media-port SELECTION
consumer** surface only (single mux port, dedicated range containment,
range-over-mux precedence, TCP mux port, force_tcp UDP suppression, `tcp_port=0`).
It is pure loopback — no route/Clash/proxy mutation and no public network dependency.