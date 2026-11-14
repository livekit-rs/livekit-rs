# SPEC-0077 C34 — `rtc.use_ice_lite` launched-MAIN paired Go/Rust differential

This harness proves that the **launched MAIN server** (not an in-process component)
consumes `rtc.use_ice_lite` identically to the locked official LiveKit Server Go
oracle. It closes the residual that the C33 STUN-port harness explicitly left open
(`tests/interop/rtc-stun-port-as-ice/run-paired.py`: "the srflx/relay candidate
surface ... remain open") for the ICE-lite selection face specifically.

## Leaf

- `YAML:rtc.use_ice_lite` (bool, default `false`).

## What the setting does (upstream contract)

Pinned `mediatransportutil-f234b534b095` `pkg/rtcconfig/webrtc_config.go`:

```go
:223  if rtcConf.UseICELite {
:224    s.SetLite(true)                                   // emit a=ice-lite in SDP
:225  } else if (NodeIP empty/auto) && !UseExternalIP {
        ... s.SetSTUNServers(...)                         // lite OFF -> gather srflx
      }
```

`pkg/service/roommanager.go` and `pkg/rtc/transport.go` then gate it per participant:

```go
roommanager.go:446  if pi.DisableICELite { rtcConf.SettingEngine.SetLite(false) }
transport.go:414    if !params.ProtocolVersion.SupportsICELite()       // protocol <= 5
                      || !params.ClientInfo.SupportsPrflxOverRelay() { // Firefox
:422                  se.SetLite(false)
                    }
```

The legacy `/rtc` query path reads the stimulus at `rtcservice.go:240`
(`ParseClientInfo(r)`) and `:253` (`pi.DisableICELite = boolValue(r.FormValue("disable_ice_lite"))`).

So `use_ice_lite=true` makes the daemon emit the session-level **`a=ice-lite`** SDP
attribute (RFC 5245 §15.4 / pion `SettingEngine.SetLite`), gated off by the
`disable_ice_lite` join param, by an old protocol (≤5), and by Firefox.

## Rust mirror (complete launched-MAIN consumer chain)

- config parse: `crates/lk-config/src/official.rs:303` — `use_ice_lite: bool = false`.
- config projection: `crates/lk-config/src/normalize.rs:587` — passthrough.
- policy: `crates/rtc-core/src/config.rs:109` — `policy.use_ice_lite = cfg.rtc.use_ice_lite`.
- join consumer: `crates/rtc-core/src/room/join.rs:72-73`
  `ice_lite = policy.use_ice_lite && !params.disable_ice_lite && params.capabilities.ice_lite`.
- transport spec: `crates/rtc-core/src/room/join.rs:183` → `TransportSpec.ice_lite`.
- capability gate: `crates/lk-compat/src/profile.rs:41` — `capabilities.ice_lite = protocol>5 && !firefox`.
- SDP emit: `crates/rtc-media/src/engine.rs:321` — `.set_ice_lite(spec.ice_lite)` (str0m `a=ice-lite`).
- srflx gate: `crates/rtc-media/src/engine.rs:233` — `gather_srflx = gather_srflx && !ice_lite`.
- join param parse: `crates/lk-compat/src/signal/params.rs:151` `disable_ice_lite`, `:164`
  `protocol`, `:180` `browser`, `:359` augment only fills an EMPTY browser (an explicit
  query browser is preserved on both sides).

## Why NOT WHIP (load-bearing)

Both Go (`pkg/service/whipservice.go:196 DisableICELite:true`) and Rust
(`crates/lk-compat/src/whip/mod.rs:374 disable_ice_lite:true`,
`crates/lk-compat/src/whip/runtime.rs:142 params.disable_ice_lite=true`) **force
ICE-lite off** on the WHIP ingress path, so a WHIP offer/answer never carries
`a=ice-lite` on either side regardless of `rtc.use_ice_lite`. WHIP is correct parity
but **non-discriminating** for this leaf. The media-free signal join is the real
launched-MAIN consumer surface.

## Discriminator (load-bearing, empirically grounded on BOTH sides)

The observer drives an authenticated, **media-free** signal join over WebSocket
`/rtc?protocol=15&sdk=js&auto_subscribe=true&browser=<B>&disable_ice_lite=<D>` with an
`Authorization: Bearer <JWT>`. It publishes no track and sends no media; the offer is
the daemon's own subscriber peer-connection negotiation, so the ICE-lite flag
originates from the daemon. The subscriber offer is sent **before** trickle ICE
gathering, so the host-candidate catalog (types/transports/ports/IPs) is **empty** on
both sides and carries no signal. With `node_ip=127.0.0.1` + `use_external_ip=false`
the Go NAT1To1 catch-all (`webrtc_config.go:111-116`) masks candidate IPs to 127.0.0.1
but does **not** touch the session-level attribute. The derived discriminator is:

```
ice_lite_present = (an exact trimmed `a=ice-lite` line in the subscriber offer SDP)
```

| profile | use_ice_lite | browser | disable_ice_lite | protocol | ice_lite_present |
|---|---|---|---|---|---|
| `ice-lite-on`               | true  | Chrome  | false | 15 | **true** |
| `ice-lite-off`              | false | Chrome  | false | 15 | false |
| `ice-lite-on-firefox`       | true  | Firefox | false | 15 | false (capability gate) |
| `ice-lite-on-disable-param` | true  | Chrome  | true  | 15 | false (param gate) |

The three gating profiles carry no `a=ice-lite` on either side, so they **survive** an
accept-but-ignore mutant while `ice-lite-on` **fails** — specific, not indiscriminate.

## Normalization (scope limit)

The offer SDP foundation/priority/ice-ufrag/ice-pwd/session-id and the (empty)
candidate catalog are nondeterministic or non-signal per launch, so the SDP is **not**
compared byte-equal cross-side. The harness compares only DERIVED deterministic
semantics: `ice_lite_present`, `join_received`, `offer_received`,
`signal_upgrade_status`, `refused`, the daemon exit code, port release and binary
immutability. The raw SDP hash, the candidate catalog and the configured ports are
recorded in `execution.json` for transparency but are not cross-compared. No
comparison field is deleted; these are explicit scope bounds. This harness proves the
`use_ice_lite` ICE-lite SDP selection consumer surface only — not the daemon's full
ICE surface.

## Privacy normalization (commit boundary)

The manifest embeds raw daemon `server_log` provenance. Any RFC1918 private IPv4
literal is rewritten to RFC 5737 TEST-NET-1 (`192.0.2.<last-octet>`) before writing,
preserving the last octet for correlation while removing the real network identity.
The redaction is pattern-based (no real literal is embedded in the harness source).
The loopback-only fixture config normally emits no private literal, so this is a
defensive no-op guard, not a data transform.

## Files

- `observe.go` — self-contained media-free signal-join observer (separate copy from
  the C33 `rtc-stun-port-as-ice` observer; swaps the WHIP stimulus for a signal join,
  adds `--use-ice-lite`/`--browser`/`--disable-ice-lite`/`--protocol`, names that single
  leaf as the driven item, keeps the C33 observer identity unchanged for anti-splicing).
- `run-paired.py` — wraps the observer per side per profile, normalizes, asserts the
  per-side contract, compares cross-side, emits `BATCH.json` + `execution.json`,
  exits 0 iff every profile and control passes.

## Reproduce

Build the immutable binaries (from the repo root):

```sh
# Rust acceptance (default target dir)
cargo build --release -p livecloud --bin livecloud

# Locked Go oracle (v1.13.10 e5655ac2; pinned mediatransportutil-f234b534b095)
cd references/livekit-stable
GOTOOLCHAIN=go1.26.0 GOPROXY=off GOWORK=off go build -mod=readonly \
  -o /tmp/c34-build/livekit-server-go ./cmd/server

# C34 observer (same module context / toolchain)
GOTOOLCHAIN=go1.26.0 GOPROXY=off GOWORK=off go build -mod=readonly \
  -o /tmp/c34-build/ice-lite-observer \
  /Users/zoujiaqing/projects/livekit-rs/tests/interop/rtc-use-ice-lite/observe.go
```

Run the paired differential (fresh output dir; shared low media-port band + loopback
host so the eight observer invocations run sequentially):

```sh
python3 tests/interop/rtc-use-ice-lite/run-paired.py \
  --go /tmp/c34-build/livekit-server-go \
  --rust /Users/zoujiaqing/projects/livekit-rs/target/release/livecloud \
  --observer /tmp/c34-build/ice-lite-observer \
  --output /tmp/c34-run
```

Expected: `PASS_LAUNCHED_MAIN_USE_ICE_LITE`, 4 profiles, 10 controls, empty
`failed_profiles`/`failed_controls`, `input_identity_stable=true`, exit 0.

## Falsifiability (positive control)

An accept-but-ignore mutant — `room/join.rs:72` shadows `use_ice_lite` to `false` so
`ice_lite` is always false regardless of config — is built in an isolated
`CARGO_TARGET_DIR` and run on the SAME harness/observer/Go oracle. It must FAIL
`ice-lite-on` (`a=ice-lite` absent on Rust, present on Go) and the
`ice_lite_present_when_enabled_rust` control, while the three gating profiles and the
remaining controls survive. The mutant receipt is nested under `positive-control/`
and is **not** cited as acceptance evidence. The mutation is then reverted
byte-identical and the acceptance binary rebuilt to confirm it reproduces the
protected identity.