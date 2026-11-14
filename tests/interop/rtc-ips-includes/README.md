# SPEC-0077 C28 — launched-MAIN `rtc.ips.includes` paired Go/Rust differential

This harness proves the **real main server** consumes `rtc.ips.includes` as an
ICE host-candidate IP allowlist, identically on the locked Go oracle and the Rust
`livecloud` candidate. It names one ledger leaf: `YAML:rtc.ips.includes`.

It is a self-contained Pion WHIP observer
([`observe.go`](observe.go)) plus a thin differential wrapper
([`run-paired.py`](run-paired.py)). The observer:

- writes `rtc.ips.{includes,excludes}` into the daemon YAML, alongside
  `node_ip=127.0.0.1`, `enable_loopback_candidate=true`, `use_external_ip=false`,
  a reserved loopback HTTP port and media TCP port, and the shared UDP media
  range 50000–60000;
- launches one immutable daemon and drives an authenticated WHIP **Create** then
  **DELETE** with a Pion offer (requires `201` then `200`);
- extracts the **host** ICE candidate connection-addresses (`a=candidate:… typ
  host`) from the daemon's SDP answer into a sorted unique IP set + count +
  candidate-type catalog;
- settles 1 s after the WHIP DELETE so the daemon's async participant close
  completes before SIGTERM (graceful shutdown then exits `0`);
- for a malformed CIDR, records the startup **refusal** (bare upstream text on
  stdout + exit `1`) before the daemon ever binds HTTP;
- shuts the daemon down with SIGTERM, checks exclusive port rebinding, and hashes
  the daemon before/after (anti-tamper), writing `result.json`.

The observer's Pion driver configures **no IP filter of its own**
(`webrtc.NewPeerConnection(Configuration{})`), so every recorded host candidate
originates from the **daemon** and depends on `rtc.ips.includes` being consumed.

## Upstream consumer

Locked Go, pinned `mediatransportutil-f234b534b095/pkg/rtcconfig/webrtc_config.go`:

```go
// :75-83
if len(rtcConf.IPs.Includes) != 0 || len(rtcConf.IPs.Excludes) != 0 {
    filter, err := IPFilterFromConf(rtcConf.IPs)
    if err != nil { return nil, err }   // startup refusal
    s.SetIPFilter(filter)
}
```

`IPFilterFromConf` (`:441-483`) parses each include/exclude CIDR with
`net.ParseCIDR` (a malformed entry returns the bare error) and returns a filter:
includes non-empty → admit only IPs inside an include CIDR; else excludes
non-empty → reject IPs inside an exclude CIDR. The filter governs which local IPs
`GetLocalIPAddresses` / the ICE agent gather as host candidates. A malformed CIDR
propagates through `NewWebRTCConfig` → `NewLocalRoomManager` → `startServer`;
`cmd/server/main.go` prints `err.Error()` verbatim to stdout and exits `1`.

The Rust mirror is `livecloud/src/interfaces.rs:78-84` `ip_allowed` (includes
precedence over excludes, `in_cidr` matching) applied by `media_interfaces_from`
(`:95-146`), plus `lk-config/src/normalize.rs:321` `check_ip_filters` which
re-parses the raw list at the wiring boundary and refuses startup with the same
bare text.

## Profiles

| Profile | Family | `rtc.ips` stimulus | Host-candidate observation |
| --- | --- | --- | --- |
| `include-loopback` | present | includes `127.0.0.1/32` | loopback admitted → `127.0.0.1` host candidate present. |
| `include-wide` | present | includes `127.0.0.0/8` | a /8 containing loopback admits → present (CIDR matching, not exact-string). |
| `include-testnet` | absent | includes `192.0.2.0/24` | TEST-NET-1 admits **no** local IP → **zero** host candidates. |
| `include-precedence` | present | includes `127.0.0.1/32` + excludes `127.0.0.0/8` | includes win over excludes → present. |
| `malformed` | refusal | includes `not-a-cidr` | startup refused: bare `invalid CIDR address: not-a-cidr` + exit `1`. |

Every alive profile also requires WHIP `201`/`200`, daemon exit `0`, exclusive
rebinding of the daemon HTTP and media TCP ports, and an unchanged daemon hash.

## Normalized contract (scope limit)

The harness compares `{refused, server_exit, whip, host_candidate_ips,
host_candidate_count, candidate_types, ports_released, binary_unchanged,
refusal_line}` cross-side. It does **not** compare byte-equal SDP answers:
foundation/priority/ice-ufrag/port and the `o=-` session id are nondeterministic
per launch, and the Go daemon logs an extra `using single-node routing` INFO line
before the refusal that the Rust daemon does not — only the bare
`invalid CIDR address: …` **line** is the contract, not the whole log. It does
**not** claim the daemon's full ICE surface is identical — only the host-candidate
allowlist consumption this harness drives. No comparison field is deleted; these
are explicit scope bounds.

Each profile is asserted against a Go-oracle-derived contract on **each side
independently**, then compared cross-side, so the differential is non-vacuous.
`cross_profile_controls` (9 per side, 18 total) add discriminating checks:
positive presence, negative absence, the presence discriminator, CIDR-matching
specificity, includes-over-excludes precedence, malformed refusal text, malformed
exit-one, WHIP lifecycle, and observer exit integrity.

## Build the observer

Build from the fixed upstream Go module with its pinned toolchain on PATH (the
observer imports `github.com/livekit/protocol/auth` and `pion/webrtc/v4`):

```sh
cd references/livekit-stable
GOTOOLCHAIN=go1.26.0 GOPROXY=off GOWORK=off go build -mod=readonly \
  -o /tmp/c28-build/ips-observer \
  /absolute/path/to/tests/interop/rtc-ips-includes/observe.go
```

## Run the paired differential

```sh
python3 tests/interop/rtc-ips-includes/run-paired.py \
  --go /tmp/c28-build/livekit-server-go \
  --rust /absolute/path/to/target/release/livecloud \
  --observer /tmp/c28-build/ips-observer \
  --output /tmp/c28-build/acceptance
```

All paths must be absolute; `--output` must not pre-exist. The harness runs the
10 observer invocations (5 profiles × 2 sides) **sequentially** (the daemon UDP
media range 50000–60000 is shared), writes `BATCH.json` (gate-recognized) and
`execution.json` (full per-side raw receipts), prints a summary, and exits `0`
iff every profile matches and every control is true, else `1`. Every original
observer exit code is preserved; no pipe filters a result.

## Falsifiability positive control

A mutant that accepts-but-ignores the filter (`interfaces.rs` `ip_allowed` forced
to `true`) gathers the loopback (and physical) host candidates even for the
TEST-NET allowlist: `include-testnet` flips `host_candidate_count` 0→1 and
`host_candidate_ips` `[]`→`["127.0.0.1"]`, FAILING the negative profile and the
`negative_profile_absent` / `presence_discriminator` / `cidr_matching_specificity`
controls. The positive profiles and the malformed refusal (owned by the separate
`check_ip_filters`) stay correct, so the mutant FAILS **specifically** on the
filtering discriminator while the Go oracle stays correct on all five profiles.
Built in an independent target dir, the mutant receipt is nested under the
evidence `positive-control/mutant/` and is **never** cited as acceptance. See the
evidence `positive-control.json`.

## Residual (honest, out of scope)

`rtc.ips.excludes` as a standalone allowlist (separate leaf
`YAML:rtc.ips.excludes`), `rtc.interfaces.{includes,excludes}`, the srflx/relay
candidate surface, the generated `--rtc.ips.includes` flag, and other
platforms/NAT/fallback profiles remain open. This harness proves the launched-MAIN
`rtc.ips.includes` host-candidate allowlist consumer surface only. It is pure
loopback — no route/Clash/proxy mutation and no public network dependency.