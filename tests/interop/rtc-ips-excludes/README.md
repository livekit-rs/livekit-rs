# SPEC-0077 C29 — launched-MAIN `rtc.ips.excludes` ICE host-candidate paired differential

One ledger leaf is named: **`YAML:rtc.ips.excludes`**. This harness launches the
locked official Go oracle and the Rust `livecloud` release candidate as the
**real main server**, five times each, drives an authenticated WHIP
Create/Delete with a filter-less Pion offer, and compares the **host ICE
candidate** set the daemon gathered into its SDP answer. It proves the
standalone-excludes branch of the IP filter (`includes` empty, `excludes`
governs) is consumed identically to Go.

This is the sibling of C28 (`rtc.ips.includes`). C29 is **pure verification** —
no production source change. The `rtc.ips.excludes` leaf was `OPEN_AUDIT` with
all five SPEC-0075 tiers `NOT_RUN` (never separately audited); C29 establishes
all five tiers with an item-named paired Go+Rust receipt on the current clean
candidate.

## Upstream contract (locked Go, pinned mediatransportutil-f234b534b095)

`rtcconfig/webrtc_config.go:75-83`:

```go
if len(rtcConf.IPs.Includes) != 0 || len(rtcConf.IPs.Excludes) != 0 {
    filter, err := IPFilterFromConf(rtcConf.IPs)
    if err != nil { return nil, err }   // startup refusal
    s.SetIPFilter(filter)
}
```

`IPFilterFromConf` (`webrtc_config.go:441-483`) parses each include/exclude CIDR
with `net.ParseCIDR` (a malformed entry returns the bare error) and returns:

```go
if len(includes) > 0 { /* admit only IPs inside an include CIDR */ }
if len(excludes) > 0 { for _, ipn := range excludes { if ipn.Contains(ip) { return false } } }
return true
```

So with `includes` empty and `excludes` non-empty, an IP inside an exclude CIDR
is **rejected**, everything else is **admitted**. The filter governs which local
IPs the ICE agent gathers as host candidates. A malformed CIDR propagates through
`NewWebRTCConfig` → `NewLocalRoomManager` → `startServer`; `cmd/server/main.go`
prints `err.Error()` verbatim to stdout and exits 1.

The Rust mirror is `livecloud/src/interfaces.rs:78-84` `ip_allowed` (the else
branch `!rtc.ips.excludes.iter().any(|c| in_cidr(ip, *c))`, includes precedence
over excludes), applied by `media_interfaces_from` (`:95-146`) at `:127` and
`nat_discovery_addresses` at `:197`, plus `lk-config/src/normalize.rs:321`
`check_ip_filters` which re-parses the raw includes+excludes list at the wiring
boundary and refuses startup with the same bare text.

## Method

The self-contained observer (`observe.go`) per profile:

- writes `rtc.ips.{includes,excludes}` into the daemon YAML (`node_ip=127.0.0.1`,
  `use_external_ip=false`, `enable_loopback_candidate=true`, fixed TCP media port,
  UDP range 50000–60000),
- launches one immutable daemon and drives an authenticated WHIP **Create**
  (`201`) then **DELETE** (`200`) with a Pion offer that configures **no IP
  filter** (`webrtc.NewPeerConnection(Configuration{})`),
- extracts the sorted-unique **host** candidate IP set and candidate-type catalog
  from the SDP answer (`a=candidate:… typ host` lines, field[4] = IP),
- settles 1 s after DELETE before SIGTERM (avoids the async participant-close /
  graceful-shutdown race), checks exclusive port rebinding, and hashes the daemon
  before/after (anti-tamper),
- strips inherited `OTEL_`/`SSL_CERT_` env so each profile is deterministic.

Because the Pion driver has no IP filter, **every recorded host candidate
originates from the daemon** and depends on `rtc.ips.excludes` being consumed —
the observable is mutant-sensitive.

### Discriminator choice (empirically grounded, load-bearing)

With `node_ip=127.0.0.1` + `use_external_ip=false` the Go daemon installs a
**catch-all NAT1To1 rewrite to 127.0.0.1** (`webrtc_config.go:111-116`), so an
admitted **physical** interface candidate is masked to `127.0.0.1` in the answer
too. A loopback-only exclude (`127.0.0.1/32`) therefore still shows `127.0.0.1`
(from the physical interface) and is **not** observable via the SDP answer — this
was confirmed by probing the Go oracle directly. The robust deterministic
discriminator is instead **reject-all vs non-local-admit**:

- an exclude of `0.0.0.0/0` contains every local IPv4 → **zero** host candidates;
- a non-local exclude (TEST-NET) contains no local IP → the loopback (and
  physical) candidates are gathered and masked to `127.0.0.1` → **count 1**.

`0.0.0.0/0 != "127.0.0.1"` as a string yet `Contains` it, so count 0 also proves
**CIDR containment matching** rather than exact-string equality.

### The five profiles

| Profile | includes | excludes | Family | Collector observation |
| --- | --- | --- | --- | --- |
| `exclude-testnet` | — | `192.0.2.0/24` (TEST-NET-1) | present | No local IP in range → loopback host candidate gathered, count 1, `127.0.0.1`. |
| `exclude-doc` | — | `203.0.113.0/24` (TEST-NET-3) | present | No local IP in range → count 1, `127.0.0.1`. |
| `exclude-everything` | — | `0.0.0.0/0` | absent | Every local IPv4 rejected → **zero** host candidates, count 0. |
| `excludes-superseded` | `127.0.0.1/32` | `0.0.0.0/0` | present | Includes non-empty → excludes branch never consulted → count 1 (includes precedence). |
| `malformed-exclude` | — | `not-a-cidr` | refusal | Startup refused, bare `invalid CIDR address: not-a-cidr` on stdout, exit 1, before binding HTTP. |

Every alive profile also requires WHIP `201`/`200`, daemon exit `0`, and exclusive
rebinding of the daemon HTTP and media TCP ports.

### Normalization (scope limit)

The harness compares the host-candidate IP set (sorted unique), the
host-candidate count, the candidate-type catalog (sorted unique), the WHIP
statuses, the daemon exit code, port release, binary immutability and (for the
refusal profile) the bare `invalid CIDR address: …` line cross-side. It does
**not** compare byte-equal SDP answers: foundation/priority/ice-ufrag/port and the
`o=` session id are nondeterministic per launch, and the Go daemon logs an extra
`using single-node routing` INFO line before the refusal that the Rust daemon does
not — only the bare refusal **line** is the contract, not the whole log. Because
of the catch-all NAT1To1 rewrite it does **not** claim to observe loopback-specific
exclusion; the reject-all vs non-local-admit axis is the discriminating
observable. It does **not** claim the daemon's full ICE surface is identical —
only the host-candidate exclude-filter consumption this harness drives. No
comparison field is deleted; these are explicit scope bounds.

Each profile is asserted against a Go-oracle-derived contract on **each side
independently** (`contract_ok`), then compared cross-side (`cross_equal`), so the
differential is non-vacuous. The 9 `cross_profile_controls` per side (18 total)
add discriminating checks: positive admission, negative rejection, presence
discriminator, CIDR-matching specificity, includes-precedence-over-excludes,
malformed refusal text, malformed exit 1, WHIP lifecycle, observer exit integrity.

## Falsifiability (positive control)

Rust applies the exclude filter in **one** place: `interfaces.rs:78` `ip_allowed`.
A mutant forcing `ip_allowed(_rtc, _ip) => true` (the prohibited
**accept-but-ignore** failure mode) gathers `127.0.0.1` even for the `0.0.0.0/0`
exclude: `exclude-everything` flips `host_candidate_count` 0→1 and
`host_candidate_ips` `[]`→`["127.0.0.1"]`, FAILING the negative profile and the
`negative_profile_absent` / `presence_discriminator` / `cidr_matching_specificity`
controls. The positive profiles (`exclude-testnet`/`exclude-doc`/
`excludes-superseded`) still gather count 1, and the malformed refusal (owned by
the separate `check_ip_filters`) still refuses, so the mutant FAILS **specifically**
on the filtering discriminator while the Go oracle stays correct on all five
profiles. Built in an independent target dir, the mutant receipt is nested under
the evidence `positive-control/mutant/` and is **never** cited as acceptance. See
the evidence `positive-control.json`.

## Identities

| role | sha256 |
| --- | --- |
| Go oracle | `4bc2378e636740b29093061036249ffc9ebbef91f4ece1de12177c8dfee1e7dc` (locked v1.13.10, source `e5655ac20267fa5005dccaf3f0e3bfc3ea688c07`) |
| Rust acceptance | `d1dad0fcdeefd355dd01f553a7b1d300c0cca6abf3d4832791b4dc7920640554` (`target/release/livecloud`, same identity as C20–C28; no production change) |
| Observer binary | `ef7232e519555e2e075cf2f3b44adf402df0e53f02b9bdaedead8ed77dafc284` |
| Observer source | `78ff67a928996de7c1b10af83032614ebad348b212b9806ea7f69ed0b1094423` (`observe.go`) |
| Harness | `0f317ad8e35e9dd1d5851a01733ef55a5ca2151332626dde81e34fbfec22db5d` (`run-paired.py`) |

The C29 observer is a copy of the C28 observer with the header updated and
`driven_items` derived from the stimulus (so it accurately names the excludes
leaf). The **C28 observer source is byte-identical** (`25aa8076…`, unchanged) —
C28's recorded identity is preserved.

## Run the paired differential

```sh
# build the observer from the locked Go module context
cd references/livekit-stable
GOTOOLCHAIN=go1.26.0 GOPROXY=off GOWORK=off go build -mod=readonly \
  -o /tmp/c29-build/ips-excludes-observer \
  /Users/zoujiaqing/projects/livekit-rs/tests/interop/rtc-ips-excludes/observe.go

python3 tests/interop/rtc-ips-excludes/run-paired.py \
  --go /tmp/c29-build/livekit-server-go \
  --rust /absolute/path/to/target/release/livecloud \
  --observer /tmp/c29-build/ips-excludes-observer \
  --output /tmp/c29-build/acceptance
```

All paths must be absolute; `--output` must not pre-exist. The harness runs the
10 observer invocations (5 profiles × 2 sides) **sequentially** (the daemon UDP
media range 50000–60000 is shared), writes `BATCH.json` (gate-recognized) and
`execution.json` (full per-side raw receipts), prints a summary, and exits `0`
iff every profile matches and every control is true, else `1`. Every original
observer exit code is preserved; no pipe filters a result.

## Residual (honest, out of scope)

`rtc.ips.includes` as a standalone allowlist (separate leaf
`YAML:rtc.ips.includes`, closed by C28), `rtc.interfaces.{includes,excludes}`, the
srflx/relay candidate surface, loopback-specific exclusion under the catch-all
NAT1To1 rewrite, the generated `--rtc.ips.excludes` flag, and other
platforms/NAT/fallback profiles remain open. This harness proves the launched-MAIN
`rtc.ips.excludes` host-candidate exclude-filter consumer surface only. It is pure
loopback — no route/Clash/proxy mutation and no public network dependency.