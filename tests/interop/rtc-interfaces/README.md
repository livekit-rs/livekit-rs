# SPEC-0077 C30 — launched-MAIN `rtc.interfaces.{includes,excludes}` ICE host-candidate paired differential

**Batch:** C30 (pure verification — no production source change)
**Leaves:** `YAML:rtc.interfaces.includes`, `YAML:rtc.interfaces.excludes`
**Verdict:** `PASS_LAUNCHED_MAIN_INTERFACES` — 6/6 profiles match cross-side, 16/16 cross-profile controls true.

This harness proves the **launched-MAIN** server genuinely consumes the
`rtc.interfaces.{includes,excludes}` **interface-name allowlist/denylist** when it
gathers ICE host candidates, on both the locked official Go oracle and the Rust
release candidate, with a falsifiable accept-but-ignore positive control.

## Consumer under test

| side | anchor | semantics |
| --- | --- | --- |
| Go (locked v1.13.10, pinned `mediatransportutil-f234b534b095`) | `rtcconfig/webrtc_config.go:70-72` (`SetInterfaceFilter`), `:422-439` (`InterfaceFilterFromConf`), `:144` (UDP mux bind) | `slices.Contains(includes, name)` when includes non-empty; else reject `slices.Contains(excludes, name)`; else admit all. **Exact string equality, not a glob.** Includes precedence over excludes. |
| Rust | `crates/livecloud/src/interfaces.rs:41-47` (`named_allowed`), applied at `media_interfaces_from:103` and `nat_discovery_addresses:180` | `includes.iter().any(\|n\| n == name)` when includes non-empty; else `!excludes.iter().any(\|n\| n == name)`. Same exact-match + precedence. |

Interface names are **never parsed**, so unlike `rtc.ips.*` CIDRs there is **no
malformed-refusal profile**. The filter selects which network devices are
eligible *before* the IP filter (`ip_allowed`) and the loopback/UP/IPv6 rules.

## Method

The self-contained [`observe.go`](observe.go) is a thin Pion WHIP driver. For each
profile it:

- resolves `@loopback` (the UP loopback name, `lo0` on macOS) and `@all` (every
  enumerated interface name) against the live OS table (`net.Interfaces`), and
  records the **enumerated table** + **resolved name lists** for transparency;
- writes `rtc.interfaces.{includes,excludes}` into the daemon YAML with
  `node_ip=127.0.0.1`, `use_external_ip=false`, `enable_loopback_candidate=true`;
- launches one immutable daemon and drives an authenticated WHIP **Create**
  (`201`) then **DELETE** (`200`) with a Pion offer that configures **no interface
  filter** (`webrtc.Configuration{}`);
- extracts the **host** ICE candidate IP set from the SDP answer
  (`a=candidate:… typ host`, fields[4]=IP) plus the candidate-type catalog;
- settles 1 s after DELETE before SIGTERM, checks exclusive port rebinding, and
  hashes the daemon before/after (anti-tamper).

Because the Pion driver has no interface filter, **every recorded host candidate
originates from the daemon** and depends on `rtc.interfaces.*` being consumed —
the observable is mutant-sensitive. Both daemons receive the **same explicit YAML
name list** (the observer resolves tokens once per invocation).

### Discriminator (empirically grounded, load-bearing)

With `node_ip=127.0.0.1` + `use_external_ip=false` the Go daemon installs a
catch-all NAT1To1 rewrite to `127.0.0.1` (`webrtc_config.go:111-116`), so **every**
admitted candidate — loopback *or* physical — is masked to `127.0.0.1`. The
host-candidate IP **set** is therefore `{127.0.0.1}` whenever any interface is
admitted and `{}` when none is. The robust deterministic discriminator is
**presence (count 1) vs absence (count 0)** of an admitted interface, not the IP
value or the interface count.

### The six profiles

| Profile | Family | Stimulus (`includes` / `excludes`) | Host-candidate observation |
| --- | --- | --- | --- |
| `include-loopback` | present | `@loopback` / — | lo0 admitted → count 1, `127.0.0.1`, types `[host]` |
| `include-absent` | absent | `c30-no-such-iface` / — | no interface matches → **count 0**, types `[]` |
| `include-exact-not-glob` | absent | `lo*` / — | exact match fails (`lo*` ≠ `lo0`) → **count 0** (proves exact-string, not glob) |
| `include-precedence` | present | `@loopback` / `@loopback` | includes win over excludes → count 1 |
| `exclude-absent` | present | — / `c30-no-such-iface` | excluding a nonexistent name keeps all → count 1 |
| `exclude-all` | absent | — / `@all` | every interface excluded → **count 0** |

Every profile also requires WHIP `201`/`200`, daemon exit `0`, and exclusive
rebinding of the daemon HTTP + media TCP ports. The observer strips inherited
`OTEL_`/`SSL_CERT_` env. The UDP media range 50000–60000 is shared, so the twelve
observer invocations run **sequentially**.

### Normalization (scope limit)

The harness compares the host-candidate IP set (sorted unique), count,
candidate-type catalog (sorted unique), WHIP statuses, daemon exit code, port
release and binary immutability. It does **not** compare byte-equal SDP answers
(foundation/priority/ice-ufrag/port and the `o=` session id are nondeterministic
per launch). Because of the catch-all NAT1To1 rewrite it does **not** claim to
observe *which* physical interface was admitted (all mask to `127.0.0.1`); the
presence/absence axis is the discriminating observable. It does **not** claim the
daemon's full ICE surface is identical — only the host-candidate name-filter
consumption this harness drives. No comparison field is deleted; these are
explicit scope boundaries.

Each profile is asserted against a Go-oracle-derived contract on **each side
independently** (`contract_ok`), then compared cross-side (`cross_equal`), so the
differential is non-vacuous. The 8 `cross_profile_controls` per side (16 total)
add discriminating checks: positive presence, negative absence, includes presence
discriminator, exact-match-not-glob, includes-precedence-over-excludes, excludes
presence discriminator, WHIP lifecycle, observer exit integrity.

## Falsifiability (positive control)

Rust applies the name filter in **one** place: `interfaces.rs:41 named_allowed`.
The mutant forces `named_allowed(_rtc, _name) => true` regardless of
`rtc.interfaces.includes/excludes` (the prohibited **accept-but-ignore** failure
mode), built in an independent target dir so the clean candidate is never
overwritten.

- Mutant harness exit **1**, status **FAIL**, failing exactly the three negative
  profiles (`include-absent`, `include-exact-not-glob`, `exclude-all`): each flips
  `host_candidate_count` 0→1 and `host_candidate_ips` `[]`→`['127.0.0.1']`.
- The **4** Rust discriminating controls flip false
  (`negative_profile_absent_rust`, `includes_presence_discriminator_rust`,
  `exact_match_not_glob_rust`, `excludes_presence_discriminator_rust`); the **4**
  other Rust controls stay true (positive presence, includes-precedence, WHIP
  lifecycle, observer integrity); all **8** Go controls stay true. The three
  present profiles still gather loopback (count 1, match). This proves the
  harness distinguishes the name-filter discriminator from the daemon
  start/serve lifecycle rather than blanket-failing the Rust side.
- `interfaces.rs` restored byte-identical after the mutant; the acceptance target
  dir rebuild reproduces the protected acceptance binary; the clean acceptance
  PASSes (6 profiles, 16 controls) on the **same harness** and **same observer**.
  The mutant FAIL receipt is nested under `positive-control/mutant/` and is
  **not** cited as acceptance evidence.

## Run

```bash
# build the observer with the pinned toolchain (from the fixed Go module)
cd references/livekit-stable
GOTOOLCHAIN=go1.26.0 GOPROXY=off GOWORK=off go build -mod=readonly \
  -o /tmp/c30-build/ifaces-observer \
  /Users/zoujiaqing/projects/livekit-rs/tests/interop/rtc-interfaces/observe.go

# run the paired differential (all paths absolute; --output must not pre-exist)
python3 /Users/zoujiaqing/projects/livekit-rs/tests/interop/rtc-interfaces/run-paired.py \
  --go       /tmp/c30-build/livekit-server-go \
  --rust     /Users/zoujiaqing/projects/livekit-rs/target/release/livecloud \
  --observer /tmp/c30-build/ifaces-observer \
  --output   <fresh-dir>
```

## Out of scope (honest residual)

The srflx/relay candidate surface, other platforms/NAT/fallback profiles,
physical-multihomed gathering beyond the loopback host candidate this harness
drives, and the generated `--rtc.interfaces.{includes,excludes}` flags are **not**
claimed by these leaves. These residuals stay recorded in the ledger
`accepted_scope` (marked `remains open`) and are not absorbed into this batch's
PASS. This harness proves the **launched-MAIN `rtc.interfaces.{includes,excludes}`
host-candidate name-filter consumer** surface only. It is pure loopback — no
route/Clash/proxy mutation and no public network dependency.