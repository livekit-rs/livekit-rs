# SPEC-0077 C31 — launched-MAIN `rtc.enable_loopback_candidate` ICE host-candidate paired differential

**Batch:** C31 (pure verification — no production source change)
**Leaf:** `YAML:rtc.enable_loopback_candidate`
**Verdict:** `PASS_LAUNCHED_MAIN_LOOPBACK_CANDIDATE` — 4/4 profiles match cross-side, 12/12 cross-profile controls true.

This harness proves the **launched-MAIN** server genuinely consumes the
`rtc.enable_loopback_candidate` **loopback host-candidate gate** when it gathers ICE
host candidates, on both the locked official Go oracle and the Rust release
candidate, with a falsifiable accept-but-ignore positive control.

## Consumer under test

| side | anchor | semantics |
| --- | --- | --- |
| Go (locked v1.13.10, pinned `mediatransportutil-f234b534b095`) | `rtcconfig/webrtc_config.go:219-220` (`SetIncludeLoopbackCandidate`) | `if rtcConf.EnableLoopbackCandidate { s.SetIncludeLoopbackCandidate(true) }`. When false the Pion setting stays at its default (false) and the loopback interface is **not** gathered as a host candidate. |
| Rust | `crates/livecloud/src/interfaces.rs:100` (`let loopback = rtc.enable_loopback_candidate`), applied at `media_interfaces_from:103` (`device.loopback && !loopback`) and `:117` (`address.is_loopback() && !loopback`) | The loopback device/address is skipped when the flag is false. Reached from the launched MAIN via `net_config:336` ← `server.rs:330`. |

The `udp_port`-mux loopback option (`webrtc_config.go:137-138`) and the
`use_external_ip` discovery path (`GetLocalIPAddresses:301` / Rust
`nat_discovery_addresses:207`) are **not** exercised here: this harness uses
`port_range_start/end` and `use_external_ip=false`. The observable loopback consumer
in this configuration is `SetIncludeLoopbackCandidate` / `media_interfaces_from`.
Those other paths stay recorded as honest residuals.

## Method

The self-contained [`observe.go`](observe.go) is a thin Pion WHIP driver (a
**separate copy** from the C30 `rtc-interfaces` observer, adding the
`--enable-loopback` toggle and naming `YAML:rtc.enable_loopback_candidate` as the
driven item, so the C30 observer identity is unchanged — anti-splicing). For each
profile it:

- resolves `@loopback` (the UP loopback name, `lo0` on macOS) against the live OS
  table (`net.Interfaces`), and records the **enumerated table** + **resolved name
  list** for transparency;
- writes `rtc.enable_loopback_candidate` (true/false) **and** the
  `rtc.interfaces.includes=@loopback` pin into the daemon YAML with
  `node_ip=127.0.0.1`, `use_external_ip=false`;
- launches one immutable daemon and drives an authenticated WHIP **Create**
  (`201`) then **DELETE** (`200`) with a Pion offer that configures **no interface
  filter and no loopback preference** (`webrtc.Configuration{}`);
- extracts the **host** ICE candidate IP set from the SDP answer
  (`a=candidate:… typ host`, fields[4]=IP) plus the candidate-type catalog;
- settles 1 s after DELETE before SIGTERM, checks exclusive port rebinding, and
  hashes the daemon before/after (anti-tamper).

Because the Pion driver has no interface filter and no loopback preference,
**every recorded host candidate originates from the daemon** and depends on
`rtc.enable_loopback_candidate` being consumed — the observable is mutant-sensitive.
Both daemons receive the **same explicit YAML name list and the same flag value**.

### Discriminator (empirically grounded, load-bearing)

With `node_ip=127.0.0.1` + `use_external_ip=false` the Go daemon installs a
catch-all NAT1To1 rewrite to `127.0.0.1` (`webrtc_config.go:111-116`), so **every**
admitted candidate — loopback *or* physical — is masked to `127.0.0.1`. A bare
loopback toggle is therefore **not observable** while physical interfaces are
admitted: the empirical probe `disabled-nopin` (no interface pin, flag=false) still
gathered `host_candidate_count 1` because a physical interface was admitted and
masked. To isolate the loopback-candidate gate the harness **pins
`rtc.interfaces.includes=@loopback`** so the only name-admitted interface is the
loopback (`lo0`). Then the flag is the sole determinant of presence:

- `enable_loopback_candidate=true` → `lo0` gathered → **count 1**, `127.0.0.1`, `[host]`;
- `enable_loopback_candidate=false` → `lo0` suppressed → **count 0**, `[]`, `[]`.

This pin was validated empirically on **both** the Go oracle and the Rust candidate
before the profile set was fixed (the C29 precedent: a loopback-only axis is not
observable without the interface pin).

### The four profiles

| Profile | Family | Stimulus (`includes` / `excludes`, flag) | Host-candidate observation |
| --- | --- | --- | --- |
| `enabled-pin` | present | `@loopback` / — , flag=true | lo0 admitted + loopback enabled → count 1, `127.0.0.1`, `[host]` |
| `disabled-pin` | absent | `@loopback` / — , flag=false | lo0 admitted by name but loopback suppressed → **count 0**, `[]` |
| `enabled-precedence` | present | `@loopback` / `@loopback`, flag=true | includes win, lo0 admitted + loopback enabled → count 1 |
| `disabled-precedence` | absent | `@loopback` / `@loopback`, flag=false | includes win admits lo0 by name, loopback gate suppresses → **count 0** |

Every profile also requires WHIP `201`/`200`, daemon exit `0`, and exclusive
rebinding of the daemon HTTP + media TCP ports. The observer strips inherited
`OTEL_`/`SSL_CERT_` env. The UDP media range 50000–60000 is shared, so the eight
observer invocations run **sequentially**. The loopback flag never refuses startup,
so there is **no refusal family**.

### Normalization (scope limit)

The harness compares the host-candidate IP set (sorted unique), count,
candidate-type catalog (sorted unique), WHIP statuses, daemon exit code, port
release and binary immutability. It does **not** compare byte-equal SDP answers
(foundation/priority/ice-ufrag/port and the `o=` session id are nondeterministic
per launch). Because of the catch-all NAT1To1 rewrite it does **not** claim to
observe *which* interface was admitted (all mask to `127.0.0.1`); the
presence/absence axis driven by the loopback-candidate flag is the discriminating
observable. It does **not** claim the daemon's full ICE surface is identical — only
the host-candidate loopback-gate consumption this harness drives. No comparison
field is deleted; these are explicit scope boundaries.

Each profile is asserted against a Go-oracle-derived contract on **each side
independently** (`contract_ok`), then compared cross-side (`cross_equal`), so the
differential is non-vacuous. The 6 `cross_profile_controls` per side (12 total) add
discriminating checks: positive presence, negative absence, the loopback toggle
discriminator (enabled-pin present ∧ disabled-pin absent), the precedence loopback
discriminator, WHIP lifecycle, observer exit integrity.

## Falsifiability (positive control)

Rust applies the loopback gate in `interfaces.rs:100,103,117`
(`media_interfaces_from`). The mutant forces the loopback flag to be **ignored** —
loopback always admitted regardless of `rtc.enable_loopback_candidate` (the
prohibited **accept-but-ignore** failure mode) — built in an independent target dir
so the clean candidate is never overwritten.

- Mutant harness exit **1**, status **FAIL**, failing exactly the two negative
  profiles (`disabled-pin`, `disabled-precedence`): each flips
  `host_candidate_count` 0→1 and `host_candidate_ips` `[]`→`['127.0.0.1']`.
- The Rust discriminating controls flip false (`negative_profile_absent_rust`,
  `loopback_toggle_discriminator_rust`, `precedence_loopback_discriminator_rust`);
  the other Rust controls stay true (positive presence, WHIP lifecycle, observer
  integrity); all Go controls stay true. The two present profiles still gather
  loopback (count 1, match). This proves the harness distinguishes the loopback-gate
  discriminator from the daemon start/serve lifecycle rather than blanket-failing
  the Rust side.
- `interfaces.rs` restored byte-identical after the mutant; the acceptance target
  dir rebuild reproduces the protected acceptance binary `d1dad0fc`; the clean
  acceptance PASSes (4 profiles, 12 controls) on the **same harness** and **same
  observer**. The mutant FAIL receipt is nested under `positive-control/mutant/`
  and is **not** cited as acceptance evidence.

## Run

```bash
# build the observer with the pinned toolchain (from the fixed Go module)
cd references/livekit-stable
GOTOOLCHAIN=go1.26.0 GOPROXY=off GOWORK=off go build -mod=readonly \
  -o /tmp/c31-build/loopback-observer \
  /Users/zoujiaqing/projects/livekit-rs/tests/interop/rtc-loopback-candidate/observe.go

# run the paired differential (all paths absolute; --output must not pre-exist)
python3 /Users/zoujiaqing/projects/livekit-rs/tests/interop/rtc-loopback-candidate/run-paired.py \
  --go       /tmp/c31-build/livekit-server-go \
  --rust     /Users/zoujiaqing/projects/livekit-rs/target/release/livecloud \
  --observer /tmp/c31-build/loopback-observer \
  --output   <fresh-dir>
```

## Out of scope (honest residual)

The srflx/relay candidate surface, the `udp_port`-mux loopback option
(`webrtc_config.go:137-138`), the `use_external_ip` `GetLocalIPAddresses` discovery
path (`:301` / Rust `nat_discovery_addresses:207`), other platforms/NAT/fallback
profiles, physical-multihomed gathering, and the generated
`--rtc.enable_loopback_candidate` flag are **not** claimed by this leaf. These
residuals stay recorded in the ledger `accepted_scope` (marked `remains open`) and
are not absorbed into this batch's PASS. This harness proves the **launched-MAIN
`rtc.enable_loopback_candidate` host-candidate loopback-gate consumer** surface
only. It is pure loopback — no route/Clash/proxy mutation and no public network
dependency.