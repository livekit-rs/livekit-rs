# SPEC-0077 C99 — launched-MAIN `rtc.node_ip` family slots and host-candidate rewrite

**Batch:** C99 (pure verification — no production source change on this leaf)
**Leafs:** `YAML:rtc.node_ip`, `generated_flags:--rtc.node_ip.ipv4`, `generated_flags:--rtc.node_ip.ipv6`
**Tier:** `main_server_consumer` (a running daemon's own advertised candidates, not a loader projection)
**Verdict:** `PARTIAL_LAUNCHED_MAIN_NODE_IP_WITH_FENCED_DIVERGENCES` — 14/14 parity rows
match cross-side, 5/5 recorded Rust divergences still reproduce, 22/22 cross-profile controls true.
"Partial" is the leaf's honest ceiling in this batch: the five fenced rows are *measured*
divergences, not untested gaps (see [Fenced divergences](#fenced-divergences)).

This harness proves what the launched server actually **advertises in the SDP answer**
when `rtc.node_ip` is set through each of its three arrival channels, and what it does
with text that never parses.

## Consumers under test

| side | anchor | semantics |
| --- | --- | --- |
| Go oracle (v1.13.10 @ `e5655ac`, pinned `mediatransportutil-f234b534b095`) | `pkg/rtcconfig/config.go:231-263` | `NodeIP.UnmarshalYAML` forwards **only** `value.Value` to `UnmarshalString`, so the YAML *mapping* form (`node_ip: {ipv4: …}`) arrives as the empty string and is silently ignored; the scalar form splits on commas (max 2 parts), `net.ParseIP`s each part after trimming spaces (`:245`) and stores `ip.String()`, so `::ffff:a.b.c.d` lands in the **V4** slot already normalised (`ip.To4() != nil` at `:249`). Refusals: `invalid node ip …, should be <ipv4>,<ipv6>`, `invalid ip …`, `multiple ipv4/ipv6 addresses provided: X and Y`. |
| Go | generated `--rtc.node_ip.ipv4` / `--rtc.node_ip.ipv6` (struct reflection in `pkg/config`) | write the **raw text** into the named leaf, bypassing `UnmarshalString` entirely, so unparseable and zone-qualified text survives. |
| Go | `pkg/rtcconfig/webrtc_config.go:91-116`, `nat1To1AddressRewriteRules:257-286` | with a node IP set and not auto-generated the daemon logs `using explicit node IP for NAT1To1Ips` and installs a **catch-all host rewrite** (`external/local` text becomes a local-matched rule); Pion keys each rule by the external address's **own family**. |
| Rust | `crates/lk-config/src/ports.rs:98,110-140` | same scalar contract, plus IPv4-mapped normalisation into the V4 slot. |
| Rust | `crates/livecloud/src/interfaces.rs:130-142`, gated by `server.rs:528` (`use_node_ip_mapping && !use_external_ip`) | `IpAddr::V4(_) => rtc.node_ip.v4`, `IpAddr::V6(_) => rtc.node_ip.v6` — the same family-keyed catch-all rewrite, applied to every admitted base address. |
| both | `pkg/service/server.go:309` / `crates/livecloud/src/cli_nodes.rs:195-198` | `PrimaryIP()` prefers V4, which is the `nodeIP` field of the readiness line. |

## Method

[`observe.go`](observe.go) is a **separate copy** of the Pion WHIP driver (not the C30 or
C31 observer — anti-splicing), rebuilt from [`observe.go`](observe.go) whose sha256 is
recorded with the driver binary's. For each profile it:

- writes the **same YAML** to both sides (`rtc.node_ip` scalar / mapping form, or the
  generated dotted flags on the command line), `enable_loopback_candidate: true`,
  `use_external_ip: false`, `port_range_start/end: 50000-60000`, `logging.level: info`;
- optionally pins `rtc.interfaces.includes=@loopback` (`--pin-loopback`, default on) so
  the only name-admitted interface is `lo0`;
- launches one immutable daemon, hashes it before and after (anti-tamper), strips
  `OTEL_`/`SSL_CERT_`/`LIVEKIT_`/`NODE_IP`/`UDP_PORT` from the child env;
- reads the `{"nodeID": …, "nodeIP": "…"}` readiness line and the
  `using explicit node IP for NAT1To1Ips {"ip": {…}, "advertiseInternalIP": …}` line;
- drives an authenticated WHIP **Create** with a Pion offer that carries **no interface
  filter and no rewrite of its own**, extracts `a=candidate:… typ host` IPs from the
  answer, then **DELETE**;
- shuts the daemon down with the escalating sequence the upstream actually needs
  (SIGTERM → SIGTERM → SIGKILL, recording `second_sigterm_sent` / `sigkill_required`)
  and re-checks exclusive port rebinding.

Because the driver has no interface filter of its own, **every recorded host candidate
originates from the daemon's consumer of `rtc.node_ip`** — the observable is
mutant-sensitive, not a config dump.

### Discriminator (empirically grounded, load-bearing)

`rtc.node_ip: 127.0.0.1` alone proves nothing: the loopback interface already holds that
address, so a rewrite that was silently dropped looks identical. The harness therefore
pins `@loopback` **and** rewrites to addresses `lo0` does not hold:

- `::ffff:127.0.0.2` → answer advertises `127.0.0.2`, never `127.0.0.1`;
- `::ffff:203.0.113.7` (TEST-NET-3) → answer advertises `203.0.113.7`;
- `2001:db8::7` (V6-only) → answer **keeps** `127.0.0.1`, which is the family-scoping
  witness: a V6 node IP must not touch an IPv4 candidate.

Both were verified on both sides before the profile set was fixed. The unpinned pair
profile does the complementary job: with no interface filter the daemon gathers its real
surface, and the catch-all rewrite masks **every** admitted IPv4 candidate, so no host
address of the machine reaches the SDP answer (recorded as `<discovered-address>`).

## Parity rows (14)

| Profile | Stimulus | Measured on both sides |
| --- | --- | --- |
| `yaml-v4-loopback` | `node_ip: 127.0.0.1` | V4 slot set, rewrite installed, candidate `127.0.0.1` |
| `yaml-v4-mapped-loopback` | `node_ip: ::ffff:127.0.0.2` | normalises into **V4** slot, candidate becomes `127.0.0.2` |
| `yaml-v4-mapped-doc` | `node_ip: ::ffff:203.0.113.7` | candidate `203.0.113.7` (documentation range, never dialled) |
| `flag-v6-mapped-doc` | `--rtc.node_ip.ipv6=::ffff:203.0.113.7` | raw text stays in the *ipv6* leaf, `nodeIP` reports it verbatim, yet the **IPv4** candidate is the one rewritten (family of the external address) |
| `yaml-v6-only` | `node_ip: 2001:db8::7` | V4 slot empty, candidate stays `127.0.0.1` |
| `yaml-pair-v6first-precedence` | `node_ip: 2001:db8::7,127.0.0.2` | V6 text first in the string, V4 still wins `PrimaryIP()`, rule order and candidate |
| `yaml-pair-spaces-trimmed` | `node_ip: "127.0.0.1 , ::1"` | `strings.Trim` per part (`config.go:245`) — parses, no refusal |
| `yaml-mapping-form-ignored` | `node_ip: {ipv4: 127.0.0.2}` | **both sides ignore it**: no rewrite log line, candidate `127.0.0.1`, `nodeIP` is the auto-discovered primary |
| `unpin-pair-both-families` | no pin, `node_ip: 203.0.113.7,2001:db8::7` | every admitted v4 candidate masked to `203.0.113.7`; no machine address in the answer |
| `unpin-no-node-ip-baseline` | no pin, no `node_ip` | absent-control: no rewrite line, loopback candidate survives as gathered (3 candidates both sides) |
| `refusal-invalid-ip` | `node_ip: 1.2.3` | exit 1, `could not parse config: invalid ip 1.2.3`, no readiness line, ports free |
| `refusal-three-parts` | `node_ip: 1.1.1.1,::1,::2` | exit 1, `invalid node ip …, should be <ipv4>,<ipv6>` |
| `refusal-duplicate-v4` | `node_ip: 1.1.1.1,5.6.7.8` | exit 1, `multiple ipv4 addresses provided: 1.1.1.1 and 5.6.7.8` |
| `refusal-duplicate-v6` | `node_ip: ::1,::2` | exit 1, `multiple ipv6 addresses provided: ::1 and ::2` |

Each refusal row is additionally asserted to leave **no listener** (HTTP refused, ports
rebindable, exit code 1) and a *distinct* message, so a single combined catch-all error
cannot satisfy the family.

## Fenced divergences (5)

These rows are recorded, not promoted. `match` for a fence means: the Go oracle behaved as
declared, the Rust side still behaves exactly as recorded, and at least one recorded field
genuinely differs (`fences_have_a_witness_field`, 3 distinct divergence shapes).

| Profile | Input (raw flag channel) | Go oracle | Rust candidate |
| --- | --- | --- | --- |
| `fence-flag-v4-zone` | `--rtc.node_ip.ipv4=127.0.0.1%lo0` | starts, then **refuses every WHIP session with 503** (`invalid address rewrite mapping`) | starts and answers `201` with the raw `127.0.0.1` candidate |
| `fence-flag-v6-zone` | `--rtc.node_ip.ipv6=fe80::1%lo0` | same 503 | same silent-ignore |
| `fence-flag-v4-nonip` | `--rtc.node_ip.ipv4=not-an-ip` | same 503 | same silent-ignore |
| `fence-flag-v4-slash-local` | `--rtc.node_ip.ipv4=203.0.113.7/127.0.0.1` | rewrites the matching local candidate → answer advertises `203.0.113.7` | text never parses as an address, rule dropped → answer advertises `127.0.0.1` |
| `fence-flag-v4-broadcast` | `--rtc.node_ip.ipv4=255.255.255.255` | advertises `255.255.255.255` as the host candidate | ICE refuses it as a local candidate → answer carries **no** host candidate, a client gets nothing to dial |

The three 503 rows are input variants of **one** behaviour: Rust ignores unusable text
(`crates/lk-config/src/lib.rs:773-809`, which asserts "invalid node_ip must be ignored" as
a deliberate earlier decision), while the oracle installs the rule and fails per session.
A parity fix therefore changes documented Rust behaviour and needs its own batch plus
`lk-config` test and spec updates — it is out of scope for a verification batch.

This batch also **corrects SPEC-0074 §C30**: raw invalid `node_ip` text does not merely
"never match" a candidate. Pion validates the rewrite mapping per session at
`SetRemoteDescription`, so the oracle turns every WHIP/peer session into a **503 with an
`invalid address rewrite mapping` body** while the daemon stays up. That is a stricter
consequence than the earlier note recorded.

## Normalization and scope limits

- Compared fields (`FIELDS`) are the daemon's own observable outputs: WHIP statuses, host
  candidate IP set, slot/primary log text, refusal text, exit code, port release,
  `binary_unchanged`. Byte-equal SDP is **not** compared (foundation, priority, ufrag,
  port and session id are nondeterministic).
- **No IPv6 host candidate is gathered in this configuration on either side** (`v6_advertised`
  is `[]` in all 19 rows), so the IPv6 dimension is observed through the slot log,
  `PrimaryIP()` text and the *absence* of a v4 rewrite — not through an advertised v6
  candidate. This is a scope boundary of the loopback fixture, not a claim about v6.
- Candidate *connectivity* is not claimed here: that is the SDK lane
  ([`sdk-node-ip-candidate`](../sdk-node-ip-candidate/README.md)).
- Addresses outside the loopback/documentation allowlists are redacted to
  `<discovered-address>` before the receipt is written, and the receipt itself is re-scanned
  (`receipt_has_no_machine_address`) so a real LAN address cannot survive into evidence.
- No comparison field was deleted; each limit above is an explicit boundary recorded in
  `execution.json`.

## Falsifiability (positive control)

The Rust-side rewrite lives in `crates/livecloud/src/interfaces.rs:130-142`. The mutant
changes the v4 arm to `IpAddr::V4(_) => None` — the prohibited **accept-but-ignore**
failure mode — and is built in a separate target dir
(`CARGO_TARGET_DIR=/tmp/c99-build/mutant-b`, sha256 `f2d488b0…`, so the cross-target-dir
hash is recorded as the mutant's own identity and never compared against the acceptance
binary).

Mutant run: status **FAIL**, and it fails *only* the rows that depend on the rewrite being
consumed:

- parity: `yaml-v4-mapped-loopback`, `yaml-v4-mapped-doc`, `flag-v6-mapped-doc`,
  `yaml-pair-v6first-precedence`, `unpin-pair-both-families`;
- controls: `rewrite_changes_advertised_text_rust`,
  `physical_addresses_masked_when_unpinned_rust`, `fences_all_reproduce`
  (`fence-flag-v4-broadcast` stops behaving as recorded);
- still green: every refusal row, `yaml-mapping-form-ignored`,
  `unpin-no-node-ip-baseline`, `yaml-v6-only` (family scoping is untouched), the four other
  fences, and **all Go-side controls**.
- `yaml-v4-loopback` staying green is itself the point: its rewrite target equals the
  gathered address, which is exactly why the promoted rows use `127.0.0.2` / `203.0.113.7`.

`interfaces.rs` was restored byte-identical afterwards and the acceptance tree rebuild
reproduces `26219632…`; the mutant FAIL lives under `positive-control/mutant-b/` and is
**not** cited as acceptance evidence.

## Port reservations and retries (C100)

The driver used to reserve its loopback ports with `net.Listen("127.0.0.1:0")`. That returns a port
from the OS ephemeral band — measured on this host as `net.inet.ip.portrange.first = 49152` — which
is the same pool the kernel draws on for an outbound socket's source port, and this profile opens a
fresh client socket every 20 ms while it waits for readiness. A reserved number is therefore free
only until the next allocation takes it, and the first C100 run lost exactly that race:

```
listen tcp :50975: bind: address already in use
```

The daemon died, the readiness loop recorded `refused: true` / `server_exit: 1`, and a fence whose
oracle is supposed to *start* scored as a non-reproducing divergence (`failed_fences:
[fence-flag-v4-nonip]`, `failed_controls: [clean_lifecycle_go, fences_all_reproduce]`). Two changes.
Reservations come from an explicit band below the ephemeral range (`reservePort`: `41000 +
4·(pid%150)`, scanning up to 600 ports, falling back to the allocator), and a daemon that dies with
a bind conflict returns `errPortConflict` *before* any refusal datum is written, so the profile is
relaunched (≤3 times) into `work/<profile>/port-retry-N/` and the winning streams are promoted to
the canonical `server.log` / `server.stdout` names. `port_conflict_retries` is recorded per row —
outside the compared field set, so a harness event cannot become a cross-side divergence — and
`conflict_retries_reached_readiness` asserts that a relaunched daemon really reached its readiness
line. Retries exhausted exits the observer with an error, and `run-paired.py` turns any observer
error into a hard stop rather than a row. Proven both ways with a mutant driver that squats the
reserved port after releasing it: one collision absorbed (`port_conflicts_recovered:
{'yaml-v4-loopback': {'go': 1, 'rust': 1}}`, `match=True`, control true) and the same injection with
the retry bound at 0 stopping the lane with `reserved loopback port was claimed before the daemon
bound it: listen tcp 127.0.0.1:41432 …`.

## Run

```bash
# driver, built from the pinned Go module (fixed oracle tree, no network)
cd references/livekit-stable
GOTOOLCHAIN=go1.26.0 GOPROXY=off GOWORK=off go build -mod=readonly \
  -o /tmp/c99-build/node-ip-observer \
  /Users/zoujiaqing/projects/livekit-rs/tests/interop/rtc-node-ip-family/observe.go

# paired differential (--output must not pre-exist; 19 profiles run sequentially
# because the UDP range 50000-60000 is shared)
python3 /Users/zoujiaqing/projects/livekit-rs/tests/interop/rtc-node-ip-family/run-paired.py \
  --go       /tmp/c99-build/livekit-server-go \
  --rust     /Users/zoujiaqing/projects/livekit-rs/target/release/livecloud \
  --observer /tmp/c99-build/node-ip-observer \
  --output   <fresh-dir>

# re-derive the verdict from a persisted receipt without starting a daemon
python3 run-paired.py --report <fresh-dir>/execution.json
```

### Verdict fields

`--report` recomputes `status` as the conjunction of six named conditions and persists them,
so a FAIL states which one broke instead of leaving the reader to infer it from the row lists:

- `status_gates` — `binaries_unchanged`, `reference_commit_matches`, `go_is_fixed_oracle`,
  `no_failed_parity`, `no_failed_fences`, `no_failed_controls`.
- `binary_hash_now` — the `--go`/`--rust` hashes at re-derivation time, beside the
  `binaries.*.sha256` recorded during the launch. Rebuilding either binary is the only way
  `binaries_unchanged` can flip on a receipt that was green when it was written.
- `unmet_gates` (stdout summary) — the names that failed. `failed_parity`, `failed_fences`
  and `failed_controls` all empty with `unmet_gates: ["binaries_unchanged"]` means every row
  reproduced and only the provenance check drifted: stale identity, not a behaviour finding.

## Out of scope (honest residual)

- `use_external_ip: true` rows: they depend on public STUN discovery, would advertise a
  real public address, and cannot be reproduced offline; excluded from the acceptance set.
- srflx/relay candidate surfaces, TURN, ICE-lite, mDNS, `advertise_internal_ip=true`
  (`ICEAddressRewriteAppend` mode), the `udp_port` mux path (`webrtc_config.go:131-171`)
  and multi-homed gathering are **not** claimed by this leaf.
- Only the two generated `--rtc.node_ip.*` flags and the YAML scalar/mapping forms are
  driven; the `LIVEKIT_RTC_NODE_IP*` env channel is a separate ledger row.
- The five fenced rows keep their own follow-up items; nothing in this batch closes them.

Pure loopback: no route/interface/proxy mutation and no public network dependency.
