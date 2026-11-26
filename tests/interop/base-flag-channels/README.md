# C102 — the base-flag channels, judged by what the launched daemon does

Lane: `tests/interop/base-flag-channels/run-paired.py`
Accepted run: `PASS_LAUNCHED_BASE_FLAG_CHANNELS` — 37 profiles, 759 legs,
49 controls, `failed_profiles []`, `failed_controls []`, `failed_legs []`, all
9 `status_gates` true. Falsification: 61 gate cases, `all_discriminated: true`.
Re-derivation: `--report` on the filed receipt reproduces the same verdict with
`harness_sha256_now == harness_sha256`.

Ledger rows served (SPEC-0077 tier 4 `main_server_consumer`, tier 5 attested by the launched pair):

| row | profiles |
| --- | --- |
| `base_flags:--region` | `region-flag`, `region-env`, `region-flag-beats-env`, `region-unset`, `region-empty` |
| `base_flags:--node-ip` | `node-ip-v4`, `node-ip-v6`, `node-ip-env`, `node-ip-flag-beats-env`, `node-ip-garbage`, `node-ip-empty` |
| `base_flags:--udp-port` | `udp-single`, `udp-env`, `udp-range`, `udp-flag-merges-into-yaml-range`, `udp-garbage` |
| `base_flags:--redis-host` | `redis-host-flag-beats-yaml`, `redis-host-env`, `redis-host-flag-broken` |
| `base_flags:--redis-password` | `redis-password-absent`, `redis-password-flag`, `redis-password-env`, `redis-password-wrong`, `redis-password-on-open-store`, `redis-password-empty` |
| `base_flags:--turn-cert` / `--turn-key` | `turn-cert-key-flags`, `turn-cert-key-flags-when-disabled`, `turn-cert-without-key`, `turn-key-without-cert`, `turn-swapped-pair`, `turn-missing-file` |
| `base_flags:--cpuprofile` / `--memprofile` | `profiles-both`, `profiles-cpu-only`, `profiles-mem-only`, `profiles-mem-after-refusal`, `profiles-empty-values`, `profiles-after-refusal` |

## Provenance

| input | value |
| --- | --- |
| Go oracle | `/tmp/c99-build/livekit-server-go` sha256 `4bc2378e636740b29093061036249ffc9ebbef91f4ece1de12177c8dfee1e7dc`, from `references/livekit-stable` @ `e5655ac20267fa5005dccaf3f0e3bfc3ea688c07` |
| Rust acceptance binary | `target/release/livecloud` sha256 `f6fe7f032536c10f075e3a540dff49111d0f2f7afb03e587f2b36be3081a21f0` |
| harness | `run-paired.py` sha256 `fff0615af856f95fc329a5aee3bbafcae2ce795474edde88176c27ca10086a22` |
| runner | CPython 3.15.0, Darwin arm64 |
| ports | allocated free per run; `7881`/`7882` and the host store `6379` are counted before/after (`listeners_bounds`, all `before == after`) and never named in a profile |
| stores | two owned `redis-server` instances on allocated ports, one plain and one `requirepass`; flushed before every profile and re-checked at teardown (`owned_stores_left_clean`) |

`rust_source_reads` in the receipt names the files the expectations were read from, and
`reference.upstream_reads` names the Go lines: `cmd/server/main.go:52-101` (the flag table) and
`:267-294` (profiles created after `ValidateKeys`), `pkg/config/config.go:1106-1141` (the
`IsSet`-guarded overrides), `pkg/service/turn.go:222-231` (`tls.LoadX509KeyPair`).

## Method

A flag is claimed by the effect the *running* process has, not by the field it fills. Each profile
starts both binaries on their own allocated ports with one argv/env difference, waits for the daemon to
answer its own HTTP port, then reads the effect back through a channel the daemon did not choose:

* **registry** — `list-nodes` against the store this profile uses, while the daemon is alive, after it
  exits, and (for the store-channel rows) a config-only read that must *refuse*;
* **sockets** — the UDP/TCP endpoints the process actually holds, taken while it lives;
* **handshake** — a real TLS client against the TURN/TLS port, compared to the DER digest of the file
  the flag named;
* **filesystem** — the profile files the flag named, checked for existence, size and gzip pprof magic;
* **exit** — the raw code, recorded per side before any normalization, plus the raw stream lengths and
  hashes (`raw_streams_recorded`, `argv_never_raw`).

Teardown is SIGINT and the lane asserts the daemon ended on its own (`refusals_self_ended`,
`sigkill_never_used`), that the registry row it wrote is gone, and that the retained `livekit_version`
marker is the only key left.

## What is compared, and what is deliberately not

Ports become role tokens (`<HTTP> <STORE> <DECOY> <AUTH> <UDP1..3> <YRA> <YRB> <TLS>`) because the lane
knows which port plays which role — that keeps "the daemon connected to the YAML address" distinguishable
from "the daemon connected to the flag address" in the stored line. Addresses become letter-keyed tags
`<IP#TTTTTTTT>` over the committed salt `c102-flags`; the whole receipt is swept by
`no_machine_address_in_receipt`, and the profile JSON is swept by `off_limits_never_named` and
`host_store_never_named` so no row can even mention the developer host.

Three things are deliberately not compared as raw bytes, and each has a gate case holding the line:

* **timestamps and durations are masked to `<TS>`/`<DUR>` before comparison** and never claimed: the
  elapsed fields in the startup line and the CLI's `Started At` cell move between the two runs by
  construction;
* **profile file sizes are recorded, not compared** — they are runtime state; the claim is the stream
  type and the existence pair (`profiles-both::go:heap_gzip` and its siblings);
* **the `list-nodes` table is located by header name, never by column index**, so a column moving
  reddens the row instead of silently reading a neighbour (`node_columns_from_header`,
  `cli_table_schema_paired`). C102 counts 14 named columns; C101's receipt reports 15 because its
  `columns` field counts the `│` delimiters of the same row rather than the cells between them. Both
  numbers describe one table, and neither lane claims a bare width without the header names that produce
  it.

## Measured findings, pinned rather than averaged

Four behaviours were written from assumption in the first draft, failed against the real binaries, and
are now claims about what was measured:

1. **A clean shutdown does not empty the store.** Both runtimes leave `livekit_version` behind after
   SIGINT and remove `nodes`. A `DBSIZE == 0` claim is therefore useless — it cannot tell a deregistered
   node from a leaked one. The rows claim the key *names* plus the presence and removal of the `ND_…`
   field the CLI had just printed.
2. **`--udp-port` merges into a YAML range instead of overriding it.** `PortRange.UnmarshalString`
   (mediatransportutil `@v0.0.0-20260821083140-f234b534b095/pkg/rtcconfig/config.go`) sets `End` only when
   the string contains `-`; the single-port branch writes `Start` and returns. With
   `rtc.udp_port: <YRA>-<YRB>` in the config and `--udp-port <UDP1>` on argv the daemon binds
   `<UDP1>..<YRB>` on *both* runtimes. The row is named for that, and `udp_flag_merge_pinned` reddens if
   a future release turns it into a clean override.
3. **The startup line changes shape with the flag.** `service/server.go:288-298` prints
   `"rtc.portUDP": {"Start":X,"End":Y}` when `RTC.UDPPort` is valid and `"rtc.portICERange": [50000, 60000]`
   otherwise, so `udp-garbage` claims the ICERange form and — measured — zero UDP sockets, because the
   range is bound lazily.
4. **`--cpuprofile`/`--memprofile` are honoured, not rejected.** The ledger carried
   `REJECTED_WHEN_ACTIVE` for both; on a launched run each writes a real gzip pprof stream, and neither is
   written when the start refuses (`cmd/server/main.go:257-294` creates them after `ValidateKeys`).

Two divergences are Go-vs-Rust behaviour and are held by controls rather than averaged away:

* **TURN refusal ordering.** A refusal on TLS material leaves the oracle with `[]` keys (the TURN server
  is built while the wire graph is constructed: `pkg/service/wire.go:300` → `turn.go:222-228`, before
  `RedisStore.Start()` at `redisstore.go:93-113`) and Rust with `['livekit_version']`
  (`crates/livecloud/src/server.rs:604-624` starts the store graph, `turn::start` runs at `:650`). Neither
  registers a node. `turn_refusal_marker_ordering_pinned` asserts that exact split.
* **NOAUTH wording.** The daemon-side sentences differ (the Rust client negotiates RESP3 `HELLO` first, so
  it reports `NOAUTH HELLO must be called with the client already authenticated`), while on the
  `list-nodes` path both runtimes print the identical short sentence. The claim is the *refusal*, with the
  per-side needles pinned; the sentence is recorded.

`--node-ip` on garbage input also diverges in wording only: Rust warns `ignoring --node-ip/NODE_IP value`
(`crates/lk-config/src/load.rs:180`) where `config.go:1132` discards the parse error, and both end on the
same auto-detected registry cell — which is what `node_ip_pair_discriminated` claims.

## Falsifiability

`--selftest-gate <execution.json>` re-derives the verdict from 61 named mutations and writes
`gate-falsification.json` next to the receipt without touching it; the last case compares the receipt's
sha256 before and after the sweep. Examples, all of which came back red:

| case | why it has to redden | what reddens |
| --- | --- | --- |
| `exit_drift_on_go` | a launched row whose exit moved no longer matches the contract | `region-flag::go:exit` |
| `region_cell_drift` | the Region column is the only projection --region has | `region-flag::go:region` |
| `udp_extra_socket` | the YAML range the flag beat must stay unbound | `udp-single::go:udp` |
| `udp_flag_override_converged` | a run where the flag really replaces the YAML range is a different contract than the merge this row pins, so the row and the ledger note have to be re-read | `udp_flag_merge_pinned` |
| `tls_leaf_swapped` | the handshake has to present the file the flag named, not another certificate | `turn-cert-key-flags::rust:tls-cert` |
| `store_row_never_appeared` | without a registry key the CLI table could have come from anywhere | `region-flag::go:nodes-key-alive` |
| `cli_bare_read_succeeds` | if the config-only read works too, the flag channel claim on that row is empty | `redis-host-flag-beats-yaml::go:cli-bare-exit` |
| `profile_not_gzip` | a heap profile that is not a decompressable pprof stream is not the same contract | `profiles-mem-only::go:heap_gzip` |
| `turn_refusal_marker_ordering_converged` | the store-marker ordering is a pinned divergence, so a run where both sides mark is a new contract the rows have to be re-read against rather than an unchanged pass | `turn_refusal_marker_ordering_pinned` |
| `shutdown_leaves_the_row` | a dead node left in the registry is exactly what a DBSIZE count could not see | `node-ip-v4::go:marker-retained` |
| `host_lan_address_leaks` | an RFC 1918 address appearing in a stored log is a privacy break | `no_machine_address_in_receipt` |
| `row_without_contrast` | the row is only a claim about --region if its five shapes differ; copying one row onto the rest leaves nothing to discriminate | `row_has_contrast_pair::base_flags:--region` |
| `candidate_hash_drift` | a receipt that records a digest other than the pinned candidate cannot be cited by the ledger | `recorded_candidate_matches_pinned` |
| `sweep_writes_no_receipt` | the audit runs on the filed artifact, so it may not alter it | `the receipt digest itself` |

Three things were written wrong first and are recorded here because they are the reason the sweep
exists: `machine_address_leaks` appended `192.0.2.55`, which is documentation space and therefore allowed
by the very control it was meant to break (it could never redden); `node_columns_from_header` pinned a width
of 15 while both CLIs print 14, so the injection that "broke" it was the receipt's own real value; and the
per-side column leg (`region-flag::go:region`) never existed at all — `evaluate` looked up `claims['region']`
where the row spells the claim `region_col`, and `nodes.get('region_col')` where the parser returns `region`.
Doubly unreachable, so no gate case could name it. Nothing passed that should not have: the absolute cell
value was still held by `region_pair_discriminated`. What was missing is the per-side leg, and 18 of them now
exist and agree with the recorded cells. Leg ids also dropped a redundant `/<side>` suffix (every leg name
already embeds its side, so no `FAILED_LEG` case could match its target), and repeated needles in one
`log_all` claim now get a `#N` suffix, because two legs sharing a name lets a passing needle hide a failing
one in the `verdict_stable_across_rederivation` map.

## Honest residuals

* `--redis-host` is never driven with an empty value: an empty address resolves to the developer host
  store on `6379`, which this lane must not touch. The empty-vs-unset guard is carried on
  `--redis-password` instead, whose empty value is refused by the owned store.
* The RSA/Ed25519 key channels (`--key-file` PEM, `key_pem`, `keys_yaml`) and the TURN *secret* channel
  are not exercised here; they belong to the follow-ups recorded in C100/C101.
* The webtransport flag rows were never passed on argv, so no row in this lane claims them.
* `--cpuprofile`/`--memprofile` have no env source, so no env row is claimed for them.
* The TURN/TLS claim is a handshake with a raw client against `tls_port`; it does not exercise a live
  relay allocation, which is the ICE/TLS behaviour lane's scope.
* Node ids (`ND_…`) are deliberately kept rather than masked: they are the tie between the CLI's table
  and the daemon's own startup line, and they are per-process ephemeral labels.

## Running it

```sh
python3 tests/interop/base-flag-channels/run-paired.py --record --out /tmp/c102-lane
python3 tests/interop/base-flag-channels/run-paired.py --selftest-gate /tmp/c102-lane/execution.json \
    --out /tmp/c102-gate
python3 tests/interop/base-flag-channels/run-paired.py --report /tmp/c102-lane/execution.json \
    --out /tmp/c102-report
```

`--only a,b` runs a subset and labels the receipt `PARTIAL_SUBSET_NOT_ACCEPTANCE` — it cannot print the
acceptance label, and `no_development_subset` is what keeps a subset from reading as a full sweep. The
pair-identity controls (`binaries_unchanged`, `go_is_fixed_oracle`, `rust_is_acceptance_candidate`,
`recorded_*_matches_pinned`) re-hash both images at the end of the run, so a receipt cannot be cited after
the tree moved underneath it.
