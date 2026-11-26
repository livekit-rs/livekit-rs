# C101 — the CLI command surface, paired against the fixed Go oracle

Lane: `tests/interop/cli-command-surfaces/run-paired.py`
Accepted run: `PASS_LAUNCHED_MAIN_CLI_SURFACES` — 33 paired profiles, 23 legs, 60 controls,
`failed_profiles []`, `failed_controls []`, `failed_legs []`, all seven `status_gates` true.
Falsification: 46 gate cases, `all_discriminated: true`. Re-derivation: `--report` on the filed
receipt reproduces the same verdict with `harness_sha256_now == harness_sha256`.

Ledger rows served (SPEC-0077 tier 4 `main_server_consumer`, tier 5 attested by the launched pair):

| row | profiles |
| --- | --- |
| `builtin_flags:--help` | `help-root-flag`, `help-short-flag`, `unknown-root-flag` |
| `builtin_flags:--version` | `version-long`, `version-short`, `version-alias-binary` |
| `commands:help` | `help-command`, `help-topic-ports`, `help-flag-topic`, `help-topic-unknown`, `unknown-command-needs-keys` |
| `commands:help-verbose` | `help-verbose` (×3 oracle repeats), `help-verbose-with-persistent-flag`, `help-dash-verbose` |
| `commands:ports` | `ports-no-config`, `ports-config-before`, `ports-config-after`, `ports-key-file`, `ports-key-file-b64` |
| `commands:generate-keys` | `generate-keys` (×2 per side) + the `generated_pair_chains_*` legs |
| `commands:create-join-token` | 6 rows + the cross-acceptance legs against both launched daemons |
| `commands:list-nodes` | `list-nodes-empty-registry`, `list-nodes-registered`, `list-nodes-after-exit`, `list-nodes-closed-redis`, `list-nodes-host-default` (scoped) |

## Provenance

| input | value |
| --- | --- |
| Go oracle | `/tmp/c99-build/livekit-server-go` sha256 `4bc2378e636740b29093061036249ffc9ebbef91f4ece1de12177c8dfee1e7dc`, from `references/livekit-stable` @ `e5655ac20267fa5005dccaf3f0e3bfc3ea688c07` |
| Rust acceptance binary | `target/release/livecloud` sha256 `f6fe7f032536c10f075e3a540dff49111d0f2f7afb03e587f2b36be3081a21f0` |
| Rust alias binary | `target/release/livekit-server` sha256 `e437a5bc182f341e…` (used only by `version-alias-binary`) |
| harness | `run-paired.py` sha256 `4dcf37b00fc121793d120b3f2b8bb614ed895ee76b2f2a566dc8499088df5c74` |
| runner | CPython 3.15.0, Darwin arm64 |
| ports | every port allocated free; `7881`/`7882` are refused at allocation and counted before/after (`listeners_before == listeners_after == 0`) |
| redis | one owned `redis-server` on an allocated port (`dbsize 0 → 1`, the residue is `livekit_version`; `nodes` is empty by the end) |

The oracle is not rebuilt by this lane, so `go_is_fixed_oracle` compares its hash to the pinned
constant rather than to a build log. `rust_source_reads` names the files the expectations were read
from: `crates/lk-config/src/cli.rs`, `crates/lk-compat/src/twirp/room_service.rs:20`,
`crates/livekit/src/main.rs`.

## Method

1. **CLI capture.** Each profile runs the same argv on both binaries in a child environment with
   `LIVEKIT_*`/`LIVECLOUD_*`/`OTEL_*` removed, with raw exit codes, raw stream lengths and raw stream
   hashes recorded before any normalization. Truncation (`stdout[:1600]` for the `keys`/`verbose`
   rows) happens only on the way into the receipt, and never on the way into a comparison:
   `lines_sha256`, `rows` and `flag_order` are computed from the full text.
2. **Owned store.** `list-nodes` runs against a redis this lane starts and stops, in three phases:
   empty registry → both daemons registered → both daemons exited.
3. **Launched daemons.** Two servers (oracle and candidate) start from their own fixture configs on
   allocated ports, and the tokens the CLIs minted are sent to `/rtc/validate` and to the Twirp
   `ListRooms` on *both* daemons — that is the tier-5 part: a Go-minted token authenticates against
   the Rust daemon and a Rust-minted token against the Go daemon, each with the recorder grant variant.
4. **Teardown.** SIGINT ×2 then SIGKILL, with `killed_not_exited` recorded; `daemons_exited_without_sigkill`
   and the two `cli_*_empty_after_exit` legs are part of the verdict, not a courtesy.

## What is compared, and what is deliberately not

Masking is a vocabulary, not a licence to make two streams equal:

* a declared volatile mask must have **matched at least once** and must have **left its placeholder**
  in the stored stream (`volatile_never_matched_*`, `volatile_placeholder_missing_*`);
* masked-stream equality is still asserted, and a row whose compared streams are both empty fails
  as `vacuous_both_streams_empty`;
* addresses become `<IP#TTTTTTTT>` — a keyed tag over the committed salt `c101-cli`, letters only so
  the table digit collapse cannot merge two different addresses into one tag. The same address tags
  the same way on both sides, so `table_addresses_differ` still discriminates while the receipt stores
  no machine address (`no_machine_address_in_receipt` sweeps the whole JSON);
* node ids are *kept* (`ND_…`, an ephemeral per-process label) because the row's value is the tie
  between the CLI's table and the daemon's own startup line (`table_ids_tied_to_daemon_logs`);
* metric digits inside `│` rows are collapsed, because the registry refreshes CPU/load between the
  two reads; ids are protected from that collapse (`normalize_table(text, keep_ids=True)`);
* product names are collapsed to `<NAME>` for comparison, and the raw delta is reported instead:
  `livekit-server` vs `livecloud` is 5 bytes on the version line and 10 on the help pages
  (`version_delta_is_name_only`). `version-alias-binary` runs the Rust tree's `livekit-server`-named
  alias and needs no normalization at all — that is the check that the collapse is only absorbing a
  name, not a behaviour.

Four things are refused by design, and each has a gate case:

* **`help-verbose` row order is not compared** unless the oracle is first shown to keep it. Three
  repeats of the same oracle binary produced three different orders (`oracle_order_unstable: true`,
  `oracle_row_sets_equal: true`, 296 rows on both sides) — urfave/cli v3.11.0 emits generated flags in
  map order, so an order claim here would be a comparison against noise.
* **Generated key material is never compared byte-wise.** `generate-keys` is claimed by shape
  (`API` + 12 base62, secret = base62 of 32 random bytes), distinctness across runs, an independent
  HMAC verification of a token minted with the printed pair, refusal with the printed-but-wrong secret,
  and `iss == printed key`. The secret's printed length is 43 or 44 depending on the random value —
  over the two accepted runs the oracle printed 44, 43, 43, 43 and Rust printed 44, 43, 44, 43 — so
  pinning one number would be a flake, not a contract (`generated_secret_lengths_in_range`).
* **JWT claim order is a scoped observation**, claim sets are compared (`claim_set_differs`), and both
  sides' tokens are accepted by both daemons — that is what makes order irrelevant rather than
  hand-waved.
* **The host-default `list-nodes` row is scoped.** It reads whatever registry the built-in
  `127.0.0.1:6379` holds, which is not this lane's fixture. It claims exit parity, the same header
  cells, the same column count, and that the default address was reached; the row count and the
  oracle's `using single-node routing` stderr notice are recorded, not compared
  (`host_default_row_scoped`, `host_default_delta_recorded_not_compared`).

## Measured divergence, pinned rather than averaged

`commands:create-join-token` with `--key-file <yaml>`: the oracle exits **2** with
`panic: reflect: reflect.Value.Set using unaddressable value` (yaml.v3 reaches the nil `Keys` map
through `createToken`), the Rust binary exits **0** and prints a token. The row asserts both sides
exactly (`go_exit`/`rust_exit`, per-side needles, `rust_volatile: ['jwt']`), so a future Rust change
that adopts the panic reddens the row just as a Rust regression would. `--key-file <base64 json>` is
the same command working on both sides (exit 1, `stat : no such file or directory` shape for the
missing-identity variants), which is what shows the panic is about the YAML file channel and not about
`create-join-token`.

## Falsifiability

`--selftest-gate <execution.json>` re-derives the verdict from 44 named mutations (46 cases with the untouched baseline and
the no-write check) and writes
`gate-falsification.json` next to the receipt without touching it (`sweep_writes_no_receipt` is one of
the cases; the first version of this mode overwrote the receipt it was auditing). Examples, all of
which came back red:

| case | mutation | what reddens |
| --- | --- | --- |
| `exit_drift` | a `help` run's exit → 1 | `declared_exit_go` |
| `exit_split_unpinned` | one side's `generate-keys` exit → 1 | `exit_split` |
| `refusal_prints_nothing` | both `list-nodes-closed-redis` streams blanked | `vacuous_both_streams_empty` |
| `volatile_mask_never_matched` | `api_key` count → 0 | `volatile_never_matched_go:api_key` |
| `secret_sample_missing` | a run's measured secret lengths → `[]` | `generated_secret_lengths_in_range` |
| `oracle_repeat_missing` | third `help-verbose` oracle sample dropped | `oracle_repeat_missing` |
| `persistent_verbose_order_claim_returns` | a second sample added to the 1-run row | `persistent_verbose_order_scoped` |
| `scoped_row_loses_its_evidence` | the scoped row's Go stdout blanked | `host_default_delta_recorded_not_compared` |
| `cross_acceptance_breaks` | the Go→Rust `/rtc/validate` 200 → 401 | `leg_validate_go_minted_on_rust_daemon` |
| `negative_control_accepts` | an unauthenticated join accepted | `leg_noauth_rejected_by_go` |
| `registry_leak_after_exit` | a node row appended after shutdown | `registry_emptied_after_exit` |
| `host_ports_were_touched` | `listeners_after['7881']` +1 | `off_limit_ports_untouched` |
| `store_phase_never_ran` | the daemon records dropped | `daemon_readiness_observed` |
| `binary_identity_drifted` | the recorded Go hash replaced | `binaries_unchanged` |

## Honest residuals

* The RSA/Ed25519 key channels (`--key-file` PEM, `--config` `key_pem`/`keys_yaml`) and the
  `--region`/`--node-ip`/`--udp-port` flag channels are not exercised here; they belong to the lane-C
  follow-ups recorded in C100.
* `commands:list-nodes` in a distributed (non-redis) configuration, and `list-keys`/`status`-style
  surfaces beyond these eight rows, are out of scope: this lane covers the eight ledger ids in the
  table above and nothing else.
* The `list-nodes-host-default` row's registry content is host state; only its shape is claimed.
* `generated_pair_chains_*` verifies a token offline against the printed secret; the daemon-side
  acceptance of a *generated* (as opposed to fixture) key pair is not claimed, since generating a key
  does not register it with a running server.
* The oracle's `help-verbose` ordering is nondeterministic in principle; three samples show it, and a
  future urfave release could make it stable, which would only let the lane claim more than it does now.

## Running it

```sh
python3 tests/interop/cli-command-surfaces/run-paired.py \
    --go /tmp/c99-build/livekit-server-go \
    --rust target/release/livecloud \
    --output /tmp/c101-laneCLI/run
python3 tests/interop/cli-command-surfaces/run-paired.py --selftest-gate /tmp/c101-laneCLI/run/execution.json --output /tmp/c101-laneCLI/run
python3 tests/interop/cli-command-surfaces/run-paired.py --report /tmp/c101-laneCLI/run/execution.json --output /tmp/c101-laneCLI/run
```

`--skip-store` runs the CLI captures without the redis/daemon phases; it labels its receipt
`PARTIAL_OFFLINE_CLI_ONLY_NOT_ACCEPTANCE` rather than passing a subset of the acceptance gates, and
`store_phase_never_ran` is what keeps the daemon controls from reading as true over an empty set.
`refuse_filed_slot()` stops any of these modes from rewriting a receipt under `server/evidence/`.
