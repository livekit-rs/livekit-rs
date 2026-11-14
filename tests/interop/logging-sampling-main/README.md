# SPEC-0077 C21 — launched-MAIN root-logger sampling paired differential

This harness proves that a **launched MAIN** server (not a module-scope oracle)
consumes the global log-sampling config and applies the upstream sampling policy
to the root WARN stream, identically on the locked Go oracle and the Rust
candidate. It closes the `main_server_consumer` and paired-binary
`official_differential_lifecycle` residuals that C11 (module-scope) left open for
the root-scope sampling leaves.

## What it covers (10 ledger leaves)

| YAML leaf | generated flag |
|---|---|
| `logging.sample` | `--logging.sample` |
| `logging.item_sample_initial` | `--logging.item_sample_initial` |
| `logging.item_sample_interval` | `--logging.item_sample_interval` |
| `logging.sample_initial` (inert upstream) | `--logging.sample_initial` (inert upstream) |
| `logging.sample_interval` (inert upstream) | `--logging.sample_interval` (inert upstream) |

`logging.item_sample_seconds` / `--logging.item_sample_seconds` select the
**room-scope** sampler (`WithItemSampler`). The root-scope stimulus here never
enters a `livekit.room_logger` span, so those two leaves are **deferred** (they
stay PARTIAL) and are NOT covered by this harness.

## Stimulus (why it is deterministic on both sides)

`probe.mjs` fires `N` concurrent real `/rtc` WebSocket joins whose JWT carries a
top-level `metadata` claim of 64 bytes, larger than `limit.max_metadata_size=16`.
The size gate runs at the RTC owner stage on **both** sides and emits exactly
**one root WARN per request**:

- **Go** — `RoomManager.StartSession` (`roommanager.go:297`) returns
  `ErrMetadataExceedsLimits` before the room is created; the signal channel closes
  and the HTTP boundary surfaces it as `"connection closed by media"`
  (`rtcservice.go:790`), logged once by `handleError` (`utils.go:85`) as
  `msg="error handling request"`, status 500.
- **Rust** — `check_owner_profile_sizes` (`signal/mod.rs:194`) returns
  `"metadata size exceeds limits"`; `start.rs:46` logs
  `msg="error starting signal session"`.

`signal_relay.connect_attempts=1` guarantees a single attempt (so a single WARN)
per request. With all `N` requests inside one 1-second sampler window, the
admitted count is the upstream formula:

```
admitted = first + floor((N - first) / thereafter)      (N > first)
first      = item_sample_initial  if != 0 else 20
thereafter = item_sample_interval if != 0 else 100
```

(Go `protocol-stable/logger/logger.go:205-216` == Rust
`crates/livecloud/src/logging.rs:39-53`; per-event window semantics in
`crates/livecloud/src/log_sampling.rs`, mirroring `zaputil/sampler.go`.)

### Count-only normalization (scope limit)

The **message text and HTTP status legitimately differ** between Go and Rust
(`"error handling request"`/500 vs `"error starting signal session"`/400). Those
belong to the SIGNAL/HTTP items, **not** to log sampling, and are **not compared**
here. The sampling observable is the **count** of root WARN lines: each side must
independently match the formula, and the two counts must be equal. No comparison
field is deleted — the text/status difference is recorded as an explicit scope
boundary, and the per-attempt WARN divergence at `connect_attempts>1` is recorded
as an OPEN signal residual in the ledger (this harness uses the valid
`connect_attempts=1` config).

### Inert named keys are genuine parity, not a masked defect

`sample_initial` / `sample_interval` (without `item_`) are **accepted-but-unused
UPSTREAM**: Go defines and copies them (`logger/config.go:39/41/82-83/95-96`) but
the root sampler reads only `ItemSampleInitial`/`ItemSampleInterval`; the
`config.go:30` comment naming `SampleInitial` is stale. Rust mirrors this exactly
(`config.rs:863` comment; `logging.rs:42-56` reads `item_*`). The inert-named
profiles set them to 999 and prove the count is unchanged on **both** sides.

## Profiles

| profile | delivery | N | expected | proves |
|---|---|---|---|---|
| `yaml-sample-on-i2-j3` | YAML `sample=true,item_initial=2,item_interval=3` | 12 | 5 | sample on + item keys |
| `yaml-sample-on-i3-j4` | YAML `item_initial=3,item_interval=4` | 15 | 6 | item keys drive the formula |
| `yaml-sample-off` | YAML `sample=false` | 12 | 12 | sample toggle disables sampler |
| `yaml-inert-named` | YAML `sample_initial=999,sample_interval=999,item_initial=2,item_interval=3` | 12 | 5 | named keys inert |
| `yaml-default-sampler` | YAML `sample=true,item_initial=0,item_interval=0` | 25 | 20 | zero → default 20/100 branch |
| `cli-sample-on` | argv `--logging.sample=true --logging.item_sample_initial=2 --logging.item_sample_interval=3` | 12 | 5 | CLI flag input path |
| `cli-inert-named` | argv `--logging.sample=true --logging.sample_initial=999 --logging.sample_interval=999 --logging.item_sample_initial=2 --logging.item_sample_interval=3` | 12 | 5 | CLI named keys inert |

For the `cli-*` profiles the base YAML sets only `logging.json/level` (for stderr
capture); the sampling keys come **only** from argv, so a flag path that dropped
them would fall back to the default and produce a different count (FAIL).

## Run

```bash
# build the locked Go oracle (v1.13.10 e5655ac2) and the Rust candidate first
python3 run-paired.py \
  --go   /path/to/livekit-server-go \
  --rust /path/to/livecloud \
  --output /path/to/evidence-dir
```

Configs are emitted as **JSON** (a valid YAML subset both loaders accept), so the
harness needs no PyYAML dependency. Each side runs a fresh server per profile on
reserved loopback ports (Go 20700+, Rust 20800+), first SIGINT clean exit. The
manifest `execution.json` records per-profile go/rust counts, config sha256,
binary sha256, probe reports and captured stderr. Status
`PASS_LAUNCHED_MAIN_LOGGING_SAMPLING_ROOT` requires every profile to match the
formula on both sides, the cross-side counts to be equal, and all six
cross-profile controls to hold.

## Falsifiability

A positive control hardcodes the Rust root sampler to `20/100`
(`logging.rs:42-51`, ignoring `item_sample_initial/interval`), built in an
**independent** target dir so the clean candidate is never overwritten. That
mutant admits all 12 events on the `i2-j3` profiles (12 ≠ 5) and FAILS the
harness; the byte-identical restore rebuilds to the honest candidate and the
final acceptance PASSes. The mutant FAIL receipt is kept under
`positive-control/mutant/` and is **not** cited as acceptance evidence.