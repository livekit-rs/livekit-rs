# Actual pinned go-version oracle for `workers::version`

> The absolute paths below are the recorded batch 22e run on the measuring host. Substitute your own
> checkout for `/Users/zoujiaqing/projects/livekit-rs` and any writable directory for the
> `/private/tmp/...` staging paths; every driver takes them as flags.

Consumer: `crates/lk-compat/src/workers/version.rs:161`
(`all_sixty_actual_go_package_rows_match_validity_and_update`, `#[ignore]`),
which reads `WORKERS_VERSION_GO_RESULTS` as a JSON **array** of exactly 60 rows
`{existing, current, valid, update}`.

## What is actually executed

`main.go` imports the real pinned `github.com/hashicorp/go-version` v1.9.0
(`references/go-modules/hashicorp-go-version-v1.9.0`, reached through a local
`replace`) and calls `NewVersion` / `GreaterThan` — the same pair the upstream
server calls in `RedisStore.Start`
(`references/livekit-stable/pkg/service/redisstore.go:107-109`). No comparison
logic is reimplemented and no expected boolean is hand-authored.

Operand preparation copies upstream exactly: the stored value is read, `""` is
replaced by `"0.0.0"` *before* parsing (`redisstore.go:104-106`), and both
`NewVersion` errors are discarded (`redisstore.go:107-108`).

## Where the 60 vectors come from

`prepare.py` extracts them from the pinned upstream
`references/go-modules/hashicorp-go-version-v1.9.0/version_test.go` at staging
time, after verifying its sha256 against `SOURCE.json`:

| upstream table | rows | `existing` | `current` |
| --- | --- | --- | --- |
| `TestGreaterThan` | 17 | `v2` | `v1` |
| `TestComparePreReleases` | 17 | `v2` | `v1` |
| `TestNewVersion` | 26 | the probed literal | the real `version.Version` constant |

17 + 17 + 26 = 60. The comparison tables put `v1` on the `current` side because
that is the operand upstream calls `GreaterThan` on. The validity probes put the
literal on the `existing` side because that is the operand read from Redis, and
pair it with `version.Version` (`"1.13.10"`, read from the pinned
`references/livekit-stable/version/version.go`) so the row exercises the real
server constant rather than an invented one.

The upstream tables' own `expected` values are **not** used: Go computes every
`valid` / `update` at run time. There is no committed corpus to drift.

## Recorded upstream failure mode (not hidden, not compared)

Upstream discards the `NewVersion` error and then calls `current.GreaterThan(existing)`.
When the stored value is malformed, `existing` is nil and `Compare` reaches
`other.String()` → `bytes()` → `v.segments` on a nil receiver
(`version.go:169-171,436-442`), which panics. The producer therefore does not
call `GreaterThan` on invalid rows; it emits `update: false` plus
`upstream_nil_deref: true` so the divergence is visible in the persisted output.
The Rust consumer only compares `update` when `valid` is true
(`version.rs:179-185`), so this field is evidence, not a comparison input.

The Rust port returns `Err("malformed stored livekit_version")` instead of
panicking. That is a real behavioural difference on the malformed-stored-version
path and is recorded here rather than absorbed into a PASS.

## Run (coordinator only)

```sh
/usr/bin/python3 tests/oracles/workers-version/prepare.py \
  --repository /Users/zoujiaqing/projects/livekit-rs --out /private/tmp/b22e-workers-version
cd /private/tmp/b22e-workers-version
GOTOOLCHAIN=local GOPROXY=off GOWORK=off \
  "$HOME/go/pkg/mod/golang.org/toolchain@v0.0.1-go1.26.3.darwin-arm64/bin/go" \
  build -mod=readonly -o workers-version-oracle .
./workers-version-oracle cases.json > /private/tmp/b22e-workers-version/results.json
```

Then run the real Cargo ignored test with
`WORKERS_VERSION_GO_RESULTS=/private/tmp/b22e-workers-version/results.json`.

## Scope

Exactly the `NewVersion` validity pair and the `GreaterThan` comparison used by
`RedisStore.Start`. This does **not** execute `RedisStore.Start`, a Redis GET/SET,
the `VersionKey` write, `egressWorker`, or any worker dispatch. It does not prove
the runtime branch is reached in a live server. A module-resolution error is not
a test verdict: preserve the original exit code and inspect the pinned paths
rather than updating versions.