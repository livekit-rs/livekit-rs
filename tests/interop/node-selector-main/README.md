# SPEC-0077 C24 — launched-MAIN node_selector construction paired differential

This harness proves that a **launched MAIN** server consumes the
`node_selector.kind` / `node_selector.regions` config and applies the upstream
`CreateNodeSelector` construction policy identically on the locked Go oracle and
the Rust candidate. It closes the `official_differential_lifecycle` residual for
two node_selector leaves on the **current clean release candidate**, re-binding
the earlier startup-surface evidence to the launched-MAIN paired-binary tier with
a **gate-recognized** receipt.

## What it covers (2 ledger leaves)

| leaf | surface |
|---|---|
| `YAML:node_selector.kind` | yaml |
| `YAML:node_selector.regions` | yaml |

## Relationship to `config-wiring-startup`

`tests/interop/config-wiring-startup/run.py` already proves this exact startup
surface paired (its `results.json` reports PASS, 9/9 rows, both binary
identities). Two gaps kept the leaves at `official_differential_lifecycle=PARTIAL`:

1. that receipt was produced on an **earlier candidate** (`08deadde…`, debug), not
   the current clean release candidate; and
2. its receipt names the binaries under the keys `official` / `rust`, which the
   SPEC-0075 ledger gate's `classify_artifact` does **not** recognize as paired
   binary identities (it looks for `go_binary`/`oracle_binary`/`reference_binary`
   and `rust_binary`/`candidate_binary` keys, or a path-keyed `binaries` map), so
   the gate cannot credit it at tier 5.

C24 is **complementary, not a replacement**: a focused node_selector harness that
emits a C23-style `BATCH.json` with a path-keyed `binaries` map the gate
recognizes, re-run on the current clean candidate. It also adds two boundary
profiles the older harness lacked (`empty-kind` and `regionaware-empty-regions`).
The older receipt and its evidence are **preserved verbatim**.

## The consumer and the observable

Upstream builds the selector while **wiring** services, not while loading config:
`InitializeServer` → `NewRoomAllocator` (`pkg/service/roomallocator.go:43`) →
`selector.CreateNodeSelector` (`pkg/routing/selector/interfaces.go:33-66`), after
`getConfig`/`ValidateKeys`/`LoadTURNSecrets`. A construction error is printed with
`fmt.Println(err)` and exits 1 (`cmd/server/main.go:190-192`), so the refusal is
**bare upstream text on stdout**. The Rust mirror is `lk-config::check_selector`
(`crates/lk-config/src/unsupported.rs:60-89`), called from
`validate_server_wiring` in `livecloud::server::run`.

`CreateNodeSelector` semantics (`interfaces.go:33-66`,
`regionaware.go:35-68`):

- `kind == ""` is treated as `any`; `any`/`cpuload`/`sysload`/`random` construct
  without a fatal validation (`random` only logs a deprecation warning).
- unknown `kind` → `ErrUnsupportedSelector` = `unsupported node selector`.
- `regionaware` → `NewRegionAwareSelector(conf.Region, conf.NodeSelector.Regions, …)`:
  - blank current region (the **top-level `region`**, not `node_selector.*`) →
    `current region cannot be blank`;
  - a **non-empty** `regions` list that omits the current region →
    `unknown lat and lon for the current region`;
  - an **empty** `regions` list with a region set, or a matching region →
    constructs OK (`regionaware.go:57` guards on `len(regions) > 0`).

The Rust `check_selector` reproduces all three refusals verbatim and the same
`len(regions) > 0` boundary (`unsupported.rs:75-86`).

## Profiles (10)

| profile | node_selector config | observable | proves |
|---|---|---|---|
| `ns-unsupported-kind` | `kind=bogus-kind` | exit 1, `unsupported node selector` | unknown-kind gate |
| `ns-regionaware-blank` | `kind=regionaware`, no region | exit 1, `current region cannot be blank` | blank-region gate |
| `ns-regionaware-unknown` | `kind=regionaware`, `region=US-west`, `regions=[US-east]` | exit 1, `unknown lat and lon for the current region` | non-empty-list omitting region gate |
| `ns-empty-kind` | `kind=""` | serves HTTP, exit 0 | "" maps to any |
| `ns-any` | `kind=any` | serves HTTP, exit 0 | any constructs |
| `ns-cpuload` | `kind=cpuload` | serves HTTP, exit 0 | cpuload constructs |
| `ns-sysload` | `kind=sysload` | serves HTTP, exit 0 | sysload constructs |
| `ns-random` | `kind=random` | serves HTTP, exit 0 | random constructs (deprecated) |
| `ns-regionaware-match` | `kind=regionaware`, `region=US-east`, `regions=[US-east]` | serves HTTP, exit 0 | matching region constructs |
| `ns-regionaware-empty-regions` | `kind=regionaware`, `region=US-east`, `regions=[]` | serves HTTP, exit 0 | **empty list + region set constructs** (len>0 boundary) |

All profiles set `rtc.tcp_port=0` (the shared host may hold the default ICE TCP
port 7881; a collision there is an environment artifact, not a config-surface
result), `bind_addresses=[127.0.0.1]` and `rtc.node_ip=127.0.0.1`. Configs are
emitted as **YAML strings** (no PyYAML dependency).

### Normalization (scope limit)

The **alive** observable is `{http reachable, exit==0, no SIGKILL}`. The per-side
shutdown timing (`sigterm_to_exit_s`, whether a second SIGTERM was needed)
legitimately differs between the Go and Rust event loops and is **recorded but
not compared**. The **refusal** observable is the exact exit code and stdout
bytes. No comparison field is deleted; these are explicit scope boundaries.

### Shutdown calibration (measured, inherited from config-wiring-startup)

The alive rows settle `--settle` seconds (default 2) after the port first accepts
before signalling. Signalling immediately is **not** a startup-acceptance
measurement: the official binary accepts TCP before `Start()` sets `s.running`
(`server.go:343-345`), and `Stop()` early-returns on `!running.Swap(false)`
(`server.go:390`) without closing `doneChan`, so `Start()` blocks; `main.go:313`
loops `for i := range 2`, so a second signal releases it. `terminate()` sends
SIGTERM, then a second SIGTERM, then SIGKILL, and records which was needed.

## Run

```bash
# build the locked Go oracle (v1.13.10 e5655ac2) and the Rust candidate first
python3 run-paired.py \
  --go   /path/to/livekit-server-go \
  --rust /path/to/livecloud \
  --output /path/to/evidence-dir
```

Each side runs a fresh server per profile on reserved loopback ports: refusal
profiles self-exit 1; alive profiles serve HTTP then shut down on SIGTERM. The
manifest `execution.json` records per-profile go/rust observables, config sha256,
binary sha256 and captured stdout/stderr/log. Status
`PASS_LAUNCHED_MAIN_NODE_SELECTOR` requires every profile to satisfy its contract
on both sides, the cross-side normalized observables to be equal, and all 22
cross-profile controls (11 per side) to hold.

## Falsifiability

Rust mirrors the upstream construction policy in **two** places: the
`lk-config::check_selector` wiring gate (`unsupported.rs:60`, reached first via
`validate_server_wiring` at `server.rs:218`) and the `lk-compat
ConfiguredSelector::new` constructor (`selector.rs:64/69/73`, reached later via
`enable_local_allocation_selector` at `server.rs:375`). Disabling **only** the
wiring gate would not flip the observable: the constructor mirror would still
refuse, but surfaced as `ServerError::Worker` (`could not start worker services:
…`) rather than the bare upstream text. The positive control therefore disables
the selector construction validation **entirely** — all three guards in each
mirror neutered with `&& false` — built in an **independent** target dir so the
clean candidate is never overwritten.

That mutant makes the three refusal profiles genuinely **start** (the bad kinds
are accepted and served, the prohibited "accept-but-ignore" failure mode), so it
FAILS exactly `ns-unsupported-kind` + `ns-regionaware-blank` +
`ns-regionaware-unknown`, passing the other seven alive profiles (they never
depended on a refusal). The byte-identical restore rebuilds to the honest
candidate and the final acceptance PASSes. The mutant FAIL receipt is kept under
`positive-control/mutant/` and is **not** cited as acceptance evidence.

## Out of scope (honest residual, W6)

Cross-node `SelectNode` **runtime** semantics — region-aware nearest-node
ordering, cpu/sysload filtering across a multi-node cluster — are **not** claimed.
Single-node local routing never invokes `SelectNode`, so this harness proves the
**startup construction** surface only. That residual stays recorded in the ledger
`accepted_scope` and is not absorbed into this batch's PASS.