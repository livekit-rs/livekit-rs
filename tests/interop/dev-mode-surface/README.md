# SPEC-0077 P2 — development mode / debug-handler HTTP surface, launched-MAIN paired Go/Rust differential

This harness proves that the **launched MAIN server** consumes `development`
(and its `--dev` CLI form) and `debug_handler.port` exactly like the locked
official LiveKit Server Go oracle. It closes seven ledger leaves with one
deterministic, **media-free, unauthenticated-by-default** HTTP probe
differential:

- `YAML:development`
- `YAML:debug_handler`
- `YAML:debug_handler.port`
- `base_flags:--dev`
- `generated_flags:--development`
- `generated_flags:--debug_handler.port`
- `CONFIG_ENABLED_CAPABILITY:debug_handler.port`

## What the behavior is (upstream contract, v1.13.10 e5655ac2, Go 1.26 stdlib)

```go
pkg/config/config.go:84,107       DebugHandler DebugHandlerConfig `yaml:"debug_handler,omitempty"`
                                  Development bool `yaml:"development,omitempty"`.
pkg/config/config.go:530-532      type DebugHandlerConfig struct { Port uint32 } — the
                                  debug_handler node has exactly ONE field, so YAML:debug_handler
                                  has no second leaf to project beyond .port.
pkg/config/config.go:1106-1107    `--dev` -> conf.Development = c.Bool("dev").
pkg/service/server.go:119-125     mux := http.NewServeMux(); if conf.Development {
                                      mux = http.DefaultServeMux
                                      mux.HandleFunc("/debug/goroutine", s.debugGoroutines)
                                      mux.HandleFunc("/debug/rooms", s.debugInfo) }
                                  So development REPLACES the API mux; it is a routing
                                  decision, not a middleware decision.
pkg/service/server.go:170-182     if conf.DebugHandler.Port > 0 { debugMux := http.NewServeMux()
                                      + five /debug/pprof patterns + /debug/goroutine
                                      + /debug/rooms; s.debugServer = &http.Server{debugMux} }
                                  A SEPARATE fresh mux, with NO "/" catch-all and NO method
                                  prefix, so an unknown path gets ServeMux's own notFound and
                                  EVERY method reaches the handler. This block is inside the
                                  same setup function but is NOT gated on conf.Development:
                                  debug_handler.port is MODE INDEPENDENT.
pkg/service/server.go:422-441     debugGoroutines = pprof.Lookup("goroutine").WriteTo(w, 2);
                                  debugInfo = roomState JSON.
pkg/service/server.go:484-503     api chain Recovery -> CORS -> BodyLimiter -> Auth -> mux, so
                                  the debug patterns sit INSIDE APIKeyAuth on the API port and
                                  OUTSIDE auth on the dedicated debug port.
net/http/pprof/pprof.go:96-106    init() registers "GET /debug/pprof/…", "GET /debug/pprof/cmdline",
                                  …profile, …symbol, …trace on http.DefaultServeMux, the "GET "
                                  method prefix applied unless GODEBUG=httpmuxgo121=1 (livekit
                                  never sets it). Hence the API-port pprof subtree is GET/HEAD-only.
net/http/server.go  findHandler   an exact "/debug/pprof" pattern does not exist, but the
                                  "/debug/pprof/" subtree does, so a request for the unsuffixed
                                  path is `matchOrRedirect`'d to the canonical subtree path with
                                  a 307; a method-only miss on a rooted-"/" mux falls through to
                                  defaultHandler, i.e. the plain `404 page not found` page — not
                                  ServeMux's "404 page not found" with a different shape.
```

So `development` is observable as: on the API port, `/debug/pprof` becomes a 307,
`/debug/pprof/…` answers **GET/HEAD only**, livekit's own `/debug/rooms` and
`/debug/goroutine` answer **every method**, and production instead 404s all of
them. `debug_handler.port` is observable as: a second listener serving the same
patterns with no auth and no method gate, and — when unset — **nothing bound at
all**, which is a connect failure, not a 404.

## Rust mirror (complete launched-MAIN consumer chain)

- `crates/lk-config/src/official.rs:237,259,275-277` — `debug_handler` (`DebugHandlerConfig.port`)
  and `development` decode from YAML; `crates/lk-config/src/cli.rs:146-151,680` — the `--dev`
  base flag; `crates/lk-config/src/load.rs:143-150` — `apply_base_flags` projects `--dev` onto
  `cfg.development`; `crates/lk-config/src/generated.rs:343-360` — `specs()` builds the generated
  flag table by walking the config tree, so `--development` and `--debug_handler.port` exist
  because the two YAML fields exist (there is no per-flag table to duplicate them in).
- `crates/livecloud/src/server.rs:144-156` — `http_app(state, manager, development)` merges
  `crate::debug::api_routes(manager)` into the API route table **only when `development`**,
  which is the faithful mirror of the Go mux replacement; `:858,:892` are the launched-MAIN
  call sites.
- `crates/livecloud/src/debug.rs:59` — `api_routes`: the seven patterns with the pprof subtree
  on `get`/`head` (Go's `GET ` registration prefix) and livekit's own two on `any`, plus the
  `/debug/pprof` → `/debug/pprof/` canonical redirect.
- `crates/livecloud/src/debug.rs:105` — `debug_routes` / `:35` `router`: the dedicated-port mux,
  every pattern `any(...)` and **no** catch-all fallback, mirroring Go's fresh `debugMux`.
- `crates/livecloud/src/debug.rs:141,196,277` — `canonical_debug_path` / `clean_debug_path` /
  `debug_redirect`: the `ServeMux` `cleanPath` + `matchOrRedirect` semantics for that mux.
- `crates/livecloud/src/server.rs:764,945` — `debug_handler.port != 0` binds a listener and
  serves `crate::debug::router(manager)` on it; port 0 binds nothing.
- `crates/lk-compat/src/http.rs:41,55` — `api_routes` (route table) and `api_router` (middleware
  chain + `not_found` fallback) are split so livecloud can swap the mux content while Recovery,
  CORS, body limit, auth and the 404 fallback stay outside it, as upstream.

## Why a media-free HTTP probe (load-bearing)

The whole leaf is a routing/dispatch decision taken at startup from two config
fields. It is observable from the request method, the path, and whether a second
listener exists — no ICE, no SDP, no published track, no WebSocket join.

## Groups (launch stimulus) and probes (request stimulus)

Seven launch groups, each daemon started once per side; the same API-port probe set runs in
every group (`api-prod` legs act as the control for the mode decision), plus 13 dedicated
debug-port probes where a listener exists. Two groups stimulate the same mode through the
generated CLI flag spelling so `generated_flags:--development` and
`generated_flags:--debug_handler.port` have their own launched-MAIN consumer evidence:

| group | daemon stimulus | API port | debug port |
|---|---|---|---|
| `api-prod` | neither dev nor debug port | every `/debug/*` must 404 | — |
| `api-dev-flag` | `--dev` base flag | dev mux live | — |
| `api-dev-yaml` | `development: true` YAML | identical to `api-dev-flag` | — |
| `api-dev-genflag` | `--development=true` generated flag | identical to `api-dev-flag` | — |
| `debug-port-set` | `debug_handler.port` only (production) | still 404s all `/debug/*` | live, all methods |
| `debug-port-genflag` | `--debug_handler.port=…` generated flag | still 404s | identical to `debug-port-set` |
| `debug-port-zero` | neither | still 404s | **no listener** (connect failure) |

The measured Go method matrix on the **API port** in a dev leg (the clean receipt,
`raw.go.api-dev-flag.probes`; every value below is a recorded status, and prod is the
same probes all 404 except the three gating probes):

```
/debug/pprof          GET 307   HEAD 307   POST 404   OPTIONS 404   canonical 307 + GET-only subtree
/debug/pprof/         GET 200   HEAD 200   POST 404   PUT 404
/debug/pprof/cmdline  GET 200   HEAD 200   POST 404   OPTIONS 404
/debug/pprof/heap     GET 200   HEAD 200   POST 404
/debug/rooms          GET 200   HEAD 200   POST 200   PUT 200       livekit's own HandleFunc: no method prefix
/debug/goroutine      GET 200   HEAD 200   POST 200
/debug/nope, /other   GET 404   POST 404                            rooted-"/" defaultHandler 404 page
```

On the **dedicated debug port** the same paths answer every method — including
`POST /debug/pprof` → 307 and `POST /debug/pprof/` → 200, which the API port refuses —
and an unknown path still 404s, because that mux has no `/` catch-all and no method
prefix.

Three mode-independent gating probes (`GET /` health → 200, unauthenticated twirp POST →
401, bearer-token room list → 401) run in all seven groups and must be identical in all
seven, which is what makes every `/debug/*` difference attributable to the mux and
listener decisions alone.

Total: 242 cases (29 API probes in each of 4 API-port groups + 42 probes in each of 3
debug-port groups) and 38 cross-case controls (12 named per side plus 7 per-group
lifecycle integrity per side).

## Discriminators

| observable | prod | dev flag | dev YAML | dev generated flag | debug port |
|---|---|---|---|---|---|
| `GET /debug/pprof` | 404 page | **307 → `/debug/pprof/`** | 307 | 307 | 307 |
| `POST /debug/pprof` | 404 page | 404 page | 404 | 404 | **307** |
| `GET /debug/pprof/cmdline` | 404 page | 200 text/plain + nosniff | 200 | 200 | 200 |
| `POST /debug/pprof/cmdline` | 404 page | **404 page** (GET-only gate) | 404 | 404 | **200** (no gate) |
| `PUT /debug/rooms` | 404 page | 200 `[]` | 200 | 200 | 200 |
| `GET /debug/goroutine` | 404 page | 200 text/plain | 200 | 200 | 200 |
| debug-port connect | refused | refused | refused | refused | **accepted** |

The four gating controls (`pprof_subtree_is_get_only`,
`livekit_patterns_are_any_method`, `dedicated_port_offers_all_methods`,
`auth_wraps_api_debug_only`) plus the three equivalence controls
(`dev_flag_equals_dev_yaml`, `dev_genflag_equals_yaml`, `debug_port_genflag_equals_set`)
each fail on a different single mutation, so the harness is specific rather than
indiscriminate.

## Normalization (scope limit) and field exclusions

The comparison is per case over `outcome`, `status`, `content_type`, `location`,
`nosniff`, `body_class`, `body`, `content_length`. `outcome` is the dispatch
outcome (`response` / `no_listener_configured`), so the missing-listener leg is
compared as data, not skipped. The daemon HTTP port and the reserved
API/debug/media ports are per-launch nondeterministic, are recorded in
`execution.json` for transparency, and are **not** cross-compared. No case is
dropped and no field is excluded wholesale.

Where a field genuinely cannot be equal, the contract marks it with the `ANY`
sentinel for that specific (group, probe), and the receipt records the excluded
field plus its reason: 46 exclusion marks covering 82 field instances, with exactly
four reasons:

- `profile_payload` (15 marks / 30 fields) — the goroutine dump and the heap profile
  are native runtime state: Go's runtime goroutine list versus the Rust `rpprof`
  native thread sampler, and two different gzip pprof protobufs. Status, content type,
  `X-Content-Type-Options` and body class are still asserted.
- `cmdline_payload` (7 / 14) — `/debug/pprof/cmdline` is the process argv, which
  cannot be equal across two different binaries (and differs even between two Go
  launches, because `--dev` is 5 more bytes than the YAML equivalent). The shape
  (200 + text/plain + nosniff + non-empty text body class) is still asserted.
- `pprof_index_html` (7 / 21) — Go's `/debug/pprof/` index enumerates the **Go
  runtime** profile registry (`goroutine`, `block`, `mutex`, `threadcreate`
  counts, the `gc` parameter, `go tool pprof` instructions). Reproducing that
  HTML would mean claiming collectors the Rust binary does not implement, so the
  Rust index lists the native collectors and says so. Status, content type and
  nosniff are still asserted; the body and its class are not compared.
- `declared_length` (17 / 17) — Go writes handler output chunked and declares no
  length (and omits `Content-Length` outright for a HEAD whose handler wrote
  nothing), while the Rust candidate buffers the response and declares it. The
  framing choice is outside this leaf; status, content type, nosniff, `Location`
  and the suppressed HEAD body are still asserted.

`api-prod` and `debug-port-zero` carry **no** exclusions at all (10 marks each in
the three dev groups, 8 in each debug-port group): there every probe is a
byte-exact 404 page or a refused connection.

## Honesty about `debug_handler` vs the Rust runtime

The Rust `livecloud` implements the debug surface natively: `/debug/rooms` from
its own room manager, `/debug/goroutine` as a native thread sample whose header
states that it is not a Go goroutine dump, and pprof-protocol profiles built by
`rpprof`. The differential therefore proves the **routing, method gating,
redirect, content-type and listener semantics** — the consumer surface of the
two config leaves — not byte equality of runtime diagnostics, which is not
attainable across two runtimes and is not what these leaves claim.

## Privacy normalization (commit boundary)

The manifest embeds raw daemon `server_log` provenance. Any RFC1918 private IPv4
literal is rewritten to RFC 5737 TEST-NET-1 (`192.0.2.<last-octet>`), preserving
the last octet for correlation while removing the real network identity. The
redaction is pattern-based; no real literal is embedded in the harness source.
The loopback-only fixture normally emits none, so this is a defensive no-op guard.

## Files

- `observe.go` — self-contained, stdlib-only launched-MAIN observer. Reserves the
  configured HTTP and media ports in a low band (40000-40999, below the macOS
  ephemeral range), releases the reservations so the daemon can bind, launches the
  real main server once per group, settles past the Go `Start()` init window, drives
  the probes, and records the exact dispatch shape of each response. `--group`
  selects the launch stimulus; the probe set per group is fixed and name-checked by
  the harness so a missing probe is a failure, not a silent skip.
- `run-paired.py` — runs the observer once per (side, group), asserts the
  Go-derived contract on **each side independently**, then compares cross-side on
  the deterministic fields, emits `BATCH.json` + `execution.json`, and exits 0 iff
  every case and control passes.
- In the evidence slot: `binaries.json` (identities and the consumer chain),
  `component-tests.log` (the `dev_api_mux_tests` and `debug.rs` unit suites run on
  the candidate, including the two falsification transcripts for the dedicated-mux
  test), `positive-control.json` (the three mutant scopes), `invocation.sh` (the exact
  acceptance command), `negative-controls.py` / `negative-controls.json` (see below).

## Identities

Fixed at acceptance and recorded in `binaries.json` of the evidence slot:

| input | identity |
|---|---|
| Go oracle | livekit v1.13.10 `e5655ac2`, sha256 `4bc2378e…e7dc` |
| Rust candidate | `livecloud` sha256 `e73411d5…53bd5` |
| observer | `observe.go` binary sha256 `4d0f2691…` |
| harness | `run-paired.py` sha256 `7e6398e3…` |
| mutants | `4857661d…`, `ef97c6af…`, `5065708a…` (each distinct from the candidate) |

## Ledger-gate falsifiability (negative controls)

`scripts/check-parity-ledger.py` is what turns these receipts into a VERIFIED state, so
the gate itself is probed with nine controls (`negative-controls.py`, run against a
temporary copy of the real ledger, which is never written): NC0 asserts the
uncorrupted upgraded ledger reports 0 findings — the specificity control that makes the
other eight meaningful — and NC1–NC8 each corrupt one of the seven leaves and assert the
exact finding kind raised: `VERIFIED_MISSING_OFFICIAL_DIFFERENTIAL`,
`VERIFIED_MISSING_MAIN_CONSUMER`, `EVIDENCE_PATH_MISSING`,
`VERIFIED_CITES_FAILING_EVIDENCE` (citing a `FAIL` mutant receipt),
`TIER5_ARTIFACT_DOES_NOT_NAME_ITEM`, `TIER5_PAIRED_ARTIFACT_NOT_REPORTED_PASS`,
`VERIFIED_TEXT_CONTRADICTS_STATE` (residual text in an accepted scope) and
`TIER5_PAIRED_RECEIPT_REPORTS_NO_CASES`. Otherwise the upgrade claimed here would be an
assertion about a gate that cannot fail.

## Reproduce

Build the immutable inputs (from the repo root):

```sh
# Rust acceptance candidate (default target dir)
cargo build --release -p livecloud --bin livecloud

# Locked Go oracle (v1.13.10 e5655ac2, sha256 4bc2378e…e7dc)
cd references/livekit-stable
GOTOOLCHAIN=go1.26.0 GOPROXY=off GOWORK=off go build -mod=readonly \
  -o /tmp/c78-build/livekit-server-go ./cmd/server

# Observer (pure stdlib, pinned toolchain for reproducibility)
GOTOOLCHAIN=go1.26.0 GOPROXY=off GOWORK=off go build -mod=readonly \
  -o /tmp/c78-run-build/devmode-observer \
  "$PWD/tests/interop/dev-mode-surface/observe.go"
```

Run the paired differential (fresh output dir; 14 sequential legs — one daemon per
group per side):

```sh
python3 tests/interop/dev-mode-surface/run-paired.py \
  --go /tmp/c78-build/livekit-server-go \
  --rust /Users/zoujiaqing/projects/livekit-rs/.qoder/worktrees/agent-Software-Dev-Team-Full-stack-engineer-3c3f453f/target/release/livecloud \
  --observer /tmp/c78-run-build/devmode-observer \
  --output /tmp/c78-run
```

Expected: `PASS_LAUNCHED_MAIN_DEV_MODE_SURFACE`, `case_count` 242, empty
`failed_cases` and `failed_controls`, `excluded_field_count` 82,
`missing_probes` empty in every group, `input_identity_stable=true`, exit 0.

## Falsifiability (positive control)

Mutants are built in an isolated `CARGO_TARGET_DIR` (`recipes/mutant*.patch`) and run
against the **same** harness, observer and Go oracle, so the only variable is the Rust
source. Each is expected to fail only its own scope, and each one does:

- **ignore `development`** (`http_app` never merges `debug::api_routes`) — 45 failed
  cases: the 15 live-surface legs (`rooms-*`, `goroutine-*`, `pprof-*-get/head`) of
  `api-dev-flag`, `api-dev-yaml` and `api-dev-genflag` all flip 307/200 → 404. The 7
  method-refusal legs are **not** in that set: a production mux 404s them too, which is
  why the harness also asserts `dev_flag_serves_debug_mux_rust` (it needs the 307 and the
  live `[]` body to be present, so the group cannot pass vacuously) and
  `livekit_patterns_are_any_method_rust`. `api-prod`, `debug-port-*` stay green.
- **pprof subtree registered `any(...)` instead of `get`/`head`** — 21 failed cases:
  exactly the 7 `pprof_subtree_is_get_only` legs (`pprof-slash-post/options`,
  `pprof-index-post/put`, `pprof-cmdline-post/options`, `pprof-heap-post`) in each of the
  three dev groups. The dedicated-port legs are unaffected — that mux is `any(...)` in the
  accepted source too — which is precisely the distinction the leaf is about.
  `pprof_subtree_is_get_only_rust` fails, and so do `dev_flag_equals_dev_yaml_rust` and
  `dev_genflag_equals_yaml_rust`: the mutated POST/PUT legs now emit live index HTML, a
  payload the unmutated contract never had to compare between two separate processes, so
  the equivalence controls become sensitive exactly where the semantics changed.
- **ignore `debug_handler.port`** (skip the listener bind) — 26 failed cases: all 13
  debug-port legs of `debug-port-set` and `debug-port-genflag` flip from response to
  connect failure. No API-port case moves, and `absent_debug_port_has_no_listener_rust`
  survives because it asserts the *unset* case — the failure is localized to the consumer
  of the configured port. `dedicated_port_offers_all_methods_rust`,
  `debug_port_is_mode_independent_rust` and `auth_wraps_api_debug_only_rust`
  (which needs a live `GET /debug/rooms` on the dedicated port) all fail.

The mutant receipts are nested under `positive-control/`, each records `FAIL` with
`case_count` 242, and are **not** cited as acceptance evidence. Each mutation is then
reverted byte-identical and the acceptance binary rebuilt to confirm it reproduces the
protected identity `e73411d5cf2df9021c2a81f517ae9d839f1236bbb3139f818fa2880b28253bd5`.
