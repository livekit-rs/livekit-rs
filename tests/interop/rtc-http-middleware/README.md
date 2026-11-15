# SPEC-0077 C36 — `HTTP_BEHAVIOR:middleware.body_limit` + `HTTP_BEHAVIOR:middleware.path_normalization` launched-MAIN paired Go/Rust differential

This harness proves that the **launched MAIN server** (not an in-process component)
rejects an over-limit declared request body with `413` before auth/decode, and strips
exactly one leading slash from a `//path`, identically to the locked official LiveKit
Server Go oracle. It closes the `middleware.body_limit` and `middleware.path_normalization`
HTTP-behavior leaves by a deterministic, **media-free, unauthenticated** HTTP probe
differential. A confirmed **router** residual (the Go `ServeMux` path-clean redirect) is
honestly split out to a separate OPEN item, not buried in the middleware PASS.

## Leaves

- `HTTP_BEHAVIOR:middleware.body_limit` (config-driven: `limit.max_api_request_body_size`).
- `HTTP_BEHAVIOR:middleware.path_normalization` (unconditional middleware; **no YAML config leaf**).
- `HTTP_BEHAVIOR:routing.serve_mux_path_clean_redirect` (**NEW, OPEN** — the confirmed
  Go `ServeMux` 307 path-clean redirect vs Rust axum 404 residual, split out below).

## What the settings do (upstream contract)

The body limiter and path normalization are wired on the main API port by
`pkg/service/server.go` and `pkg/service/utils.go`:

```go
server.go:484-503  api negroni chain: Recovery -> CORS -> RequestBodyLimiter -> APIKeyAuth -> mux
server.go:490      NewRequestBodyLimiter(conf.Limit.MaxAPIRequestBodySize)   // BEFORE auth
server.go:515      return WithPathNormalization(dispatch)                   // normalization outermost
server.go:119,135  mux = http.NewServeMux(); mux.HandleFunc("/", defaultHandler)

utils.go:49        ErrRequestBodyTooLarge = errors.New("request body too large")
utils.go:131-154   RequestBodyLimiter: if maxBytes<=0 || Body==nil { next; return };
                   if r.ContentLength > maxBytes { HandleError(413, ErrRequestBodyTooLarge); return };
                   r.Body = http.MaxBytesReader(w, r.Body, maxBytes); next
utils.go:103-113   RemoveDoubleSlashes: if strings.HasPrefix(r.URL.EscapedPath(),"//") {
                       r.URL.Path = r.URL.Path[1:]; if RawPath!="" { RawPath = RawPath[1:] } }
utils.go:117-121   WithPathNormalization wraps RemoveDoubleSlashes ahead of the handler

config.go:397,631  Limit.MaxAPIRequestBodySize int64 `yaml:"max_api_request_body_size"` (default 10<<20; 0 unbounded)
```

So an over-limit **declared** `Content-Length` is rejected with `413 request body too large`
**before** auth and before the body is decoded; a `0` limit disables the limiter; and a
`//path` has exactly **one** leading slash stripped before routing.

## Rust mirror (complete launched-MAIN consumer chain)

- middleware order: `crates/lk-compat/src/http.rs:94-104` — `recover → cors →
  body_limit → auth → routes` (body_limit runs **before** auth).
- body limit: `crates/lk-compat/src/http.rs:273-287` — `body_limit_layer`: if `max>0`
  and declared `Content-Length > max` → `413 "request body too large"`; else wrap the
  body with a lazy bound `max`.
- path normalization: `crates/lk-compat/src/http.rs:149-164` — `normalize_path_layer` /
  `strip_double_slash`: strip **one** leading `/` when `path_and_query` starts with `//`.
- 404 route: `crates/lk-compat/src/http.rs:132-142` — `not_found()`.
- twirp body read: `crates/lk-compat/src/twirp/mod.rs:188-196` — reads the body with
  `limit` (`config.http.max_request_body`, `0 → usize::MAX`) **before** the service auth
  check; `ReadError::Limit → codec.body_read_error()`.
- body-read error: `crates/lk-compat/src/twirp/codec.rs:42-47` — `Json → malformed`;
  `crates/lk-compat/src/twirp/error.rs:65` — `Code::Malformed → HTTP 400`.
- config projection: `crates/lk-config/src/official.rs:1049`
  (`max_api_request_body_size: i64 = 10<<20`) → `crates/lk-config/src/normalize.rs:437`
  (`max_request_body = limit.max_api_request_body_size.max(0)`) →
  `crates/rtc-core/src/config.rs:127,935` (`pub max_request_body: u64`, default `10<<20`) →
  `crates/lk-compat/src/http.rs:50` (`state.config.http.max_request_body`).

## Why a media-free HTTP probe (load-bearing)

Both middlewares are deterministic HTTP request-shape consumers on the main API port.
`body_limit` rejects an over-limit declared `Content-Length` with `413` before auth/decode;
`path_normalization` strips one leading slash so `//twirp/<svc>/<method>` reaches the real
route. Neither depends on `rtc` config, ICE, SDP or a published track. No WebSocket join,
no track, no media.

## Config groups (body_limit is config-driven)

Unlike C35 CORS (unconditional, one launch), `body_limit` depends on
`limit.max_api_request_body_size`, so the daemon is launched **once per group** per side:

| group | `limit.max_api_request_body_size` | probes |
|---|---|---|
| `body-limit-small`   | `1024`     | body-oversize-declared, body-undersize-declared, body-boundary-equal |
| `body-limit-zero`    | `0`        | body-zero-unlimited |
| `path-normalization` | `10<<20` (default) | path-double-slash-twirp, path-single-slash-twirp, path-triple-slash-twirp, path-double-slash-unknown |

## Discriminators (load-bearing, empirically grounded on the Go oracle)

The seven **acceptance** profiles vary the request stimulus (method/path/Content-Length/body):

| profile | group | stimulus | status | body |
|---|---|---|---|---|
| `body-oversize-declared`    | small | POST /twirp/…/ListRooms CL=2048 | 413 | `request body too large` (before auth) — **DISCRIMINATING** |
| `body-undersize-declared`   | small | POST /twirp/…/ListRooms CL=2 `{}` | 401 | unauthenticated (not 413) — GATING |
| `body-boundary-equal`       | small | POST /twirp/…/ListRooms CL=1024 | 401 | unauthenticated (Go check is `>`, not `>=`) — GATING |
| `body-zero-unlimited`       | zero  | POST /twirp/…/ListRooms CL=2048 | 401 | unauthenticated (limiter disabled) — GATING |
| `path-double-slash-twirp`   | path  | POST //twirp/…/ListRooms `{}` | 401 | normalized to the real route (non-404) — **DISCRIMINATING** |
| `path-single-slash-twirp`   | path  | POST /twirp/…/ListRooms `{}` | 401 | real route — CONTROL |
| `path-double-slash-unknown` | path  | POST //nonexistent-path `{}` | 404 | normalized, still no route — GATING |

The unauthenticated twirp error is the static `{"code":"unauthenticated","msg":"permissions denied"}`
on both sides. A **body_limit** accept-but-ignore mutant (`body_limit_layer` no-op) drops
the declared-size `413`; the oversize request then reaches the twirp body reader whose own
limit yields `malformed(400)` — `body-oversize-declared` **FAILS** (413→400) while the three
gating body profiles **SURVIVE**. A **path_normalization** accept-but-ignore mutant
(`strip_double_slash → None`) leaves `//twirp` unnormalized → `404`, so
`path-double-slash-twirp` **FAILS** (401→404) while the single-slash control and the
unknown-path gating profile **SURVIVE**. Specific, not indiscriminate.

## Honest split — confirmed router residual (NOT a middleware failure)

The eighth probe `path-triple-slash-twirp` (POST `///twirp/…/ListRooms`) confirms the
**middleware** strips exactly **one** leading slash on **both** sides (`///twirp → //twirp`).
The residual `//twirp` is then handled by the Go `http.ServeMux` path cleaner, which issues
a **307 redirect** to `/twirp/…` (`Location` header, empty body), whereas the Rust axum
router has no `ServeMux` path-clean redirect and returns **404**:

| side | status | body | content-type | location |
|---|---|---|---|---|
| Go   | 307 | `` | `` | `/twirp/livekit.RoomService/ListRooms` |
| Rust | 404 | `404 page not found\n` | `text/plain; charset=utf-8` | `` |

This is a **distinct router (`ServeMux` `cleanPath`) concern**, outside the
strip-one-leading-slash middleware leaf. The full `ServeMux` path cleaning (leading/internal
`//`, `/./`, `/../`, trailing-slash redirects, version-dependent 301/307) is a broader
router behavior than the middleware. It is recorded as a **confirmed OPEN gap** under the
separate ledger item `HTTP_BEHAVIOR:routing.serve_mux_path_clean_redirect` and a separate
residual receipt (`router-redirect-residual.json`, status `DOCUMENTED_OPEN_GAP`). It is
**not** counted toward the middleware acceptance PASS and **not** deleted. The seven
acceptance profiles are the middleware consumer surface this harness proves.

## Normalization (scope limit)

The `status`/`body`/`content_type`/`location` semantics of the seven acceptance profiles are
fully deterministic (no SDP, no candidates, no per-launch nonce), so they **are** compared
byte-equal cross-side. The daemon HTTP port and the reserved media ports are per-launch
nondeterministic and are recorded in `execution.json` for transparency but are **not**
cross-compared. No comparison field is deleted; these are explicit scope bounds. This
harness proves the `body_limit` declared-size reject and the `path_normalization`
strip-one-leading-slash middleware consumer surfaces only — not the chunked/unknown-length
`MaxBytesReader` lazy face beyond the declared-size fast path, not the `ServeMux` full
path-clean redirect (split out), and not the full HTTP surface.

## Privacy normalization (commit boundary)

The manifest embeds raw daemon `server_log` provenance. Any RFC1918 private IPv4 literal
is rewritten to RFC 5737 TEST-NET-1 (`192.0.2.<last-octet>`) before writing, preserving
the last octet for correlation while removing the real network identity. The redaction is
pattern-based (no real literal is embedded in the harness source). The loopback-only
fixture config normally emits no private literal, so this is a defensive no-op guard, not
a data transform.

## Files

- `observe.go` — self-contained media-free HTTP middleware observer (separate copy from the
  C35 `rtc-http-cors` observer; swaps the CORS header stimulus for body-limit/path-normalization
  request-shape probes, names the two driven items per group, keeps the C35 observer identity
  unchanged for anti-splicing). Pure Go stdlib (no livekit module imports). Takes `--group`.
- `run-paired.py` — wraps the observer once per (side, group), normalizes, asserts the per-side
  Go-derived contract per acceptance profile, compares cross-side, emits `BATCH.json` +
  `execution.json` + `router-redirect-residual.json`, exits 0 iff every acceptance profile and
  control passes (the residual is documented, not gated).

## Reproduce

Build the immutable binaries (from the repo root):

```sh
# Rust acceptance (default target dir)
cargo build --release -p livecloud --bin livecloud

# Locked Go oracle (v1.13.10 e5655ac2)
cd references/livekit-stable
GOTOOLCHAIN=go1.26.0 GOPROXY=off GOWORK=off go build -mod=readonly \
  -o /tmp/c36-build/livekit-server-go ./cmd/server

# C36 observer (pure stdlib; pinned toolchain for reproducibility)
GOTOOLCHAIN=go1.26.0 GOPROXY=off GOWORK=off go build -mod=readonly \
  -o /tmp/c36-build/mw-observer \
  /Users/zoujiaqing/projects/livekit-rs/tests/interop/rtc-http-middleware/observe.go
```

Run the paired differential (fresh output dir; six observer invocations run sequentially,
each launching one daemon for one config group):

```sh
python3 tests/interop/rtc-http-middleware/run-paired.py \
  --go /tmp/c36-build/livekit-server-go \
  --rust /Users/zoujiaqing/projects/livekit-rs/target/release/livecloud \
  --observer /tmp/c36-build/mw-observer \
  --output /tmp/c36-run
```

Expected: `PASS_LAUNCHED_MAIN_HTTP_MIDDLEWARE`, 7 acceptance profiles, 16 controls, empty
`failed_profiles`/`failed_controls`, `input_identity_stable=true`, exit 0; the
`path-triple-slash-twirp` residual recorded with `cross_side_equal=false` (Go 307 / Rust 404).

## Falsifiability (positive control)

Two accept-but-ignore mutants are each built in an isolated `CARGO_TARGET_DIR` and run on the
SAME harness/observer/Go oracle:

- **body_limit** — `http.rs:273` `body_limit_layer` becomes a no-op (`next.run(req)`), ignoring
  `max_request_body`. It must FAIL `body-oversize-declared` (413→400) and the
  `oversize_emits_413_rust` control, while the three gating body profiles, the three path
  acceptance profiles and the remaining controls survive.
- **path_normalization** — `http.rs:156` `strip_double_slash` returns `None` unconditionally.
  It must FAIL `path-double-slash-twirp` (401→404) and the `double_slash_twirp_normalized_rust`
  control, while the single-slash control, the unknown-path gating profile, the four body
  profiles and the remaining controls survive.

Each mutant receipt is nested under `positive-control/` and is **not** cited as acceptance
evidence. Each mutation is then reverted byte-identical and the acceptance binary rebuilt to
confirm it reproduces the protected identity `d1dad0fc…`.