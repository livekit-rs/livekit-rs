# SPEC-0077 C35 — `HTTP_BEHAVIOR:middleware.cors` launched-MAIN paired Go/Rust differential

This harness proves that the **launched MAIN server** (not an in-process component)
emits the rs/cors response-header semantics identically to the locked official
LiveKit Server Go oracle. It closes the `middleware.cors` HTTP-behavior leaf by a
deterministic, **media-free, unauthenticated** HTTP probe differential.

## Leaf

- `HTTP_BEHAVIOR:middleware.cors` (unconditional HTTP middleware; **no YAML config leaf**).

## What the setting does (upstream contract)

CORS is wired on the main API port by `pkg/service/server.go`:

```go
:488      API cors.New(corsOptions([]string{"OPTIONS","HEAD","GET","POST","PATCH","DELETE"}))
:525-536  corsOptions: AllowOriginFunc=func(string)bool{return true},
          AllowedMethods=methods, AllowedHeaders=["*"], ExposedHeaders=["*"], MaxAge=86400
```

The vendored `rs/cors@v1.11.1` negroni `ServeHTTP` (the form `configureMiddlewares`
uses) is the ground truth:

```go
:308-325  if OPTIONS && Access-Control-Request-Method!="" { handlePreflight; WriteHeader(204) }
          else { handleActualRequest; next }
:329-397  handlePreflight: ALWAYS Vary=preflightVary; if origin=="" return;
          if !isMethodAllowed(reqMethod) return; ACAO=echoed Origin, ACAM=echoed ACRM,
          ACAH=echoed ACRH (if non-empty), ACMA=86400
:400-444  handleActualRequest: ALWAYS Vary=Origin; if origin=="" return;
          if !isMethodAllowed(method) return; ACAO=echoed Origin, Expose-Headers=["*"]
:234      preflightVary=["Origin, Access-Control-Request-Method, Access-Control-Request-Headers"]
```

So a preflight is **always 204** (even when the origin is absent or the method is
disallowed — `handlePreflight` returns early but `WriteHeader(204)` still runs), and
an actual request **always carries `Vary: Origin`** with `ACAO`/`Expose-Headers`
added only when an allowed origin and method are present.

## Rust mirror (complete launched-MAIN consumer chain)

- middleware order: `crates/lk-compat/src/http.rs:94-104` — `recover → cors →
  body_limit → auth → routes` (CORS runs **before** auth; a preflight short-circuits
  at CORS and never reaches auth).
- constants: `crates/lk-compat/src/http.rs:31-35` — `CORS_MAX_AGE="86400"`,
  `CORS_ALLOWED_METHODS=[OPTIONS,HEAD,GET,POST,PATCH,DELETE]`, `PREFLIGHT_VARY`.
- preflight: `crates/lk-compat/src/http.rs:205-240` — `204 + Vary=preflight`, then if
  `origin_present && method_ok`: `ACAO=origin`, `ACAM=acrm`, `ACAH=request_headers`,
  `ACMA=86400`.
- actual: `crates/lk-compat/src/http.rs:242-261` — `Vary=Origin` (unconditional), then
  if `origin && method_ok`: `ACAO=origin`, `Expose="*"`.
- 404 route: `crates/lk-compat/src/http.rs:132-142` — `not_found()` (the deterministic
  actual-request route).
- auth pass-through: `crates/lk-compat/src/auth/mod.rs:208-216` —
  `authenticate_token(None)=Ok(None)`, so an unauthenticated actual request passes auth
  and reaches the 404 fallback on both sides.

## Why a media-free HTTP probe (load-bearing)

CORS is an **unconditional** HTTP middleware on the main API port. It does not depend
on `rtc` config, ICE, SDP or a published track. The clean deterministic observable is
the response-header set of an OPTIONS preflight (204, short-circuited before auth) and
an actual GET to a fixed nonexistent path (`/cors-probe-absent`, 404 on both sides,
CORS headers merged regardless of auth). No WebSocket join, no track, no media.

## Discriminator (load-bearing, empirically grounded on the Go oracle)

The five profiles vary the **request stimulus** (method/path/Origin/ACRM/ACRH); the
daemon config is identical, so a single daemon launch per side exercises all five.

| profile | method | path | Origin | ACRM | ACRH | status | CORS response |
|---|---|---|---|---|---|---|---|
| `preflight-allowed`           | OPTIONS | / | yes | POST  | X-Custom-Header | 204 | ACAO=origin, ACAM=POST, ACAH=X-Custom-Header, ACMA=86400, Vary=preflight |
| `preflight-disallowed-method` | OPTIONS | / | yes | TRACE | —               | 204 | Vary=preflight ONLY (method gate) |
| `preflight-no-origin`         | OPTIONS | / | —   | POST  | X-Custom-Header | 204 | Vary=preflight ONLY (origin gate) |
| `actual-get-origin`           | GET | /cors-probe-absent | yes | — | — | 404 | ACAO=origin, Expose=*, Vary=Origin |
| `actual-no-origin`            | GET | /cors-probe-absent | —   | — | — | 404 | Vary=Origin ONLY (origin gate) |

`preflight-allowed` and `actual-get-origin` are **DISCRIMINATING**; the other three are
**GATING**. An accept-but-ignore mutant that shadows the request `Origin` to `None`
keeps the structural `204+Vary` (preflight) and `404+Vary:Origin` (actual) but drops
every origin-conditional header (`ACAO/ACAM/ACAH/ACMA`, `ACAO/Expose`). The two
discriminating profiles then **FAIL** while the three gating profiles **SURVIVE** —
specific, not indiscriminate.

## Normalization (scope limit)

The CORS response-header semantics are fully deterministic (no SDP, no candidates, no
per-launch nonce), so they **are** compared byte-equal cross-side: `status`, `acao`,
`acam`, `acah`, `acma`, `expose`, `credentials`, `private_network`, `vary`. The daemon
HTTP port and the reserved media ports are per-launch nondeterministic and are recorded
in `execution.json` for transparency but are **not** cross-compared. The actual-request
route is a fixed nonexistent path that 404s on both sides, so the status is deterministic
and media-free. No comparison field is deleted; these are explicit scope bounds. This
harness proves the `middleware.cors` response-header consumer surface only — not the
agent-endpoint PUT CORS variant, not the `/rtc/validate` `ACAO="*"` auth special-case,
and not the full HTTP surface.

## Privacy normalization (commit boundary)

The manifest embeds raw daemon `server_log` provenance. Any RFC1918 private IPv4 literal
is rewritten to RFC 5737 TEST-NET-1 (`192.0.2.<last-octet>`) before writing, preserving
the last octet for correlation while removing the real network identity. The redaction is
pattern-based (no real literal is embedded in the harness source). The loopback-only
fixture config normally emits no private literal, so this is a defensive no-op guard, not
a data transform.

## Files

- `observe.go` — self-contained media-free HTTP CORS observer (separate copy from the C34
  `rtc-use-ice-lite` observer; swaps the signal-join stimulus for five deterministic CORS
  probes, names `HTTP_BEHAVIOR:middleware.cors` as the driven item, keeps the C34 observer
  identity unchanged for anti-splicing). Pure Go stdlib (no livekit module imports).
- `run-paired.py` — wraps the observer once per side, normalizes, asserts the per-side
  Go-derived contract per profile, compares cross-side, emits `BATCH.json` + `execution.json`,
  exits 0 iff every profile and control passes.

## Reproduce

Build the immutable binaries (from the repo root):

```sh
# Rust acceptance (default target dir)
cargo build --release -p livecloud --bin livecloud

# Locked Go oracle (v1.13.10 e5655ac2)
cd references/livekit-stable
GOTOOLCHAIN=go1.26.0 GOPROXY=off GOWORK=off go build -mod=readonly \
  -o /tmp/c35-build/livekit-server-go ./cmd/server

# C35 observer (pure stdlib; pinned toolchain for reproducibility)
GOTOOLCHAIN=go1.26.0 GOPROXY=off GOWORK=off go build -mod=readonly \
  -o /tmp/c35-build/cors-observer \
  /Users/zoujiaqing/projects/livekit-rs/tests/interop/rtc-http-cors/observe.go
```

Run the paired differential (fresh output dir; the two observer invocations run
sequentially, each launching one daemon that exercises all five probes):

```sh
python3 tests/interop/rtc-http-cors/run-paired.py \
  --go /tmp/c35-build/livekit-server-go \
  --rust /Users/zoujiaqing/projects/livekit-rs/target/release/livecloud \
  --observer /tmp/c35-build/cors-observer \
  --output /tmp/c35-cors-run
```

Expected: `PASS_LAUNCHED_MAIN_HTTP_CORS`, 5 profiles, 12 controls, empty
`failed_profiles`/`failed_controls`, `input_identity_stable=true`, exit 0.

## Falsifiability (positive control)

An accept-but-ignore mutant — `http.rs:192` shadows the request `Origin` to `None` so
every origin-conditional CORS header is dropped while the structural `204+Vary` /
`404+Vary:Origin` survive — is built in an isolated `CARGO_TARGET_DIR` and run on the
SAME harness/observer/Go oracle. It must FAIL `preflight-allowed` and `actual-get-origin`
(and the `preflight_allowed_emits_cors_rust` / `actual_get_origin_emits_cors_rust`
controls), while the three gating profiles and the remaining controls survive. The
mutant receipt is nested under `positive-control/` and is **not** cited as acceptance
evidence. The mutation is then reverted byte-identical and the acceptance binary rebuilt
to confirm it reproduces the protected identity.