# SPEC-0077 C37 — `HTTP_BEHAVIOR:routing.serve_mux_path_clean_redirect` launched-MAIN paired Go/Rust differential

This harness proves that the **launched MAIN server** (not an in-process component)
issues Go `http.ServeMux`'s `cleanPath` **307 path-clean redirect** identically to the
locked official LiveKit Server Go oracle. It closes the
`routing.serve_mux_path_clean_redirect` HTTP-behavior leaf — the **distinct downstream
router concern split out of C36** (`router-redirect-residual.json`,
`DOCUMENTED_OPEN_GAP`) — by a deterministic, **media-free, unauthenticated** HTTP probe
differential.

The C36 strip-one-leading-slash **middleware** (`middleware.path_normalization`) is
faithful on both sides (`///twirp → //twirp`). The residual `//twirp` is then handled by
Go's `http.ServeMux`, which `cleanPath`'s it to `/twirp` and issues a **307**; the pre-C37
Rust axum router had no `ServeMux` path cleaner and returned **404**. C37 adds the faithful
Rust `cleanPath` redirect layer; this harness proves the reconciled cross-side parity.

## Leaf

- `HTTP_BEHAVIOR:routing.serve_mux_path_clean_redirect` (unconditional router behavior;
  **no YAML config leaf**).

## What the behavior is (upstream contract, Go 1.26 stdlib)

```go
net/http/server.go:2593-2612 cleanPath: prepend root if missing, path.Clean, restore a
                             trailing slash path.Clean removed (except root). Operates on
                             the ESCAPED path, so percent-encoded bytes stay literal.
net/http/server.go:2676-2697 findHandler (non-CONNECT): path = cleanPath(escapedPath);
                             matchOrRedirect (trailing-slash) first; then if
                             path != escapedPath -> RedirectHandler(
                             url.URL{Path: path, RawQuery}.String(), StatusTemporaryRedirect).
                             BOTH redirects are 307 UNCONDITIONALLY (the old 301 needs
                             GODEBUG=httpmuxgo121, which livekit does not set).
net/http/server.go:2367-2419 Redirect: Location = hexEscapeNonASCII(url); GET/HEAD with no
                             prior Content-Type -> "text/html; charset=utf-8"; only GET
                             writes `<a href="htmlEscape(url)">Temporary Redirect</a>.\n`
                             plus Fprintln's trailing newline.
net/http/server.go:2665      CONNECT is authority-form and is NOT canonicalized.

pkg/service/server.go:484-503 api chain Recovery -> CORS -> BodyLimiter -> Auth -> mux: the
                             mux cleanPath is INNERMOST, after auth.
pkg/service/server.go:505-514 agentFront is a SEPARATE chain (AgentRecovery -> AgentCORS ->
                             auth), NO mux, so agent endpoint paths are NOT redirected.
pkg/service/server.go:119,135 mux = http.NewServeMux(); "/" -> defaultHandler. The only
                             registered pattern ending in "/" is the root catch-all;
                             twirp/rtc/agent are exact and WHIP is exact + {participant_id}
                             wildcard, so the trailing-slash matchOrRedirect is VACUOUS
                             (no exact pattern P+"/" exists for P != "").
```

So a request path that cleans to a different path is **307-redirected to the cleaned
path before routing**, with the query preserved; GET gets a `text/html` body, HEAD gets
the Content-Type but no body, POST/other get `Location` alone.

## Rust mirror (complete launched-MAIN consumer chain)

- middleware order: `crates/lk-compat/src/http.rs` `middleware_chain` — `recover → cors →
  body_limit → auth → serve_mux_redirect → routes` (`serve_mux_redirect_layer` is the
  **innermost** layer, at the mux boundary, after auth, before route dispatch).
- redirect layer: `serve_mux_redirect_layer` — CONNECT skip; `clean_path(escaped)`; if
  `cleaned != escaped` → `serve_mux_redirect(method, redirect_url(cleaned, query))`.
- cleaner: `clean_path` / `path_clean_rooted` — Go `cleanPath` + rooted `path.Clean`
  segment stack (collapse `//`, drop `.`, resolve `..` without escaping root, restore
  trailing slash, keep percent-encoded bytes literal).
- response: `serve_mux_redirect` — 307; GET → `text/html` CT + exact html body; HEAD → CT
  no body; other → `Location` only. `hex_escape_non_ascii` / `html_escape` mirror Go.
- agent chain: `agent_endpoint_router` — `auth → agent_cors → recover`, **no**
  `serve_mux_redirect` (Go's agents chain has no mux).

## Why a media-free HTTP probe (load-bearing)

The `cleanPath` redirect is a deterministic HTTP request-shape consumer on the main API
port. It depends only on the request method and escaped path, not on `rtc` config, ICE,
SDP or a published track. No WebSocket join, no track, no media.

## Config group (unconditional)

The redirect is unconditional, so the daemon is launched **once** per side under a single
default-limit group `serve-mux-redirect` (`limit.max_api_request_body_size = 10<<20`).

## Discriminators (load-bearing, empirically grounded on the Go oracle)

The ten profiles vary the request stimulus (method/path/query):

| profile | stimulus | status | body / content-type / location |
|---|---|---|---|
| `triple-slash-post`            | POST `///twirp/…/ListRooms` `{}` | 307 | `` / `` / `/twirp/livekit.RoomService/ListRooms` — **DISCRIMINATING** |
| `triple-slash-get`             | GET `///twirp/…/ListRooms` | 307 | `<a href="…">Temporary Redirect</a>.\n\n` / `text/html; charset=utf-8` / `…ListRooms` — **DISCRIMINATING** |
| `triple-slash-head`            | HEAD `///twirp/…/ListRooms` | 307 | `` / `text/html; charset=utf-8` / `…ListRooms` — **DISCRIMINATING** |
| `internal-double-slash-post`   | POST `/twirp//livekit.RoomService/ListRooms` `{}` | 307 | `` / `` / `…ListRooms` — **DISCRIMINATING** |
| `dot-segment-post`             | POST `/twirp/./livekit.RoomService/ListRooms` `{}` | 307 | `` / `` / `…ListRooms` — **DISCRIMINATING** |
| `dotdot-segment-post`          | POST `/twirp/foo/../livekit.RoomService/ListRooms` `{}` | 307 | `` / `` / `…ListRooms` — **DISCRIMINATING** |
| `trailing-slash-redirect-post` | POST `/twirp//livekit.RoomService/ListRooms/` `{}` | 307 | `` / `` / `…ListRooms/` (trailing slash kept) — **DISCRIMINATING** |
| `query-preservation-post`      | POST `/twirp//livekit.RoomService/ListRooms?room=y&x=1` `{}` | 307 | `` / `` / `…ListRooms?room=y&x=1` (raw query kept) — **DISCRIMINATING** |
| `clean-path-control-post`      | POST `/twirp/livekit.RoomService/ListRooms` `{}` | 401 | unauthenticated (cleanPath identity, non-307) — GATING |
| `double-slash-control-post`    | POST `//twirp/livekit.RoomService/ListRooms` `{}` | 401 | strip ONE → `/twirp` → identity → real route (non-307) — GATING |

The unauthenticated twirp error is the static
`{"code":"unauthenticated","msg":"permissions denied"}` on both sides. A **cleanPath**
accept-but-ignore mutant (`serve_mux_redirect_layer` always `next.run`) drops every 307:
the residual `//twirp` then 404s and the internal-clean paths reach the twirp route
(401/404), so all eight DISCRIMINATING profiles **FAIL** (307 → non-307) while the two
GATING controls **SURVIVE** at 401. Specific, not indiscriminate.

### Auth ordering (load-bearing for the query probe)

The Go `api` chain is `Recovery → CORS → RequestBodyLimiter → APIKeyAuth → mux`, so the
`ServeMux` `cleanPath` redirect is **innermost, after auth**. An **invalid** token
(`?access_token=x`) is rejected `401 invalid authorization token` (`text/plain`) by
`APIKeyAuth` **before** the mux ever sees the path — it would confound the redirect probe.
The query-preservation stimulus therefore uses a **non-auth** query (`?room=y&x=1`): with no
token the request passes anonymous through auth, reaches the mux, and the `cleanPath` 307
fires with the raw query preserved. This was confirmed empirically on the locked Go oracle
(the first derivation used `access_token=x` and the Go side returned 401, not 307 — the
per-side `go_contract_ok` gate caught the derivation error against empirical Go truth, and
the probe was corrected; the Rust side is never forced to match a wrong contract).

## Normalization (scope limit)

The `status`/`body`/`content_type`/`location` semantics of the ten profiles are fully
deterministic (no SDP, no candidates, no per-launch nonce), so they **are** compared
byte-equal cross-side. The daemon HTTP port and the reserved media ports are per-launch
nondeterministic and are recorded in `execution.json` for transparency but are **not**
cross-compared. No comparison field is deleted; these are explicit scope bounds. This
harness proves the `ServeMux` `cleanPath` 307 redirect consumer surface (leading/internal
`//`, `/./`, `/../`, trailing-slash preservation, query preservation, GET/HEAD/POST method
shape) — **not** the percent-encoding/RawPath re-escape edge (`url.URL.String` encodePath),
**not** the vacuous trailing-slash `matchOrRedirect`, and **not** the full HTTP surface.

## Privacy normalization (commit boundary)

The manifest embeds raw daemon `server_log` provenance. Any RFC1918 private IPv4 literal
is rewritten to RFC 5737 TEST-NET-1 (`192.0.2.<last-octet>`) before writing, preserving
the last octet for correlation while removing the real network identity. The redaction is
pattern-based (no real literal is embedded in the harness source). The loopback-only
fixture config normally emits no private literal, so this is a defensive no-op guard, not
a data transform.

## Files

- `observe.go` — self-contained media-free HTTP `ServeMux` redirect observer (separate copy
  from the C36 `rtc-http-middleware` observer; swaps the body-limit/strip-one-slash stimulus
  for the downstream `cleanPath` redirect probes, names the one driven item, keeps the C36
  observer identity unchanged for anti-splicing). Pure Go stdlib (no livekit module imports).
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
  -o /tmp/c37-build/livekit-server-go ./cmd/server

# C37 observer (pure stdlib; pinned toolchain for reproducibility)
GOTOOLCHAIN=go1.26.0 GOPROXY=off GOWORK=off go build -mod=readonly \
  -o /tmp/c37-build/mux-observer \
  /Users/zoujiaqing/projects/livekit-rs/tests/interop/rtc-serve-mux-redirect/observe.go
```

Run the paired differential (fresh output dir; two observer invocations run sequentially,
each launching one daemon):

```sh
python3 tests/interop/rtc-serve-mux-redirect/run-paired.py \
  --go /tmp/c37-build/livekit-server-go \
  --rust /Users/zoujiaqing/projects/livekit-rs/target/release/livecloud \
  --observer /tmp/c37-build/mux-observer \
  --output /tmp/c37-run
```

Expected: `PASS_LAUNCHED_MAIN_SERVE_MUX_REDIRECT`, 10 profiles, 18 controls, empty
`failed_profiles`/`failed_controls`, `input_identity_stable=true`, exit 0.

## Falsifiability (positive control)

A cleanPath accept-but-ignore mutant is built in an isolated `CARGO_TARGET_DIR` and run on
the SAME harness/observer/Go oracle:

- **cleanPath** — `serve_mux_redirect_layer` becomes a no-op (`next.run(req)`), ignoring
  `clean_path`. It must FAIL all eight DISCRIMINATING profiles (307 → 404/401) and the
  `redirect_307_present_rust` / `get_html_body_rust` / `internal_clean_307_rust` controls,
  while the two GATING controls (`clean-path-control-post`, `double-slash-control-post`)
  and the remaining gating controls survive at 401.

The mutant receipt is nested under `positive-control/` and is **not** cited as acceptance
evidence. The mutation is then reverted byte-identical and the acceptance binary rebuilt to
confirm it reproduces the protected identity.