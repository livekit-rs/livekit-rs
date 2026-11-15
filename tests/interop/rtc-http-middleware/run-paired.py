#!/usr/bin/env python3
"""SPEC-0077 C36 — launched-MAIN HTTP_BEHAVIOR:middleware.body_limit +
HTTP_BEHAVIOR:middleware.path_normalization paired Go/Rust differential.

Two ledger leaves are named: HTTP_BEHAVIOR:middleware.body_limit and
HTTP_BEHAVIOR:middleware.path_normalization. The locked Go oracle (v1.13.10
e5655ac2 == binary 4bc2378e) and the Rust livecloud release candidate are each
launched as the REAL main server by the self-contained rtc-http-middleware observer
(tests/interop/rtc-http-middleware/observe.go), ONCE PER CONFIG GROUP. body_limit is
config driven (limit.max_api_request_body_size), so three daemon launches per side
exercise the eight deterministic, media-free, unauthenticated HTTP probes:

    body-limit-small     limit=1024   -> body-oversize/undersize/boundary-equal
    body-limit-zero      limit=0      -> body-zero-unlimited
    path-normalization   limit=default-> path-double/single/triple-slash + unknown

The observer reserves the configured media ports in a LOW band (40000-40999, below
the macOS ephemeral range 49152-65535), releases the reservations so the daemon can
bind, settles past the Go Start() init window, drives the group's probes, and records
the exact status/body/content-type/location semantics of each. This harness wraps the
observer per (side, group), asserts the per-side Go-derived contract per profile, then
compares cross-side.

Upstream consumer (locked Go):
    pkg/service/utils.go:49      ErrRequestBodyTooLarge = "request body too large".
    pkg/service/utils.go:131-154 RequestBodyLimiter: maxBytes<=0||Body==nil -> next;
                                 ContentLength>maxBytes -> HandleError(413, ErrRequestBodyTooLarge);
                                 else MaxBytesReader. BEFORE auth (server.go:484-503).
    pkg/service/utils.go:103-121 RemoveDoubleSlashes/WithPathNormalization: strip ONE
                                 leading '/' when EscapedPath starts with "//".
    pkg/service/server.go:490    NewRequestBodyLimiter(conf.Limit.MaxAPIRequestBodySize).
    pkg/service/server.go:515    WithPathNormalization(dispatch) (normalization outermost).
    pkg/service/server.go:119,135 mux = http.NewServeMux(); "/" -> defaultHandler.
    pkg/config/config.go:397,631 Limit.MaxAPIRequestBodySize int64 (default 10<<20; 0 unbounded).

Rust mirror (complete launched-MAIN consumer chain, all anchors re-verified):
    lk-compat/src/http.rs:94-104   middleware_chain recover->cors->body_limit->auth->routes.
    lk-compat/src/http.rs:273-287  body_limit_layer: max>0 && declared Content-Length>max ->
                                   413 "request body too large"; else lazy wrap bound max.
    lk-compat/src/http.rs:149-164  normalize_path_layer/strip_double_slash: strip ONE leading '/'.
    lk-compat/src/http.rs:132-142  not_found(): 404 "404 page not found\\n".
    lk-compat/src/twirp/mod.rs:188-196 body read with limit BEFORE service auth;
                                   ReadError::Limit -> codec.body_read_error().
    lk-compat/src/twirp/codec.rs:42-47 body_read_error(): Json -> malformed.
    lk-compat/src/twirp/error.rs:65    Code::Malformed -> HTTP 400.
    lk-config/src/official.rs:1049     max_api_request_body_size: i64 = 10<<20.
    lk-config/src/normalize.rs:437     max_request_body = limit.max_api_request_body_size.max(0).
    rtc-core/src/config.rs:127,935     pub max_request_body: u64 (default 10<<20).

Discriminator choice (empirically grounded on the Go oracle, load-bearing): the eight
profiles vary the REQUEST stimulus (method/path/Content-Length/body); the daemon config
varies per group. The observable is the deterministic status/body/content-type/location:
  - body-oversize-declared    POST /twirp/.../ListRooms CL=2048 limit=1024
                              -> 413 "request body too large" (before auth)   [DISCRIMINATING]
  - body-undersize-declared   POST /twirp/.../ListRooms CL=2 {} limit=1024
                              -> 401 unauthenticated (not 413)                [GATING]
  - body-boundary-equal       POST /twirp/.../ListRooms CL=1024 limit=1024
                              -> 401 unauthenticated (Go check is '>', not '>=')[GATING]
  - body-zero-unlimited       POST /twirp/.../ListRooms CL=2048 limit=0
                              -> 401 unauthenticated (limiter disabled)       [GATING]
  - path-double-slash-twirp   POST //twirp/.../ListRooms {}
                              -> 401 (normalized to the real route, non-404)  [DISCRIMINATING]
  - path-single-slash-twirp   POST /twirp/.../ListRooms {}
                              -> 401 (real route)                             [CONTROL]
  - path-double-slash-unknown POST //nonexistent-path {}
                              -> 404 (normalized, still no route)             [GATING]
A body_limit accept-but-ignore mutant (body_limit_layer no-op) drops the declared-size
413; the oversize request reaches the twirp body reader whose own limit yields
malformed(400) — body-oversize-declared FAILS (413->400) while the three gating body
profiles SURVIVE. A path_normalization accept-but-ignore mutant (strip_double_slash ->
None) leaves //twirp unnormalized -> 404, so path-double-slash-twirp FAILS (401->404)
while the single-slash control and unknown-path gating profile SURVIVE. Specific.

HONEST SPLIT (confirmed router residual, NOT a middleware failure): the eighth probe
path-triple-slash-twirp (POST ///twirp/.../ListRooms) confirms the MIDDLEWARE strips
exactly ONE leading slash on BOTH sides (///twirp -> //twirp). The residual //twirp is
then handled by the Go http.ServeMux path cleaner, which issues a 307 redirect to
/twirp/... (Location header, empty body), whereas the Rust axum router has no ServeMux
path-clean redirect and returns 404. This is a DISTINCT router (ServeMux cleanPath)
concern, outside the strip-one-leading-slash middleware leaf. It is recorded as a
CONFIRMED OPEN gap under a separate ledger item
HTTP_BEHAVIOR:routing.serve_mux_path_clean_redirect and a separate residual receipt
(router-redirect-residual.json, status DOCUMENTED_OPEN_GAP). It is NOT counted toward
the middleware acceptance PASS and NOT deleted. The seven acceptance profiles above
are the middleware consumer surface this harness proves.

Normalization (scope limit): the status/body/content-type/location semantics of the
seven acceptance profiles are fully deterministic (no SDP, no candidates, no per-launch
nonce; the unauthenticated twirp error is the static {"code":"unauthenticated","msg":
"permissions denied"}), so they ARE compared byte-equal cross-side. The daemon HTTP port
and reserved media ports are per-launch nondeterministic and recorded for transparency
but NOT cross-compared. No comparison field is deleted; these are explicit scope bounds.
This harness proves the body_limit declared-size reject and the path_normalization
strip-one-leading-slash middleware consumer surfaces only — not the chunked/unknown-length
MaxBytesReader lazy face beyond the declared-size fast path, not the ServeMux full
path-clean redirect (split out), and not the full HTTP surface.

Privacy normalization (commit boundary): the manifest embeds raw daemon server_log
provenance. Any RFC1918 private IPv4 literal is rewritten to RFC 5737 TEST-NET-1
(192.0.2.<last-octet>) before writing, preserving the last octet for correlation while
removing the real network identity. The redaction is pattern-based (no real literal is
embedded in this source). The loopback-only fixture config normally emits no private
literal, so this is a defensive no-op guard, not a data transform.

Out of scope (honest residual): the chunked/unknown-length body lazy bound beyond the
declared-size fast path, the ServeMux full path-clean redirect (split to a separate OPEN
item), other HTTP middleware (auth, recovery) and the full HTTP surface remain open.
Every original observer exit code is preserved; no pipe filters a result.
"""
import argparse, hashlib, json, pathlib, re, subprocess

FIXED_GO_SHA = '4bc2378e636740b29093061036249ffc9ebbef91f4ece1de12177c8dfee1e7dc'
COVERS = ['HTTP_BEHAVIOR:middleware.body_limit', 'HTTP_BEHAVIOR:middleware.path_normalization']
RESIDUAL_ITEM = 'HTTP_BEHAVIOR:routing.serve_mux_path_clean_redirect'
GROUPS = ['body-limit-small', 'body-limit-zero', 'path-normalization']
# Seven ACCEPTANCE profiles (the two middleware consumer surfaces). The eighth probe
# (path-triple-slash-twirp) is a confirmed router residual, split out below.
PROFILES = ['body-oversize-declared', 'body-undersize-declared', 'body-boundary-equal',
            'body-zero-unlimited', 'path-double-slash-twirp', 'path-single-slash-twirp',
            'path-double-slash-unknown']
RESIDUAL_PROFILES = ['path-triple-slash-twirp']

UNAUTH_BODY = '{"code":"unauthenticated","msg":"permissions denied"}'
NOT_FOUND_BODY = '404 page not found\n'
TOO_LARGE_BODY = 'request body too large'
JSON_CT = 'application/json'
TEXT_CT = 'text/plain; charset=utf-8'
TWIRP_LOCATION = '/twirp/livekit.RoomService/ListRooms'

# Expected DERIVED observables per acceptance profile, EMPIRICALLY probed on the locked
# Go oracle (/tmp/c36-probe-go) and confirmed identical on the Rust candidate
# (/tmp/c36-probe-rust). The differential asserts these on EACH side independently,
# then compares cross-side.
CONTRACT = {
    'body-oversize-declared': {'status': 413, 'body': TOO_LARGE_BODY, 'content_type': TEXT_CT, 'location': ''},
    'body-undersize-declared': {'status': 401, 'body': UNAUTH_BODY, 'content_type': JSON_CT, 'location': ''},
    'body-boundary-equal': {'status': 401, 'body': UNAUTH_BODY, 'content_type': JSON_CT, 'location': ''},
    'body-zero-unlimited': {'status': 401, 'body': UNAUTH_BODY, 'content_type': JSON_CT, 'location': ''},
    'path-double-slash-twirp': {'status': 401, 'body': UNAUTH_BODY, 'content_type': JSON_CT, 'location': ''},
    'path-single-slash-twirp': {'status': 401, 'body': UNAUTH_BODY, 'content_type': JSON_CT, 'location': ''},
    'path-double-slash-unknown': {'status': 404, 'body': NOT_FOUND_BODY, 'content_type': TEXT_CT, 'location': ''},
}
# The confirmed router residual: Go ServeMux 307 path-clean redirect vs Rust axum 404.
RESIDUAL_GO = {'status': 307, 'body': '', 'content_type': '', 'location': TWIRP_LOCATION}
RESIDUAL_RUST = {'status': 404, 'body': NOT_FOUND_BODY, 'content_type': TEXT_CT, 'location': ''}

COMPARED = ['status', 'body', 'content_type', 'location']
TRANSPARENT = ['configured_port_range_start', 'configured_port_range_end', 'configured_tcp_port']

# RFC1918 private IPv4 -> RFC 5737 TEST-NET-1, preserving the last octet.
_PRIVATE_IP = re.compile(
    r'\b(?:192\.168|10|172\.(?:1[6-9]|2[0-9]|3[01]))\.\d{1,3}\.(\d{1,3})\b')


def redact_private_ips(text):
    return _PRIVATE_IP.sub(lambda m: '192.0.2.' + m.group(1), text)


def sha(path):
    return hashlib.sha256(pathlib.Path(path).read_bytes()).hexdigest()


def normalize_probe(p):
    return {
        'status': p.get('status'),
        'body': p.get('body'),
        'content_type': p.get('content_type'),
        'location': p.get('location'),
    }


def normalize_group(result, observer_exit):
    lifecycle = {
        'refused': result.get('refused') is True,
        'server_exit': result.get('server_exit'),
        'ports_released': result.get('server_ports_released') is True,
        'binary_unchanged': result.get('binary_unchanged') is True,
        'observer_exit': observer_exit,
    }
    lifecycle['exit_integrity'] = (
        observer_exit == 0 and lifecycle['binary_unchanged'] and lifecycle['ports_released']
        and lifecycle['server_exit'] == 0 and not lifecycle['refused'])
    names = result.get('probe_names', []) or list(result.get('probes', {}).keys())
    probes = {name: normalize_probe(result.get('probes', {}).get(name, {})) for name in names}
    transparent = {k: result.get(k) for k in TRANSPARENT}
    return {'lifecycle': lifecycle, 'probes': probes, 'transparent': transparent,
            'configured_body_limit': result.get('configured_body_limit')}


def contract_ok(n, profile):
    return all(n[k] == v for k, v in CONTRACT[profile].items())


def cross_equal(g, r):
    return all(g[k] == r[k] for k in COMPARED)


p = argparse.ArgumentParser()
p.add_argument('--go', required=True, type=pathlib.Path)
p.add_argument('--rust', required=True, type=pathlib.Path)
p.add_argument('--observer', required=True, type=pathlib.Path)
p.add_argument('--output', required=True, type=pathlib.Path)
a = p.parse_args()
for name in ('go', 'rust', 'observer'):
    path = getattr(a, name)
    if not path.is_absolute():
        raise SystemExit(f'--{name} must be an absolute path: {path}')
a.output = a.output.resolve()
a.output.mkdir(parents=True, exist_ok=False)

binaries = {'go': {'path': str(a.go), 'sha256': sha(a.go)},
            'rust': {'path': str(a.rust), 'sha256': sha(a.rust)}}
observer_sha = sha(a.observer)

NORM = {}
RAW = {}
ALL_PROBES = {}
LIFE_OK = {}
for side in ('go', 'rust'):
    binary = a.go if side == 'go' else a.rust
    NORM[side] = {}
    RAW[side] = {}
    ALL_PROBES[side] = {}
    for group in GROUPS:
        out = a.output / side / group
        cmd = [str(a.observer), '--binary', str(binary), '--output', str(out),
               '--group', group, '--enable-loopback=true']
        proc = subprocess.run(cmd, capture_output=True, text=True)
        result_path = out / 'result.json'
        result = json.loads(result_path.read_text()) if result_path.exists() else {}
        ng = normalize_group(result, proc.returncode)
        NORM[side][group] = ng
        RAW[side][group] = {'exit': proc.returncode, 'result': result, 'stderr': proc.stderr[-2000:]}
        ALL_PROBES[side].update(ng['probes'])
    LIFE_OK[side] = all(NORM[side][g]['lifecycle']['exit_integrity'] for g in GROUPS)

P = {}
for profile in PROFILES:
    g, r = ALL_PROBES['go'][profile], ALL_PROBES['rust'][profile]
    go_ok = contract_ok(g, profile)
    rust_ok = contract_ok(r, profile)
    equal = cross_equal(g, r)
    life_ok = LIFE_OK['go'] and LIFE_OK['rust']
    P[profile] = {
        'match': go_ok and rust_ok and equal and life_ok,
        'go_contract_ok': go_ok,
        'rust_contract_ok': rust_ok,
        'cross_side_equal': equal,
        'lifecycle_ok': life_ok,
        'go_observables': g,
        'rust_observables': r,
    }

controls = {}
for side in ('go', 'rust'):
    pr = ALL_PROBES[side]
    # body_limit POSITIVE control: the discriminating 413 CAN be present.
    controls[f'oversize_emits_413_{side}'] = (
        pr['body-oversize-declared']['status'] == 413
        and pr['body-oversize-declared']['body'] == TOO_LARGE_BODY)
    # body_limit GATING controls: undersize/boundary/zero are NOT rejected with 413.
    controls[f'undersize_not_413_{side}'] = pr['body-undersize-declared']['status'] != 413
    controls[f'boundary_not_413_{side}'] = pr['body-boundary-equal']['status'] != 413
    controls[f'zero_unlimited_not_413_{side}'] = pr['body-zero-unlimited']['status'] != 413
    # path_normalization POSITIVE control: //twirp normalizes to the real route (non-404).
    controls[f'double_slash_twirp_normalized_{side}'] = (
        pr['path-double-slash-twirp']['status'] == 401
        and pr['path-double-slash-twirp']['body'] == UNAUTH_BODY)
    # path_normalization CONTROL/GATING: single slash reaches the route; unknown 404s.
    controls[f'single_slash_twirp_route_{side}'] = pr['path-single-slash-twirp']['status'] == 401
    controls[f'double_slash_unknown_404_{side}'] = pr['path-double-slash-unknown']['status'] == 404
    # Lifecycle integrity: clean launch/shutdown across all three groups, ports released.
    controls[f'lifecycle_integrity_{side}'] = LIFE_OK[side]

# Confirmed router residual (split out, NOT an acceptance profile, NOT deleted).
RESIDUAL = {}
for profile in RESIDUAL_PROFILES:
    g, r = ALL_PROBES['go'][profile], ALL_PROBES['rust'][profile]
    RESIDUAL[profile] = {
        'go_observables': g,
        'rust_observables': r,
        'cross_side_equal': cross_equal(g, r),
        'go_matches_serve_mux_redirect': g == RESIDUAL_GO,
        'rust_matches_axum_404': r == RESIDUAL_RUST,
        'item': RESIDUAL_ITEM,
        'note': ('Go http.ServeMux cleanPath issues a 307 redirect for the residual '
                 '//twirp (after the middleware strips ONE leading slash from ///twirp); '
                 'the Rust axum router has no ServeMux path-clean redirect and returns 404. '
                 'The strip-one-leading-slash MIDDLEWARE itself is faithful on both sides '
                 '(///twirp -> //twirp); the divergence is the downstream router path cleaner, '
                 'a distinct concern tracked OPEN under ' + RESIDUAL_ITEM + '.'),
    }

failed_profiles = [name for name, e in P.items() if not e['match']]
failed_controls = [k for k, v in controls.items() if not v]
all_match = not failed_profiles and not failed_controls

manifest = {
    'status': 'PASS_LAUNCHED_MAIN_HTTP_MIDDLEWARE' if all_match else 'FAIL',
    'case_count': len(PROFILES),
    'items': COVERS,
    'profiles': P,
    'cross_profile_controls': controls,
    'failed_profiles': failed_profiles,
    'failed_controls': failed_controls,
    'open_residuals': RESIDUAL,
    'residual_item': RESIDUAL_ITEM,
    'binaries': binaries,
    'observer_sha256': observer_sha,
    'input_identity_stable': binaries['go']['sha256'] == FIXED_GO_SHA,
    'harness_sha256': sha(pathlib.Path(__file__).resolve()),
    'lifecycle': {side: {g: NORM[side][g]['lifecycle'] for g in GROUPS} for side in ('go', 'rust')},
    'transparent': {side: {g: NORM[side][g]['transparent'] for g in GROUPS} for side in ('go', 'rust')},
    'configured_body_limit': {side: {g: NORM[side][g]['configured_body_limit'] for g in GROUPS}
                              for side in ('go', 'rust')},
    'privacy_normalization': 'RFC1918 private IPv4 literals in raw server_log rewritten to RFC5737 TEST-NET-1 (last octet preserved); pattern-based, no real literal embedded.',
    'raw': RAW,
}
(a.output / 'execution.json').write_text(
    redact_private_ips(json.dumps(manifest, indent=2, ensure_ascii=False) + '\n'))

batch = {
    'status': manifest['status'],
    'case_count': len(PROFILES),
    'items': COVERS,
    'profiles': {name: {'match': e['match'], 'go_contract_ok': e['go_contract_ok'],
                        'rust_contract_ok': e['rust_contract_ok'],
                        'cross_side_equal': e['cross_side_equal'], 'lifecycle_ok': e['lifecycle_ok'],
                        'go_observables': e['go_observables'], 'rust_observables': e['rust_observables']}
                 for name, e in P.items()},
    'cross_profile_controls': controls,
    'failed_profiles': failed_profiles,
    'failed_controls': failed_controls,
    'open_residuals': RESIDUAL,
    'residual_item': RESIDUAL_ITEM,
    'binaries': binaries,
    'observer_sha256': observer_sha,
    'input_identity_stable': manifest['input_identity_stable'],
    'harness_sha256': manifest['harness_sha256'],
    'lifecycle': manifest['lifecycle'],
}
(a.output / 'BATCH.json').write_text(
    redact_private_ips(json.dumps(batch, indent=2, ensure_ascii=False) + '\n'))

# Separate residual receipt for the NEW OPEN router item (a confirmed cross-side gap,
# NOT a PASS, NOT cited for the middleware VERIFIED leaves).
residual_receipt = {
    'status': 'DOCUMENTED_OPEN_GAP',
    'item': RESIDUAL_ITEM,
    'case_count': len(RESIDUAL_PROFILES),
    'profiles': RESIDUAL,
    'binaries': binaries,
    'observer_sha256': observer_sha,
    'input_identity_stable': manifest['input_identity_stable'],
    'harness_sha256': manifest['harness_sha256'],
    'scope': ('Confirmed launched-MAIN cross-side difference: Go http.ServeMux cleanPath '
              '307 path-clean redirect vs Rust axum 404 for residual double-slash paths '
              '(///twirp -> middleware strips one -> //twirp -> Go mux redirects, Rust 404). '
              'The strip-one-leading-slash middleware (HTTP_BEHAVIOR:middleware.path_normalization) '
              'is faithful; this is the downstream ServeMux full path-clean redirect '
              '(leading/internal //, /./, /../, trailing-slash), a distinct router concern. '
              'Remains open: a faithful Rust ServeMux path-clean redirect layer.'),
}
(a.output / 'router-redirect-residual.json').write_text(
    redact_private_ips(json.dumps(residual_receipt, indent=2, ensure_ascii=False) + '\n'))

print(json.dumps({'status': manifest['status'], 'case_count': len(PROFILES),
                  'failed_profiles': failed_profiles, 'failed_controls': failed_controls,
                  'input_identity_stable': manifest['input_identity_stable'],
                  'residual_cross_side_equal': {k: v['cross_side_equal'] for k, v in RESIDUAL.items()}}))
raise SystemExit(0 if all_match else 1)