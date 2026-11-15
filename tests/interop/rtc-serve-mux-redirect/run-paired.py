#!/usr/bin/env python3
"""SPEC-0077 C37 — launched-MAIN HTTP_BEHAVIOR:routing.serve_mux_path_clean_redirect
paired Go/Rust differential.

One ledger leaf is named: HTTP_BEHAVIOR:routing.serve_mux_path_clean_redirect. This is
the DISTINCT downstream router concern split out of C36 (router-redirect-residual.json,
DOCUMENTED_OPEN_GAP): the strip-one-leading-slash MIDDLEWARE is faithful on both sides
(///twirp -> //twirp), but Go's http.ServeMux then cleanPath's the residual //twirp and
issues a 307 to /twirp, whereas the pre-C37 Rust axum router had no ServeMux path cleaner
and returned 404. C37 adds the faithful Rust cleanPath redirect layer; this harness
proves the reconciled cross-side parity.

The locked Go oracle (v1.13.10 e5655ac2 == binary 4bc2378e) and the Rust livecloud
release candidate are each launched as the REAL main server by the self-contained
rtc-serve-mux-redirect observer (tests/interop/rtc-serve-mux-redirect/observe.go), ONCE
(the cleanPath redirect is unconditional, not config driven). The observer reserves the
configured media ports in a LOW band (40000-40999, below the macOS ephemeral range
49152-65535), releases the reservations so the daemon can bind, settles past the Go
Start() init window, drives the ten deterministic, media-free, unauthenticated HTTP
probes, and records the exact status/body/content-type/location semantics of each. This
harness wraps the observer per side, asserts the per-side Go-derived contract per
profile, then compares cross-side.

Upstream consumer (locked Go, Go 1.26 stdlib):
    net/http/server.go:2593-2612 cleanPath (root prepend, path.Clean, trailing-slash
                                 restore) over the ESCAPED path.
    net/http/server.go:2676-2697 findHandler: non-CONNECT path = cleanPath(escapedPath);
                                 if path != escapedPath -> RedirectHandler(
                                 url.URL{Path: path, RawQuery}.String(),
                                 StatusTemporaryRedirect). 307 UNCONDITIONALLY (the old
                                 301 needs GODEBUG=httpmuxgo121, which livekit omits).
    net/http/server.go:2367-2419 Redirect: Location = hexEscapeNonASCII(url); GET/HEAD
                                 with no prior Content-Type -> text/html; charset=utf-8;
                                 only GET writes `<a href="htmlEscape(url)">Temporary
                                 Redirect</a>.\\n` + Fprintln newline.
    net/http/server.go:2665      CONNECT is not canonicalized.
    pkg/service/server.go:484-503 api chain Recovery->CORS->BodyLimiter->Auth->mux: the
                                 mux cleanPath is INNERMOST, after auth.
    pkg/service/server.go:505-514 agentFront is a separate chain with NO mux, so agent
                                 endpoint paths are not cleanPath-redirected.

Rust mirror (C37 consumer chain, all anchors re-verified):
    lk-compat/src/http.rs middleware_chain recover->cors->body_limit->auth->
                          serve_mux_redirect->routes (redirect innermost, after auth).
    lk-compat/src/http.rs serve_mux_redirect_layer: CONNECT skip; clean_path(escaped);
                          cleaned != escaped -> serve_mux_redirect(method, redirect_url).
    lk-compat/src/http.rs clean_path/path_clean_rooted: Go cleanPath + rooted path.Clean.
    lk-compat/src/http.rs serve_mux_redirect: 307; GET text/html + exact html body; HEAD
                          text/html no body; other Location only; hex_escape_non_ascii/
                          html_escape mirror Go.
    lk-compat/src/http.rs agent_endpoint_router: NO serve_mux_redirect (Go agents chain
                          has no mux).

Discriminator choice (empirically grounded on the Go oracle, load-bearing): the ten
profiles vary the REQUEST stimulus (method/path/query); the daemon config is the single
default-limit group. The observable is the deterministic status/body/content-type/location:
  - triple-slash-post            POST ///twirp/.../ListRooms {}
                                 -> strip ONE -> //twirp -> cleanPath -> 307 /twirp [DISCRIMINATING]
  - triple-slash-get             GET  ///twirp/.../ListRooms
                                 -> 307 + text/html + exact Go html body            [DISCRIMINATING]
  - triple-slash-head            HEAD ///twirp/.../ListRooms
                                 -> 307 + text/html, no body                        [DISCRIMINATING]
  - internal-double-slash-post   POST /twirp//livekit.RoomService/ListRooms {}
                                 -> cleanPath collapses internal // -> 307          [DISCRIMINATING]
  - dot-segment-post             POST /twirp/./livekit.RoomService/ListRooms {}
                                 -> cleanPath drops "." -> 307                      [DISCRIMINATING]
  - dotdot-segment-post          POST /twirp/foo/../livekit.RoomService/ListRooms {}
                                 -> cleanPath resolves ".." -> 307                  [DISCRIMINATING]
  - trailing-slash-redirect-post POST /twirp//livekit.RoomService/ListRooms/ {}
                                 -> 307 Location keeps the trailing slash           [DISCRIMINATING]
  - query-preservation-post      POST /twirp//livekit.RoomService/ListRooms?room=y&x=1 {}
                                 -> 307 Location keeps the raw query                [DISCRIMINATING]
                                 (NON-auth query: access_token would be 401-rejected
                                  by APIKeyAuth BEFORE the mux cleanPath redirect)
  - clean-path-control-post      POST /twirp/livekit.RoomService/ListRooms {}
                                 -> cleanPath identity -> real route 401 (non-307)  [GATING]
  - double-slash-control-post    POST //twirp/livekit.RoomService/ListRooms {}
                                 -> strip ONE -> /twirp -> identity -> 401 (non-307)[GATING]
A cleanPath accept-but-ignore mutant (serve_mux_redirect_layer always next.run) drops
every 307: the residual //twirp then 404s and the internal-clean paths reach the twirp
route (401/404), so all eight DISCRIMINATING profiles FAIL (307 -> non-307) while the two
GATING controls SURVIVE at 401. Specific, not indiscriminate.

Contract provenance (honest): the CONTRACT below is DERIVED from the Go 1.26 stdlib
source above. The holistic phase empirically confirms it on the locked Go oracle; the
harness asserts it on EACH side independently (go_contract_ok / rust_contract_ok), so a
derivation error FAILS on the Go side and is corrected to the empirical Go truth — the
Rust side is never forced to match a wrong contract.

Normalization (scope limit): the status/body/content-type/location semantics of the ten
profiles are fully deterministic (no SDP, no candidates, no per-launch nonce; the
unauthenticated twirp error is the static {"code":"unauthenticated","msg":"permissions
denied"}), so they ARE compared byte-equal cross-side. The daemon HTTP port and reserved
media ports are per-launch nondeterministic and recorded for transparency but NOT
cross-compared. No comparison field is deleted; these are explicit scope bounds. This
harness proves the ServeMux cleanPath 307 redirect consumer surface (leading/internal //,
/./, /../, trailing-slash preservation, query preservation, GET/HEAD/POST method shape)
— not the percent-encoding/RawPath re-escape edge (url.URL.String encodePath), not the
vacuous trailing-slash matchOrRedirect (livekit registers no exact pattern ending in "/"
but the root catch-all), and not the full HTTP surface.

Privacy normalization (commit boundary): the manifest embeds raw daemon server_log
provenance. Any RFC1918 private IPv4 literal is rewritten to RFC 5737 TEST-NET-1
(192.0.2.<last-octet>) before writing, preserving the last octet for correlation while
removing the real network identity. The redaction is pattern-based (no real literal is
embedded in this source). The loopback-only fixture config normally emits no private
literal, so this is a defensive no-op guard, not a data transform.

Out of scope (honest residual): the percent-encoding/RawPath re-escape edge, the vacuous
trailing-slash matchOrRedirect, other HTTP middleware and the full HTTP surface remain
open. Every original observer exit code is preserved; no pipe filters a result.
"""
import argparse, hashlib, json, pathlib, re, subprocess

FIXED_GO_SHA = '4bc2378e636740b29093061036249ffc9ebbef91f4ece1de12177c8dfee1e7dc'
COVERS = ['HTTP_BEHAVIOR:routing.serve_mux_path_clean_redirect']
GROUPS = ['serve-mux-redirect']
PROFILES = ['triple-slash-post', 'triple-slash-get', 'triple-slash-head',
            'internal-double-slash-post', 'dot-segment-post', 'dotdot-segment-post',
            'trailing-slash-redirect-post', 'query-preservation-post',
            'clean-path-control-post', 'double-slash-control-post']
DISCRIMINATING = ['triple-slash-post', 'triple-slash-get', 'triple-slash-head',
                  'internal-double-slash-post', 'dot-segment-post', 'dotdot-segment-post',
                  'trailing-slash-redirect-post', 'query-preservation-post']
GATING = ['clean-path-control-post', 'double-slash-control-post']

TWIRP_LOC = '/twirp/livekit.RoomService/ListRooms'
HTML_CT = 'text/html; charset=utf-8'
GET_BODY = '<a href="/twirp/livekit.RoomService/ListRooms">Temporary Redirect</a>.\n\n'
UNAUTH_BODY = '{"code":"unauthenticated","msg":"permissions denied"}'
JSON_CT = 'application/json'

# Expected DERIVED observables per profile (Go 1.26 stdlib source; empirically confirmed
# on the locked Go oracle in the holistic phase). Asserted on EACH side independently,
# then compared cross-side.
CONTRACT = {
    'triple-slash-post': {'status': 307, 'body': '', 'content_type': '', 'location': TWIRP_LOC},
    'triple-slash-get': {'status': 307, 'body': GET_BODY, 'content_type': HTML_CT, 'location': TWIRP_LOC},
    'triple-slash-head': {'status': 307, 'body': '', 'content_type': HTML_CT, 'location': TWIRP_LOC},
    'internal-double-slash-post': {'status': 307, 'body': '', 'content_type': '', 'location': TWIRP_LOC},
    'dot-segment-post': {'status': 307, 'body': '', 'content_type': '', 'location': TWIRP_LOC},
    'dotdot-segment-post': {'status': 307, 'body': '', 'content_type': '', 'location': TWIRP_LOC},
    'trailing-slash-redirect-post': {'status': 307, 'body': '', 'content_type': '', 'location': TWIRP_LOC + '/'},
    'query-preservation-post': {'status': 307, 'body': '', 'content_type': '', 'location': TWIRP_LOC + '?room=y&x=1'},
    'clean-path-control-post': {'status': 401, 'body': UNAUTH_BODY, 'content_type': JSON_CT, 'location': ''},
    'double-slash-control-post': {'status': 401, 'body': UNAUTH_BODY, 'content_type': JSON_CT, 'location': ''},
}

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
    # POSITIVE control: the discriminating 307 CAN be present (cleanPath redirect fires).
    controls[f'redirect_307_present_{side}'] = (
        pr['triple-slash-post']['status'] == 307
        and pr['triple-slash-post']['location'] == TWIRP_LOC)
    # Method shape: GET writes the exact html body + text/html; HEAD text/html no body.
    controls[f'get_html_body_{side}'] = (
        pr['triple-slash-get']['body'] == GET_BODY
        and pr['triple-slash-get']['content_type'] == HTML_CT)
    controls[f'head_no_body_{side}'] = (
        pr['triple-slash-head']['status'] == 307
        and pr['triple-slash-head']['body'] == ''
        and pr['triple-slash-head']['content_type'] == HTML_CT)
    # Internal // collapses to the canonical path.
    controls[f'internal_clean_307_{side}'] = (
        pr['internal-double-slash-post']['status'] == 307
        and pr['internal-double-slash-post']['location'] == TWIRP_LOC)
    # Trailing slash and raw query are preserved in the redirect target.
    controls[f'trailing_preserved_{side}'] = (
        pr['trailing-slash-redirect-post']['location'] == TWIRP_LOC + '/')
    controls[f'query_preserved_{side}'] = (
        pr['query-preservation-post']['location'] == TWIRP_LOC + '?room=y&x=1')
    # GATING controls: already-clean and strip-one-to-clean paths are NOT redirected.
    controls[f'clean_not_redirect_{side}'] = pr['clean-path-control-post']['status'] == 401
    controls[f'double_slash_not_redirect_{side}'] = pr['double-slash-control-post']['status'] == 401
    # Lifecycle integrity: clean launch/shutdown, ports released.
    controls[f'lifecycle_integrity_{side}'] = LIFE_OK[side]

failed_profiles = [name for name, e in P.items() if not e['match']]
failed_controls = [k for k, v in controls.items() if not v]
all_match = not failed_profiles and not failed_controls

manifest = {
    'status': 'PASS_LAUNCHED_MAIN_SERVE_MUX_REDIRECT' if all_match else 'FAIL',
    'case_count': len(PROFILES),
    'items': COVERS,
    'discriminating_profiles': DISCRIMINATING,
    'gating_profiles': GATING,
    'profiles': P,
    'cross_profile_controls': controls,
    'failed_profiles': failed_profiles,
    'failed_controls': failed_controls,
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
    'discriminating_profiles': DISCRIMINATING,
    'gating_profiles': GATING,
    'profiles': {name: {'match': e['match'], 'go_contract_ok': e['go_contract_ok'],
                        'rust_contract_ok': e['rust_contract_ok'],
                        'cross_side_equal': e['cross_side_equal'], 'lifecycle_ok': e['lifecycle_ok'],
                        'go_observables': e['go_observables'], 'rust_observables': e['rust_observables']}
                 for name, e in P.items()},
    'cross_profile_controls': controls,
    'failed_profiles': failed_profiles,
    'failed_controls': failed_controls,
    'binaries': binaries,
    'observer_sha256': observer_sha,
    'input_identity_stable': manifest['input_identity_stable'],
    'harness_sha256': manifest['harness_sha256'],
    'lifecycle': manifest['lifecycle'],
}
(a.output / 'BATCH.json').write_text(
    redact_private_ips(json.dumps(batch, indent=2, ensure_ascii=False) + '\n'))

print(json.dumps({'status': manifest['status'], 'case_count': len(PROFILES),
                  'failed_profiles': failed_profiles, 'failed_controls': failed_controls,
                  'input_identity_stable': manifest['input_identity_stable']}))
raise SystemExit(0 if all_match else 1)