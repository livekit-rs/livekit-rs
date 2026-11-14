#!/usr/bin/env python3
"""SPEC-0077 C35 — launched-MAIN HTTP_BEHAVIOR:middleware.cors paired Go/Rust differential.

One ledger leaf is named: HTTP_BEHAVIOR:middleware.cors. The locked Go oracle
(v1.13.10 e5655ac2 == binary 4bc2378e) and the Rust livecloud release candidate are
each launched ONCE as the REAL main server by the self-contained rtc-http-cors
observer (tests/interop/rtc-http-cors/observe.go). CORS is an UNCONDITIONAL HTTP
middleware on the main API port (not config driven), so a single daemon launch per
side exercises all five deterministic, media-free, unauthenticated HTTP probes.
The observer reserves the configured media ports in a LOW band (40000-40999, below
the macOS ephemeral range 49152-65535), releases the reservations so the daemon can
bind, settles past the Go Start() init window, drives the five CORS probes, and
records the exact CORS response-header semantics of each. This harness wraps the
observer per side, asserts the per-side Go-derived contract per profile, then
compares cross-side.

Upstream consumer (locked Go, vendored rs/cors v1.11.1):
    pkg/service/server.go:488      API cors.New(corsOptions([OPTIONS,HEAD,GET,POST,PATCH,DELETE]))
    pkg/service/server.go:525-536  corsOptions: AllowOriginFunc=func(string)bool{return true},
                                     AllowedHeaders=["*"], ExposedHeaders=["*"], MaxAge=86400.
    rs/cors@v1.11.1/cors.go:308-325 negroni ServeHTTP: OPTIONS+ACRM -> handlePreflight +
                                     WriteHeader(204); else handleActualRequest + next.
    rs/cors@v1.11.1/cors.go:329-397 handlePreflight: ALWAYS Vary=preflightVary; origin==""
                                     return; !isMethodAllowed return; ACAO=echoed Origin,
                                     ACAM=echoed ACRM, ACAH=echoed ACRH, ACMA=86400.
    rs/cors@v1.11.1/cors.go:400-444 handleActualRequest: ALWAYS Vary=Origin; origin==""
                                     return; !isMethodAllowed return; ACAO=echoed Origin,
                                     Expose-Headers=["*"].
    rs/cors@v1.11.1/cors.go:234     preflightVary=["Origin, Access-Control-Request-Method,
                                     Access-Control-Request-Headers"].

Rust mirror (complete launched-MAIN consumer chain, all anchors re-verified):
    lk-compat/src/http.rs:31-35    CORS_MAX_AGE="86400", CORS_ALLOWED_METHODS, PREFLIGHT_VARY.
    lk-compat/src/http.rs:94-104   middleware_chain recover->cors->body_limit->auth->routes
                                   (CORS BEFORE auth; preflight short-circuits before auth).
    lk-compat/src/http.rs:183-262  cors(): preflight 204+Vary then origin&&method gated
                                   ACAO/ACAM/ACAH/ACMA; actual Vary=Origin then origin&&method
                                   gated ACAO/Expose.
    lk-compat/src/http.rs:132-142  not_found(): 404 (the deterministic actual-request route).
    auth/mod.rs:208-216            authenticate_token(None)=Ok(None): unauthenticated actual
                                   request passes auth and reaches the 404 fallback both sides.

Discriminator choice (empirically grounded on the Go oracle, load-bearing): the five
profiles vary the REQUEST stimulus (method/path/Origin/ACRM/ACRH); the daemon config
is identical. The observable is the deterministic CORS response-header set:
  - preflight-allowed           OPTIONS / Origin+ACRM=POST+ACRH=X-Custom-Header
                                -> 204, ACAO=origin, ACAM=POST, ACAH=X-Custom-Header,
                                   ACMA=86400, Vary=preflight                 [DISCRIMINATING]
  - preflight-disallowed-method OPTIONS / Origin+ACRM=TRACE
                                -> 204, Vary=preflight ONLY (method gate)      [GATING]
  - preflight-no-origin         OPTIONS / ACRM=POST+ACRH (NO Origin)
                                -> 204, Vary=preflight ONLY (origin gate)      [GATING]
  - actual-get-origin           GET /cors-probe-absent Origin
                                -> 404, ACAO=origin, Expose=*, Vary=Origin     [DISCRIMINATING]
  - actual-no-origin            GET /cors-probe-absent (NO Origin)
                                -> 404, Vary=Origin ONLY (origin gate)         [GATING]
An accept-but-ignore mutant that shadows the request Origin to None keeps the
structural 204+Vary (preflight) and 404+Vary:Origin (actual) but drops every
origin-conditional header. The two DISCRIMINATING profiles FAIL while the three
GATING profiles SURVIVE — specific, not indiscriminate.

Normalization (scope limit): the CORS response-header semantics are fully
deterministic (no SDP, no candidates, no per-launch nonce), so they ARE compared
byte-equal cross-side: status, acao, acam, acah, acma, expose, credentials,
private_network, vary. The daemon HTTP port and the reserved media ports are
per-launch nondeterministic and are recorded for transparency but NOT cross-compared.
The actual-request route is a fixed nonexistent path (/cors-probe-absent) that 404s
on both sides, so the status is deterministic and media-free. No comparison field is
deleted; these are explicit scope bounds. This harness proves the middleware.cors
response-header consumer surface only — not the agent-endpoint PUT CORS variant, not
the /rtc/validate ACAO="*" auth special-case, and not the full HTTP surface.

Privacy normalization (commit boundary): the manifest embeds raw daemon server_log
provenance. Any RFC1918 private IPv4 literal is rewritten to RFC 5737 TEST-NET-1
(192.0.2.<last-octet>) before writing, preserving the last octet for correlation while
removing the real network identity. The redaction is pattern-based (no real literal is
embedded in this source). The loopback-only fixture config normally emits no private
literal, so this is a defensive no-op guard, not a data transform.

Out of scope (honest residual): the agent-endpoint CORS PUT variant, the /rtc/validate
ACAO="*" auth special-case, preflight Access-Control-Request-Private-Network, credentialed
CORS, other HTTP middleware (path normalization, body limit, recovery) and the full HTTP
surface remain open. This harness proves the launched-MAIN middleware.cors response-header
consumer surface only. Every original observer exit code is preserved; no pipe filters a
result.
"""
import argparse, hashlib, json, pathlib, re, subprocess

FIXED_GO_SHA = '4bc2378e636740b29093061036249ffc9ebbef91f4ece1de12177c8dfee1e7dc'
COVERS = ['HTTP_BEHAVIOR:middleware.cors']
PROFILES = ['preflight-allowed', 'preflight-disallowed-method', 'preflight-no-origin',
            'actual-get-origin', 'actual-no-origin']

PROBE_ORIGIN = 'https://probe.example'
PREFLIGHT_VARY = ['Origin, Access-Control-Request-Method, Access-Control-Request-Headers']
ACTUAL_VARY = ['Origin']

# Expected DERIVED observables per profile, EMPIRICALLY probed on the locked Go oracle
# (/tmp/c35-cors-probe-go) and confirmed identical on the Rust candidate. The
# differential asserts these on EACH side independently, then compares cross-side.
CONTRACT = {
    'preflight-allowed': {'status': 204, 'acao': PROBE_ORIGIN, 'acam': 'POST',
                          'acah': ['X-Custom-Header'], 'acma': '86400', 'expose': None,
                          'credentials': None, 'private_network': None, 'vary': PREFLIGHT_VARY},
    'preflight-disallowed-method': {'status': 204, 'acao': None, 'acam': None, 'acah': [],
                          'acma': None, 'expose': None, 'credentials': None,
                          'private_network': None, 'vary': PREFLIGHT_VARY},
    'preflight-no-origin': {'status': 204, 'acao': None, 'acam': None, 'acah': [],
                          'acma': None, 'expose': None, 'credentials': None,
                          'private_network': None, 'vary': PREFLIGHT_VARY},
    'actual-get-origin': {'status': 404, 'acao': PROBE_ORIGIN, 'acam': None, 'acah': [],
                          'acma': None, 'expose': '*', 'credentials': None,
                          'private_network': None, 'vary': ACTUAL_VARY},
    'actual-no-origin': {'status': 404, 'acao': None, 'acam': None, 'acah': [],
                          'acma': None, 'expose': None, 'credentials': None,
                          'private_network': None, 'vary': ACTUAL_VARY},
}
# Cross-side comparison uses the deterministic CORS response-header semantics. The
# per-launch HTTP/media ports are recorded for transparency but NOT in COMPARED.
COMPARED = ['status', 'acao', 'acam', 'acah', 'acma', 'expose', 'credentials',
            'private_network', 'vary']
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
        'acao': p.get('acao'),
        'acam': p.get('acam'),
        'acah': sorted(p.get('acah', []) or []),
        'acma': p.get('acma'),
        'expose': p.get('expose'),
        'credentials': p.get('credentials'),
        'private_network': p.get('private_network'),
        'vary': sorted(p.get('vary', []) or []),
    }


def normalize_side(result, observer_exit):
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
    probes = {name: normalize_probe(result.get('probes', {}).get(name, {})) for name in PROFILES}
    transparent = {k: result.get(k) for k in TRANSPARENT}
    return {'lifecycle': lifecycle, 'probes': probes, 'transparent': transparent}


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
for side in ('go', 'rust'):
    binary = a.go if side == 'go' else a.rust
    out = a.output / side
    cmd = [str(a.observer), '--binary', str(binary), '--output', str(out),
           '--enable-loopback=true']
    proc = subprocess.run(cmd, capture_output=True, text=True)
    result_path = out / 'result.json'
    result = json.loads(result_path.read_text()) if result_path.exists() else {}
    NORM[side] = normalize_side(result, proc.returncode)
    RAW[side] = {'exit': proc.returncode, 'result': result, 'stderr': proc.stderr[-2000:]}

P = {}
for profile in PROFILES:
    g, r = NORM['go']['probes'][profile], NORM['rust']['probes'][profile]
    go_ok = contract_ok(g, profile)
    rust_ok = contract_ok(r, profile)
    equal = cross_equal(g, r)
    life_ok = NORM['go']['lifecycle']['exit_integrity'] and NORM['rust']['lifecycle']['exit_integrity']
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
    pr = NORM[side]['probes']
    lc = NORM[side]['lifecycle']
    # POSITIVE controls: the discriminating observables CAN be present (not vacuously absent).
    controls[f'preflight_allowed_emits_cors_{side}'] = (
        pr['preflight-allowed']['acao'] == PROBE_ORIGIN
        and pr['preflight-allowed']['acam'] == 'POST'
        and pr['preflight-allowed']['acma'] == '86400')
    controls[f'actual_get_origin_emits_cors_{side}'] = (
        pr['actual-get-origin']['acao'] == PROBE_ORIGIN
        and pr['actual-get-origin']['expose'] == '*')
    # GATING controls: the origin/method gates suppress the headers.
    controls[f'preflight_method_gated_{side}'] = pr['preflight-disallowed-method']['acao'] is None
    controls[f'preflight_origin_gated_{side}'] = pr['preflight-no-origin']['acao'] is None
    controls[f'actual_origin_gated_{side}'] = pr['actual-no-origin']['acao'] is None
    # Lifecycle integrity: clean launch, clean shutdown, ports released, binary immutable.
    controls[f'lifecycle_integrity_{side}'] = lc['exit_integrity']

failed_profiles = [name for name, e in P.items() if not e['match']]
failed_controls = [k for k, v in controls.items() if not v]
all_match = not failed_profiles and not failed_controls

manifest = {
    'status': 'PASS_LAUNCHED_MAIN_HTTP_CORS' if all_match else 'FAIL',
    'case_count': len(PROFILES),
    'items': COVERS,
    'profiles': P,
    'cross_profile_controls': controls,
    'failed_profiles': failed_profiles,
    'failed_controls': failed_controls,
    'binaries': binaries,
    'observer_sha256': observer_sha,
    'input_identity_stable': binaries['go']['sha256'] == FIXED_GO_SHA,
    'harness_sha256': sha(pathlib.Path(__file__).resolve()),
    'lifecycle': {side: NORM[side]['lifecycle'] for side in ('go', 'rust')},
    'transparent': {side: NORM[side]['transparent'] for side in ('go', 'rust')},
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