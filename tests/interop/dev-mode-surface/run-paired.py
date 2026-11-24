#!/usr/bin/env python3
"""SPEC-0077 P2 — launched-MAIN YAML:development / base_flags:--dev / YAML:debug_handler
paired Go/Rust differential.

Seven ledger leaves are named: YAML:development, YAML:debug_handler,
YAML:debug_handler.port, base_flags:--dev, generated_flags:--development,
generated_flags:--debug_handler.port, CONFIG_ENABLED_CAPABILITY:debug_handler.port.
The locked Go oracle (v1.13.10 e5655ac2 == binary 4bc2378e) and the Rust livecloud
release candidate are each launched as the REAL main server SEVEN times per side by the
self-contained dev-mode-surface observer (tests/interop/dev-mode-surface/observe.go),
once per launch group, and driven with deterministic, MEDIA-FREE, (mostly)
UNAUTHENTICATED HTTP probes against BOTH the main API port and the dedicated debug port.
The observer reserves the configured API/media/debug ports (media and debug in a LOW
band 40000-40999, below the macOS ephemeral range 49152-65535), releases the
reservations so the daemon can bind, settles past the Go Start() init window, drives the
probes and records the dispatch semantics of each response. This harness wraps the
observer per group per side, asserts the per-side Go-derived contract, then compares
cross-side.

Upstream consumer (locked Go v1.13.10, Go 1.26 stdlib):
    pkg/config/config.go:107           Development bool `yaml:"development,omitempty"`.
    pkg/config/config.go:1106-1107     `--dev` -> conf.Development = c.Bool("dev").
    pkg/service/server.go:119-125      mux := http.NewServeMux(); if conf.Development
                                       { mux = http.DefaultServeMux
                                         mux.HandleFunc("/debug/goroutine", s.debugGoroutines)
                                         mux.HandleFunc("/debug/rooms", s.debugInfo) }
                                       — development mode SWAPS the API mux to the process
                                       DefaultServeMux, which net/http/pprof's init() already
                                       filled with /debug/pprof/{,cmdline,profile,symbol,trace}.
    pkg/service/server.go:170-186      conf.DebugHandler.Port > 0 -> a SEPARATE fresh debugMux
                                       (five pprof patterns + /debug/goroutine + /debug/rooms,
                                       registered with NO method prefix) served by s.debugServer
                                       on that port — mode-INDEPENDENT, and with no "/"
                                       catch-all an unknown path there gets ServeMux's own 404.
    pkg/service/server.go:135          mux.HandleFunc("/", s.defaultHandler) — the rooted
                                       catch-all that makes a method-only miss a plain 404 page.
    pkg/service/server.go:422-441      debugGoroutines = pprof.Lookup("goroutine").WriteTo(w,2);
                                       debugInfo = json.Marshal(room.DebugInfo()) written with NO
                                       Content-Type, so Go sniffs: a bare JSON array -> text/plain.
    pkg/service/server.go:484-503      api chain Recovery->CORS->BodyLimiter->APIKeyAuth->mux,
                                       so the debug patterns sit INSIDE auth.
    net/http/pprof/pprof.go init()     registers its five patterns with a "GET " method prefix
                                       unless GODEBUG=httpmuxgo121=1, which livekit never sets;
                                       net/http's HEAD-shares-GET rule makes HEAD match too, but
                                       POST/PUT/OPTIONS match NO pprof pattern.
    net/http/server.go:2676-2697       ServeMux cleanPath + subtree matchOrRedirect: an exact
                                       "/debug/pprof" against the "/debug/pprof/" pattern is a
                                       307 to "/debug/pprof/", offered only to the methods the
                                       subtree registration accepts (GET/HEAD on the API mux,
                                       every method on the dedicated debug mux).

Rust mirror (this candidate, all anchors re-verified):
    livecloud/src/server.rs:141-156   http_app(state, manager, development) merges
        livecloud/src/debug.rs:59-101 debug::api_routes(manager) — the same seven patterns plus
        the explicit "/debug/pprof" slash redirect, pprof patterns get-only with the plain 404
        page as the method-miss fallback and /debug/{goroutine,rooms} any-method — into
        lk_compat::http::api_routes(state) ONLY when development, then hands the table to
        lk-compat/src/http.rs:52 api_router(state, mux), which keeps the middleware chain and
        the ServeMux 404 fallback exactly as NewHTTPHandler wraps the mux it was handed.
    livecloud/src/server.rs:764,944-945 debug_handler.port -> a bound listener served by
        livecloud/src/debug.rs:105-119 debug_routes(manager) on a fresh mux with no method
        filters and no "/" catch-all.
    crates/lk-config/src/unsupported.rs no longer carries a debug_handler rejection, so
        YAML:debug_handler is accepted and consumed rather than refused.

Groups (the launch stimulus is the mode/port config; the request stimulus is the probe):
    api-prod          production daemon, no debug_handler.port: every /debug/* on the API port
                      must be the plain 404 page.
    api-dev-flag      the same daemon started with `--dev`: /debug/rooms, /debug/goroutine and
                      the pprof subtree become live and GET/HEAD /debug/pprof is a 307.
    api-dev-yaml      `development: true` in the config file, no flag: must be IDENTICAL to
                      api-dev-flag (one consumer, two config paths).
    api-dev-genflag   the generated CLI spelling `--development=true` with nothing in the
                      config file: must be IDENTICAL to both spellings above.
    debug-port-set    production daemon WITH debug_handler.port: the API port still 404s while
                      the debug port serves the same patterns — mode independence, and the
                      dedicated mux answers POST where the API mux refuses it.
    debug-port-genflag the generated CLI spelling `--debug_handler.port=N` with no YAML
                      listener block: must be IDENTICAL to debug-port-set.
    debug-port-zero   production daemon without it: NOTHING is bound on the debug port
                      (no_listener), while the API port still 404s.

Discriminator choice (empirically grounded on the locked Go oracle, load-bearing): the
observed dispatch table measured on the oracle (dev mode, API port) is

                      GET   HEAD  POST  PUT   OPTIONS
    /debug/pprof      307   307   404   404   404
    /debug/pprof/     200   200   404   404   404
    /debug/pprof/cmdline 200 200  404   404   404
    /debug/pprof/heap 200   200   404   404   404
    /debug/rooms      200   200   200   200   200
    /debug/goroutine  200   200   200   200   200
    /debug/nope, /other 404 404   404   404   404

so a mutant that always passes development=false (or drops the debug merge) keeps api-prod,
debug-port-set and debug-port-zero green and FAILS every api-dev-* debug probe (307/200 ->
404); a mutant that ignores debug_handler.port FAILS only debug-port-set; a mutant that
registers the pprof subtree any-method FAILS only the dev-mode POST/PUT/OPTIONS legs; a
mutant that registers the two livekit patterns GET-only FAILS only the /debug/rooms and
/debug/goroutine POST/PUT legs; a mutant that moves the patterns OUTSIDE APIKeyAuth FAILS
only rooms-bearer-get. Specific, not indiscriminate.

Normalization (scope limit): compared fields per probe are outcome, status, content_type,
location, nosniff, body_class, body and content_length, and every one of them is asserted
against the contract on EACH side independently before cross-side comparison. Three field
groups are excluded, per (group, probe), with the reason recorded in the manifest:
  payload_profile_body — /debug/goroutine and /debug/pprof/heap payloads are native runtime
      state (Go goroutine dumps vs the Rust rpprof wall-sampler; two different gzip pprof
      protobufs), so body/content_length are excluded while status, content_type,
      X-Content-Type-Options and body_class (text/plain non-empty) stay asserted.
  payload_cmdline — /debug/pprof/cmdline is the process argv, which cannot be equal across
      two different binaries, so body/content_length are excluded and the shape
      (200 + text/plain + nosniff + non-empty body_class) stays asserted.
  pprof_index_html — Go's /debug/pprof/ index renders the Go runtime profile registry with
      per-profile counts; the Rust index lists the native collectors and says so, so the
      body and body_class are excluded while status/content_type/nosniff stay asserted.
  head_content_length — Go's transfer writer omits Content-Length for a HEAD whose handler
      wrote no bytes (the 307 redirect) while hyper emits 0; the framing difference is
      excluded, the 307 + Location + text/html shape is still asserted.
In api-prod / debug-port-set(API) / debug-port-zero no debug handler runs at all, so those
legs assert every field including the exact 404 page body. The daemon HTTP port and the
reserved media/debug ports are per-launch nondeterministic and recorded for transparency
but NOT cross-compared. No comparison field is deleted for a probe whose response is
deterministic; these are explicit scope bounds. This harness proves the development-mode
mux swap, the `--dev`/`development: true` config paths reaching that one consumer, the
debug_handler.port dedicated listener and its method-prefix difference, the auth position
of the debug patterns, and the production refusal of every /debug/* path — NOT the profile
payloads themselves, and not the full HTTP surface.

Privacy normalization (commit boundary): the manifest embeds raw daemon server_log
provenance. Any RFC1918 private IPv4 literal is rewritten to RFC 5737 TEST-NET-1
(192.0.2.<last-octet>) before writing, preserving the last octet for correlation while
removing the real network identity. The redaction is pattern-based (no real literal is
embedded in this source). The loopback-only fixture config normally emits no private
literal, so this is a defensive no-op guard, not a data transform. The raw cmdline probe
bodies are never embedded in the manifest (only their length class is used), so no host
path leaks through them either.

Out of scope (honest residual): the pprof profile payload semantics, the index page HTML,
the /debug/pprof/{profile,symbol,trace} handler bodies (covered by the C77 debug-profiles
harness), the F33 CLI-process help-presentation residuals recorded on base_flags:--dev,
other HTTP middleware and the full HTTP surface remain open. Every original observer exit
code is preserved; no pipe filters a result.
"""
import argparse, hashlib, json, pathlib, re, subprocess

FIXED_GO_SHA = '4bc2378e636740b29093061036249ffc9ebbef91f4ece1de12177c8dfee1e7dc'
COVERS = ['YAML:development', 'YAML:debug_handler', 'YAML:debug_handler.port',
          'base_flags:--dev', 'generated_flags:--development',
          'generated_flags:--debug_handler.port',
          'CONFIG_ENABLED_CAPABILITY:debug_handler.port']
GROUPS = ['api-prod', 'api-dev-flag', 'api-dev-yaml', 'api-dev-genflag',
          'debug-port-set', 'debug-port-genflag', 'debug-port-zero']

ANY = 'ANY'
TEXT_PLAIN = 'text/plain; charset=utf-8'
TEXT_HTML = 'text/html; charset=utf-8'
OCTET = 'application/octet-stream'
PAGE_404 = '404 page not found\n'
REDIRECT_BODY = '<a href="/debug/pprof/">Temporary Redirect</a>.\n\n'
UNAUTH = '{"code":"unauthenticated","msg":"permissions denied"}'

# The observer owns the stimulus; the harness only needs the probe names per group so it
# can fail on a missing probe instead of silently skipping it.
API_DEBUG_PROBES = ['rooms-get', 'rooms-head', 'rooms-post', 'rooms-put',
                    'goroutine-get', 'goroutine-head', 'goroutine-post',
                    'pprof-slash-get', 'pprof-slash-head', 'pprof-slash-post',
                    'pprof-slash-options', 'pprof-index-get', 'pprof-index-head',
                    'pprof-index-post', 'pprof-index-put', 'pprof-cmdline-get',
                    'pprof-cmdline-head', 'pprof-cmdline-post', 'pprof-cmdline-options',
                    'pprof-heap-get', 'pprof-heap-head', 'pprof-heap-post',
                    'unknown-debug-get', 'unknown-debug-post', 'other-get', 'other-post']
DEBUG_TARGET_PROBES = ['debug-rooms-get', 'debug-rooms-post', 'debug-goroutine-get',
                       'debug-goroutine-post', 'debug-pprof-slash-get',
                       'debug-pprof-slash-post', 'debug-pprof-index-get',
                       'debug-pprof-index-post', 'debug-pprof-cmdline-get',
                       'debug-pprof-cmdline-head', 'debug-pprof-cmdline-post',
                       'debug-pprof-heap-post', 'debug-unknown-get']
GATE_PROBES = ['health-get', 'twirp-unauth-post', 'rooms-bearer-get']
GROUP_PROBES = {
    'api-prod': GATE_PROBES + API_DEBUG_PROBES,
    'api-dev-flag': GATE_PROBES + API_DEBUG_PROBES,
    'api-dev-yaml': GATE_PROBES + API_DEBUG_PROBES,
    'api-dev-genflag': GATE_PROBES + API_DEBUG_PROBES,
    'debug-port-set': GATE_PROBES + API_DEBUG_PROBES + DEBUG_TARGET_PROBES,
    'debug-port-genflag': GATE_PROBES + API_DEBUG_PROBES + DEBUG_TARGET_PROBES,
    'debug-port-zero': GATE_PROBES + API_DEBUG_PROBES + DEBUG_TARGET_PROBES,
}


def resp(status, ct, cls, body, cl='', nosniff='', loc=''):
    return {'outcome': 'response', 'status': status, 'content_type': ct, 'location': loc,
            'nosniff': nosniff, 'body_class': cls, 'body': body, 'content_length': cl}


# The rooted "/" catch-all answer, per method: HEAD keeps the declared length and loses
# the body, everything else gets the exact page.
def not_found(head=False):
    return resp(404, TEXT_PLAIN, 'other' if head else 'serve_mux_404_page',
                '' if head else PAGE_404, cl='19', nosniff='nosniff')


NO_LISTENER = {'outcome': 'no_listener_configured'}

# Mode-independent gating probes: they must look the same in every group, which is what
# makes the /debug/* differences attributable to the mux decision alone.
GATE = {
    'health-get': resp(200, TEXT_PLAIN, 'text_plain_body', 'OK', cl='2'),
    'twirp-unauth-post': resp(401, 'application/json', 'other', UNAUTH, cl='53'),
    'rooms-bearer-get': resp(401, TEXT_PLAIN, 'other', 'invalid authorization token', cl='27'),
}

# api-prod / debug-port-set API port: the production mux registers no debug pattern, so
# every /debug/* and the non-route /other get the defaultHandler 404 page.
PROD_API = {}
for _n in API_DEBUG_PROBES:
    PROD_API[_n] = not_found(head=_n.endswith('-head'))

# api-dev-flag / api-dev-yaml: the API mux IS http.DefaultServeMux, so the pprof subtree
# answers GET/HEAD only (the "GET " registration prefix), livekit's own two patterns
# answer every method, and /debug/pprof is the subtree's exact-path hole.
DEV_API = {
    'rooms-get': resp(200, TEXT_PLAIN, 'empty_json_array_body', '[]', cl='2'),
    'rooms-head': resp(200, TEXT_PLAIN, 'other', '', cl='2'),
    'rooms-post': resp(200, TEXT_PLAIN, 'empty_json_array_body', '[]', cl='2'),
    'rooms-put': resp(200, TEXT_PLAIN, 'empty_json_array_body', '[]', cl='2'),
    'goroutine-get': resp(200, TEXT_PLAIN, 'text_plain_body', ANY, cl=ANY),
    'goroutine-head': resp(200, TEXT_PLAIN, 'other', '', cl=ANY),
    'goroutine-post': resp(200, TEXT_PLAIN, 'text_plain_body', ANY, cl=ANY),
    'pprof-slash-get': resp(307, TEXT_HTML, 'other', REDIRECT_BODY, cl='49',
                            loc='/debug/pprof/'),
    'pprof-slash-head': resp(307, TEXT_HTML, 'other', '', cl=ANY, loc='/debug/pprof/'),
    'pprof-slash-post': not_found(),
    'pprof-slash-options': not_found(),
    'pprof-index-get': resp(200, TEXT_HTML, ANY, ANY, cl=ANY, nosniff='nosniff'),
    'pprof-index-head': resp(200, TEXT_HTML, 'other', '', cl=ANY, nosniff='nosniff'),
    'pprof-index-post': not_found(),
    'pprof-index-put': not_found(),
    'pprof-cmdline-get': resp(200, TEXT_PLAIN, 'text_plain_body', ANY, cl=ANY,
                              nosniff='nosniff'),
    'pprof-cmdline-head': resp(200, TEXT_PLAIN, 'other', '', cl=ANY, nosniff='nosniff'),
    'pprof-cmdline-post': not_found(),
    'pprof-cmdline-options': not_found(),
    'pprof-heap-get': resp(200, OCTET, 'other', ANY, cl=ANY, nosniff='nosniff'),
    'pprof-heap-head': resp(200, OCTET, 'other', '', cl=ANY, nosniff='nosniff'),
    'pprof-heap-post': not_found(),
    'unknown-debug-get': not_found(),
    'unknown-debug-post': not_found(),
    'other-get': not_found(),
    'other-post': not_found(),
}

# debug-port-set on the dedicated port: a fresh mux, no method prefix, no "/" catch-all,
# no auth middleware and no CORS, so every method reaches the handler and an unknown path
# gets ServeMux's own notFound.
DEBUG_PORT = {
    'debug-rooms-get': resp(200, TEXT_PLAIN, 'empty_json_array_body', '[]', cl='2'),
    'debug-rooms-post': resp(200, TEXT_PLAIN, 'empty_json_array_body', '[]', cl='2'),
    'debug-goroutine-get': resp(200, TEXT_PLAIN, 'text_plain_body', ANY, cl=ANY),
    'debug-goroutine-post': resp(200, TEXT_PLAIN, 'text_plain_body', ANY, cl=ANY),
    'debug-pprof-slash-get': resp(307, TEXT_HTML, 'other', REDIRECT_BODY, cl='49',
                                  loc='/debug/pprof/'),
    'debug-pprof-slash-post': resp(307, '', 'other', '', cl='0', loc='/debug/pprof/'),
    'debug-pprof-index-get': resp(200, TEXT_HTML, ANY, ANY, cl=ANY, nosniff='nosniff'),
    'debug-pprof-index-post': resp(200, TEXT_HTML, ANY, ANY, cl=ANY, nosniff='nosniff'),
    'debug-pprof-cmdline-get': resp(200, TEXT_PLAIN, 'text_plain_body', ANY, cl=ANY,
                                    nosniff='nosniff'),
    'debug-pprof-cmdline-head': resp(200, TEXT_PLAIN, 'other', '', cl=ANY,
                                     nosniff='nosniff'),
    'debug-pprof-cmdline-post': resp(200, TEXT_PLAIN, 'text_plain_body', ANY, cl=ANY,
                                     nosniff='nosniff'),
    'debug-pprof-heap-post': resp(200, OCTET, 'other', ANY, cl=ANY, nosniff='nosniff'),
    'debug-unknown-get': not_found(),
}

CONTRACT = {
    'api-prod': {**GATE, **PROD_API},
    'api-dev-flag': {**GATE, **DEV_API},
    'api-dev-yaml': {**GATE, **DEV_API},
    # The generated CLI spelling must dispatch exactly like the two config spellings.
    'api-dev-genflag': {**GATE, **DEV_API},
    'debug-port-set': {**GATE, **PROD_API, **DEBUG_PORT},
    'debug-port-genflag': {**GATE, **PROD_API, **DEBUG_PORT},
    'debug-port-zero': {**GATE, **PROD_API,
                        **{n: NO_LISTENER for n in DEBUG_TARGET_PROBES}},
}

# Why each excluded field is out of the asserted set, cited per (group, probe, field).
REASONS = {
    'profile_payload': 'the collector payload is native runtime state (Go goroutine dumps vs '
                       'the Rust rpprof wall sampler; two different gzip pprof protobufs), so '
                       'body and content_length cannot be equal; status, content_type, '
                       'X-Content-Type-Options and body_class are still asserted',
    'cmdline_payload': 'cmdline is the process argv, which cannot be equal across two different '
                       'binaries; the shape (200 + text/plain + nosniff + non-empty text '
                       'body_class) is still asserted',
    'pprof_index_html': "Go's index renders the Go runtime profile registry with per-profile "
                        'counts; the Rust index lists the native collectors and says so, so the '
                        'HTML body and its class are excluded while status, content_type and '
                        'X-Content-Type-Options are still asserted',
    'declared_length': 'Go writes handler output through a chunked transfer and never declares '
                       'a length (and omits Content-Length outright for a HEAD whose handler '
                       'wrote nothing), while the Rust candidate buffers the response and '
                       'declares it; the status, content_type, X-Content-Type-Options, Location '
                       'and the suppressed HEAD body are still asserted',
}
COMPARED = ['outcome', 'status', 'content_type', 'location', 'nosniff', 'body_class',
            'body', 'content_length']
TRANSPARENT = ['configured_port_range_start', 'configured_port_range_end',
               'configured_tcp_port', 'configured_debug_port']

_PRIVATE_IP = re.compile(
    r'\b(?:192\.168|10|172\.(?:1[6-9]|2[0-9]|3[01]))\.\d{1,3}\.(\d{1,3})\b')


def redact_private_ips(text):
    return _PRIVATE_IP.sub(lambda m: '192.0.2.' + m.group(1), text)


def sha(path):
    return hashlib.sha256(pathlib.Path(path).read_bytes()).hexdigest()


def normalize_probe(p):
    return {
        'outcome': p.get('outcome'),
        'status': p.get('status'),
        'content_type': p.get('content_type'),
        'location': p.get('location'),
        'nosniff': p.get('nosniff'),
        'body_class': p.get('body_class'),
        'body': p.get('body'),
        'content_length': p.get('content_length'),
    }


def normalize_group(result, observer_exit, names):
    lifecycle = {
        'refused': result.get('refused') is True,
        'server_exit': result.get('server_exit'),
        'ports_released': result.get('server_ports_released') is True,
        'binary_unchanged': result.get('binary_unchanged') is True,
        'dev_flag': result.get('dev_flag'),
        'dev_yaml': result.get('dev_yaml'),
        'dev_gen_flag': result.get('dev_gen_flag'),
        'debug_port_set': result.get('debug_port_set'),
        'debug_port_genflag': result.get('debug_port_genflag'),
        'daemon_args': result.get('daemon_args'),
        'observer_exit': observer_exit,
    }
    lifecycle['exit_integrity'] = (
        observer_exit == 0 and lifecycle['binary_unchanged'] and lifecycle['ports_released']
        and lifecycle['server_exit'] == 0 and not lifecycle['refused'])
    recorded = result.get('probes', {})
    missing = [n for n in names if n not in recorded]
    probes = {n: normalize_probe(recorded.get(n, {'outcome': 'missing_from_observer'}))
              for n in names}
    transparent = {k: result.get(k) for k in TRANSPARENT if k in (result or {})}
    return {'lifecycle': lifecycle, 'probes': probes, 'missing': missing,
            'transparent': transparent}


def asserted_fields(expected):
    """Every compared field whose contract value is not the payload ANY sentinel."""
    return [f for f in COMPARED if f in expected and expected[f] != ANY]


def contract_ok(n, group, profile):
    expected = CONTRACT[group][profile]
    if n['outcome'] != expected['outcome']:
        return False
    if expected['outcome'] != 'response':
        return True
    return all(n[f] == expected[f] for f in asserted_fields(expected))


def agrees(a, b, group, profile):
    """Two records agree when every field the contract asserts of them is equal. Payload
    ANY fields are per-design incomparable, so they never reach this path."""
    expected = CONTRACT[group][profile]
    if a['outcome'] != b['outcome']:
        return False
    if expected['outcome'] != 'response':
        return True
    return all(a[f] == b[f] for f in asserted_fields(expected))


def cross_equal(g, r, group, profile):
    return agrees(g, r, group, profile)


def exclusions(group, profile):
    expected = CONTRACT[group][profile]
    if expected['outcome'] != 'response':
        return []
    fields = [field for field in COMPARED if expected.get(field) == ANY]
    if not fields:
        return []
    if fields == ['content_length']:
        reason = 'declared_length'
    elif 'body_class' in fields:
        reason = 'pprof_index_html'
    elif 'cmdline' in profile:
        reason = 'cmdline_payload'
    else:
        reason = 'profile_payload'
    return [{'group': group, 'probe': profile, 'fields': fields, 'reason': reason,
             'why': REASONS[reason]}]


# A contract entry that does not exist is silently skipped and a probe that the contract
# never mentions is silently unasserted, so the two sides are required to agree by name
# before any daemon starts.
for _g in GROUPS:
    _missing = sorted(set(GROUP_PROBES[_g]) - set(CONTRACT[_g]))
    _extra = sorted(set(CONTRACT[_g]) - set(GROUP_PROBES[_g]))
    if _missing or _extra:
        raise SystemExit(f'contract/probe mismatch in {_g}: missing={_missing} extra={_extra}')

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
LIFE_OK = {}
for side in ('go', 'rust'):
    binary = a.go if side == 'go' else a.rust
    NORM[side] = {}
    RAW[side] = {}
    for group in GROUPS:
        out = a.output / side / group
        cmd = [str(a.observer), '--binary', str(binary), '--output', str(out),
               '--group', group]
        proc = subprocess.run(cmd, capture_output=True, text=True)
        result_path = out / 'result.json'
        result = json.loads(result_path.read_text()) if result_path.exists() else {}
        NORM[side][group] = normalize_group(result, proc.returncode, GROUP_PROBES[group])
        RAW[side][group] = {'exit': proc.returncode, 'result': result,
                           'stderr': proc.stderr[-2000:]}
    LIFE_OK[side] = all(NORM[side][g]['lifecycle']['exit_integrity'] and
                        not NORM[side][g]['missing'] for g in GROUPS)

G = {}
for group in GROUPS:
    G[group] = {}
    for profile in GROUP_PROBES[group]:
        g = NORM['go'][group]['probes'][profile]
        r = NORM['rust'][group]['probes'][profile]
        go_ok = contract_ok(g, group, profile)
        rust_ok = contract_ok(r, group, profile)
        equal = cross_equal(g, r, group, profile)
        life_ok = LIFE_OK['go'] and LIFE_OK['rust']
        G[group][profile] = {
            'match': go_ok and rust_ok and equal and life_ok,
            'go_contract_ok': go_ok,
            'rust_contract_ok': rust_ok,
            'cross_side_equal': equal,
            'lifecycle_ok': life_ok,
            'asserted_fields': asserted_fields(CONTRACT[group][profile]),
            'excluded_fields': exclusions(group, profile),
            'go_observables': g,
            'rust_observables': r,
        }

controls = {}
for side in ('go', 'rust'):
    pr = NORM[side]
    # GATING: the mode-independent surface really is identical across all five launches,
    # so every /debug/* difference below is attributable to the mux decision alone.
    controls[f'gate_stable_across_groups_{side}'] = all(
        all(pr[group]['probes'][n] == pr['api-prod']['probes'][n]
            for n in GATE_PROBES) for group in GROUPS)
    # DISCRIMINATING: production refuses every debug path on the API port. The refusal is
    # the exact 404 page, except on a HEAD leg where the body is suppressed by definition.
    controls[f'prod_api_refuses_debug_{side}'] = all(
        pr['api-prod']['probes'][n]['status'] == 404
        and pr['api-prod']['probes'][n]['body']
        == ('' if n.endswith('-head') else PAGE_404) for n in API_DEBUG_PROBES)
    # ...while --dev really does swap in the DefaultServeMux (positive control: the 307
    # and the live handlers can appear, so the dev legs are not vacuously 404).
    controls[f'dev_flag_serves_debug_mux_{side}'] = (
        pr['api-dev-flag']['probes']['pprof-slash-get']['status'] == 307
        and pr['api-dev-flag']['probes']['pprof-slash-get']['location'] == '/debug/pprof/'
        and pr['api-dev-flag']['probes']['rooms-get']['body'] == '[]'
        and pr['api-dev-flag']['probes']['goroutine-get']['status'] == 200)
    # One consumer, two config paths: the flag leg and the YAML leg must dispatch
    # identically. The comparison is over the contract-asserted fields, because the two
    # legs are two different processes whose live payloads (goroutine ids, argv length)
    # cannot be byte-equal even between two identical Go launches.
    controls[f'dev_flag_equals_dev_yaml_{side}'] = all(
        agrees(pr['api-dev-flag']['probes'][n], pr['api-dev-yaml']['probes'][n],
               'api-dev-flag', n)
        for n in GROUP_PROBES['api-dev-flag'])
    # A parser that reads only the YAML half of `development` would launch api-dev-genflag
    # as production, so this leg has to agree with both spellings above.
    controls[f'dev_genflag_equals_yaml_{side}'] = all(
        agrees(pr['api-dev-genflag']['probes'][n], pr['api-dev-yaml']['probes'][n],
               'api-dev-genflag', n)
        for n in GROUP_PROBES['api-dev-genflag'])
    # Same argument for the generated `--debug_handler.port=N` spelling: a scanner that only
    # walks the YAML tree leaves the debug listener unbound.
    controls[f'debug_port_genflag_equals_set_{side}'] = all(
        agrees(pr['debug-port-genflag']['probes'][n], pr['debug-port-set']['probes'][n],
               'debug-port-genflag', n)
        for n in GROUP_PROBES['debug-port-genflag'])
    # The GET-only subtree registration: a method-only miss falls to the "/" catch-all.
    controls[f'pprof_subtree_is_get_only_{side}'] = all(
        pr['api-dev-flag']['probes'][n]['status'] == 404
        and pr['api-dev-flag']['probes'][n]['body'] == PAGE_404
        for n in ['pprof-slash-post', 'pprof-slash-options', 'pprof-index-post',
                  'pprof-index-put', 'pprof-cmdline-post', 'pprof-cmdline-options',
                  'pprof-heap-post'])
    # livekit's own two patterns carry no method prefix, so POST/PUT reach them.
    controls[f'livekit_patterns_are_any_method_{side}'] = all(
        pr['api-dev-flag']['probes'][n]['status'] == 200
        for n in ['rooms-post', 'rooms-put', 'goroutine-post'])
    # The dedicated mux is a DIFFERENT registration: POST reaches the subtree there,
    # including the 307 that the API port refuses to offer to POST.
    controls[f'dedicated_port_offers_all_methods_{side}'] = (
        pr['debug-port-set']['probes']['debug-pprof-slash-post']['status'] == 307
        and pr['debug-port-set']['probes']['debug-pprof-cmdline-post']['status'] == 200
        and pr['debug-port-set']['probes']['debug-rooms-post']['status'] == 200)
    # Mode independence: debug_handler.port is orthogonal to development — the API port of
    # a production daemon with the listener configured dispatches exactly like api-prod.
    controls[f'debug_port_is_mode_independent_{side}'] = (
        all(pr['debug-port-set']['probes'][n] == pr['api-prod']['probes'][n]
            for n in GATE_PROBES + API_DEBUG_PROBES)
        and all(pr['debug-port-set']['probes'][n]['status'] == 200
                or pr['debug-port-set']['probes'][n]['status'] == 307
                for n in ['debug-rooms-get', 'debug-goroutine-get', 'debug-pprof-index-get',
                          'debug-pprof-slash-get']))
    # An unconfigured debug_handler.port means NOTHING is bound there, not a 404 server.
    controls[f'absent_debug_port_has_no_listener_{side}'] = all(
        pr['debug-port-zero']['probes'][n]['outcome'] == 'no_listener_configured'
        for n in DEBUG_TARGET_PROBES) and all(
        pr['debug-port-zero']['probes'][n] == pr['api-prod']['probes'][n]
        for n in GATE_PROBES + API_DEBUG_PROBES)
    # The debug patterns sit INSIDE APIKeyAuth on the API port and OUTSIDE it on the
    # dedicated port, so the same malformed bearer is a 401 there but a 200 here.
    controls[f'auth_wraps_api_debug_only_{side}'] = (
        all(pr[group]['probes']['rooms-bearer-get']['status'] == 401 for group in GROUPS)
        and pr['debug-port-set']['probes']['debug-rooms-get']['status'] == 200)
    for group in GROUPS:
        controls[f'lifecycle_integrity_{side}/{group}'] = (
            pr[group]['lifecycle']['exit_integrity'] and not pr[group]['missing'])

cases = [(group, profile) for group in GROUPS for profile in GROUP_PROBES[group]]
failed_cases = [f'{group}/{profile}' for group, profile in cases
                if not G[group][profile]['match']]
failed_controls = [k for k, v in controls.items() if not v]
all_match = not failed_cases and not failed_controls
exclusions_all = [e for group, profile in cases for e in G[group][profile]['excluded_fields']]
excluded_field_count = sum(len(e['fields']) for e in exclusions_all)

manifest = {
    'status': 'PASS_LAUNCHED_MAIN_DEV_MODE_SURFACE' if all_match else 'FAIL',
    'case_count': len(cases),
    'items': COVERS,
    'groups': GROUPS,
    'launch_stimulus': {group: NORM['go'][group]['lifecycle']['daemon_args'] for group in GROUPS},
    'cases': G,
    'cross_case_controls': controls,
    'failed_cases': failed_cases,
    'failed_controls': failed_controls,
    'excluded_field_count': excluded_field_count,
    'exclusion_reasons': REASONS,
    'binaries': binaries,
    'observer_sha256': observer_sha,
    'input_identity_stable': binaries['go']['sha256'] == FIXED_GO_SHA,
    'harness_sha256': sha(pathlib.Path(__file__).resolve()),
    'lifecycle': {side: {g: NORM[side][g]['lifecycle'] for g in GROUPS} for side in ('go', 'rust')},
    'missing_probes': {side: {g: NORM[side][g]['missing'] for g in GROUPS}
                       for side in ('go', 'rust')},
    'transparent': {side: {g: NORM[side][g]['transparent'] for g in GROUPS}
                    for side in ('go', 'rust')},
    'privacy_normalization': 'RFC1918 private IPv4 literals in raw server_log rewritten to RFC5737 TEST-NET-1 (last octet preserved); pattern-based, no real literal embedded.',
    'raw': RAW,
}
(a.output / 'execution.json').write_text(
    redact_private_ips(json.dumps(manifest, indent=2, ensure_ascii=False) + '\n'))

batch = {
    'status': manifest['status'],
    'case_count': len(cases),
    'items': COVERS,
    'groups': GROUPS,
    'cases': {f'{group}/{profile}': {k: v for k, v in G[group][profile].items()
                                     if k not in ('go_observables', 'rust_observables')}
              for group, profile in cases},
    'cross_case_controls': controls,
    'failed_cases': failed_cases,
    'failed_controls': failed_controls,
    'excluded_field_count': excluded_field_count,
    'binaries': binaries,
    'observer_sha256': observer_sha,
    'input_identity_stable': manifest['input_identity_stable'],
    'harness_sha256': manifest['harness_sha256'],
    'lifecycle': manifest['lifecycle'],
}
(a.output / 'BATCH.json').write_text(
    redact_private_ips(json.dumps(batch, indent=2, ensure_ascii=False) + '\n'))

print(json.dumps({'status': manifest['status'], 'case_count': len(cases),
                  'failed_cases': failed_cases, 'failed_controls': failed_controls,
                  'excluded_field_count': excluded_field_count,
                  'input_identity_stable': manifest['input_identity_stable']}))
raise SystemExit(0 if all_match else 1)
