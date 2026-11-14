#!/usr/bin/env python3
"""SPEC-0077 C27 — launched-MAIN trace.jaeger_url paired Go/Rust differential.

One ledger leaf is named: YAML:trace.jaeger_url. The locked Go oracle
(v1.13.10 e5655ac2 == binary 4bc2378e) and the Rust livecloud release candidate
are each launched as the REAL main server, five times, by the self-contained
trace-https observer (tests/interop/trace-https/observe.go). The observer writes
`trace.jaeger_url` into the daemon YAML, drives an authenticated WHIP Create then
DELETE with a Pion offer, and owns a loopback OTLP/HTTP protobuf collector. This
harness wraps that observer per side per profile and compares the normalized
export observables cross-side.

Upstream consumer (locked Go): cmd/server/main.go:253-254
    if url := conf.Trace.JaegerURL; url != "" { jaeger.Configure(ctx, url, "livekit") }
jaeger.Configure (references/protocol-stable/tracer/jaeger/jaeger.go) normalizes
the endpoint (prepend http://, default port 4318, default path /v1/traces), builds
a batched OTLP/HTTP exporter with resource service.name=livekit, and installs it
as the global tracer provider. The Rust mirror is livecloud/src/jaeger.rs
configure() called from server.rs:155-157 under the same `!jaeger_url.is_empty()`
guard. With the URL set, the daemon's WHIP server handler (Recv.WHIP.Create) and
its internal PSRPC client interceptors (Sent.WHIP.Create x2,
Sent.WHIPParticipant.DeleteSession x2) export to the collector.

Span provenance (load-bearing): the collector records ONLY spans whose resource
carries service.name=livekit (observe.go:82). The observer's Pion driver
configure NO tracer (exerciseWHIP uses webrtc.NewPeerConnection(Configuration{})),
so every recorded span originates from the DAEMON and depends on jaeger_url being
consumed. A mutant that accepts-but-ignores jaeger_url (forces the provider to
None) exports nothing: the three positive profiles flip to exported=False and
FAIL (see the positive-control mutant receipt).

Five profiles, two observable families:
  export (positive) — http, trusted, mtls: the daemon MUST export the WHIP
      lifecycle to the collector (requests>=1, Recv.WHIP.Create present,
      Sent.WHIP.Create x2), serve WHIP 201/200, exit 0 and release its ports.
      trusted/mtls additionally negotiate TLS HTTP/2 (a real ClientHello is
      observed); mtls also verifies the daemon's owned temporary client cert.
  suppress (negative) — untrusted, wrong-root: the collector MUST observe a real
      TLS ClientHello but ZERO HTTP requests (the daemon trusts the wrong root,
      so the export TLS handshake fails before any span is delivered). WHIP still
      serves 201/200 and the daemon exits 0. wrong-root proves the trace-specific
      OTEL_EXPORTER_OTLP_TRACES_CERTIFICATE overrides the correct generic
      OTEL_EXPORTER_OTLP_CERTIFICATE.

Normalization (scope limit): we compare the boolean export observables
{exported, recv_whip_create, sent_whip_create, tls_hello, http2, client_auth,
server_clean, ports_released, whip, passed} and the WHIP-lifecycle span-name
catalog (order-insensitive multiset). We do NOT compare byte-equal OTLP payloads:
the Go resource detector merges WithContainer (cgroup) attributes that have no
Rust equivalent, so resource attributes legitimately differ. We do NOT claim the
daemon's FULL telemetry surface is identical — only the WHIP Create/Delete
lifecycle this harness actually drives. No comparison field is deleted; these are
explicit scope bounds. The observer sets OTEL_BSP_SCHEDULE_DELAY=250 so the batch
exports inside its 6s window on both sides (Go main has no owned shutdown flush;
Rust flushes on Drop — both deliver within the window).

Out of scope (honest residual): the WithContainer/cgroup resource-attribute gap,
the broader non-WHIP telemetry surface, and other platforms remain open. This
harness proves the launched-MAIN trace.jaeger_url OTLP-export consumer surface
only. Every original observer exit code is preserved; no pipe filters a result.
"""
import argparse, hashlib, json, pathlib, subprocess

FIXED_GO_SHA = '4bc2378e636740b29093061036249ffc9ebbef91f4ece1de12177c8dfee1e7dc'
COVERS = ['YAML:trace.jaeger_url']
PROFILES = ['http', 'trusted', 'mtls', 'untrusted', 'wrong-root']
POSITIVE = ('http', 'trusted', 'mtls')
NEGATIVE = ('untrusted', 'wrong-root')

# Expected normalized observables per profile, derived from the locked Go oracle
# and confirmed identical on the Rust candidate. These are the ground truth the
# differential asserts on EACH side independently, then compares cross-side.
CONTRACT = {
    'http':       {'exported': True,  'recv_whip_create': True,  'sent_whip_create': 2, 'tls_hello': False, 'http2': False, 'client_auth': False},
    'trusted':    {'exported': True,  'recv_whip_create': True,  'sent_whip_create': 2, 'tls_hello': True,  'http2': True,  'client_auth': False},
    'mtls':       {'exported': True,  'recv_whip_create': True,  'sent_whip_create': 2, 'tls_hello': True,  'http2': True,  'client_auth': True},
    'untrusted':  {'exported': False, 'recv_whip_create': False, 'sent_whip_create': 0, 'tls_hello': True,  'http2': False, 'client_auth': False},
    'wrong-root': {'exported': False, 'recv_whip_create': False, 'sent_whip_create': 0, 'tls_hello': True,  'http2': False, 'client_auth': False},
}
COMPARED = ['exported', 'recv_whip_create', 'sent_whip_create', 'tls_hello', 'http2',
            'client_auth', 'server_clean', 'ports_released', 'whip', 'passed']


def sha(path):
    return hashlib.sha256(pathlib.Path(path).read_bytes()).hexdigest()


def normalize(r):
    names = r.get('collector_span_names', [])
    return {
        'exported': r.get('collector_requests', 0) >= 1,
        'recv_whip_create': 'Recv.WHIP.Create' in names,
        'sent_whip_create': names.count('Sent.WHIP.Create'),
        'tls_hello': r.get('tls_client_hellos', 0) > 0,
        'http2': r.get('collector_http_protocol') == 'HTTP/2.0',
        'client_auth': r.get('collector_authenticated_client') is True,
        'server_clean': r.get('server_exit') == 0,
        'ports_released': r.get('server_ports_released') is True and r.get('collector_port_released') is True,
        'whip': [r.get('whip_create_status'), r.get('whip_delete_status')],
        'passed': r.get('passed') is True,
        'span_names': names,
    }


def contract_ok(n, profile):
    if any(n[k] != v for k, v in CONTRACT[profile].items()):
        return False
    return n['server_clean'] and n['ports_released'] and n['whip'] == [201, 200] and n['passed']


def cross_equal(g, r):
    return (all(g[k] == r[k] for k in COMPARED)
            and sorted(g['span_names']) == sorted(r['span_names']))


p = argparse.ArgumentParser()
p.add_argument('--go', required=True, type=pathlib.Path)
p.add_argument('--rust', required=True, type=pathlib.Path)
p.add_argument('--observer', required=True, type=pathlib.Path)
p.add_argument('--fixture', required=True, type=pathlib.Path)
p.add_argument('--output', required=True, type=pathlib.Path)
a = p.parse_args()
for name in ('go', 'rust', 'observer', 'fixture'):
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
for profile in PROFILES:
    NORM[profile] = {}
    RAW[profile] = {}
    for side in ('go', 'rust'):
        binary = a.go if side == 'go' else a.rust
        out = a.output / f'{side}-{profile}'
        cmd = [str(a.observer), '-binary', str(binary), '-fixture', str(a.fixture),
               '-profile', profile, '-out', str(out)]
        proc = subprocess.run(cmd, capture_output=True, text=True)
        result_path = out / 'result.json'
        result = json.loads(result_path.read_text()) if result_path.exists() else {}
        # The observer exits 0 iff passed; a disagreement is an integrity failure.
        integrity = (proc.returncode == 0) == (result.get('passed') is True)
        n = normalize(result)
        n['observer_exit'] = proc.returncode
        n['exit_matches_passed'] = integrity
        NORM[profile][side] = n
        RAW[profile][side] = {'exit': proc.returncode, 'result': result,
                              'stderr': proc.stderr[-2000:]}

P = {}
for profile in PROFILES:
    g, r = NORM[profile]['go'], NORM[profile]['rust']
    go_ok = contract_ok(g, profile) and g['exit_matches_passed']
    rust_ok = contract_ok(r, profile) and r['exit_matches_passed']
    equal = cross_equal(g, r)
    P[profile] = {
        'match': go_ok and rust_ok and equal,
        'kind': 'export' if CONTRACT[profile]['exported'] else 'suppress',
        'go_contract_ok': go_ok,
        'rust_contract_ok': rust_ok,
        'cross_side_equal': equal,
        'go_observables': {k: g[k] for k in COMPARED},
        'rust_observables': {k: r[k] for k in COMPARED},
        'go_span_names': g['span_names'],
        'rust_span_names': r['span_names'],
    }

controls = {}
for side in ('go', 'rust'):
    n = {profile: NORM[profile][side] for profile in PROFILES}
    controls[f'positive_profiles_export_{side}'] = all(
        n[profile]['exported'] and n[profile]['recv_whip_create'] for profile in POSITIVE)
    controls[f'negative_profiles_suppress_{side}'] = all(
        not n[profile]['exported'] and not n[profile]['recv_whip_create'] for profile in NEGATIVE)
    controls[f'negative_tls_hello_present_{side}'] = all(n[profile]['tls_hello'] for profile in NEGATIVE)
    controls[f'tls_http2_discriminator_{side}'] = (
        n['http']['http2'] is False and n['trusted']['http2'] is True and n['mtls']['http2'] is True)
    controls[f'mtls_client_auth_discriminator_{side}'] = (
        n['mtls']['client_auth'] is True and n['trusted']['client_auth'] is False)
    controls[f'wrong_root_overrides_generic_{side}'] = (
        not n['wrong-root']['exported'] and n['wrong-root']['tls_hello'])
    controls[f'daemon_recv_span_present_{side}'] = all(n[profile]['recv_whip_create'] for profile in POSITIVE)
    controls[f'whip_lifecycle_ok_{side}'] = all(
        n[profile]['whip'] == [201, 200] and n[profile]['server_clean'] and n[profile]['ports_released']
        for profile in PROFILES)
    controls[f'observer_exit_integrity_{side}'] = all(n[profile]['exit_matches_passed'] for profile in PROFILES)

failed_profiles = [name for name, e in P.items() if not e['match']]
failed_controls = [k for k, v in controls.items() if not v]
all_match = not failed_profiles and not failed_controls

manifest = {
    'status': 'PASS_LAUNCHED_MAIN_JAEGER_URL' if all_match else 'FAIL',
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
    'raw': RAW,
}
(a.output / 'execution.json').write_text(json.dumps(manifest, indent=2, ensure_ascii=False) + '\n')

batch = {
    'status': manifest['status'],
    'case_count': len(PROFILES),
    'items': COVERS,
    'profiles': {name: {'match': e['match'], 'kind': e['kind'],
                        'go_contract_ok': e['go_contract_ok'], 'rust_contract_ok': e['rust_contract_ok'],
                        'cross_side_equal': e['cross_side_equal'],
                        'go_observables': e['go_observables'], 'rust_observables': e['rust_observables'],
                        'go_span_names': e['go_span_names'], 'rust_span_names': e['rust_span_names']}
                 for name, e in P.items()},
    'cross_profile_controls': controls,
    'failed_profiles': failed_profiles,
    'failed_controls': failed_controls,
    'binaries': binaries,
    'observer_sha256': observer_sha,
    'input_identity_stable': manifest['input_identity_stable'],
    'harness_sha256': manifest['harness_sha256'],
}
(a.output / 'BATCH.json').write_text(json.dumps(batch, indent=2, ensure_ascii=False) + '\n')
print(json.dumps({'status': manifest['status'], 'case_count': len(PROFILES),
                  'failed_profiles': failed_profiles, 'failed_controls': failed_controls,
                  'input_identity_stable': manifest['input_identity_stable']}))
raise SystemExit(0 if all_match else 1)