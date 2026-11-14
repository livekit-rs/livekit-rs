#!/usr/bin/env python3
"""SPEC-0077 C31 — launched-MAIN rtc.enable_loopback_candidate paired Go/Rust differential.

One ledger leaf is named: YAML:rtc.enable_loopback_candidate. The locked Go oracle
(v1.13.10 e5655ac2 == binary 4bc2378e) and the Rust livecloud release candidate are
each launched as the REAL main server, four times, by the self-contained
rtc-loopback-candidate observer (tests/interop/rtc-loopback-candidate/observe.go).
The observer writes `rtc.enable_loopback_candidate` (true/false) into the daemon
YAML, PINS `rtc.interfaces.includes=@loopback` so the only name-admitted interface
is the loopback, drives an authenticated WHIP Create then DELETE with a Pion offer,
and extracts the HOST ICE candidate IP set the daemon gathered into its SDP answer.
This harness wraps that observer per side per profile and compares the normalized
candidate observables cross-side.

Upstream consumer (locked Go, pinned mediatransportutil-f234b534b095):
    rtcconfig/webrtc_config.go:219-220
        if rtcConf.EnableLoopbackCandidate {
            s.SetIncludeLoopbackCandidate(true)
        }
    SetIncludeLoopbackCandidate is the Pion setting that admits loopback interfaces
    as ICE host-candidate sources. When the flag is false the setting is left at its
    default (false) and the loopback interface is NOT gathered. (The UDPMux loopback
    option :137-138 and GetLocalIPAddresses :301 are on the udp_port / use_external_ip
    paths, which this harness does not exercise: it uses port_range_start/end and
    use_external_ip=false. The observable loopback consumer here is :219-220.)

The Rust mirror is livecloud/src/interfaces.rs:100 `let loopback =
rtc.enable_loopback_candidate;` applied by media_interfaces_from at :103
(`device.loopback && !loopback` skips the loopback device) and :117
(`address.is_loopback() && !loopback` skips the loopback address). media_interfaces_from
is reached from the launched MAIN via net_config:336 <- server.rs:330.

Candidate provenance (load-bearing): the observer's Pion driver configures NO
interface filter and NO loopback preference of its own
(webrtc.NewPeerConnection(Configuration{})), so every recorded host candidate
originates from the DAEMON and depends on rtc.enable_loopback_candidate being
consumed.

Discriminator choice (empirically grounded, load-bearing): with node_ip=
127.0.0.1 + use_external_ip=false the Go daemon installs a catch-all NAT1To1
rewrite to 127.0.0.1 (webrtc_config.go:111-116), so EVERY admitted candidate
(loopback or physical) is masked to 127.0.0.1 in the answer. A bare loopback toggle
is therefore NOT observable while physical interfaces are admitted: the empirical
probe `disabled-nopin` (no interface pin, flag=false) still gathered count 1 because
a physical interface was admitted and masked. To isolate the loopback-candidate gate
the harness PINS rtc.interfaces.includes=@loopback (resolved to the UP loopback name,
lo0 on macOS) so the ONLY name-admitted interface is the loopback. Then the flag is
the sole determinant of presence:
  - enable_loopback_candidate=true  -> lo0 gathered   -> host_candidate_count 1;
  - enable_loopback_candidate=false -> lo0 suppressed -> host_candidate_count 0.
A mutant that accepts-but-ignores the flag (loopback always admitted) gathers lo0
even for the false profiles, so the two negative profiles flip to
host_candidate_count 1 and FAIL (see the positive-control mutant receipt). The
observer resolves @loopback against the live OS interface table (net.Interfaces) and
records the enumerated table and resolved name list; both daemons receive the SAME
explicit YAML name list and the SAME flag value.

Four profiles, two observable families (no refusal family — the flag never refuses
startup), all confirmed empirically on BOTH the Go oracle and the Rust candidate:
  present (positive) — enabled-pin (includes=@loopback, flag=true),
      enabled-precedence (includes=@loopback + excludes=@loopback, flag=true:
      includes win, lo0 admitted, loopback enabled): the daemon MUST gather the
      loopback host candidate (host_candidate_count>=1, 127.0.0.1 present), serve
      WHIP 201/200, exit 0 and release its ports.
  absent (negative) — disabled-pin (includes=@loopback, flag=false),
      disabled-precedence (includes=@loopback + excludes=@loopback, flag=false:
      includes win admits lo0 by name, but the loopback gate suppresses it): the
      daemon MUST gather ZERO host candidates (host_candidate_count==0). WHIP still
      serves 201/200 and the daemon exits 0.

Normalization (scope limit): we compare the host-candidate IP set (sorted unique),
the host-candidate count, the candidate-type catalog (sorted unique), the WHIP
statuses, the daemon exit code, port release and binary immutability. We do NOT
compare byte-equal SDP answers: foundation/priority/ice-ufrag/port and the o=-
session id are nondeterministic per launch. Because of the catch-all NAT1To1 rewrite
we do NOT claim to observe WHICH interface was admitted (all are masked to
127.0.0.1); the presence/absence axis driven by the loopback-candidate flag is the
discriminating observable. We do NOT claim the daemon's full ICE surface is identical
— only the host-candidate loopback-gate consumption this harness drives. No
comparison field is deleted; these are explicit scope bounds. The UDP media range
50000-60000 is shared, so the eight observer invocations run sequentially.

Out of scope (honest residual): the srflx/relay candidate surface, the
udp_port-mux loopback option (:137-138) and the use_external_ip GetLocalIPAddresses
discovery path (:301 / Rust nat_discovery_addresses:207), other platforms/NAT/fallback
profiles, physical-multihomed gathering, and the generated
--rtc.enable_loopback_candidate flag remain open. This harness proves the
launched-MAIN rtc.enable_loopback_candidate host-candidate loopback-gate consumer
surface only. Every original observer exit code is preserved; no pipe filters a
result.
"""
import argparse, hashlib, json, pathlib, subprocess

FIXED_GO_SHA = '4bc2378e636740b29093061036249ffc9ebbef91f4ece1de12177c8dfee1e7dc'
COVERS = ['YAML:rtc.enable_loopback_candidate']
PROFILES = ['enabled-pin', 'disabled-pin', 'enabled-precedence', 'disabled-precedence']
POSITIVE = ('enabled-pin', 'enabled-precedence')
NEGATIVE = ('disabled-pin', 'disabled-precedence')
ALIVE = tuple(PROFILES)  # no refusal profile: the loopback flag never refuses startup

# Per-profile observer stimulus. The interface pin (@loopback) isolates the
# loopback-candidate gate; enable_loopback is the variable under test. @loopback is
# resolved by the observer against the live OS table.
STIMULUS = {
    'enabled-pin':         {'enable_loopback': True,  'if_includes': '@loopback', 'if_excludes': ''},
    'disabled-pin':        {'enable_loopback': False, 'if_includes': '@loopback', 'if_excludes': ''},
    'enabled-precedence':  {'enable_loopback': True,  'if_includes': '@loopback', 'if_excludes': '@loopback'},
    'disabled-precedence': {'enable_loopback': False, 'if_includes': '@loopback', 'if_excludes': '@loopback'},
}

# Expected normalized observables per profile, derived from the locked Go oracle
# (empirically probed) and confirmed identical on the Rust candidate. The
# differential asserts these on EACH side independently, then compares cross-side.
CONTRACT = {
    'enabled-pin':         {'refused': False, 'host_candidate_count': 1, 'host_candidate_ips': ['127.0.0.1'], 'candidate_types': ['host'], 'whip': [201, 200], 'server_exit': 0},
    'disabled-pin':        {'refused': False, 'host_candidate_count': 0, 'host_candidate_ips': [],            'candidate_types': [],       'whip': [201, 200], 'server_exit': 0},
    'enabled-precedence':  {'refused': False, 'host_candidate_count': 1, 'host_candidate_ips': ['127.0.0.1'], 'candidate_types': ['host'], 'whip': [201, 200], 'server_exit': 0},
    'disabled-precedence': {'refused': False, 'host_candidate_count': 0, 'host_candidate_ips': [],            'candidate_types': [],       'whip': [201, 200], 'server_exit': 0},
}
COMPARED = ['refused', 'server_exit', 'whip', 'host_candidate_ips',
            'host_candidate_count', 'candidate_types', 'ports_released',
            'binary_unchanged']


def sha(path):
    return hashlib.sha256(pathlib.Path(path).read_bytes()).hexdigest()


def normalize(r):
    return {
        'refused': r.get('refused') is True,
        'server_exit': r.get('server_exit'),
        'whip': [r.get('whip_create_status'), r.get('whip_delete_status')],
        'host_candidate_ips': sorted(r.get('host_candidate_ips', [])),
        'host_candidate_count': r.get('host_candidate_count', 0),
        'candidate_types': sorted(r.get('candidate_types', [])),
        'ports_released': r.get('server_ports_released') is True,
        'binary_unchanged': r.get('binary_unchanged') is True,
    }


def contract_ok(n, profile):
    if any(n[k] != v for k, v in CONTRACT[profile].items()):
        return False
    return n['ports_released'] and n['binary_unchanged']


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
for profile in PROFILES:
    NORM[profile] = {}
    RAW[profile] = {}
    stim = STIMULUS[profile]
    for side in ('go', 'rust'):
        binary = a.go if side == 'go' else a.rust
        out = a.output / f'{side}-{profile}'
        cmd = [str(a.observer), '--binary', str(binary), '--output', str(out),
               '--profile', profile,
               '--enable-loopback=' + ('true' if stim['enable_loopback'] else 'false'),
               '--if-includes', stim['if_includes'],
               '--if-excludes', stim['if_excludes']]
        proc = subprocess.run(cmd, capture_output=True, text=True)
        result_path = out / 'result.json'
        result = json.loads(result_path.read_text()) if result_path.exists() else {}
        n = normalize(result)
        n['observer_exit'] = proc.returncode
        # The observer exits 0 iff the profile ran cleanly (alive shutdown); a
        # nonzero exit is an infrastructure integrity fail.
        n['exit_integrity'] = (proc.returncode == 0) and n['binary_unchanged'] and n['ports_released']
        NORM[profile][side] = n
        RAW[profile][side] = {'exit': proc.returncode, 'result': result,
                              'stderr': proc.stderr[-2000:]}

P = {}
for profile in PROFILES:
    g, r = NORM[profile]['go'], NORM[profile]['rust']
    go_ok = contract_ok(g, profile) and g['exit_integrity']
    rust_ok = contract_ok(r, profile) and r['exit_integrity']
    equal = cross_equal(g, r)
    kind = 'present' if profile in POSITIVE else 'absent'
    P[profile] = {
        'match': go_ok and rust_ok and equal,
        'kind': kind,
        'go_contract_ok': go_ok,
        'rust_contract_ok': rust_ok,
        'cross_side_equal': equal,
        'go_observables': {k: g[k] for k in COMPARED},
        'rust_observables': {k: r[k] for k in COMPARED},
    }

controls = {}
for side in ('go', 'rust'):
    n = {profile: NORM[profile][side] for profile in PROFILES}
    controls[f'positive_profiles_present_{side}'] = all(
        n[profile]['host_candidate_count'] >= 1 and '127.0.0.1' in n[profile]['host_candidate_ips']
        for profile in POSITIVE)
    controls[f'negative_profile_absent_{side}'] = all(
        n[profile]['host_candidate_count'] == 0 and n[profile]['host_candidate_ips'] == []
        for profile in NEGATIVE)
    controls[f'loopback_toggle_discriminator_{side}'] = (
        n['enabled-pin']['host_candidate_count'] >= 1
        and n['disabled-pin']['host_candidate_count'] == 0)
    controls[f'precedence_loopback_discriminator_{side}'] = (
        n['enabled-precedence']['host_candidate_count'] >= 1
        and n['disabled-precedence']['host_candidate_count'] == 0)
    controls[f'whip_lifecycle_ok_{side}'] = all(
        n[profile]['whip'] == [201, 200] and n[profile]['server_exit'] == 0
        for profile in ALIVE)
    controls[f'observer_exit_integrity_{side}'] = all(n[profile]['exit_integrity'] for profile in PROFILES)

failed_profiles = [name for name, e in P.items() if not e['match']]
failed_controls = [k for k, v in controls.items() if not v]
all_match = not failed_profiles and not failed_controls

manifest = {
    'status': 'PASS_LAUNCHED_MAIN_LOOPBACK_CANDIDATE' if all_match else 'FAIL',
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
                        'go_observables': e['go_observables'], 'rust_observables': e['rust_observables']}
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