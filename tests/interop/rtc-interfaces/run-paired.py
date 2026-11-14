#!/usr/bin/env python3
"""SPEC-0077 C30 — launched-MAIN rtc.interfaces.{includes,excludes} paired Go/Rust differential.

Two ledger leaves are named: YAML:rtc.interfaces.includes and
YAML:rtc.interfaces.excludes. The locked Go oracle (v1.13.10 e5655ac2 == binary
4bc2378e) and the Rust livecloud release candidate are each launched as the REAL
main server, six times, by the self-contained rtc-interfaces observer
(tests/interop/rtc-interfaces/observe.go). The observer writes
`rtc.interfaces.{includes,excludes}` NAME lists into the daemon YAML, drives an
authenticated WHIP Create then DELETE with a Pion offer, and extracts the HOST
ICE candidate IP set the daemon gathered into its SDP answer. This harness wraps
that observer per side per profile and compares the normalized candidate
observables cross-side.

Upstream consumer (locked Go, pinned mediatransportutil-f234b534b095):
    rtcconfig/webrtc_config.go:70-72
        if len(rtcConf.Interfaces.Includes) != 0 || len(rtcConf.Interfaces.Excludes) != 0 {
            ifFilter = InterfaceFilterFromConf(rtcConf.Interfaces)
            s.SetInterfaceFilter(ifFilter)
        }
    rtcconfig/webrtc_config.go:422-439 InterfaceFilterFromConf returns a
    func(name string) bool: includes non-empty -> slices.Contains(includes, name)
    (admit only exact-name matches); ELSE excludes non-empty -> reject exact-name
    matches, admit everything else; ELSE admit all. slices.Contains is EXACT
    string equality, NOT a glob/prefix. The filter governs which local interfaces
    the ICE agent gathers host candidates from (and the UDP mux bind, :144).
    Interface names are never parsed, so there is NO malformed-refusal path.

The Rust mirror is livecloud/src/interfaces.rs:41-47 named_allowed (includes
non-empty -> iter().any(|n| n == name); else !excludes.iter().any(|n| n == name),
includes precedence over excludes) applied by media_interfaces_from (:95-146) at
:103 and nat_discovery_addresses at :180.

Candidate provenance (load-bearing): the observer's Pion driver configures NO
interface filter of its own (webrtc.NewPeerConnection(Configuration{})), so every
recorded host candidate originates from the DAEMON and depends on
rtc.interfaces.{includes,excludes} being consumed.

Discriminator choice (empirically grounded, load-bearing): with node_ip=
127.0.0.1 + use_external_ip=false the Go daemon installs a catch-all NAT1To1
rewrite to 127.0.0.1 (webrtc_config.go:111-116), so EVERY admitted candidate
(loopback or physical) is masked to 127.0.0.1 in the answer. The host-candidate
IP set is therefore {127.0.0.1} whenever ANY interface is admitted, and {} when
NONE is. The robust deterministic discriminator is presence (count 1) vs absence
(count 0) of an admitted interface, NOT the IP value or the interface count:
  - includes an existing name (@loopback -> lo0) -> lo0 admitted -> count 1;
  - includes a nonexistent name -> no interface admitted -> count 0;
  - includes a glob 'lo*' -> exact match fails ('lo*' != 'lo0') -> count 0,
    proving exact-string semantics rather than prefix/glob matching;
  - excludes a nonexistent name -> every interface still admitted -> count 1;
  - excludes @all (every enumerated interface name) -> no interface admitted ->
    count 0.
A mutant that accepts-but-ignores the name filter (named_allowed always true)
admits every interface even for the nonexistent/glob/@all stimuli, so the three
negative profiles flip to host_candidate_count 1 and FAIL (see the
positive-control mutant receipt). The observer resolves @loopback / @all against
the live OS interface table (net.Interfaces) and records the enumerated table and
resolved name lists; both daemons receive the SAME explicit YAML name list.

Six profiles, two observable families (no refusal family — names never parse):
  present (positive) — include-loopback (@loopback), include-precedence
      (@loopback includes + @loopback excludes: includes win), exclude-absent
      (nonexistent exclude): the daemon MUST gather the loopback host candidate
      (host_candidate_count>=1, 127.0.0.1 present), serve WHIP 201/200, exit 0 and
      release its ports.
  absent (negative) — include-absent (nonexistent include), include-exact-not-glob
      ('lo*'), exclude-all (@all): the daemon MUST gather ZERO host candidates
      (host_candidate_count==0). WHIP still serves 201/200 and the daemon exits 0.

Normalization (scope limit): we compare the host-candidate IP set (sorted
unique), the host-candidate count, the candidate-type catalog (sorted unique),
the WHIP statuses, the daemon exit code, port release and binary immutability. We
do NOT compare byte-equal SDP answers: foundation/priority/ice-ufrag/port and the
o=- session id are nondeterministic per launch. Because of the catch-all NAT1To1
rewrite we do NOT claim to observe WHICH physical interface was admitted (all are
masked to 127.0.0.1); the presence/absence axis is the discriminating observable.
We do NOT claim the daemon's full ICE surface is identical — only the
host-candidate interface-name-filter consumption this harness drives. No
comparison field is deleted; these are explicit scope bounds. The UDP media range
50000-60000 is shared, so the twelve observer invocations run sequentially.

Out of scope (honest residual): the srflx/relay candidate surface, other
platforms/NAT/fallback profiles, physical-multihomed gathering beyond the
loopback host candidate this harness drives, and the generated
--rtc.interfaces.{includes,excludes} flags remain open. This harness proves the
launched-MAIN rtc.interfaces.{includes,excludes} host-candidate name-filter
consumer surface only. Every original observer exit code is preserved; no pipe
filters a result.
"""
import argparse, hashlib, json, pathlib, subprocess

FIXED_GO_SHA = '4bc2378e636740b29093061036249ffc9ebbef91f4ece1de12177c8dfee1e7dc'
COVERS = ['YAML:rtc.interfaces.includes', 'YAML:rtc.interfaces.excludes']
PROFILES = ['include-loopback', 'include-absent', 'include-exact-not-glob',
            'include-precedence', 'exclude-absent', 'exclude-all']
POSITIVE = ('include-loopback', 'include-precedence', 'exclude-absent')
NEGATIVE = ('include-absent', 'include-exact-not-glob', 'exclude-all')
ALIVE = tuple(PROFILES)  # no refusal profile: interface names never parse

# Per-profile observer stimulus (rtc.interfaces.includes / excludes NAME lists).
# @loopback / @all are resolved by the observer against the live OS table.
STIMULUS = {
    'include-loopback':       {'if_includes': '@loopback',          'if_excludes': ''},
    'include-absent':         {'if_includes': 'c30-no-such-iface',  'if_excludes': ''},
    'include-exact-not-glob': {'if_includes': 'lo*',                'if_excludes': ''},
    'include-precedence':     {'if_includes': '@loopback',          'if_excludes': '@loopback'},
    'exclude-absent':         {'if_includes': '',                   'if_excludes': 'c30-no-such-iface'},
    'exclude-all':            {'if_includes': '',                   'if_excludes': '@all'},
}

# Expected normalized observables per profile, derived from the locked Go oracle
# (empirically probed) and confirmed identical on the Rust candidate. The
# differential asserts these on EACH side independently, then compares cross-side.
CONTRACT = {
    'include-loopback':       {'refused': False, 'host_candidate_count': 1, 'host_candidate_ips': ['127.0.0.1'], 'candidate_types': ['host'], 'whip': [201, 200], 'server_exit': 0},
    'include-absent':         {'refused': False, 'host_candidate_count': 0, 'host_candidate_ips': [],            'candidate_types': [],       'whip': [201, 200], 'server_exit': 0},
    'include-exact-not-glob': {'refused': False, 'host_candidate_count': 0, 'host_candidate_ips': [],            'candidate_types': [],       'whip': [201, 200], 'server_exit': 0},
    'include-precedence':     {'refused': False, 'host_candidate_count': 1, 'host_candidate_ips': ['127.0.0.1'], 'candidate_types': ['host'], 'whip': [201, 200], 'server_exit': 0},
    'exclude-absent':         {'refused': False, 'host_candidate_count': 1, 'host_candidate_ips': ['127.0.0.1'], 'candidate_types': ['host'], 'whip': [201, 200], 'server_exit': 0},
    'exclude-all':            {'refused': False, 'host_candidate_count': 0, 'host_candidate_ips': [],            'candidate_types': [],       'whip': [201, 200], 'server_exit': 0},
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
               '--profile', profile, '--if-includes', stim['if_includes'],
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
    controls[f'includes_presence_discriminator_{side}'] = (
        n['include-loopback']['host_candidate_count'] >= 1
        and n['include-absent']['host_candidate_count'] == 0)
    controls[f'exact_match_not_glob_{side}'] = (
        n['include-exact-not-glob']['host_candidate_count'] == 0
        and n['include-loopback']['host_candidate_count'] >= 1)
    controls[f'includes_precedence_over_excludes_{side}'] = (
        n['include-precedence']['host_candidate_count'] >= 1
        and '127.0.0.1' in n['include-precedence']['host_candidate_ips'])
    controls[f'excludes_presence_discriminator_{side}'] = (
        n['exclude-absent']['host_candidate_count'] >= 1
        and n['exclude-all']['host_candidate_count'] == 0)
    controls[f'whip_lifecycle_ok_{side}'] = all(
        n[profile]['whip'] == [201, 200] and n[profile]['server_exit'] == 0
        for profile in ALIVE)
    controls[f'observer_exit_integrity_{side}'] = all(n[profile]['exit_integrity'] for profile in PROFILES)

failed_profiles = [name for name, e in P.items() if not e['match']]
failed_controls = [k for k, v in controls.items() if not v]
all_match = not failed_profiles and not failed_controls

manifest = {
    'status': 'PASS_LAUNCHED_MAIN_INTERFACES' if all_match else 'FAIL',
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