#!/usr/bin/env python3
"""SPEC-0077 C29 — launched-MAIN rtc.ips.excludes paired Go/Rust differential.

One ledger leaf is named: YAML:rtc.ips.excludes. The locked Go oracle
(v1.13.10 e5655ac2 == binary 4bc2378e) and the Rust livecloud release candidate
are each launched as the REAL main server, five times, by the self-contained
rtc-ips-excludes observer (tests/interop/rtc-ips-excludes/observe.go). The
observer writes `rtc.ips.{includes,excludes}` into the daemon YAML, drives an
authenticated WHIP Create then DELETE with a Pion offer, and extracts the HOST
ICE candidate IP set the daemon gathered into its SDP answer. This harness wraps
that observer per side per profile and compares the normalized candidate
observables cross-side.

Upstream consumer (locked Go, pinned mediatransportutil-f234b534b095):
    rtcconfig/webrtc_config.go:75-83
        if len(rtcConf.IPs.Includes) != 0 || len(rtcConf.IPs.Excludes) != 0 {
            filter, err := IPFilterFromConf(rtcConf.IPs)
            if err != nil { return nil, err }   // startup refusal
            s.SetIPFilter(filter)
        }
    rtcconfig/webrtc_config.go:441-483 IPFilterFromConf parses each include /
    exclude CIDR (net.ParseCIDR; a malformed entry returns the bare error) and
    returns a filter: includes non-empty -> admit only IPs inside an include
    CIDR; ELSE excludes non-empty -> reject IPs inside an exclude CIDR, admit
    everything else. The filter governs which local IPs the ICE agent gathers
    as host candidates. A malformed CIDR propagates through NewWebRTCConfig ->
    NewLocalRoomManager -> startServer; cmd/server/main.go prints err.Error()
    verbatim to stdout and exits 1.

The Rust mirror is livecloud/src/interfaces.rs:78-84 ip_allowed (the else branch
`!rtc.ips.excludes.iter().any(|c| in_cidr(ip, *c))`, includes precedence over
excludes) applied by media_interfaces_from (:95-146) at :127 and
nat_discovery_addresses at :197, plus lk-config/src/normalize.rs:321
check_ip_filters which re-parses the raw includes+excludes list at the wiring
boundary and refuses startup with the same bare text.

Candidate provenance (load-bearing): the observer's Pion driver configures NO IP
filter of its own (webrtc.NewPeerConnection(Configuration{})), so every recorded
host candidate originates from the DAEMON and depends on rtc.ips.excludes being
consumed.

Discriminator choice (empirically grounded, load-bearing): with node_ip=
127.0.0.1 + use_external_ip=false the Go daemon installs a catch-all NAT1To1
rewrite to 127.0.0.1 (webrtc_config.go:111-116), so an admitted PHYSICAL
interface candidate is masked to 127.0.0.1 in the answer too. A loopback-only
exclude (127.0.0.1/32) therefore still shows 127.0.0.1 (from the physical
interface) and is NOT observable via the SDP answer. The robust deterministic
discriminator is instead reject-all vs admit: an exclude of 0.0.0.0/0 contains
every local IPv4 -> ZERO host candidates; a non-local exclude (TEST-NET) contains
no local IP -> the loopback (and physical) candidates are gathered and masked to
127.0.0.1 -> count 1. 0.0.0.0/0 != "127.0.0.1" as a string yet Contains it, so
count 0 also proves CIDR containment matching rather than exact-string equality.
A mutant that accepts-but-ignores the filter (ip_allowed always true) gathers
127.0.0.1 even for the 0.0.0.0/0 exclude, so the negative profile flips to
host_candidate_count>0 and FAILS (see the positive-control mutant receipt).

Five profiles, three observable families:
  present (positive) — exclude-testnet (192.0.2.0/24, TEST-NET-1), exclude-doc
      (203.0.113.0/24, TEST-NET-3): the exclude contains no local IP, so the
      daemon MUST gather the loopback host candidate (host_candidate_count>=1,
      127.0.0.1 present), serve WHIP 201/200, exit 0 and release its ports.
      excludes-superseded (includes 127.0.0.1/32 + excludes 0.0.0.0/0): includes
      are non-empty so the excludes branch is never consulted -> count 1, proving
      includes take precedence over excludes from the excludes side.
  absent (negative) — exclude-everything (0.0.0.0/0): the exclude contains every
      local IPv4, so the daemon MUST gather ZERO host candidates
      (host_candidate_count==0). WHIP still serves 201/200 and the daemon exits 0.
  refusal — malformed-exclude (not-a-cidr): the daemon MUST refuse startup with
      the bare upstream text 'invalid CIDR address: not-a-cidr' on stdout and
      exit 1, before binding HTTP.

Normalization (scope limit): we compare the host-candidate IP set (sorted
unique), the host-candidate count, the candidate-type catalog (sorted unique),
the WHIP statuses, the daemon exit code, port release, binary immutability and
(for the refusal profile) the bare 'invalid CIDR address: ...' line. We do NOT
compare byte-equal SDP answers: foundation/priority/ice-ufrag/port and the
o=- session id are nondeterministic per launch, and the Go daemon logs an extra
'using single-node routing' INFO line before the refusal that the Rust daemon
does not — only the bare refusal LINE is the contract, not the whole log. Because
of the catch-all NAT1To1 rewrite we do NOT claim to observe loopback-specific
exclusion (a 127.0.0.1/32 exclude is masked by the physical interface); the
reject-all vs non-local-admit axis is the discriminating observable. We do NOT
claim the daemon's full ICE surface is identical — only the host-candidate
exclude-filter consumption this harness drives. No comparison field is deleted;
these are explicit scope bounds. The UDP media range 50000-60000 is shared, so
the ten observer invocations run sequentially.

Out of scope (honest residual): rtc.ips.includes as a standalone allowlist
(separate ledger leaf YAML:rtc.ips.includes, closed by C28),
rtc.interfaces.{includes,excludes}, the srflx/relay candidate surface, other
platforms/NAT/fallback profiles, and the generated --rtc.ips.excludes flag remain
open. This harness proves the launched-MAIN rtc.ips.excludes host-candidate
exclude-filter consumer surface only. Every original observer exit code is
preserved; no pipe filters a result.
"""
import argparse, hashlib, json, pathlib, subprocess

FIXED_GO_SHA = '4bc2378e636740b29093061036249ffc9ebbef91f4ece1de12177c8dfee1e7dc'
COVERS = ['YAML:rtc.ips.excludes']
PROFILES = ['exclude-testnet', 'exclude-doc', 'exclude-everything',
            'excludes-superseded', 'malformed-exclude']
POSITIVE = ('exclude-testnet', 'exclude-doc', 'excludes-superseded')
NEGATIVE = ('exclude-everything',)
REFUSAL = 'malformed-exclude'
ALIVE = tuple(p for p in PROFILES if p != REFUSAL)

# Per-profile observer stimulus (rtc.ips.includes / excludes written to YAML).
STIMULUS = {
    'exclude-testnet':     {'includes': '',              'excludes': '192.0.2.0/24'},
    'exclude-doc':         {'includes': '',              'excludes': '203.0.113.0/24'},
    'exclude-everything':  {'includes': '',              'excludes': '0.0.0.0/0'},
    'excludes-superseded': {'includes': '127.0.0.1/32',  'excludes': '0.0.0.0/0'},
    'malformed-exclude':   {'includes': '',              'excludes': 'not-a-cidr'},
}

# Expected normalized observables per profile, derived from the locked Go oracle
# (empirically probed) and confirmed identical on the Rust candidate. The
# differential asserts these on EACH side independently, then compares cross-side.
CONTRACT = {
    'exclude-testnet':     {'refused': False, 'host_candidate_count': 1, 'host_candidate_ips': ['127.0.0.1'], 'candidate_types': ['host'], 'whip': [201, 200], 'server_exit': 0},
    'exclude-doc':         {'refused': False, 'host_candidate_count': 1, 'host_candidate_ips': ['127.0.0.1'], 'candidate_types': ['host'], 'whip': [201, 200], 'server_exit': 0},
    'exclude-everything':  {'refused': False, 'host_candidate_count': 0, 'host_candidate_ips': [],            'candidate_types': [],       'whip': [201, 200], 'server_exit': 0},
    'excludes-superseded': {'refused': False, 'host_candidate_count': 1, 'host_candidate_ips': ['127.0.0.1'], 'candidate_types': ['host'], 'whip': [201, 200], 'server_exit': 0},
    'malformed-exclude':   {'refused': True,  'host_candidate_count': 0, 'host_candidate_ips': [],            'candidate_types': [],       'whip': [0, 0],     'server_exit': 1, 'refusal_line': 'invalid CIDR address: not-a-cidr'},
}
COMPARED = ['refused', 'server_exit', 'whip', 'host_candidate_ips',
            'host_candidate_count', 'candidate_types', 'ports_released',
            'binary_unchanged', 'refusal_line']


def sha(path):
    return hashlib.sha256(pathlib.Path(path).read_bytes()).hexdigest()


def refusal_line(log):
    for line in (log or '').splitlines():
        line = line.strip()
        if line.startswith('invalid CIDR address:'):
            return line
    return ''


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
        'refusal_line': refusal_line(r.get('server_log', '')),
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
               '--profile', profile, '--includes', stim['includes'],
               '--excludes', stim['excludes']]
        proc = subprocess.run(cmd, capture_output=True, text=True)
        result_path = out / 'result.json'
        result = json.loads(result_path.read_text()) if result_path.exists() else {}
        n = normalize(result)
        n['observer_exit'] = proc.returncode
        # The observer exits 0 iff the profile ran cleanly (alive shutdown or an
        # expected refusal); a nonzero exit is an infrastructure integrity fail.
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
    kind = 'refusal' if profile == REFUSAL else ('present' if profile in POSITIVE else 'absent')
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
    controls[f'presence_discriminator_{side}'] = (
        n['exclude-doc']['host_candidate_count'] >= 1
        and n['exclude-everything']['host_candidate_count'] == 0)
    controls[f'cidr_matching_specificity_{side}'] = (
        n['exclude-everything']['host_candidate_count'] == 0
        and n['exclude-testnet']['host_candidate_count'] >= 1)
    controls[f'includes_precedence_over_excludes_{side}'] = (
        n['excludes-superseded']['host_candidate_count'] >= 1
        and '127.0.0.1' in n['excludes-superseded']['host_candidate_ips'])
    controls[f'malformed_refusal_text_{side}'] = (
        n['malformed-exclude']['refusal_line'] == 'invalid CIDR address: not-a-cidr')
    controls[f'malformed_exit_one_{side}'] = (
        n['malformed-exclude']['refused'] and n['malformed-exclude']['server_exit'] == 1)
    controls[f'whip_lifecycle_ok_{side}'] = all(
        n[profile]['whip'] == [201, 200] and n[profile]['server_exit'] == 0
        for profile in ALIVE)
    controls[f'observer_exit_integrity_{side}'] = all(n[profile]['exit_integrity'] for profile in PROFILES)

failed_profiles = [name for name, e in P.items() if not e['match']]
failed_controls = [k for k, v in controls.items() if not v]
all_match = not failed_profiles and not failed_controls

manifest = {
    'status': 'PASS_LAUNCHED_MAIN_IPS_EXCLUDES' if all_match else 'FAIL',
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