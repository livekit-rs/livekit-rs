#!/usr/bin/env python3
"""SPEC-0077 C99 — launched-MAIN rtc.node_ip family/candidate paired Go/Rust differential.

Leafs named: `YAML:rtc.node_ip`, `generated_flags:--rtc.node_ip.ipv4`,
`generated_flags:--rtc.node_ip.ipv6` at the launched tier (`main_server_consumer`),
i.e. what a running daemon actually advertises and what a client sees as the node IP —
not what the loader projected.

Contract under test (frozen Go oracle e5655ac, mediatransportutil f234b534b095):
  * `pkg/rtcconfig/config.go:231-233` `NodeIP.UnmarshalYAML` forwards ONLY
    `value.Value` to `UnmarshalString`, so a YAML *mapping* (`node_ip: {ipv4: ..}`)
    arrives as the empty string and is silently ignored (:236-238); the scalar form
    goes through `UnmarshalString` (:235-263), which splits on commas (max 2 parts),
    `net.ParseIP`s each part after trimming spaces (:245) and stores `ip.String()` —
    so `::ffff:a.b.c.d` lands in the **V4** slot already normalised (`ip.To4() != nil`
    at :249).
  * the generated dotted flags (`pkg/config/config.go` reflection over the struct,
    `--rtc.node_ip.ipv4/ipv6` visible in `help-verbose`) write the RAW text straight
    into the named leaf, bypassing `UnmarshalString` entirely, so unparseable or
    zone-qualified text survives into the rewrite rules.
  * `PrimaryIP()` (:280-285) prefers V4 → the `nodeIP` field of the readiness line
    (`pkg/service/server.go:309`), and `ToStringSlice()` (:269-278) emits V4 first.
  * `pkg/rtcconfig/webrtc_config.go:91-116`: when the node IP is set and NOT
    auto-generated, the daemon logs `using explicit node IP for NAT1To1Ips` and calls
    `SetNAT1To1AddressRewriteRules(ToStringSlice(), AdvertiseInternalIP)`;
    `nat1To1AddressRewriteRules` (:257-286) turns a bare address into a catch-all host
    rewrite and an `external/local` text into a local-matched rule. Pion then keys each
    external address by ITS OWN family, so a V6-only node IP never rewrites IPv4
    candidates, and an unparseable external address fails at
    `SetRemoteDescription` with `invalid address rewrite mapping` — per session, not at
    startup (observed as WHIP 503 + that JSON body).

Method: `observe.go` launches ONE immutable daemon per profile with the same YAML,
drives an authenticated WHIP Create/DELETE with a Pion offer that has no interface
filter and no rewrite of its own, and records the daemon's own log lines plus the host
candidates of the SDP answer. Every recorded candidate therefore originates from the
daemon's consumer of `rtc.node_ip`, which makes the observable mutant-sensitive.

Gate shape (non-vacuous by construction):
  * each parity profile carries a **declared expectation derived from the Go source
    contract**, checked against the Go oracle run AND the Rust run *independently*
    (`go_declared`, `rust_declared`), plus cross-side equality (`cross_equal`);
  * the five `fence` profiles record a **measured Rust divergence** from the oracle. A
    fence passes only while the divergence still reproduces exactly as recorded
    (`rust_observed == recorded_divergence` and `go != rust`). When a future Rust fix
    lands, the fence goes red and the row must be promoted into the parity set. Divergent
    rows are NEVER absorbed into the parity claim; they are open ledger items.
"""
import argparse
import hashlib
import ipaddress
import json
import pathlib
import re
import shutil
import subprocess
import sys

FIXED_GO_SHA256 = '4bc2378e636740b29093061036249ffc9ebbef91f4ece1de12177c8dfee1e7dc'
FIXED_GO_COMMIT = 'e5655ac20267fa5005dccaf3f0e3bfc3ea688c07'
COVERS = [
    'YAML:rtc.node_ip',
    'generated_flags:--rtc.node_ip.ipv4',
    'generated_flags:--rtc.node_ip.ipv6',
]
OBSERVER_SOURCE = '/Users/zoujiaqing/projects/livekit-rs/tests/interop/rtc-node-ip-family/observe.go'

# Addresses the harness may persist verbatim: loopback plus the documentation ranges
# the profiles deliberately use. Anything else (a real LAN address the daemon
# auto-discovered, a public address) is redacted before it reaches the receipt.
ALLOWED_TEXT = {'', '0.0.0.0', '::', '255.255.255.255'}
ALLOWED_NETS = [ipaddress.ip_network(n) for n in
                ('127.0.0.0/8', '::1/128', '203.0.113.0/24', '192.0.2.0/24',
                 '198.51.100.0/24', '2001:db8::/32', 'fe80::/10')]
IP_TOKEN_RE = re.compile(r'[0-9A-Fa-f:.]+(?:%[0-9A-Za-z._-]+)?')


def as_address(token):
    """Parse an address literal, unwrapping the IPv4-mapped form Go stores verbatim."""
    core = token.split('%')[0].strip('[]')
    try:
        ip = ipaddress.ip_address(core)
    except ValueError:
        return None
    if ip.version == 6 and getattr(ip, 'ipv4_mapped', None) is not None:
        return ip.ipv4_mapped
    return ip


def keeps(addr_text):
    """True when an address literal is safe to record verbatim."""
    if addr_text in ALLOWED_TEXT:
        return True
    ip = as_address(addr_text)
    if ip is None:
        return False
    return any(ip.version == net.version and ip in net for net in ALLOWED_NETS)


def leaks(text):
    """Every real address literal in `text` that must not be persisted."""
    found = []
    for match in IP_TOKEN_RE.finditer(text or ''):
        token = match.group(0)
        if as_address(token) is not None and not keeps(token):
            found.append(token)
    return found


def redact(value):
    """Replace each non-allowlisted address literal with a tag, recursively."""
    if isinstance(value, list):
        return [redact(v) for v in value]
    if isinstance(value, dict):
        return {k: redact(v) for k, v in value.items()}
    if not isinstance(value, str):
        return value

    def sub(match):
        token = match.group(0)
        # Only genuine address literals are candidates; a timestamp or a `key: value`
        # colon must survive untouched, otherwise redaction destroys the very text
        # the row is comparing.
        if as_address(token) is None or keeps(token):
            return token
        return '<discovered-address>'

    return IP_TOKEN_RE.sub(sub, value)


p = argparse.ArgumentParser()
p.add_argument('--go', type=pathlib.Path, help='fixed Go oracle binary')
p.add_argument('--rust', type=pathlib.Path, help='Rust candidate binary')
p.add_argument('--observer', type=pathlib.Path, help='compiled observe.go driver')
p.add_argument('--output', type=pathlib.Path)
p.add_argument('--reference', type=pathlib.Path,
               default=pathlib.Path('/Users/zoujiaqing/projects/livekit-rs'))
p.add_argument('--only', default='')
p.add_argument('--report', type=pathlib.Path, default=None,
               help='recompute the verdict from a persisted execution.json; no daemon is started')
a = p.parse_args()
if a.report:
    a.report = a.report.resolve()
    a.output = a.report.parent
else:
    for name in ('go', 'rust', 'observer', 'output'):
        if getattr(a, name) is None:
            p.error(f'--{name} is required unless --report is used')
    a.output = a.output.resolve()
    a.output.mkdir(parents=True, exist_ok=False)

NOT_AN_IP = 'not-an-ip'
REFUSE_PREFIX = 'could not parse config: '


def refusal(text):
    """The declared startup-refusal row: exit 1 before readiness, nothing bound."""
    return {'stdout_exact': REFUSE_PREFIX + text + '\n', 'server_exit': 1,
            'refused': True, 'readiness_line_found': False, 'whip_create_status': 0,
            'whip_delete_status': 0, 'host_candidate_ips': [], 'v4_advertised': [],
            'v6_advertised': [], 'node_ip_slots': None, 'explicit_node_ip_log': False,
            'node_ip_primary': None, 'server_ports_released': True,
            'binary_unchanged': True}


# ------------------------------------------------------------------ profiles
# expect: the declared Go-source-contract row. Fields not listed are unconstrained.
# fence:  the recorded Rust divergence for the same field set.
PROFILES = [
    {'name': 'yaml-v4-loopback', 'group': 'parity',
     'argv': ['--node-ip-value', '127.0.0.1'],
     'why': 'plain IPv4 scalar: V4 slot, rewrite installed, candidate stays 127.0.0.1',
     'expect': {'explicit_node_ip_log': True, 'node_ip_slots': '{"V4":"127.0.0.1","V6":""}',
                'node_ip_primary': '127.0.0.1', 'host_candidate_ips': ['127.0.0.1'],
                'v6_advertised': [], 'whip_create_status': 201, 'whip_delete_status': 200,
                'server_exit': 0, 'refused': False}},
    {'name': 'yaml-v4-mapped-loopback', 'group': 'parity',
     'argv': ['--node-ip-value', '::ffff:127.0.0.2'],
     'why': 'IPv4-mapped scalar normalises into the V4 slot and the advertised '
            'candidate becomes 127.0.0.2, an address lo0 does not hold — the rewrite '
            'is proven, not assumed',
     'expect': {'explicit_node_ip_log': True, 'node_ip_slots': '{"V4":"127.0.0.2","V6":""}',
                'node_ip_primary': '127.0.0.2', 'host_candidate_ips': ['127.0.0.2'],
                'v6_advertised': [], 'whip_create_status': 201, 'whip_delete_status': 200,
                'server_exit': 0, 'refused': False}},
    {'name': 'yaml-v4-mapped-doc', 'group': 'parity',
     'argv': ['--node-ip-value', '::ffff:203.0.113.7'],
     'why': 'IPv4-mapped TEST-NET-3 scalar: candidate text crosses out of the loopback '
            'range while nothing is ever sent there',
     'expect': {'explicit_node_ip_log': True, 'node_ip_slots': '{"V4":"203.0.113.7","V6":""}',
                'node_ip_primary': '203.0.113.7', 'host_candidate_ips': ['203.0.113.7'],
                'whip_create_status': 201, 'server_exit': 0}},
    {'name': 'flag-v6-mapped-doc', 'group': 'parity',
     'argv': ['--node-ip-flag', 'rtc.node_ip.ipv6', '--node-ip-flag-value',
              '::ffff:203.0.113.7'],
     'why': 'the generated flag keeps the RAW text in the *ipv6* leaf, yet the mapper '
            'keys the external address by its own family, so the IPv4 candidate is the '
            'one rewritten and PrimaryIP() reports the raw mapped text',
     'expect': {'explicit_node_ip_log': True,
                'node_ip_slots': '{"V4":"","V6":"::ffff:203.0.113.7"}',
                'node_ip_primary': '::ffff:203.0.113.7',
                'host_candidate_ips': ['203.0.113.7'], 'v6_advertised': [],
                'whip_create_status': 201, 'server_exit': 0}},
    {'name': 'yaml-v6-only', 'group': 'parity',
     'argv': ['--node-ip-value', '2001:db8::7'],
     'why': 'family scoping: a V6-only node IP must NOT rewrite the IPv4 candidate, so '
            'the loopback candidate stays raw while the slot and primary are IPv6',
     'expect': {'explicit_node_ip_log': True, 'node_ip_slots': '{"V4":"","V6":"2001:db8::7"}',
                'node_ip_primary': '2001:db8::7', 'host_candidate_ips': ['127.0.0.1'],
                'v6_advertised': [], 'whip_create_status': 201, 'server_exit': 0}},
    {'name': 'yaml-pair-v6first-precedence', 'group': 'parity',
     'argv': ['--node-ip-value', '2001:db8::7,127.0.0.2'],
     'why': 'explicit IPv4 precedence: the v6 address comes FIRST in the text but the '
            'V4 slot still wins for PrimaryIP(), ToStringSlice order and the rewrite',
     'expect': {'explicit_node_ip_log': True,
                'node_ip_slots': '{"V4":"127.0.0.2","V6":"2001:db8::7"}',
                'node_ip_primary': '127.0.0.2', 'host_candidate_ips': ['127.0.0.2'],
                'whip_create_status': 201, 'server_exit': 0}},
    {'name': 'yaml-pair-spaces-trimmed', 'group': 'parity',
     'argv': ['--node-ip-value', '127.0.0.1, ::1'],
     'why': 'UnmarshalString trims each comma part (config.go:245), so the spaced pair '
            'still parses into both slots instead of refusing',
     'expect': {'explicit_node_ip_log': True,
                'node_ip_slots': '{"V4":"127.0.0.1","V6":"::1"}',
                'node_ip_primary': '127.0.0.1', 'host_candidate_ips': ['127.0.0.1'],
                'whip_create_status': 201, 'server_exit': 0}},
    {'name': 'yaml-mapping-form-ignored', 'group': 'parity',
     'argv': ['--node-ip-mapping', '{"ipv4": "127.0.0.2"}'],
     'why': 'the mapping form reaches UnmarshalYAML with an empty value.Value, so BOTH '
            'sides ignore it: no rewrite log and the candidate stays 127.0.0.1 rather '
            'than the mapping value',
     'expect': {'explicit_node_ip_log': False, 'node_ip_slots': None,
                'host_candidate_ips': ['127.0.0.1'], 'whip_create_status': 201,
                'server_exit': 0}},
    {'name': 'unpin-pair-both-families', 'group': 'parity', 'pin': False,
     'argv': ['--node-ip-value', '203.0.113.7,2001:db8::7'],
     'why': 'no interface filter: the daemon gathers its real surface and the catch-all '
            'rewrite masks EVERY admitted v4 candidate to the node IP, so no host '
            'address reaches the SDP answer',
     'expect': {'explicit_node_ip_log': True, 'whip_create_status': 201,
                'whip_delete_status': 200, 'v4_advertised': ['203.0.113.7'],
                'subset': {'v6_advertised': ['2001:db8::7']}, 'server_exit': 0}},
    {'name': 'unpin-no-node-ip-baseline', 'group': 'parity', 'pin': False, 'argv': [],
     'why': 'absent-control for the rewrite rows: with no node IP nothing is '
            'auto-generated-away, no rewrite line is logged and the loopback candidate '
            'survives as gathered',
     'expect': {'explicit_node_ip_log': False, 'node_ip_slots': None,
                'contains': ['127.0.0.1'], 'whip_create_status': 201,
                'whip_delete_status': 200, 'server_exit': 0, 'refused': False}},
    {'name': 'refusal-invalid-ip', 'group': 'parity', 'argv': ['--node-ip-value', NOT_AN_IP],
     'why': 'scalar path validates: net.ParseIP fails inside UnmarshalString (:246-248)',
     'expect': refusal('invalid ip ' + NOT_AN_IP)},
    {'name': 'refusal-three-parts', 'group': 'parity',
     'argv': ['--node-ip-value', '127.0.0.1,::1,203.0.113.7'],
     'why': 'more than two comma parts refuses with the documented shape (:240-242)',
     'expect': refusal('invalid node ip 127.0.0.1,::1,203.0.113.7, '
                       'should be <ipv4>,<ipv6>')},
    {'name': 'refusal-duplicate-v4', 'group': 'parity',
     'argv': ['--node-ip-value', '127.0.0.1,127.0.0.2'],
     'why': 'two v4 parts refuse by name and value (:250-252)',
     'expect': refusal('multiple ipv4 addresses provided: 127.0.0.1 and 127.0.0.2')},
    {'name': 'refusal-duplicate-v6', 'group': 'parity',
     'argv': ['--node-ip-value', '::1,2001:db8::7'],
     'why': 'two v6 parts refuse by name and value (:255-257)',
     'expect': refusal('multiple ipv6 addresses provided: ::1 and 2001:db8::7')},
    # ---------------------------------------------------------------- fences
    {'name': 'fence-flag-v4-zone', 'group': 'fence',
     'argv': ['--node-ip-flag', 'rtc.node_ip.ipv4', '--node-ip-flag-value', '127.0.0.1%lo0'],
     'why': 'zone-qualified text survives the flag bypass; Pion rejects the mapping when '
            'the session installs it, so the oracle refuses the WHIP create with 503',
     'expect': {'explicit_node_ip_log': True, 'node_ip_primary': '127.0.0.1%lo0',
                'whip_create_status': 503, 'answer_error': 'invalid address rewrite mapping',
                'host_candidate_ips': [], 'server_exit': 0},
     'divergence': {'rust': {'whip_create_status': 201, 'host_candidate_ips': ['127.0.0.1'],
                            'answer_error': None, 'server_exit': 0}}},
    {'name': 'fence-flag-v6-zone', 'group': 'fence',
     'argv': ['--node-ip-flag', 'rtc.node_ip.ipv6', '--node-ip-flag-value', 'fe80::1%lo0'],
     'why': 'same defect through the ipv6 leaf',
     'expect': {'explicit_node_ip_log': True, 'node_ip_primary': 'fe80::1%lo0',
                'whip_create_status': 503, 'answer_error': 'invalid address rewrite mapping',
                'host_candidate_ips': [], 'server_exit': 0},
     'divergence': {'rust': {'whip_create_status': 201, 'host_candidate_ips': ['127.0.0.1'],
                            'answer_error': None, 'server_exit': 0}}},
    {'name': 'fence-flag-v4-nonip', 'group': 'fence',
     'argv': ['--node-ip-flag', 'rtc.node_ip.ipv4', '--node-ip-flag-value', NOT_AN_IP],
     'why': 'unparseable text via the flag bypass: the oracle still installs the rule and '
            'fails per session, while a silent ignore advertises the raw interface address',
     'expect': {'explicit_node_ip_log': True, 'node_ip_primary': NOT_AN_IP,
                'whip_create_status': 503, 'answer_error': 'invalid address rewrite mapping',
                'host_candidate_ips': [], 'server_exit': 0},
     'divergence': {'rust': {'whip_create_status': 201, 'host_candidate_ips': ['127.0.0.1'],
                            'answer_error': None, 'server_exit': 0}}},
    {'name': 'fence-flag-v4-slash-local', 'group': 'fence',
     'argv': ['--node-ip-flag', 'rtc.node_ip.ipv4', '--node-ip-flag-value',
              '203.0.113.7/127.0.0.1'],
     'why': 'the external/local rule form (webrtc_config.go:266-272) is reachable only '
            'through the raw flag channel; the oracle rewrites the matching local '
            'candidate, Rust drops the rule because the text never parses as an address',
     'expect': {'explicit_node_ip_log': True, 'node_ip_primary': '203.0.113.7/127.0.0.1',
                'host_candidate_ips': ['203.0.113.7'], 'whip_create_status': 201,
                'server_exit': 0},
     'divergence': {'rust': {'host_candidate_ips': ['127.0.0.1'], 'whip_create_status': 201,
                            'node_ip_primary': '203.0.113.7/127.0.0.1'}}},
    {'name': 'fence-flag-v4-broadcast', 'group': 'fence',
     'argv': ['--node-ip-flag', 'rtc.node_ip.ipv4', '--node-ip-flag-value', '255.255.255.255'],
     'why': 'the oracle advertises the mapped broadcast address as the host candidate; '
            'the Rust ICE stack rejects it as a local candidate and answers with an empty '
            'candidate set, so a client gets nothing to dial',
     'expect': {'explicit_node_ip_log': True, 'node_ip_primary': '255.255.255.255',
                'host_candidate_ips': ['255.255.255.255'], 'whip_create_status': 201,
                'server_exit': 0},
     'divergence': {'rust': {'host_candidate_ips': [], 'whip_create_status': 201,
                            'node_ip_primary': '255.255.255.255'}}},
]

FIELDS = ('refused', 'readiness_line_found', 'whip_create_status', 'whip_delete_status',
          'host_candidate_ips', 'v4_advertised', 'v6_advertised', 'node_ip_primary',
          'node_ip_slots', 'explicit_node_ip_log', 'server_exit', 'server_ports_released',
          'answer_error', 'stdout_refusal', 'binary_unchanged', 'second_sigterm_sent',
          'sigkill_required')


def sha(path):
    return hashlib.sha256(pathlib.Path(path).read_bytes()).hexdigest()


def normalise(record):
    """Project an observer result onto the compared field set, redacted."""
    answer = record.get('answer_sdp') or ''
    error = None
    if answer.startswith('{'):
        try:
            error = json.loads(answer).get('error')
        except json.JSONDecodeError:
            error = '<unparsed json body>'
    out = {
        'refused': record.get('refused'),
        'readiness_line_found': record.get('readiness_line_found', False),
        'whip_create_status': record.get('whip_create_status'),
        'whip_delete_status': record.get('whip_delete_status'),
        'host_candidate_ips': redact(record.get('host_candidate_ips') or []),
        'v4_advertised': redact(record.get('v4_advertised') or []),
        'v6_advertised': redact(record.get('v6_advertised') or []),
        'node_ip_primary': redact(record.get('node_ip_primary')),
        'node_ip_slots': redact(record.get('node_ip_slots')),
        'explicit_node_ip_log': record.get('explicit_node_ip_log', False),
        'server_exit': record.get('server_exit'),
        'server_ports_released': record.get('server_ports_released'),
        'answer_error': error,
        'stdout_refusal': redact(record.get('daemon_stdout') or ''),
        'binary_unchanged': record.get('binary_unchanged'),
        'second_sigterm_sent': record.get('second_sigterm_sent'),
        'sigkill_required': record.get('sigkill_required'),
        'observer_error': record.get('error'),
        'candidate_lines': redact(record.get('candidate_lines') or []),
        'loopback_name': record.get('loopback_name'),
        'interfaces_pinned': record.get('interfaces_pinned'),
        # Harness metadata, deliberately outside the compared field set: a profile whose first
        # daemon lost the bind race and was relaunched still has a valid behaviour record, and
        # comparing retry counts across the two sides would turn a harness event into a
        # divergence. It is reported once at manifest level instead.
        'port_conflict_retries': record.get('port_conflict_retries', 0),
    }
    return out


def run(binary, observer, profile, side, work):
    out = work / f'{profile["name"]}-{side}'
    argv = [str(observer), '--binary', str(binary), '--output', str(out),
            '--profile', f'{profile["name"]}-{side}'] + list(profile['argv'])
    if not profile.get('pin', True):
        argv.append('--pin-loopback=false')
    proc = subprocess.run(argv, capture_output=True, text=True, timeout=300)
    path = out / 'result.json'
    if not path.exists():
        raise SystemExit(f'{profile["name"]}/{side}: observer wrote no result '
                         f'(exit {proc.returncode}): {proc.stderr[-400:]}')
    record = json.loads(path.read_text())
    record['_observer_exit'] = proc.returncode
    # An observer that reports an error (a port collision its retries could not absorb, a
    # gather timeout) produced no behaviour to compare. Normalising its partial record would
    # file the infrastructure failure as a parity or fence finding, which is the exact failure
    # mode the relaunch above exists to remove -- so the lane stops here instead.
    if record.get('error'):
        raise SystemExit(f'{profile["name"]}/{side}: observer error: {record["error"]} '
                         f'(see {path})')
    return record, out


def main():
    work = a.output / 'work'
    work.mkdir()
    go, rust, observer = a.go.resolve(), a.rust.resolve(), a.observer.resolve()
    ref = a.reference.resolve()
    manifest = {
        'status': 'INCOMPLETE',
        'leafs': COVERS,
        'scope': ('launched-MAIN rtc.node_ip: scalar vs generated-flag vs mapping-form '
                  'arrival channels, the V4/V6 slot classification, PrimaryIP() text, the '
                  'family-scoped catch-all host-candidate rewrite (including masking of '
                  'physical addresses when no interface filter is set), the '
                  'UnmarshalString refusal texts, and the per-session rewrite validation. '
                  'Loopback only; no claim about srflx/relay candidates, STUN discovery, '
                  'TURN, or a media connection.'),
        'binaries': {
            'go': {'path': str(go), 'sha256': sha(go),
                   'is_fixed_oracle': sha(go) == FIXED_GO_SHA256},
            'rust': {'path': str(rust), 'sha256': sha(rust)},
            'observer': {'path': str(observer), 'sha256': sha(observer),
                         'source': OBSERVER_SOURCE,
                         'source_sha256': sha(OBSERVER_SOURCE)},
        },
        'reference': {
            'go_checkout': str(ref / 'references/livekit-stable'),
            'go_checkout_commit': subprocess.run(
                ['git', '-C', str(ref / 'references/livekit-stable'), 'rev-parse', 'HEAD'],
                capture_output=True, text=True).stdout.strip(),
            'rust_head': subprocess.run(['git', '-C', str(ref), 'rev-parse', 'HEAD'],
                                        capture_output=True, text=True).stdout.strip(),
            'rust_dirty_paths': subprocess.run(
                ['git', '-C', str(ref), 'status', '--short'],
                capture_output=True, text=True).stdout.splitlines(),
        },
        'profiles': {},
    }
    path = a.output / 'execution.json'

    def save():
        tmp = path.with_suffix('.tmp')
        tmp.write_text(json.dumps(manifest, indent=2, ensure_ascii=False) + '\n')
        tmp.replace(path)

    save()
    selected = [pr for pr in PROFILES if not a.only or pr['name'] in a.only.split(',')]
    for profile in selected:
        entry = {'name': profile['name'], 'group': profile['group'],
                 'argv': profile['argv'], 'pinned': profile.get('pin', True),
                 'why': profile['why'], 'declared': redact(profile['expect'])}
        raw = {}
        for side, binary in (('go', go), ('rust', rust)):
            record, out = run(binary, observer, profile, side, work)
            raw[side] = normalise(record)
            for name in ('server.log', 'server.stdout'):
                src = out / name
                if src.exists():
                    shutil.copyfile(src, a.output /
                                    f'{profile["name"]}-{side}.{name.split(".")[-1]}')
        entry['go'], entry['rust'] = raw['go'], raw['rust']

        declared = {k: redact(v) for k, v in profile['expect'].items()}
        for side in ('go', 'rust'):
            mismatches = {}
            for key, want in declared.items():
                if key in ('stdout_exact', 'contains', 'subset'):
                    continue
                got = entry[side].get(key)
                if got != want:
                    mismatches[key] = {'want': want, 'got': got}
            # `contains` / `subset` carry the host-dependent rows: which physical
            # interfaces a daemon can gather varies, so only the family-scoped rewrite
            # outcome is declared, never the exact candidate list.
            for value in declared.get('contains') or []:
                got = entry[side].get('host_candidate_ips') or []
                if redact(value) not in got:
                    mismatches[f'contains:{value}'] = {'want': 'member', 'got': got}
            for key, allowed in (declared.get('subset') or {}).items():
                got = entry[side].get(key) or []
                extra = [v for v in got if v not in allowed]
                if extra:
                    mismatches[f'subset:{key}'] = {'want': allowed, 'got': got}
            entry[side + '_declared_ok'] = not mismatches
            entry[side + '_declared_mismatches'] = mismatches
        if 'stdout_exact' in profile['expect']:
            want = redact(profile['expect']['stdout_exact'])
            for side in ('go', 'rust'):
                got = entry[side]['stdout_refusal']
                ok = got == want
                entry[side + '_declared_ok'] = entry[side + '_declared_ok'] and ok
                if not ok:
                    entry[side + '_declared_mismatches']['stdout_exact'] = {
                        'want': want, 'got': got}
            entry['declared']['stdout_exact'] = want

        if profile['group'] == 'parity':
            compared = {f: entry['go'].get(f) for f in FIELDS
                        if f not in ('second_sigterm_sent', 'sigkill_required')}
            entry['cross_equal'] = all(compared[f] == entry['rust'].get(f)
                                       for f in compared)
            entry['match'] = (entry['go_declared_ok'] and entry['rust_declared_ok']
                              and entry['cross_equal'])
        else:
            div = {k: redact(v) for k, v in profile['divergence']['rust'].items()}
            entry['recorded_rust_divergence'] = div
            entry['rust_diverges_as_recorded'] = all(
                entry['rust'].get(k) == v for k, v in div.items())
            entry['divergence_still_present'] = any(
                entry['go'].get(k) != entry['rust'].get(k) for k in div)
            entry['match'] = (entry['go_declared_ok']
                              and entry['rust_diverges_as_recorded']
                              and entry['divergence_still_present'])
        manifest['profiles'][profile['name']] = entry
        save()
        (a.output / f'{profile["name"]}.json').write_text(json.dumps(
            {k: v for k, v in entry.items() if k not in ('go', 'rust')}, indent=2) + '\n')
        print(f'{profile["group"]:6s} {profile["name"]}: match={entry["match"]} '
              f'go_ok={entry["go_declared_ok"]} '
              f'{("cross=" + str(entry["cross_equal"])) if profile["group"] == "parity" else ("rust_div=" + str(entry["rust_diverges_as_recorded"]))}',
              flush=True)

    return finish(manifest, a.output)




def refuse_filed_slot(path):
    """--report rewrites execution.json and BATCH.json next to the receipt it reads, so pointing it
    at a committed evidence slot destroys that evidence instead of checking it. Filed slots are
    immutable: copy the receipt to a scratch directory and report the copy."""
    resolved = pathlib.Path(path).resolve()
    if '/server/evidence/' in str(resolved):
        raise SystemExit(json.dumps({
            'error': 'refusing_to_touch_filed_evidence', 'path': str(resolved),
            'why': 'finish() writes execution.json/BATCH.json beside the receipt it reads',
            'use': 'cp the receipt to a scratch dir and run --report on the copy'}))


def finish(manifest, out):
    """Recompute the verdict from the per-profile records in `manifest`.

    Kept separate from the launch loop so a persisted execution.json can be
    re-checked without re-running a single daemon.
    """
    refuse_filed_slot(out)

    P = manifest['profiles']
    parity = [n for n, e in P.items() if e['group'] == 'parity']
    fences = [n for n, e in P.items() if e['group'] == 'fence']
    controls = {}
    for side in ('go', 'rust'):
        g = lambda n, k: (P[n][side].get(k) if n in P else '<row-not-run>')
        controls[f'rewrite_changes_advertised_text_{side}'] = (
            g('yaml-v4-mapped-loopback', 'host_candidate_ips') == ['127.0.0.2']
            and g('yaml-v4-loopback', 'host_candidate_ips') == ['127.0.0.1'])
        controls[f'family_scoping_{side}'] = (
            g('yaml-v6-only', 'host_candidate_ips') == ['127.0.0.1']
            and g('yaml-v4-loopback', 'host_candidate_ips') == ['127.0.0.1']
            and g('yaml-pair-v6first-precedence', 'node_ip_primary') == '127.0.0.2')
        controls[f'mapped_normalises_into_v4_slot_{side}'] = (
            g('yaml-v4-mapped-loopback', 'node_ip_slots') == '{"V4":"127.0.0.2","V6":""}'
            and g('flag-v6-mapped-doc', 'node_ip_slots')
            == '{"V4":"","V6":"::ffff:203.0.113.7"}')
        controls[f'mapping_form_ignored_{side}'] = (
            g('yaml-mapping-form-ignored', 'explicit_node_ip_log') is False
            and g('yaml-mapping-form-ignored', 'host_candidate_ips') == ['127.0.0.1']
            and g('yaml-v4-mapped-loopback', 'explicit_node_ip_log') is True)
        refusal_rows = [n for n in P if n.startswith('refusal-')]
        controls[f'refusal_text_specificity_{side}'] = len(refusal_rows) == 4 and len({
            P[n][side]['stdout_refusal'] for n in refusal_rows}) == 4
        controls[f'refusal_has_no_listener_{side}'] = bool(refusal_rows) and all(
            P[n][side]['server_exit'] == 1 and not P[n][side]['readiness_line_found']
            and P[n][side]['server_ports_released'] for n in refusal_rows)
        controls[f'flag_channel_bypasses_validation_{side}'] = (
            g('flag-v6-mapped-doc', 'node_ip_primary') == '::ffff:203.0.113.7')
        controls[f'physical_addresses_masked_when_unpinned_{side}'] = (
            g('unpin-pair-both-families', 'v4_advertised') == ['203.0.113.7']
            and g('unpin-no-node-ip-baseline', 'explicit_node_ip_log') is False)
        controls[f'clean_lifecycle_{side}'] = all(
            P[n][side]['server_exit'] == 0 and P[n][side]['server_ports_released']
            and P[n][side]['binary_unchanged'] for n in P
            if not n.startswith('refusal-'))
    controls['fences_all_reproduce'] = len(fences) == 5 and all(
        P[n]['match'] for n in fences)
    # Each fence declares what the Rust side still does. Distinctness is measured on
    # the recorded divergence VALUES, not on the profile count: three of the five rows
    # are input variants of the same oracle behaviour (Pion refuses the session), and
    # claiming five shapes would be a claim about the labels, not about the behaviour.
    shapes = {json.dumps(P[n]['recorded_rust_divergence'], sort_keys=True)
              for n in fences}
    controls['fence_divergence_shapes'] = len(fences) == 5 and len(shapes) == 3
    controls['fences_have_a_witness_field'] = all(
        P[n]['recorded_rust_divergence'] and any(
            P[n]['go'].get(k) != P[n]['rust'].get(k) and P[n]['rust'].get(k) is not None
            for k in P[n]['recorded_rust_divergence'])
        for n in fences)
    # Falsifiable: re-scan the persisted receipt and fail if any non-allowlisted
    # address literal survived redaction (a real LAN/public address in evidence).
    surviving = sorted(set(leaks(json.dumps({'profiles': P, 'binaries':
                                            manifest['binaries']}))))
    controls['receipt_has_no_machine_address'] = not surviving
    manifest['redaction_survivors'] = surviving
    manifest['redaction_applied'] = '<discovered-address>' in json.dumps(P)
    # A port the harness reserved and lost is retried inside the observer. That recovery has to
    # be visible here: assert that every relaunched row actually reached the daemon's readiness
    # line (or was a row expected to refuse), so a retry cannot quietly stand in for a launch.
    conflicts = {n: {s: P[n][s].get('port_conflict_retries') or 0
                     for s in ('go', 'rust')
                     if P[n][s].get('port_conflict_retries')}
                 for n in P if any(P[n][s].get('port_conflict_retries')
                                   for s in ('go', 'rust'))}
    manifest['port_conflicts_recovered'] = conflicts
    controls['conflict_retries_reached_readiness'] = all(
        P[n][s].get('readiness_line_found') or n.startswith('refusal-')
        for n, sides in conflicts.items() for s in sides)
    # The harness identity check has to run before failed_controls is assembled: it is the one
    # control whose answer changes between the launch and an offline --report of the same receipt,
    # and computing it afterwards let a receipt re-derived with a edited harness report
    # `failed_controls: []` while its own cross_profile_controls said `harness_unchanged_since_run:
    # false` -- the drift was visible only to a reader who diffed the two by hand.
    manifest['harness_sha256_now'] = sha(pathlib.Path(__file__).resolve())
    controls['harness_unchanged_since_run'] = (
        manifest.get('harness_sha256', manifest['harness_sha256_now'])
        == manifest['harness_sha256_now'])
    manifest['harness_sha256'] = manifest.get('harness_sha256') or manifest['harness_sha256_now']

    manifest['parity_rows'] = len(parity)
    manifest['fence_rows'] = len(fences)
    manifest['failed_parity'] = [n for n in parity if not P[n]['match']]
    manifest['failed_fences'] = [n for n in fences if not P[n]['match']]
    manifest['cross_profile_controls'] = controls
    manifest['failed_controls'] = [k for k, v in controls.items() if not v]
    recorded = {side: pathlib.Path(manifest['binaries'][side]['path'])
                for side in ('go', 'rust')}
    manifest['binaries_unchanged'] = all(
        recorded[side].exists()
        and sha(recorded[side]) == manifest['binaries'][side]['sha256']
        for side in ('go', 'rust'))
    manifest['binary_hash_now'] = {
        side: (sha(recorded[side]) if recorded[side].exists() else None)
        for side in ('go', 'rust')}
    manifest['reference_commit_matches'] = (
        manifest['reference']['go_checkout_commit'] == FIXED_GO_COMMIT)
    # A FAIL must name the condition that broke, otherwise the offline re-derivation reads
    # as "every row failed" when in fact only an identity check drifted after the run.
    gate = {
        'binaries_unchanged': manifest['binaries_unchanged'],
        'reference_commit_matches': manifest['reference_commit_matches'],
        'go_is_fixed_oracle': manifest['binaries']['go']['is_fixed_oracle'],
        'no_failed_parity': not manifest['failed_parity'],
        'no_failed_fences': not manifest['failed_fences'],
        'no_failed_controls': not manifest['failed_controls'],
        'harness_unchanged': controls['harness_unchanged_since_run'],
    }
    manifest['status_gates'] = gate
    manifest['unmet_gates'] = [k for k, v in gate.items() if not v]
    all_ok = all(gate.values())
    manifest['status'] = ('PARTIAL_LAUNCHED_MAIN_NODE_IP_WITH_FENCED_DIVERGENCES'
                          if all_ok else 'FAIL')
    manifest['open_divergences'] = {
        n: {'oracle': {k: P[n]['go'].get(k) for k in P[n]['recorded_rust_divergence']},
            'rust': {k: P[n]['rust'].get(k) for k in P[n]['recorded_rust_divergence']},
            'why': P[n]['why']} for n in fences}
    out.joinpath('execution.json').write_text(
        json.dumps(manifest, indent=2, ensure_ascii=False) + '\n')
    batch = {'status': manifest['status'], 'parity_rows': len(parity),
             'fence_rows': len(fences),
             'profiles': {n: {'group': e['group'], 'match': e['match'],
                              'go_declared_ok': e['go_declared_ok'],
                              'rust_declared_ok': e['rust_declared_ok'],
                              'cross_equal': e.get('cross_equal'),
                              'rust_diverges_as_recorded': e.get('rust_diverges_as_recorded')}
                          for n, e in P.items()},
             'controls': controls, 'status_gates': manifest['status_gates'],
             'binary_hash_now': manifest['binary_hash_now'],
             'failed_parity': manifest['failed_parity'],
             'failed_fences': manifest['failed_fences'],
             'failed_controls': manifest['failed_controls'],
             'open_divergences': manifest['open_divergences'],
             'binaries': manifest['binaries'], 'reference': manifest['reference'],
             'harness_sha256': manifest['harness_sha256']}
    (out / 'BATCH.json').write_text(json.dumps(batch, indent=2, ensure_ascii=False) + '\n')
    print(json.dumps({'status': manifest['status'], 'parity': len(parity),
                      'fences': len(fences), 'failed_parity': manifest['failed_parity'],
                      'failed_fences': manifest['failed_fences'],
                      'failed_controls': manifest['failed_controls'],
                      'unmet_gates': [k for k, v in gate.items() if not v]}))
    return 0 if all_ok else 1


if __name__ == '__main__':
    if a.report:
        sys.exit(finish(json.loads(a.report.read_text()), a.report.parent))
    sys.exit(main())
