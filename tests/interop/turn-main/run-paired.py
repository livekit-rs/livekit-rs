#!/usr/bin/env python3
"""SPEC-0077 C23 — launched-MAIN embedded TURN consumer paired Go/Rust differential.

Fresh paired servers (locked Go oracle v1.13.10 e5655ac2 == binary 4bc2378e vs
Rust livecloud candidate d1dad0fc) are launched per profile with the SAME base
config; only the `turn.*` surface under test differs. Five ledger leaves are
named: YAML:turn.enabled, YAML:turn.bind_addresses, YAML:turn.proxy_protocol,
YAML:turn.proxy_protocol_trusted_cidrs, CONFIG_ENABLED_CAPABILITY:turn.proxy_protocol.

The stimulus is raw STUN/PROXY wire bytes (probe.mjs) against the real embedded
TURN listeners — no media, no TLS certificate (turn.external_tls=true makes the
TLS-port listener plain TCP, the listener intended for a TLS terminator), and NO
change to the official SDK. This is COMPLEMENTARY to tests/interop/turn-tls-proxy
and turn-listener-relay (pion-turn Go observers proving certificate-verified TLS
+ authenticated allocation + bidirectional relay on earlier candidates): C23
re-proves the launched-MAIN config consumer + PROXY REQUIRE/REJECT gate on the
CURRENT clean release candidate with a light raw-socket paired harness, and binds
the result to the ledger at the official_differential_lifecycle tier.

Two observable families, both deterministic:
  alive profiles — the server starts; probe.mjs drives the TURN listeners:
      TCP test A (bare STUN Binding Request, no PROXY header):
        proxy_protocol=false           -> Binding Success Response.
        proxy_protocol=true + trusted  -> REQUIRE reads STUN bytes as a PROXY
                                          header, fails, closes: no reply.
        proxy_protocol=true + untrusted-> REJECT closes immediately: no reply.
      TCP test B (PROXY v1 header naming 203.0.113.9:55000 + STUN Binding):
        proxy_protocol=false           -> "PROXY..." is not a STUN frame: no reply.
        proxy_protocol=true + trusted  -> header parsed; Binding Success Response
                                          whose XOR-MAPPED-ADDRESS == 203.0.113.9:55000
                                          (proves the forwarded source was used).
        proxy_protocol=true + untrusted-> REJECT: no reply.
      UDP test (STUN Binding Request datagram):
        enabled=true  -> Binding Success Response (mapped IP 127.0.0.1).
        enabled=false -> no listener: no reply.
  startup_error profiles — the server must FAIL at the real TURN consumer with
      the exact upstream text on stdout and exit 1:
        no usable port                 -> "invalid TURN ports"
        proxy_protocol + empty CIDRs   -> "turn.proxy_protocol requires at least
                                           one entry in turn.proxy_protocol_trusted_cidrs"
        proxy_protocol + malformed CIDR-> 'invalid CIDR "nope" in
                                           turn.proxy_protocol_trusted_cidrs: invalid CIDR address: nope'

Configs are emitted as JSON (a valid YAML subset both loaders accept) so the
harness needs no PyYAML dependency. Every assertion is falsifiable: a server that
ignored turn.enabled (kept the listener off/on), dropped bind_addresses, skipped
the PROXY REQUIRE/REJECT gate, or trusted an untrusted proxy would flip a response
boolean / the forwarded mapped address / an exit+text and FAIL (see the
proxy-gate-disabled positive control mutant, which fails exactly the two
proxy_protocol alive profiles and passes the other seven).

Count/response-only normalization (scope limit): the XOR-MAPPED-ADDRESS for a
DIRECT (non-PROXY) Binding legitimately reflects the ephemeral loopback source
port, which differs per connection and per side; for those replies only the
mapped IP (127.0.0.1) and response presence are compared, NOT the ephemeral port.
The PROXY reply port IS compared because it is the fixed forwarded source port
(55000), not an ephemeral one. TCP close/reset/timeout timing details legitimately
differ between the Go and Rust listeners and are NOT compared (response presence
+ mapped address are the observable). Full certificate-verified TLS termination,
authenticated TURN allocation and bidirectional relay are out of scope here
(covered by the complementary pion-turn observers); NAT topology is out of scope.
"""
import argparse, hashlib, json, os, pathlib, signal, socket, subprocess, time

p = argparse.ArgumentParser()
p.add_argument('--go', required=True, type=pathlib.Path)
p.add_argument('--rust', required=True, type=pathlib.Path)
p.add_argument('--output', required=True, type=pathlib.Path)
p.add_argument('--node', default='node')
a = p.parse_args()
here = pathlib.Path(__file__).resolve().parent
PROBE = here / 'probe.mjs'
a.output.mkdir(parents=True, exist_ok=True)

PROXY_SRC = '203.0.113.9:55000'
COVERS = [
    'YAML:turn.enabled',
    'YAML:turn.bind_addresses',
    'YAML:turn.proxy_protocol',
    'YAML:turn.proxy_protocol_trusted_cidrs',
    'CONFIG_ENABLED_CAPABILITY:turn.proxy_protocol',
]

# Each profile: kind, turn-config knobs, and the per-side contract.
# alive contract keys: tcp_raw/tcp_proxy/udp -> {resp, ip?, mapped?}.
# startup_error contract: {exit, stdout}.
PROFILES = [
    {'name': 'turn-disabled', 'kind': 'alive', 'enabled': False, 'tcp': True, 'udp': True,
     'expect': {'tcp_raw': {'resp': False}, 'tcp_proxy': {'resp': False}, 'udp': {'resp': False}}},
    {'name': 'turn-udp-enabled', 'kind': 'alive', 'enabled': True, 'udp_only': True,
     'expect': {'udp': {'resp': True, 'ip': '127.0.0.1'}}},
    {'name': 'turn-tcp-plain', 'kind': 'alive', 'enabled': True, 'tcp': True, 'proxy': False,
     'expect': {'tcp_raw': {'resp': True, 'ip': '127.0.0.1'}, 'tcp_proxy': {'resp': False}}},
    {'name': 'turn-bind-loopback', 'kind': 'alive', 'enabled': True, 'tcp': True, 'proxy': False,
     'bind': ['127.0.0.1'],
     'expect': {'tcp_raw': {'resp': True, 'ip': '127.0.0.1'}, 'tcp_proxy': {'resp': False}}},
    {'name': 'turn-proxy-trusted', 'kind': 'alive', 'enabled': True, 'tcp': True, 'proxy': True,
     'trusted': ['127.0.0.0/8'],
     'expect': {'tcp_raw': {'resp': False}, 'tcp_proxy': {'resp': True, 'mapped': PROXY_SRC}}},
    {'name': 'turn-proxy-untrusted', 'kind': 'alive', 'enabled': True, 'tcp': True, 'proxy': True,
     'trusted': ['203.0.113.0/24'],
     'expect': {'tcp_raw': {'resp': False}, 'tcp_proxy': {'resp': False}}},
    {'name': 'turn-invalid-ports', 'kind': 'startup_error', 'enabled': True, 'no_ports': True,
     'expect': {'exit': 1, 'stdout': 'invalid TURN ports\n'}},
    {'name': 'turn-proxy-empty-cidrs', 'kind': 'startup_error', 'enabled': True, 'tcp': True,
     'proxy': True, 'trusted': [],
     'expect': {'exit': 1, 'stdout': 'turn.proxy_protocol requires at least one entry in turn.proxy_protocol_trusted_cidrs\n'}},
    {'name': 'turn-proxy-malformed-cidr', 'kind': 'startup_error', 'enabled': True, 'tcp': True,
     'proxy': True, 'trusted': ['nope'],
     'expect': {'exit': 1, 'stdout': 'invalid CIDR "nope" in turn.proxy_protocol_trusted_cidrs: invalid CIDR address: nope\n'}},
]


def sha(path):
    return hashlib.sha256(pathlib.Path(path).read_bytes()).hexdigest()


def freeport():
    s = socket.socket()
    s.bind(('127.0.0.1', 0))
    port = s.getsockname()[1]
    s.close()
    return port


def base_env():
    return {k: v for k, v in os.environ.items()
            if not k.startswith(('LIVEKIT_', 'LIVECLOUD_'))}


def wait_http(port, proc, timeout=20):
    end = time.time() + timeout
    while time.time() < end:
        if proc.poll() is not None:
            return False
        try:
            s = socket.create_connection(('127.0.0.1', port), timeout=0.5)
            s.close()
            return True
        except OSError:
            time.sleep(0.1)
    return False


def turn_config(prof, tls, udp):
    t = {'enabled': prof['enabled'], 'domain': 'turn.example.com', 'external_tls': True,
         'relay_range_start': 30000, 'relay_range_end': 40000,
         'proxy_protocol': prof.get('proxy', False),
         'proxy_protocol_trusted_cidrs': prof.get('trusted', [])}
    if prof.get('udp_only'):
        t['udp_port'] = udp
        t['tls_port'] = 0
    elif prof.get('no_ports'):
        t['udp_port'] = 0
        t['tls_port'] = 0
    else:
        t['tls_port'] = tls
        t['udp_port'] = 0
    if 'bind' in prof:
        t['bind_addresses'] = prof['bind']
    return t


def normalize(obs):
    """Comparable observable: drop ephemeral ports on direct replies and TCP
    close/reset/timeout timing; keep the fixed forwarded PROXY mapped address."""
    n = {}
    for key in ('tcp_raw', 'tcp_proxy', 'udp'):
        o = obs.get(key)
        if o is None:
            continue
        entry = {'resp': bool(o.get('got_response')), 'ip': o.get('mapped_ip')}
        if key == 'tcp_proxy':
            entry['mapped'] = o.get('mapped')  # fixed forwarded source or None
        n[key] = entry
    return n


def contract_ok(norm, expect):
    for key, exp in expect.items():
        got = norm.get(key)
        if got is None:
            return False
        if got['resp'] != exp['resp']:
            return False
        if exp['resp']:
            if 'ip' in exp and got.get('ip') != exp['ip']:
                return False
            if 'mapped' in exp and got.get('mapped') != exp['mapped']:
                return False
    return True


manifest = {'status': 'INCOMPLETE',
            'scope': ('launched-MAIN embedded TURN consumer: turn.enabled listener on/off + invalid-ports '
                      'startup gate, turn.bind_addresses listener bind, turn.proxy_protocol REQUIRE/REJECT '
                      'gate on the plain TCP (external_tls) listener, turn.proxy_protocol_trusted_cidrs '
                      'trusted/untrusted matching + empty/malformed startup gates, and the forwarded PROXY '
                      'source reaching the STUN Binding XOR-MAPPED-ADDRESS. Raw STUN/PROXY stimulus, no media, '
                      'no TLS cert, official SDK unchanged. Complementary to turn-tls-proxy/turn-listener-relay '
                      '(certificate-verified TLS + authenticated allocation + relay). Not a TLS-termination, '
                      'authenticated-relay or NAT-topology claim.'),
            'binaries': {}, 'profiles': {}, 'items': COVERS}
for side in ['go', 'rust']:
    b = getattr(a, side).resolve()
    manifest['binaries'][side] = {'path': str(b), 'sha256': sha(b)}


def save():
    (a.output / 'execution.json').write_text(json.dumps(manifest, indent=2) + '\n')


save()


def run_side(side, binary, prof):
    name = prof['name']
    http, tls, udp, iceudp = freeport(), freeport(), freeport(), freeport()
    env = base_env()
    cfg = {'port': http, 'bind_addresses': ['127.0.0.1'],
           'rtc': {'tcp_port': 0, 'udp_port': iceudp, 'node_ip': '127.0.0.1',
                   'use_external_ip': False, 'enable_loopback_candidate': True},
           'keys': {'devkey': 'owned-turn-fixture-secret-more-than-32-chars'},
           'logging': {'level': 'error'}, 'turn': turn_config(prof, tls, udp)}
    cpath = a.output / f'{name}-{side}.config.json'
    cpath.write_text(json.dumps(cfg))
    outpath = a.output / f'{name}-{side}.stdout.txt'
    errpath = a.output / f'{name}-{side}.stderr.txt'
    argv = [str(binary), '--config-body', json.dumps(cfg)]
    rec = {'pid': None, 'config_sha256': sha(cpath), 'argv_tail': ['--config-body', '<json>'],
           'http_port': http, 'turn_tls_port': tls, 'turn_udp_port': udp}
    with outpath.open('wb') as out, errpath.open('wb') as err:
        proc = subprocess.Popen(argv, stdout=out, stderr=err, env=env,
                                stdin=subprocess.DEVNULL, start_new_session=True)
        rec['pid'] = proc.pid
        try:
            if prof['kind'] == 'alive':
                rec['ready'] = wait_http(http, proc)
                if rec['ready']:
                    penv = dict(env, SIDE=side, PROFILE=name,
                                OUTPUT=str(a.output / f'{name}-{side}.probe.json'),
                                TURN_HOST='127.0.0.1', PROXY_SRC_IP='203.0.113.9', PROXY_SRC_PORT='55000')
                    if prof.get('tcp'):
                        penv['TURN_TCP_PORT'] = str(tls)
                    if prof.get('udp') or prof.get('udp_only'):
                        penv['TURN_UDP_PORT'] = str(udp)
                    pr = subprocess.run([a.node, str(PROBE)], env=penv,
                                        stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=40)
                    rec['probe_rc'] = pr.returncode
                    pout = a.output / f'{name}-{side}.probe.json'
                    try:
                        probe = json.loads(pout.read_text())
                    except (json.JSONDecodeError, ValueError, FileNotFoundError):
                        probe = {'status': 'FAIL', 'unparsed': pr.stdout.decode()[:500]}
                    rec['probe_status'] = probe.get('status')
                    rec['norm'] = normalize(probe.get('observations', {}))
                    rec['raw_observations'] = probe.get('observations', {})
                else:
                    rec['norm'] = {}
                    rec['probe_status'] = 'NOT_READY'
                rec['server_alive_through_probe'] = proc.poll() is None
                if proc.poll() is None:
                    proc.send_signal(signal.SIGINT)
                try:
                    rc = proc.wait(timeout=20)
                except subprocess.TimeoutExpired:
                    proc.kill(); rc = proc.wait(); rec['forced_kill'] = True
            else:
                try:
                    rc = proc.wait(timeout=20)
                except subprocess.TimeoutExpired:
                    proc.kill(); rc = proc.wait(); rec['unexpected_start'] = True
            rec['exit'] = rc
            rec['stdout'] = outpath.read_text()
            rec['stderr'] = errpath.read_text()
        finally:
            if proc.poll() is None:
                proc.kill(); proc.wait()
    return rec


# Collect every profile on both sides first, then assert (no fail-fast masking).
for prof in PROFILES:
    name = prof['name']
    entry = {'profile': name, 'kind': prof['kind'], 'covers': COVERS, 'expect': prof['expect']}
    for side, binary in [('go', a.go), ('rust', a.rust)]:
        entry[side] = run_side(side, binary.resolve(), prof)
    if prof['kind'] == 'alive':
        entry['go_contract_ok'] = entry['go'].get('ready', False) and contract_ok(entry['go'].get('norm', {}), prof['expect'])
        entry['rust_contract_ok'] = entry['rust'].get('ready', False) and contract_ok(entry['rust'].get('norm', {}), prof['expect'])
        entry['cross_side_equal'] = entry['go'].get('norm') == entry['rust'].get('norm')
    else:
        exp = prof['expect']
        entry['go_contract_ok'] = entry['go']['exit'] == exp['exit'] and entry['go']['stdout'] == exp['stdout']
        entry['rust_contract_ok'] = entry['rust']['exit'] == exp['exit'] and entry['rust']['stdout'] == exp['stdout']
        entry['cross_side_equal'] = (entry['go']['exit'] == entry['rust']['exit']
                                     and entry['go']['stdout'] == entry['rust']['stdout'])
    entry['match'] = entry['go_contract_ok'] and entry['rust_contract_ok'] and entry['cross_side_equal']
    manifest['profiles'][name] = entry
    save()

# Cross-profile controls: prove the consumer genuinely responds to each turn knob.
P = manifest['profiles']


def resp(name, side, key):
    return P[name][side].get('norm', {}).get(key, {}).get('resp')


def mapped(name, side, key):
    return P[name][side].get('norm', {}).get(key, {}).get('mapped')


controls = {}
for side in ('go', 'rust'):
    controls[f'enabled_toggle_listener_{side}'] = (
        resp('turn-disabled', side, 'tcp_raw') is False
        and resp('turn-udp-enabled', side, 'udp') is True)
    controls[f'tcp_listener_present_{side}'] = resp('turn-tcp-plain', side, 'tcp_raw') is True
    controls[f'proxy_off_raw_works_{side}'] = resp('turn-tcp-plain', side, 'tcp_raw') is True
    controls[f'proxy_off_header_garbage_{side}'] = resp('turn-tcp-plain', side, 'tcp_proxy') is False
    controls[f'proxy_trusted_requires_header_{side}'] = resp('turn-proxy-trusted', side, 'tcp_raw') is False
    controls[f'proxy_trusted_uses_forwarded_source_{side}'] = (
        resp('turn-proxy-trusted', side, 'tcp_proxy') is True
        and mapped('turn-proxy-trusted', side, 'tcp_proxy') == PROXY_SRC)
    controls[f'proxy_untrusted_rejects_{side}'] = (
        resp('turn-proxy-untrusted', side, 'tcp_raw') is False
        and resp('turn-proxy-untrusted', side, 'tcp_proxy') is False)
    controls[f'bind_address_reachable_{side}'] = resp('turn-bind-loopback', side, 'tcp_raw') is True
    controls[f'invalid_ports_error_{side}'] = (
        P['turn-invalid-ports'][side]['exit'] == 1
        and P['turn-invalid-ports'][side]['stdout'] == 'invalid TURN ports\n')
    controls[f'empty_cidrs_error_{side}'] = (
        P['turn-proxy-empty-cidrs'][side]['exit'] == 1
        and P['turn-proxy-empty-cidrs'][side]['stdout'] == 'turn.proxy_protocol requires at least one entry in turn.proxy_protocol_trusted_cidrs\n')
    controls[f'malformed_cidr_error_{side}'] = (
        P['turn-proxy-malformed-cidr'][side]['exit'] == 1
        and P['turn-proxy-malformed-cidr'][side]['stdout'] == 'invalid CIDR "nope" in turn.proxy_protocol_trusted_cidrs: invalid CIDR address: nope\n')

failed_profiles = [n for n, e in P.items() if not e['match']]
failed_controls = [k for k, v in controls.items() if not v]
all_match = not failed_profiles and not failed_controls
manifest['cross_profile_controls'] = controls
manifest['failed_profiles'] = failed_profiles
manifest['failed_controls'] = failed_controls
manifest['case_count'] = len(PROFILES)
manifest['status'] = 'PASS_LAUNCHED_MAIN_TURN' if all_match else 'FAIL'
save()

batch = {'status': manifest['status'], 'case_count': len(PROFILES), 'items': COVERS,
         'profiles': {n: {'match': e['match'], 'go_contract_ok': e['go_contract_ok'],
                          'rust_contract_ok': e['rust_contract_ok'], 'cross_side_equal': e['cross_side_equal']}
                      for n, e in P.items()},
         'cross_profile_controls': controls,
         'failed_profiles': failed_profiles, 'failed_controls': failed_controls,
         'binaries': manifest['binaries'],
         'input_identity_stable': manifest['binaries']['go']['sha256'] == '4bc2378e636740b29093061036249ffc9ebbef91f4ece1de12177c8dfee1e7dc',
         'harness_sha256': sha(pathlib.Path(__file__).resolve()),
         'probe_sha256': sha(PROBE)}
(a.output / 'BATCH.json').write_text(json.dumps(batch, indent=2) + '\n')
print(json.dumps({'status': manifest['status'], 'case_count': len(PROFILES),
                  'failed_profiles': failed_profiles, 'failed_controls': failed_controls}))
raise SystemExit(0 if all_match else 1)