#!/usr/bin/env python3
"""SPEC-0077 C26 — launched-MAIN rtc.require_ipv4 STUN-udp4 paired Go/Rust differential.

Fresh paired servers (locked Go oracle v1.13.10 e5655ac2 == binary 4bc2378e vs the
Rust livecloud release candidate) are launched per profile with the SAME base
config; only the `rtc.require_ipv4` / `rtc.use_external_ip` / `rtc.stun_servers`
surface under test differs. Two ledger leaves are named:
YAML:rtc.require_ipv4 and generated_flags:--rtc.require_ipv4.

Upstream consumes require_ipv4 while VALIDATING the RTC config, not while loading
it: the pinned module `github.com/livekit/mediatransportutil/pkg/rtcconfig`
(go.mod:23) `RTCConfig.Validate` -> `resolveNodeIP` -> `determineIP`
(ip.go:35-77). With UseExternalIP, determineIP picks GetExternalIPv4 (dial
network "udp4") when RequireIPv4 is set, else GetExternalIP (network "udp").
A resolution failure is wrapped by livekit-stable NewConfig (config.go:696-698)
as `could not validate RTC config: %v`, returned through getConfig/startServer,
and printed with `fmt.Println(err)` + exit 1 (cmd/server/main.go:190-193), so the
refusal is bare upstream text on STDOUT. The Rust mirror is lk-config
`netdetect::determine_ip` (netdetect.rs:232-268) -> `external_ip` -> `stun_binding`
(netdetect.rs:413) which selects StunNetwork::V4 when ipv4_only else ::Any, called
from `load::rtc_validate` (load.rs:194-218) and wrapped at load.rs:587-588 as
`could not validate RTC config: {e}` — byte-identical to the Go wrapping.

The load-bearing discriminator is the STUN ADDRESS FAMILY. A fake STUN binding
responder is started on IPv6 loopback (::1) and IPv4 loopback (127.0.0.1), each
replying to a Binding Request with XOR-MAPPED-ADDRESS 192.0.2.1 (TEST-NET-1). With
require_ipv4=true and an IPv6-only STUN server, the "udp4" dial cannot resolve ::1
and fails BEFORE any packet is sent: the IPv6 responder records ZERO hits and the
server exits 1 with the exact text. With require_ipv4=false the "udp" dial reaches
::1, the responder records hits, and the server serves HTTP and exits 0. This is
pure loopback — no route/Clash/proxy mutation, no public network dependency.

Two observable families, both deterministic:
  startup_error profiles — the server must FAIL at the real require_ipv4 consumer
      with the exact upstream text on stdout and exit 1, AND the IPv6 responder
      must see 0 new hits (udp4 never reaches ::1):
        require_ipv4=true + IPv6 STUN (YAML, CLI override, ENV override).
  alive profiles — require_ipv4 must NOT block the normal paths; the server serves
      HTTP and shuts down cleanly (exit 0) on both sides, AND the STUN-contact
      boolean matches the configured family:
        require_ipv4=false + IPv6 STUN (udp reaches ::1, responder hit),
        require_ipv4=true/false + IPv4 STUN (udp4/udp both reach 127.0.0.1),
        CLI/ENV false overriding a true YAML (override turns the gate OFF),
        use_external_ip=false local path (NO STUN contacted at all),
        require_ipv4 unset (defaults false, udp reaches ::1).

The CLI/ENV override profiles are load-bearing: the YAML carries the OPPOSITE
value, so a consumer that ignored the CLI flag / ENV var would flip the exit+text
and the contact boolean. The IPv6 discriminator boundary (true->0 hits+fatal vs
false->hits+serves) is the keystone: a consumer that ignored ipv4_only (forced
StunNetwork::Any) would reach ::1 even with require_ipv4=true and FAIL exactly the
three refusal profiles (see the positive-control mutant).

Shutdown calibration (measured, inherited from the proven node_selector harness):
the alive rows settle --settle seconds after the port first accepts before
signalling. terminate() sends SIGTERM, then a second SIGTERM, then SIGKILL, and
records which was needed.

Normalization (scope limit): the alive observable is {http reachable, exit==0, no
SIGKILL} plus the deterministic STUN-contact boolean. The per-side shutdown timing
(sigterm_to_exit_s, second SIGTERM) and the EXACT STUN hit count (retry/refresh
dependent) legitimately differ between the Go and Rust event loops and are
RECORDED but NOT compared. The refusal observable is the exact exit code, stdout
bytes, and the zero-hit boolean. No comparison field is deleted; these are
explicit scope bounds. logging.level=error suppresses the Go INFO "found external
IP via STUN" discovery log (which Rust does not emit) so it cannot leak into the
compared stdout/stderr.

Out of scope (honest residual): real public NAT traversal / a routable external
IPv4, other platforms, and the IPv6-only-host LOCAL path (require_ipv4=true with
use_external_ip=false on a host with no local IPv4 -> "no IPv4 address available")
remain open. On this dual-stack host the local path prefers IPv4 for both
true/false, so the local profile here proves only that use_external_ip=false
bypasses STUN entirely (contact==none), not the IPv6-only local refusal. This
harness proves the STARTUP config-validation STUN-network-selection surface only.
Configs are emitted as YAML strings (no PyYAML dependency). Every original exit
code is preserved; no pipe filters the result.
"""
import argparse, hashlib, json, os, pathlib, signal, socket, struct, subprocess, tempfile, threading, time

p = argparse.ArgumentParser()
p.add_argument('--go', required=True, type=pathlib.Path)
p.add_argument('--rust', required=True, type=pathlib.Path)
p.add_argument('--output', required=True, type=pathlib.Path)
p.add_argument('--settle', type=float, default=2.0)
p.add_argument('--shutdown-window', type=float, default=45.0)
a = p.parse_args()
a.output = a.output.resolve()
a.output.mkdir(parents=True, exist_ok=False)

COVERS = ['YAML:rtc.require_ipv4', 'generated_flags:--rtc.require_ipv4']

FATAL = ('could not validate RTC config: could not resolve external IP: '
         'dial udp4: address ::1: no suitable address found\n')
MAPPED_IP = '192.0.2.1'
MAPPED_PORT = 9999
MAGIC = 0x2112A442

# kind: startup_error -> expect {exit, stdout}; alive -> serve HTTP + exit 0.
# stun: which fake responder family the config lists. yaml_req4: value written
# into YAML (None omits the key). override: (mode, value) applied via CLI/ENV with
# YAML carrying the OPPOSITE value. expect_contact: the deterministic STUN-contact
# boolean each side must exhibit (the load-bearing family discriminator).
PROFILES = [
    {'name': 'req4-true-ipv6-fatal', 'kind': 'startup_error', 'stun': 'ipv6',
     'yaml_req4': True, 'use_external': True,
     'expect': {'exit': 1, 'stdout': FATAL},
     'expect_contact': {'ipv6': False, 'ipv4': False}},
    {'name': 'req4-false-ipv6-serves', 'kind': 'alive', 'stun': 'ipv6',
     'yaml_req4': False, 'use_external': True,
     'expect_contact': {'ipv6': True, 'ipv4': False}},
    {'name': 'req4-true-ipv4-serves', 'kind': 'alive', 'stun': 'ipv4',
     'yaml_req4': True, 'use_external': True,
     'expect_contact': {'ipv6': False, 'ipv4': True}},
    {'name': 'req4-false-ipv4-serves', 'kind': 'alive', 'stun': 'ipv4',
     'yaml_req4': False, 'use_external': True,
     'expect_contact': {'ipv6': False, 'ipv4': True}},
    {'name': 'cli-true-overrides-false-yaml', 'kind': 'startup_error', 'stun': 'ipv6',
     'yaml_req4': False, 'override': ('cli', True), 'use_external': True,
     'expect': {'exit': 1, 'stdout': FATAL},
     'expect_contact': {'ipv6': False, 'ipv4': False}},
    {'name': 'cli-false-overrides-true-yaml', 'kind': 'alive', 'stun': 'ipv6',
     'yaml_req4': True, 'override': ('cli', False), 'use_external': True,
     'expect_contact': {'ipv6': True, 'ipv4': False}},
    {'name': 'env-true-overrides-false-yaml', 'kind': 'startup_error', 'stun': 'ipv6',
     'yaml_req4': False, 'override': ('env', True), 'use_external': True,
     'expect': {'exit': 1, 'stdout': FATAL},
     'expect_contact': {'ipv6': False, 'ipv4': False}},
    {'name': 'env-false-overrides-true-yaml', 'kind': 'alive', 'stun': 'ipv6',
     'yaml_req4': True, 'override': ('env', False), 'use_external': True,
     'expect_contact': {'ipv6': True, 'ipv4': False}},
    {'name': 'local-path-no-stun', 'kind': 'alive', 'stun': 'ipv6',
     'yaml_req4': True, 'use_external': False,
     'expect_contact': {'ipv6': False, 'ipv4': False}},
    {'name': 'default-unset-ipv6-serves', 'kind': 'alive', 'stun': 'ipv6',
     'yaml_req4': None, 'use_external': True,
     'expect_contact': {'ipv6': True, 'ipv4': False}},
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


def xor_mapped_response(tid):
    ip_int = struct.unpack('!I', socket.inet_aton(MAPPED_IP))[0]
    xip = ip_int ^ MAGIC
    xport = MAPPED_PORT ^ (MAGIC >> 16)
    attr_val = struct.pack('!BBHI', 0, 1, xport, xip)  # reserved, family=IPv4
    attr = struct.pack('!HH', 0x0020, len(attr_val)) + attr_val
    hdr = struct.pack('!HHI', 0x0101, len(attr), MAGIC) + tid
    return hdr + attr


class FakeStun(threading.Thread):
    """Loopback STUN binding responder. Counts Binding Requests it answers."""
    def __init__(self, host):
        super().__init__(daemon=True)
        self.sock = socket.socket(socket.AF_INET6 if ':' in host else socket.AF_INET,
                                  socket.SOCK_DGRAM)
        self.sock.bind((host, 0))
        self.port = self.sock.getsockname()[1]
        self.host = host
        self.hits = 0
        self._stop = False

    def run(self):
        self.sock.settimeout(0.2)
        while not self._stop:
            try:
                data, addr = self.sock.recvfrom(2048)
            except socket.timeout:
                continue
            except OSError:
                break
            if len(data) >= 20 and struct.unpack('!H', data[0:2])[0] == 0x0001:
                self.hits += 1
                tid = data[8:20]
                try:
                    self.sock.sendto(xor_mapped_response(tid), addr)
                except OSError:
                    pass

    def stop(self):
        self._stop = True
        self.sock.close()


def stun_addr(prof, stun6, stun4):
    if prof['stun'] == 'ipv6':
        return f'[::1]:{stun6.port}'
    return f'127.0.0.1:{stun4.port}'


def config_yaml(prof, http, iceudp, stun6, stun4):
    """YAML string base config; rtc.tcp_port=0 avoids the shared 7881 ICE port.
    node_ip is intentionally OMITTED so use_external_ip selects the STUN path and
    use_external_ip=false selects the local-discovery path (NodeIP.IsEmpty()).

    The ICE media bind is pinned to IPv4 loopback (enable_loopback_candidate +
    ips.includes 127.0.0.1/32), exactly like the sibling force-tcp/rtc-http-boundary
    launched-MAIN harnesses. Without this pin the UDP mux enumerates EVERY host
    interface address (interfaces.rs media_interfaces_from -> Go localInterfaces)
    and binds each one; a rotating macOS privacy global IPv6 can deprecate between
    the Go launch and the Rust launch, so the later side aborts on an unassignable
    address (Go createmux.go:58-84 and Rust udp.rs:55 BOTH return the bind error
    fatally — an environment artifact, not a divergence). The pin removes that
    host-address dependency. It does NOT touch the surface under test: require_ipv4
    is consumed in determine_ip -> external_ip -> stun_binding (config validation),
    which dials the configured STUN server directly and is independent of the ICE
    interface filter, so the STUN-family discriminator and contact boolean are
    unchanged."""
    lines = [f'port: {http}', 'bind_addresses: [127.0.0.1]',
             'keys:', '  devkey: secretsecretsecretsecretsecretsecret',
             'rtc:', '  tcp_port: 0', f'  udp_port: {iceudp}',
             f'  use_external_ip: {"true" if prof["use_external"] else "false"}',
             '  enable_loopback_candidate: true',
             '  ips:', '    includes:', '      - "127.0.0.1/32"']
    if prof['yaml_req4'] is not None:
        lines.append(f'  require_ipv4: {"true" if prof["yaml_req4"] else "false"}')
    lines.append('  stun_servers:')
    lines.append(f'    - "{stun_addr(prof, stun6, stun4)}"')
    lines.append('logging:')
    lines.append('  level: error')
    return '\n'.join(lines) + '\n'


def launch_args_env(binary, prof, cfg):
    args = [str(binary), '--config', str(cfg)]
    env = base_env()
    ov = prof.get('override')
    if ov:
        mode, value = ov
        if mode == 'cli':
            args.append(f'--rtc.require_ipv4={"true" if value else "false"}')
        elif mode == 'env':
            env['LIVEKIT_RTC_REQUIRE_IPV4'] = 'true' if value else 'false'
    return args, env


def wait_http(proc, port, timeout=20):
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        if proc.poll() is not None:
            return False
        try:
            s = socket.create_connection(('127.0.0.1', port), timeout=0.5)
            s.close()
            return True
        except OSError:
            time.sleep(0.05)
    return False


def terminate(proc, window):
    """SIGTERM, then a second SIGTERM, then SIGKILL. Records which was needed."""
    started = time.monotonic()
    proc.send_signal(signal.SIGTERM)
    second = False
    try:
        code = proc.wait(timeout=window)
    except subprocess.TimeoutExpired:
        proc.send_signal(signal.SIGTERM)
        second = True
        try:
            code = proc.wait(timeout=window)
        except subprocess.TimeoutExpired:
            os.killpg(proc.pid, signal.SIGKILL)
            code = proc.wait()
    return {'exit_code': code, 'sigterm_to_exit_s': round(time.monotonic() - started, 3),
            'sigkill_required': code == -signal.SIGKILL, 'second_sigterm_sent': second}


def run_refusal(binary, prof, work, stun6, stun4):
    http, iceudp = freeport(), freeport()
    cfg = work / 'c.yaml'
    cfg.write_text(config_yaml(prof, http, iceudp, stun6, stun4))
    args, env = launch_args_env(binary, prof, cfg)
    outpath, errpath = work / 'stdout.txt', work / 'stderr.txt'
    # A genuine refusal self-exits 1 in well under a second. Use Popen + a bounded
    # wait (not subprocess.run's hard timeout) so a server that STARTS instead of
    # refusing — the accept-but-ignore mutant — is recorded as a contract failure
    # rather than crashing the harness. Collect-all-then-assert needs every row.
    with outpath.open('wb') as out, errpath.open('wb') as err:
        proc = subprocess.Popen(args, stdout=out, stderr=err, env=env,
                                stdin=subprocess.DEVNULL, start_new_session=True)
        started = False
        try:
            rc = proc.wait(timeout=20)
        except subprocess.TimeoutExpired:
            started = True
            os.killpg(proc.pid, signal.SIGKILL)
            rc = proc.wait()
        finally:
            if proc.poll() is None:
                os.killpg(proc.pid, signal.SIGKILL)
                proc.wait()
    return {'config_sha256': sha(cfg), 'http_port': http, 'exit': rc,
            'started_instead_of_refusing': started,
            'stdout': outpath.read_text(errors='replace'),
            'stderr': errpath.read_text(errors='replace')}


def run_alive(binary, prof, work, stun6, stun4):
    http, iceudp = freeport(), freeport()
    cfg = work / 'c.yaml'
    cfg.write_text(config_yaml(prof, http, iceudp, stun6, stun4))
    args, env = launch_args_env(binary, prof, cfg)
    log = (work / 'server.log').open('wb')
    proc = subprocess.Popen(args, stdout=log, stderr=subprocess.STDOUT, env=env,
                            stdin=subprocess.DEVNULL, start_new_session=True)
    rec = {'config_sha256': sha(cfg), 'http_port': http, 'pid': proc.pid}
    try:
        reached = wait_http(proc, http)
        alive = proc.poll() is None
        if alive:
            time.sleep(a.settle)
            alive = proc.poll() is None
        details = terminate(proc, a.shutdown_window) if alive else {
            'exit_code': proc.returncode, 'sigterm_to_exit_s': None,
            'sigkill_required': False, 'second_sigterm_sent': False}
    finally:
        if proc.poll() is None:
            os.killpg(proc.pid, signal.SIGKILL)
            proc.wait()
        log.close()
    rec.update({'http_reachable': reached, 'survived_grace': alive, **details,
                'log': (work / 'server.log').read_text(errors='replace')})
    return rec


def alive_norm(rec):
    return {'http': bool(rec.get('http_reachable')),
            'exit0': rec.get('exit_code') == 0,
            'clean': not rec.get('sigkill_required', False)}


def alive_ok(rec):
    n = alive_norm(rec)
    return n['http'] and n['exit0'] and n['clean'] and rec.get('survived_grace', False)


manifest = {'status': 'INCOMPLETE',
            'scope': ('launched-MAIN rtc.require_ipv4 STARTUP config-validation surface: '
                      'mediatransportutil RTCConfig.Validate -> determineIP STUN network '
                      'selection (udp4 when require_ipv4 else udp) / lk-config netdetect '
                      'stun_binding StunNetwork::V4 vs ::Any. require_ipv4=true + IPv6-only '
                      'STUN fails with bare stdout "could not validate RTC config: could not '
                      'resolve external IP: dial udp4: address ::1: no suitable address found" '
                      '+ exit 1 + ZERO IPv6 responder hits; require_ipv4=false reaches ::1 and '
                      'serves. YAML/CLI/ENV projection (overrides carry the OPPOSITE YAML), '
                      'IPv4 STUN, use_external_ip=false local bypass, and default-unset. '
                      'NOT a public NAT traversal / routable-external-IPv4 claim, NOT other '
                      'platforms, NOT the IPv6-only-host local refusal (dual-stack host).'),
            'binaries': {}, 'profiles': {}, 'items': COVERS,
            'settle_s': a.settle, 'shutdown_window_s': a.shutdown_window,
            'mapped_ip': MAPPED_IP}
for side in ('go', 'rust'):
    b = getattr(a, side).resolve()
    manifest['binaries'][side] = {'path': str(b), 'sha256': sha(b)}


def save():
    (a.output / 'execution.json').write_text(json.dumps(manifest, indent=2, ensure_ascii=False) + '\n')


save()

stun6 = FakeStun('::1')
stun4 = FakeStun('127.0.0.1')
stun6.start()
stun4.start()
time.sleep(0.3)
manifest['fake_stun'] = {'ipv6': {'host': '::1', 'port': stun6.port},
                         'ipv4': {'host': '127.0.0.1', 'port': stun4.port}}
save()
print(f'fake STUN ::1 port={stun6.port}  127.0.0.1 port={stun4.port}', flush=True)

# Collect every profile on both sides first, then assert (no fail-fast masking).
try:
    for prof in PROFILES:
        name = prof['name']
        entry = {'profile': name, 'kind': prof['kind'], 'covers': COVERS,
                 'expect': prof.get('expect'), 'expect_contact': prof['expect_contact'],
                 'stun_family': prof['stun'], 'yaml_req4': prof['yaml_req4'],
                 'override': prof.get('override'), 'use_external': prof['use_external']}
        for side, binary in (('go', a.go), ('rust', a.rust)):
            h6_before, h4_before = stun6.hits, stun4.hits
            with tempfile.TemporaryDirectory(prefix=f'req4-{side}-') as d:
                work = pathlib.Path(d)
                if prof['kind'] == 'alive':
                    rec = run_alive(binary.resolve(), prof, work, stun6, stun4)
                else:
                    rec = run_refusal(binary.resolve(), prof, work, stun6, stun4)
            rec['ipv6_hits_delta'] = stun6.hits - h6_before
            rec['ipv4_hits_delta'] = stun4.hits - h4_before
            rec['contact'] = {'ipv6': rec['ipv6_hits_delta'] > 0,
                              'ipv4': rec['ipv4_hits_delta'] > 0}
            entry[side] = rec
        ec = prof['expect_contact']
        if prof['kind'] == 'alive':
            entry['go_contract_ok'] = alive_ok(entry['go']) and entry['go']['contact'] == ec
            entry['rust_contract_ok'] = alive_ok(entry['rust']) and entry['rust']['contact'] == ec
            entry['go_norm'] = alive_norm(entry['go'])
            entry['rust_norm'] = alive_norm(entry['rust'])
            entry['cross_side_equal'] = (entry['go_norm'] == entry['rust_norm']
                                         and entry['go']['contact'] == entry['rust']['contact'])
        else:
            exp = prof['expect']
            entry['go_contract_ok'] = (entry['go']['exit'] == exp['exit']
                                       and entry['go']['stdout'] == exp['stdout']
                                       and entry['go']['contact'] == ec)
            entry['rust_contract_ok'] = (entry['rust']['exit'] == exp['exit']
                                         and entry['rust']['stdout'] == exp['stdout']
                                         and entry['rust']['contact'] == ec)
            entry['cross_side_equal'] = (entry['go']['exit'] == entry['rust']['exit']
                                         and entry['go']['stdout'] == entry['rust']['stdout']
                                         and entry['go']['contact'] == entry['rust']['contact'])
        entry['match'] = entry['go_contract_ok'] and entry['rust_contract_ok'] and entry['cross_side_equal']
        manifest['profiles'][name] = entry
        save()
        print(f'{prof["kind"]:13s} {name}: match={entry["match"]} '
              f'go_ok={entry["go_contract_ok"]} rust_ok={entry["rust_contract_ok"]} '
              f'go_contact={entry["go"]["contact"]} rust_contact={entry["rust"]["contact"]}', flush=True)
finally:
    stun6.stop()
    stun4.stop()

P = manifest['profiles']


def refused(name, side, text):
    return P[name][side]['exit'] == 1 and P[name][side]['stdout'] == text


def serves(name, side):
    return alive_ok(P[name][side])


def contact(name, side):
    return P[name][side]['contact']


controls = {}
for side in ('go', 'rust'):
    controls[f'req4_true_ipv6_fatal_{side}'] = (
        refused('req4-true-ipv6-fatal', side, FATAL)
        and contact('req4-true-ipv6-fatal', side) == {'ipv6': False, 'ipv4': False})
    controls[f'req4_false_ipv6_serves_{side}'] = (
        serves('req4-false-ipv6-serves', side)
        and contact('req4-false-ipv6-serves', side)['ipv6'])
    controls[f'req4_true_ipv4_serves_{side}'] = (
        serves('req4-true-ipv4-serves', side)
        and contact('req4-true-ipv4-serves', side)['ipv4'])
    controls[f'req4_false_ipv4_serves_{side}'] = serves('req4-false-ipv4-serves', side)
    controls[f'cli_override_true_fatal_{side}'] = refused('cli-true-overrides-false-yaml', side, FATAL)
    controls[f'cli_override_false_serves_{side}'] = serves('cli-false-overrides-true-yaml', side)
    controls[f'env_override_true_fatal_{side}'] = refused('env-true-overrides-false-yaml', side, FATAL)
    controls[f'env_override_false_serves_{side}'] = serves('env-false-overrides-true-yaml', side)
    controls[f'local_path_bypasses_stun_{side}'] = (
        serves('local-path-no-stun', side)
        and contact('local-path-no-stun', side) == {'ipv6': False, 'ipv4': False})
    controls[f'default_unset_serves_{side}'] = (
        serves('default-unset-ipv6-serves', side)
        and contact('default-unset-ipv6-serves', side)['ipv6'])
    # The family boundary is load-bearing: true -> 0 IPv6 hits + fatal, false ->
    # IPv6 hits + serves. A consumer forcing StunNetwork::Any flips both.
    controls[f'ipv6_discriminator_specificity_{side}'] = (
        refused('req4-true-ipv6-fatal', side, FATAL)
        and not contact('req4-true-ipv6-fatal', side)['ipv6']
        and serves('req4-false-ipv6-serves', side)
        and contact('req4-false-ipv6-serves', side)['ipv6'])
    # CLI override precedence in BOTH directions over an opposite YAML value.
    controls[f'cli_override_precedence_{side}'] = (
        refused('cli-true-overrides-false-yaml', side, FATAL)
        and serves('cli-false-overrides-true-yaml', side))
    controls[f'env_override_precedence_{side}'] = (
        refused('env-true-overrides-false-yaml', side, FATAL)
        and serves('env-false-overrides-true-yaml', side))

failed_profiles = [n for n, e in P.items() if not e['match']]
failed_controls = [k for k, v in controls.items() if not v]
all_match = not failed_profiles and not failed_controls
manifest['cross_profile_controls'] = controls
manifest['failed_profiles'] = failed_profiles
manifest['failed_controls'] = failed_controls
manifest['case_count'] = len(PROFILES)
manifest['status'] = 'PASS_LAUNCHED_MAIN_REQUIRE_IPV4' if all_match else 'FAIL'
save()

batch = {'status': manifest['status'], 'case_count': len(PROFILES), 'items': COVERS,
         'profiles': {n: {'match': e['match'], 'kind': e['kind'],
                          'go_contract_ok': e['go_contract_ok'], 'rust_contract_ok': e['rust_contract_ok'],
                          'cross_side_equal': e['cross_side_equal'],
                          'go_contact': e['go']['contact'], 'rust_contact': e['rust']['contact']}
                      for n, e in P.items()},
         'cross_profile_controls': controls,
         'failed_profiles': failed_profiles, 'failed_controls': failed_controls,
         'binaries': manifest['binaries'],
         'input_identity_stable': manifest['binaries']['go']['sha256'] == '4bc2378e636740b29093061036249ffc9ebbef91f4ece1de12177c8dfee1e7dc',
         'harness_sha256': sha(pathlib.Path(__file__).resolve())}
(a.output / 'BATCH.json').write_text(json.dumps(batch, indent=2, ensure_ascii=False) + '\n')
print(json.dumps({'status': manifest['status'], 'case_count': len(PROFILES),
                  'failed_profiles': failed_profiles, 'failed_controls': failed_controls}))
raise SystemExit(0 if all_match else 1)