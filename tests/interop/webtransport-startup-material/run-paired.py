#!/usr/bin/env python3
"""SPEC-0077 C99 — launched-MAIN WebTransport TLS material paired Go/Rust differential.

Two ledger families are named: the `webtransport.tls_cert_file`/`tls_key_file`/`port`
leaves and the `CONFIG_ENABLED_CAPABILITY:webtransport.port` capability row, at the
launched tier (`component_behavior` + `main_server_consumer`) rather than the C98
in-process projection tier.

Contract under test (frozen Go oracle e5655ac, v1.13.10):
  * `pkg/service/webtransport.go:44-54` `WebTransportTLS(certFile, keyFile, dev)` —
    the file branch needs BOTH names (`if certFile != "" && keyFile != ""`); one name
    alone falls through to `errors.New("webtransport requires
    webtransport.tls_cert_file/tls_key_file")` outside dev.
  * `tls.LoadX509KeyPair` (`references/go-stdlib-go1.26.3/src/crypto/tls/tls.go:246-300`)
    reads the certificate first and returns the `os.ReadFile` error UNWRAPPED, so a
    missing path prints `open <path>: no such file or directory` and a directory prints
    `read <dir>: is a directory`; no PEM block in the certificate prints `tls: failed to
    find any PEM data in certificate input`, in the key `tls: failed to find any PEM data
    in key input`, and a cert/key mismatch `tls: private key does not match public key`.
  * the call sits in `pkg/service/server.go:184-195` (`InitializeServer`), i.e. before
    `router.RemoveDeadNodes()` (:197) and far before `Start()`'s `RegisterNode()` (:222)
    and the HTTP/WebTransport binds (:248-276). So a material failure must leave NO node
    record and NO listener, while the UDP socket only appears in `Start()` and its
    `webtransport listener started` line (webtransport.go:207) precedes the
    `starting LiveKit server` readiness line (server.go:309).
  * `main.go:190-192` prints the error with `fmt.Println(err)` on STDOUT and exits 1.

Rust mirrors: `livecloud::server::webtransport_tls` (crates/livecloud/src/server.rs:241-254)
called from `run()` at :659-661 during construction, and `livecloud::webtransport::bind`
(crates/livecloud/src/webtransport.rs:161-208) called with the other listeners at :848-855;
`lib.rs:190-196` prints the bare message on stdout and exits 1.

Observables, per profile, both sides independently then cross-compared:
  refusal rows — raw STDOUT bytes, raw STDERR bytes, exit code, HTTP accept, WebTransport
    UDP bindability, and (redis rows) the `nodes` hash contents;
  start rows   — readiness, the ordered WebTransport log lines, UDP occupancy while alive
    and release after exit, graceful exit code.

Every original exit code is preserved; no pipe filters a result. Shutdown settles past the
upstream `running.Swap(false)` window (see tests/interop/config-wiring-startup) so the
graceful exit code is comparable instead of racing the reference's own early-signal hang.
"""
import argparse
import hashlib
import json
import os
import pathlib
import shutil
import signal
import socket
import subprocess
import sys
import time

FIXED_GO_SHA256 = '4bc2378e636740b29093061036249ffc9ebbef91f4ece1de12177c8dfee1e7dc'
FIXED_GO_COMMIT = 'e5655ac20267fa5005dccaf3f0e3bfc3ea688c07'
COVERS = [
    'YAML:webtransport.tls_cert_file',
    'YAML:webtransport.tls_key_file',
    'YAML:webtransport.port',
    'CONFIG_ENABLED_CAPABILITY:webtransport.port',
    'generated_flags:--webtransport.tls_cert_file',
    'generated_flags:--webtransport.tls_key_file',
    'generated_flags:--webtransport.port',
]
MISSING_CERT = '/nonexistent/livekit-wt-cert.pem'
MISSING_KEY = '/nonexistent/livekit-wt-key.pem'
NOT_FOUND_TEXT = 'no such file or directory'
ZAP_LEVELS = ('DEBUG', 'INFO', 'WARN', 'ERROR', 'DPANIC', 'PANIC', 'FATAL')
# Raw stream files kept per run, copied to the receipt root as evidence.
OUT_NAMES = {'refusal': ('stdout.txt', 'stderr.txt'),
             'start': ('server.stdout', 'server.log')}
REDIS_PORT = None


def refuse_filed_slot(path):
    """Both offline modes rewrite execution.json and BATCH.json beside the receipt they read, so
    pointing them at a committed evidence slot would destroy that evidence instead of checking it.
    Filed slots are immutable: copy the receipt to a scratch directory and report the copy."""
    resolved = pathlib.Path(path).resolve()
    if '/server/evidence/' in str(resolved):
        raise SystemExit(json.dumps({
            'error': 'refusing_to_touch_filed_evidence', 'path': str(resolved),
            'why': 'the offline modes write their re-derivation next to the receipt they read',
            'use': 'cp the receipt to a scratch dir and point --report/--selftest-gate there'}))


p = argparse.ArgumentParser()
p.add_argument('--go', type=pathlib.Path, help='fixed Go oracle binary')
p.add_argument('--rust', type=pathlib.Path, help='Rust candidate binary')
p.add_argument('--output', type=pathlib.Path)
p.add_argument('--reference', type=pathlib.Path,
               default=pathlib.Path('/Users/zoujiaqing/projects/livekit-rs'))
p.add_argument('--settle', type=float, default=1.5)
p.add_argument('--shutdown-window', type=float, default=30.0)
p.add_argument('--skip-redis', action='store_true')
p.add_argument('--report', type=pathlib.Path, default=None,
               help='re-derive every verdict from a persisted execution.json; no daemon is started')
p.add_argument('--selftest-gate', type=pathlib.Path, default=None,
               help='re-derive the verdicts under named in-memory mutations and require each '
                    'mutation to redden the check it targets; nothing is launched')
a = p.parse_args()
OFFLINE = bool(a.report or a.selftest_gate)
if OFFLINE:
    src = (a.report or a.selftest_gate).resolve()
    refuse_filed_slot(src)
    if a.report:
        a.report = src
    else:
        a.selftest_gate = src
    a.output = src.parent
else:
    for name in ('go', 'rust', 'output'):
        if getattr(a, name) is None:
            p.error(f'--{name} is required unless --report/--selftest-gate is used')
    a.output = a.output.resolve()
    if '/server/evidence/' in str(a.output):
        # This harness writes execution.json and BATCH.json into --output, so an --output aimed at a
        # committed slot would overwrite filed evidence. Filed slots are immutable: write a scratch dir
        # and copy the receipt in as part of filing.
        raise SystemExit(json.dumps({'error': 'refusing_to_touch_filed_evidence',
                                     'path': str(a.output),
                                     'use': 'pass a scratch --output, then copy the receipt'}))
    a.output.mkdir(parents=True, exist_ok=False)


def sha(path):
    return hashlib.sha256(pathlib.Path(path).read_bytes()).hexdigest()


def free_port(family=socket.AF_INET, host='127.0.0.1'):
    with socket.socket(family, socket.SOCK_STREAM) as s:
        s.bind((host, 0))
        return s.getsockname()[1]


def free_udp_port(host='127.0.0.1'):
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
        s.bind((host, 0))
        return s.getsockname()[1]


# ---------------------------------------------------------------- fixtures
def make_fixtures(work):
    """Two distinct EC self-signed pairs plus the degenerate inputs.

    Generated with the host OpenSSL; the private keys stay inside this fresh
    output directory (removed with the receipt) and are loopback test material.
    """
    fx = work / 'fixtures'
    fx.mkdir()
    (fx / 'empty.pem').write_bytes(b'')
    (fx / 'junk.pem').write_bytes(bytes(range(256)) * 4)
    (fx / 'certdir').mkdir()
    log = {}
    for name in ('a', 'b'):
        cmd = ['openssl', 'req', '-x509', '-newkey', 'ec',
               '-pkeyopt', 'ec_paramgen_curve:P-256', '-nodes',
               '-keyout', str(fx / f'key{name}.pem'),
               '-out', str(fx / f'cert{name}.pem'),
               '-days', '2', '-subj', f'/CN=wt-{name}.localhost',
               '-addext', 'subjectAltName=IP:127.0.0.1,IP:127.0.0.2,DNS:localhost']
        r = subprocess.run(cmd, capture_output=True, timeout=120)
        if r.returncode != 0:
            raise SystemExit(f'fixture generation failed for {name}: '
                             f'{r.stderr.decode(errors="replace")}')
        log[name] = {'cert': str(fx / f'cert{name}.pem'),
                     'key': str(fx / f'key{name}.pem')}
    version = subprocess.run(['openssl', 'version'], capture_output=True,
                             timeout=30).stdout.decode().strip()
    return fx, log, version


# ---------------------------------------------------------------- profiles
# kind=refusal  -> must self-exit with the verbatim upstream message
# kind=start    -> must serve, log the ordered listener lines, stop cleanly
PROFILES = [
    {'name': 'wt-disabled-port0', 'kind': 'start', 'wt_port': 'zero'},
    {'name': 'wt-port-no-material', 'kind': 'refusal',
     'expect_stdout': 'webtransport requires webtransport.tls_cert_file/tls_key_file\n'},
    {'name': 'wt-cert-only-no-key', 'kind': 'refusal', 'material': 'cert-only',
     'expect_stdout': 'webtransport requires webtransport.tls_cert_file/tls_key_file\n'},
    {'name': 'wt-key-only-no-cert', 'kind': 'refusal', 'material': 'key-only',
     'expect_stdout': 'webtransport requires webtransport.tls_cert_file/tls_key_file\n'},
    {'name': 'wt-missing-cert-file', 'kind': 'refusal', 'material': 'missing-cert'},
    {'name': 'wt-missing-key-file', 'kind': 'refusal', 'material': 'missing-key'},
    {'name': 'wt-missing-both-files', 'kind': 'refusal', 'material': 'missing-both'},
    {'name': 'wt-empty-cert-file', 'kind': 'refusal', 'material': 'empty-cert',
     'expect_stdout': 'tls: failed to find any PEM data in certificate input\n'},
    {'name': 'wt-junk-key-file', 'kind': 'refusal', 'material': 'junk-key',
     'expect_stdout': 'tls: failed to find any PEM data in key input\n'},
    {'name': 'wt-cert-key-mismatch', 'kind': 'refusal', 'material': 'mismatch',
     'expect_stdout': 'tls: private key does not match public key\n'},
    {'name': 'wt-cert-is-directory', 'kind': 'refusal', 'material': 'dir-cert'},
    {'name': 'wt-valid-material', 'kind': 'start', 'material': 'valid'},
    {'name': 'wt-dev-selfsigned', 'kind': 'start', 'dev': True},
]
REDIS_PROFILES = [
    {'name': 'wt-refusal-keeps-node-unregistered', 'kind': 'refusal',
     'expect_stdout': 'webtransport requires webtransport.tls_cert_file/tls_key_file\n'},
    {'name': 'wt-valid-registers-node', 'kind': 'start', 'material': 'valid'},
]
# The offline modes re-derive a verdict from the receipt alone, where the dicts that drove the
# launch are gone; a profile name is the only one of their keys that survives into the record.
ALL_PROFILES = PROFILES + REDIS_PROFILES
PROFILE_BY_NAME = {pr['name']: pr for pr in ALL_PROFILES}
REDIS_NAMES = {pr['name'] for pr in REDIS_PROFILES}
# The two startup lines the WebTransport surface is expected to emit, used as an ordered pair.
WT_STARTED = 'webtransport listener started'
WT_STOPPED = 'webtransport listener stopped'


def expect_missing(profile, fx):
    """The unwrapped `os.ReadFile` rows, derived per profile.

    Both files existing-but-unreachable is the load-bearing pair: with only the
    certificate missing the message names the certificate, and with only the key
    missing it names the key, so the reader cannot be a single combined check.
    """
    if profile.get('material') in ('missing-cert', 'missing-both'):
        return f'open {MISSING_CERT}: {NOT_FOUND_TEXT}\n'
    if profile.get('material') == 'missing-key':
        return f'open {MISSING_KEY}: {NOT_FOUND_TEXT}\n'
    return None


def config_for(profile, fx, ports, redis_port):
    """Identical YAML for both sides; only the WebTransport surface varies."""
    wt = {'port': 0 if profile.get('wt_port') == 'zero' else ports['wt']}
    material = profile.get('material')
    if material in ('cert-only',):
        wt['tls_cert_file'] = f'{fx}/certa.pem'
    elif material == 'key-only':
        wt['tls_key_file'] = f'{fx}/keya.pem'
    elif material == 'missing-cert':
        wt['tls_cert_file'] = MISSING_CERT
        wt['tls_key_file'] = f'{fx}/keya.pem'
    elif material == 'missing-key':
        wt['tls_cert_file'] = f'{fx}/certa.pem'
        wt['tls_key_file'] = MISSING_KEY
    elif material == 'missing-both':
        wt['tls_cert_file'] = MISSING_CERT
        wt['tls_key_file'] = MISSING_KEY
    elif material == 'empty-cert':
        wt['tls_cert_file'] = f'{fx}/empty.pem'
        wt['tls_key_file'] = f'{fx}/keya.pem'
    elif material == 'junk-key':
        wt['tls_cert_file'] = f'{fx}/certa.pem'
        wt['tls_key_file'] = f'{fx}/junk.pem'
    elif material == 'mismatch':
        wt['tls_cert_file'] = f'{fx}/certa.pem'
        wt['tls_key_file'] = f'{fx}/keyb.pem'
    elif material == 'dir-cert':
        wt['tls_cert_file'] = f'{fx}/certdir'
        wt['tls_key_file'] = f'{fx}/keya.pem'
    elif material == 'valid':
        wt['tls_cert_file'] = f'{fx}/certa.pem'
        wt['tls_key_file'] = f'{fx}/keya.pem'
    lines = [
        f'port: {ports["http"]}',
        'bind_addresses: [127.0.0.1]',
        'keys:',
        '  ownedwt: fixture-secret-only-for-local-loopback-tests-32b',
        'rtc:',
        f'  udp_port: {ports["rtc_udp"]}',
        '  tcp_port: 0',
        '  node_ip: 127.0.0.1',
        '  use_external_ip: false',
        'logging:',
        '  level: info',
        'webtransport:',
        f'  port: {wt["port"]}',
    ]
    if 'tls_cert_file' in wt:
        lines.append(f'  tls_cert_file: {wt["tls_cert_file"]}')
    if 'tls_key_file' in wt:
        lines.append(f'  tls_key_file: {wt["tls_key_file"]}')
    if redis_port:
        lines.append('redis:')
        lines.append(f'  address: 127.0.0.1:{redis_port}')
    return '\n'.join(lines) + '\n'


def base_env():
    return {k: v for k, v in os.environ.items()
            if not k.startswith(('LIVEKIT_', 'LIVECLOUD_', 'OTEL_'))}


def udp_bindable(port, host='127.0.0.1'):
    """True when nothing holds this UDP endpoint (Go binds without SO_REUSEADDR)."""
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.bind((host, port))
        s.close()
        return True
    except OSError:
        return False


def http_accepts(port, timeout=0.5):
    try:
        with socket.create_connection(('127.0.0.1', port), timeout=timeout):
            return True
    except OSError:
        return False


def wait_ready(proc, port, timeout=25):
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        if proc.poll() is not None:
            return False
        if http_accepts(port):
            return True
        time.sleep(0.05)
    return False


def terminate(proc, window):
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


def run_refusal(binary, profile, work, cfg_text, wt_port):
    cfg = work / 'c.yaml'
    cfg.write_text(cfg_text)
    out, err = work / 'stdout.txt', work / 'stderr.txt'
    with out.open('wb') as o, err.open('wb') as e:
        proc = subprocess.Popen([str(binary), '--config', str(cfg)], stdout=o,
                                stderr=e, env=base_env(), stdin=subprocess.DEVNULL,
                                start_new_session=True)
        started_instead = False
        try:
            rc = proc.wait(timeout=25)
        except subprocess.TimeoutExpired:
            started_instead = True
            os.killpg(proc.pid, signal.SIGKILL)
            rc = proc.wait()
        finally:
            if proc.poll() is None:
                os.killpg(proc.pid, signal.SIGKILL)
                proc.wait()
    return {'config_sha256': sha(cfg), 'exit': rc,
            'self_exited': not started_instead,
            'stdout': out.read_text(errors='replace'),
            'stderr': err.read_text(errors='replace'),
            'http_accepts_after': http_accepts(profile['_http_port'], timeout=0.5),
            'wt_udp_free_after': udp_bindable(wt_port)}


def run_start(binary, profile, work, cfg_text, http_port, wt_port, dev, redis_port):
    """One launched daemon, with stdout and stderr kept as separate raw streams.

    The zap writer captures `os.Stderr` when the core is built
    (`references/protocol-stable/logger/logger.go:195`), while a fatal startup error
    is printed with `fmt.Println` on stdout (`cmd/server/main.go:190-192`) — so the
    listener ordering lives in stderr and the refusal text in stdout, and merging
    the two would hide exactly that split.
    """
    cfg = work / 'c.yaml'
    cfg.write_text(cfg_text)
    out_path, log_path = work / 'server.stdout', work / 'server.log'
    argv = [str(binary), '--config', str(cfg)]
    if dev:
        argv.insert(1, '--dev')
    with out_path.open('wb') as out, log_path.open('wb') as log:
        proc = subprocess.Popen(argv, stdout=out, stderr=log,
                                env=base_env(), stdin=subprocess.DEVNULL,
                                start_new_session=True)
        rec = {'config_sha256': sha(cfg), 'pid': proc.pid}
        try:
            rec['ready'] = wait_ready(proc, http_port)
            alive = proc.poll() is None
            if alive:
                time.sleep(a.settle)
                alive = proc.poll() is None
            rec['survived_grace'] = alive
            rec['wt_udp_occupied_while_alive'] = alive and not udp_bindable(wt_port)
            # RegisterNode happens inside Start(), so the node hash must be read
            # while the daemon is alive; reading it only after shutdown would
            # report the post-deregistration state on both sides and prove nothing.
            rec['nodes_while_alive'] = node_records() if redis_port else None
            details = terminate(proc, a.shutdown_window) if alive else {
                'exit_code': proc.returncode, 'sigterm_to_exit_s': None,
                'sigkill_required': False, 'second_sigterm_sent': False}
            rec.update(details)
        finally:
            if proc.poll() is None:
                os.killpg(proc.pid, signal.SIGKILL)
                proc.wait()
    text = log_path.read_text(errors='replace')
    rec['stdout'] = out_path.read_text(errors='replace')
    rec['log'] = text
    rec['messages'] = message_sequence(text)
    rec['listener_idx'] = text.find('webtransport listener started')
    rec['ready_idx'] = text.find('starting LiveKit server')
    rec['dev_warning_idx'] = text.find('webtransport using a generated self-signed cert')
    rec['listener_started'] = rec['listener_idx'] >= 0
    rec['listener_before_ready'] = (rec['listener_idx'] >= 0 and rec['ready_idx'] >= 0
                                    and rec['listener_idx'] < rec['ready_idx'])
    rec['dev_warning_emitted'] = rec['dev_warning_idx'] >= 0
    rec['dev_warning_before_listener'] = (rec['dev_warning_idx'] >= 0
                                         and rec['listener_idx'] >= 0
                                         and rec['dev_warning_idx'] < rec['listener_idx'])
    rec['wt_bind_failure_in_log'] = 'listen udp' in text and 'bind' in text
    rec['wt_udp_free_after_exit'] = udp_bindable(wt_port)
    rec['http_accepts_after_exit'] = http_accepts(http_port, timeout=0.5)
    if redis_port:
        # After the graceful exit the daemon must have deregistered itself.
        rec['nodes'] = node_records()
    return rec


def message_sequence(text):
    """Ordered zap message values, so stream identity survives timestamp/node-id noise.

    Both encoders have to be read. The JSON encoder writes `"msg":"…"`, but the server logs with the
    console encoder unless `logging.json` is set, whose lines are tab-separated
    `time\tLEVEL\tlogger\tservice/file.go:NNN\tmessage\t{json}` and never contain that literal.
    A reader that only matched the JSON field returned [] for every launched daemon here, and the
    WebTransport ordering control then compared two empty lists and reported agreement on nothing.
    """
    msgs = []
    for line in text.splitlines():
        marker = '"msg":"'
        start = line.find(marker)
        if start >= 0:
            end = line.find('"', start + len(marker))
            if end >= 0:
                msgs.append(line[start + len(marker):end])
            continue
        parts = line.split('\t')
        if len(parts) >= 5 and parts[1] in ZAP_LEVELS:
            msgs.append(parts[4])
    return msgs


def redis_command(port, *args):
    """Minimal RESP reader: byte-safe HVALS, no redis-cli escaping."""
    payload = b'*%d\r\n' % len(args) + b''.join(
        b'$%d\r\n%s\r\n' % (len(str(arg).encode()), str(arg).encode()) for arg in args)
    with socket.create_connection(('127.0.0.1', port), timeout=5) as sock:
        sock.sendall(payload)
        buf = b''
        while b'\r\n' not in buf:
            chunk = sock.recv(4096)
            if not chunk:
                break
            buf += chunk
        return parse_reply(sock, buf)


class _Reader:
    def __init__(self, sock):
        self.sock = sock
        self.buf = b''

    def line(self):
        while b'\r\n' not in self.buf:
            chunk = self.sock.recv(65536)
            if not chunk:
                raise EOFError
            self.buf += chunk
        head, self.buf = self.buf.split(b'\r\n', 1)
        return head

    def read_exact(self, n):
        while len(self.buf) < n + 2:
            chunk = self.sock.recv(65536)
            if not chunk:
                raise EOFError
            self.buf += chunk
        data, self.buf = self.buf[:n], self.buf[n + 2:]
        return data


def parse_reply(sock, initial):
    r = _Reader(sock)
    r.buf = initial

    def one():
        head = r.line()
        kind, rest = head[:1], head[1:]
        if kind == b'+':
            return rest.decode('utf-8', 'replace')
        if kind == b'-':
            raise RuntimeError(rest.decode('utf-8', 'replace'))
        if kind == b':':
            return int(rest)
        if kind == b'$':
            length = int(rest)
            return None if length == -1 else r.read_exact(length)
        if kind == b'*':
            count = int(rest)
            return [one() for _ in range(count)]
        raise RuntimeError(f'unexpected reply type {head!r}')

    return one()


def node_records():
    """`nodes` hash values, decoded far enough to read the node id and ip text."""
    port = REDIS_PORT
    try:
        values = redis_command(port, 'HVALS', 'nodes')
    except (OSError, RuntimeError, EOFError) as error:
        return {'error': f'{type(error).__name__}: {error}'}
    records = [decode_node(v) for v in values if v]
    return {'count': len(records), 'entries': records}


def decode_node(buf):
    """Walk the livekit.Node wire format (`livekit_internal.proto:49-57`).

    Only field 1 (id) and field 2 (ip) are read; other wire types are skipped so a
    later field cannot truncate the decode.
    """
    i, fields = 0, {}
    while i + 1 <= len(buf) - 1:
        key = buf[i]
        wire, field = key & 7, key >> 3
        i += 1
        if wire == 0:
            while i < len(buf) and buf[i] & 0x80:
                i += 1
            i += 1
            continue
        if wire == 1:
            i += 8
            continue
        if wire == 5:
            i += 4
            continue
        if wire != 2:
            break
        length = 0
        shift = 0
        while i < len(buf):
            b = buf[i]
            i += 1
            length |= (b & 0x7F) << shift
            if not b & 0x80:
                break
            shift += 7
        value = buf[i:i + length]
        i += length
        if field in (1, 2):
            try:
                fields[field] = value.decode()
            except UnicodeDecodeError:
                fields[field] = value.hex()
    return {'id': fields.get(1), 'ip': fields.get(2)}


def wt_pair_order(seq):
    """Whether a WebTransport message sequence names a listener start, and names its stop after it."""
    starts = [i for i, m in enumerate(seq) if m == WT_STARTED]
    stops = [i for i, m in enumerate(seq) if m == WT_STOPPED]
    return bool(starts) and (not stops or min(starts) < max(stops))


def derive_profile(entry, manifest):
    """Recompute one profile's verdict from the fields the receipt already carries.

    The launch loop and the offline modes call the same function, so a filed receipt cannot answer
    differently depending on which path read it. The start-row ordering fields are re-read out of the
    raw `log` instead of trusted from the record: that is what lets a re-derivation notice a receipt
    whose lines no longer say what its booleans claim.
    """
    name = entry['profile']
    prof = PROFILE_BY_NAME[name]
    use_redis = bool((manifest.get('redis') or {}).get('owned')) and name in REDIS_NAMES
    if entry['kind'] == 'refusal':
        expected = entry.get('expected_stdout')
        for side in ('go', 'rust'):
            rec = entry[side]
            rec['contract_ok'] = (
                rec['exit'] == 1 and rec['self_exited']
                and rec['stdout'] == expected
                and not rec['http_accepts_after'] and rec['wt_udp_free_after']
                and (not use_redis or (rec.get('nodes') or {}).get('count', -1) == 0))
        entry['cross_equal'] = (entry['go']['exit'] == entry['rust']['exit']
                                and entry['go']['stdout'] == entry['rust']['stdout']
                                and entry['go']['stderr'] == entry['rust']['stderr'])
    else:
        dev = bool(prof.get('dev'))
        disabled = prof.get('wt_port') == 'zero'
        for side in ('go', 'rust'):
            rec = entry[side]
            log = rec.get('log', '')
            rec['messages'] = message_sequence(log)
            rec['listener_idx'] = log.find(WT_STARTED)
            rec['ready_idx'] = log.find('starting LiveKit server')
            rec['dev_warning_idx'] = log.find('webtransport using a generated self-signed cert')
            rec['listener_started'] = rec['listener_idx'] >= 0
            rec['listener_before_ready'] = (rec['listener_idx'] >= 0 and rec['ready_idx'] >= 0
                                            and rec['listener_idx'] < rec['ready_idx'])
            rec['dev_warning_emitted'] = rec['dev_warning_idx'] >= 0
            rec['dev_warning_before_listener'] = (rec['dev_warning_idx'] >= 0
                                                 and rec['listener_idx'] >= 0
                                                 and rec['dev_warning_idx'] < rec['listener_idx'])
            core = (rec['ready'] and rec['survived_grace'] and rec['exit_code'] == 0
                    and not rec['sigkill_required'] and rec['wt_bind_failure_in_log'] is False
                    and rec['wt_udp_free_after_exit'] and not rec['http_accepts_after_exit'])
            if disabled:
                rec['contract_ok'] = core and not rec['listener_started']
            elif dev:
                rec['contract_ok'] = (core and rec['listener_started']
                                     and rec['listener_before_ready']
                                     and rec['dev_warning_emitted']
                                     and rec['dev_warning_before_listener']
                                     and rec['wt_udp_occupied_while_alive'])
            else:
                rec['contract_ok'] = (core and rec['listener_started']
                                      and rec['listener_before_ready']
                                      and rec['wt_udp_occupied_while_alive']
                                      and (not use_redis
                                           or ((rec.get('nodes_while_alive') or {}).get('count') == 1
                                               and (rec.get('nodes') or {}).get('count') == 0)))
        entry['cross_equal'] = (entry['go']['contract_ok'] == entry['rust']['contract_ok'])
        entry['expected_stdout'] = None
        entry['wt_message_subsequence'] = {
            s: [m for m in entry[s]['messages'] if 'webtransport' in m.lower()]
            for s in ('go', 'rust')}
        entry['wt_message_order_equal'] = (
            entry['wt_message_subsequence']['go'] == entry['wt_message_subsequence']['rust'])
    entry['match'] = entry['go']['contract_ok'] and entry['rust']['contract_ok']
    return entry


def write_receipt(manifest, out):
    tmp = out / 'execution.json.tmp'
    tmp.write_text(json.dumps(manifest, indent=2, ensure_ascii=False) + '\n')
    tmp.replace(out / 'execution.json')


def finish(manifest, out, live=False, write=True):
    """Assemble the lane verdict: per-profile derivations, cross-profile controls, identity gates,
    status, and both receipts.

    `write=False` is the gate sweep's mode: it re-derives an in-memory copy and must not touch the
    receipt it is auditing. `live` changes only what the harness-identity question can observe:
    during a run the file on disk is the one that just executed, so a difference means the script was
    edited mid-run; after a run the same difference means the re-derivation is being read with a newer
    script, which is provenance drift and has to be named rather than folded into a pass.
    """
    if write:
        refuse_filed_slot(out)
    P = manifest['profiles']
    for e in P.values():
        derive_profile(e, manifest)
    controls = {}
    for side in ('go', 'rust'):
        ok = lambda n: P[n][side]['contract_ok']
        controls[f'valid_material_starts_and_binds_{side}'] = ok('wt-valid-material')
        controls[f'disabled_port_starts_without_listener_{side}'] = ok('wt-disabled-port0')
        controls[f'both_names_gate_specificity_{side}'] = (
            ok('wt-cert-only-no-key') and ok('wt-key-only-no-cert')
            and ok('wt-port-no-material'))
        controls[f'cert_read_before_key_{side}'] = (
            P['wt-missing-both-files'][side]['stdout']
            == f'open {MISSING_CERT}: {NOT_FOUND_TEXT}\n'
            and P['wt-missing-key-file'][side]['stdout']
            == f'open {MISSING_KEY}: {NOT_FOUND_TEXT}\n')
        controls[f'refusal_leaves_no_listener_and_no_node_{side}'] = bool(
            [n for n in P if P[n]['kind'] == 'refusal']) and all(
            not P[n][side].get('http_accepts_after', True)
            and P[n][side].get('wt_udp_free_after', False)
            for n in P if P[n]['kind'] == 'refusal')
        controls[f'listener_line_precedes_readiness_{side}'] = ok('wt-valid-material') and (
            P['wt-valid-material'][side]['listener_before_ready'])
        controls[f'dev_warning_precedes_listener_{side}'] = ok('wt-dev-selfsigned')
        controls[f'pem_error_text_specificity_{side}'] = (
            P['wt-empty-cert-file'][side]['stdout']
            != P['wt-junk-key-file'][side]['stdout']
            and P['wt-junk-key-file'][side]['stdout']
            != P['wt-cert-key-mismatch'][side]['stdout'])
    # The rows that bound a listener are the only ones whose WebTransport message order can mean
    # anything: a receipt where none of them exists would make an equality over empty lists green.
    started = [e for e in P.values() if e['kind'] == 'start' and e['go']['listener_started']]
    manifest['webtransport_listener_rows'] = sorted(e['profile'] for e in started)
    manifest['webtransport_message_counts'] = {
        e['profile']: {s: len(e['wt_message_subsequence'][s]) for s in ('go', 'rust')}
        for e in P.values() if e['kind'] == 'start'}
    controls['webtransport_message_sequence_equal'] = bool(started) and all(
        e['wt_message_order_equal'] for e in P.values() if e['kind'] == 'start')
    controls['webtransport_listener_lines_ordered'] = bool(started) and all(
        wt_pair_order(e['wt_message_subsequence'][side]) for e in started
        for side in ('go', 'rust'))
    if (manifest.get('redis') or {}).get('owned'):
        for side in ('go', 'rust'):
            refused_nodes = P['wt-refusal-keeps-node-unregistered'][side].get('nodes') or {}
            valid = P['wt-valid-registers-node'][side]
            alive = valid.get('nodes_while_alive') or {}
            controls[f'registration_ordering_{side}'] = (
                refused_nodes.get('count') == 0
                and alive.get('count') == 1 and len(alive.get('entries') or []) == 1
                and alive['entries'][0].get('ip') == '127.0.0.1'
                and (valid.get('nodes') or {}).get('count') == 0)
    bins = manifest['binaries']
    now = {}
    for side in ('go', 'rust'):
        path = pathlib.Path(bins.get(side, {}).get('path', ''))
        now[side] = sha(path) if path.exists() else None
    manifest['binary_hash_now'] = now
    manifest['binaries_unchanged'] = all(
        now[side] is not None and now[side] == bins[side]['sha256'] for side in ('go', 'rust'))
    bins['go']['is_fixed_oracle'] = (now['go'] == FIXED_GO_SHA256)
    manifest['reference_commit_matches'] = (
        manifest['reference']['go_checkout_commit'] == FIXED_GO_COMMIT)
    harness_now = sha(pathlib.Path(__file__).resolve())
    canonical = manifest.get('harness_sha256') or manifest['harness_sha256_at_start']
    manifest['harness_sha256_now'] = harness_now
    controls['harness_unchanged_during_run'] = (
        harness_now == manifest['harness_sha256_at_start'] if live
        else canonical == manifest['harness_sha256_at_start'])
    controls['harness_unchanged_since_run'] = (canonical == harness_now)
    manifest['cross_profile_controls'] = controls
    manifest['failed_profiles'] = [n for n, e in P.items() if not e['match']]
    manifest['failed_controls'] = [k for k, v in controls.items() if not v]
    # The same shape lanes B and C publish, plus the since-run half, so one reader can ask the same
    # question of all three slots: which condition, if any, stopped holding?
    manifest['status_gates'] = {
        'binaries_unchanged': manifest['binaries_unchanged'],
        'reference_commit_matches': manifest['reference_commit_matches'],
        'go_is_fixed_oracle': bins['go']['is_fixed_oracle'],
        'no_failed_profiles': not manifest['failed_profiles'],
        'no_failed_controls': not manifest['failed_controls'],
        'harness_unchanged_during_run': controls['harness_unchanged_during_run'],
        'harness_unchanged_since_run': controls['harness_unchanged_since_run'],
    }
    all_ok = all(manifest['status_gates'].values())
    manifest['unmet_gates'] = [k for k, v in manifest['status_gates'].items() if not v]
    manifest['status'] = ('PASS_LAUNCHED_MAIN_WEBTRANSPORT_MATERIAL'
                          if all_ok else 'FAIL')
    # The canonical identity field names the harness that produced this run, not whichever file
    # happens to be on disk when the manifest closes; `harness_sha256_now` carries the latter and
    # the difference between the two is the drift the since-run gate reports.
    manifest['harness_sha256'] = (manifest['harness_sha256_at_start'] if live else canonical)
    if not write:
        # The gate sweep re-derives in-memory copies; writing them would replace the receipt it is
        # auditing with the last injection, which is how this mode first clobbered its own input.
        return {'all_ok': all_ok, 'summary': {
            'status': manifest['status'], 'cases': len(P), 'controls': len(controls),
            'failed_profiles': manifest['failed_profiles'],
            'failed_controls': manifest['failed_controls'],
            'unmet_gates': manifest['unmet_gates'],
        }}
    write_receipt(manifest, out)
    batch = {'status': manifest['status'], 'case_count': len(P),
             'profiles': {n: {'match': e['match'], 'kind': e['kind'],
                              'go_contract_ok': e['go']['contract_ok'],
                              'rust_contract_ok': e['rust']['contract_ok'],
                              'expected_stdout': e.get('expected_stdout'),
                              'wt_message_counts': {
                                  s: len(e.get('wt_message_subsequence', {}).get(s, []))
                                  for s in ('go', 'rust')} if e['kind'] == 'start' else None,
                              'go_stdout': e['go'].get('stdout'),
                              'rust_stdout': e['rust'].get('stdout')}
                          for n, e in P.items()},
             'controls': controls, 'failed_profiles': manifest['failed_profiles'],
             'failed_controls': manifest['failed_controls'],
             'status_gates': manifest['status_gates'],
             'unmet_gates': manifest['unmet_gates'],
             'binary_hash_now': manifest['binary_hash_now'],
             'harness_sha256_now': manifest['harness_sha256_now'],
             'binaries': manifest['binaries'],
             'reference': manifest['reference'],
             'harness_sha256': manifest['harness_sha256']}
    (out / 'BATCH.json').write_text(json.dumps(batch, indent=2, ensure_ascii=False) + '\n')
    return {'all_ok': all_ok, 'summary': {
        'status': manifest['status'], 'cases': len(P),
        'controls': len(controls),
        'failed_profiles': manifest['failed_profiles'],
        'failed_controls': manifest['failed_controls'],
        'unmet_gates': manifest['unmet_gates'],
        'harness_sha256_now': manifest['harness_sha256_now'],
        'binaries_unchanged': manifest['binaries_unchanged'],
    }}


# Each injection names one way a green lane A receipt could be wrong, and the check that must
# redden because of it. An injection that reddens nothing means the check it targets is decorative.
GATE_INJECTIONS = (
    ('listener_never_bound', 'FAILED_PROFILE', 'wt-valid-material', 'listener_started',
     'a start row whose listener line never appears did not bind anything'),
    ('listener_after_readiness', 'FAILED_PROFILE', 'wt-valid-material', 'log',
     'the WebTransport listener is documented to come up before the HTTP readiness line'),
    ('dev_warning_missing', 'FAILED_PROFILE', 'wt-dev-selfsigned', 'log',
     'the generated self-signed certificate has to be announced before it is used'),
    ('port_left_occupied', 'FAILED_PROFILE', 'wt-valid-material', 'wt_udp_occupied_while_alive',
     'if the UDP port was free while the daemon lived, no WebTransport socket was held'),
    ('port_leaked_after_exit', 'FAILED_PROFILE', 'wt-valid-material', 'wt_udp_free_after_exit',
     'a daemon that still holds the UDP port after exit never released it'),
    ('killed_not_exited', 'FAILED_PROFILE', 'wt-valid-material', 'sigkill_required',
     'a process that needed SIGKILL never demonstrated a graceful stop'),
    ('never_reaches_readiness', 'FAILED_PROFILE', 'wt-valid-material', 'ready',
     'a daemon that never became ready cannot attest listener ordering'),
    ('refusal_survives_the_wait', 'FAILED_PROFILE', 'wt-cert-only-no-key', 'self_exited',
     'a refusal the harness had to signal did not end itself'),
    ('refusal_prints_nothing', 'FAILED_PROFILE', 'wt-junk-key-file', 'stdout',
     'an empty refusal stream matches no needle and proves nothing was printed'),
    ('refusal_leaves_a_listener', 'FAILED_PROFILE', 'wt-cert-key-mismatch', 'http_accepts_after',
     'a startup refusal that still serves is not a refusal'),
    ('refusal_leaks_the_port', 'FAILED_CONTROL', 'refusal_leaves_no_listener_and_no_node_go',
     'wt_udp_free_after', 'a refused start must give the WebTransport port back'),
    ('pem_text_not_specific', 'FAILED_CONTROL', 'pem_error_text_specificity_rust',
     'wt-junk-key-file-stdout', 'two different bad inputs printing one message is one check, not two'),
    ('message_order_swapped', 'FAILED_CONTROL', 'webtransport_listener_lines_ordered', 'log',
     'a sequence that names the stop before the start is a different startup order'),
    ('registration_ordering_split', 'FAILED_CONTROL', 'registration_ordering_rust', 'entries',
     'the node hash has to be written by Start() and cleared by deregistration'),
    ('binary_identity_drifted', 'IDENTITY_DRIFT', 'binaries_unchanged', 'rust',
     'a receipt re-read after the candidate was rebuilt is provenance about an older build'),
    ('harness_identity_drifted', 'IDENTITY_DRIFT', 'harness_unchanged_since_run', 'harness',
     'a receipt re-read after the harness was edited describes an older lane'),
    ('oracle_moved_off_commit', 'IDENTITY_DRIFT', 'reference_commit_matches', 'reference',
     'the upstream sentences are quoted from one checkout, so a receipt from another cannot attest them'),
)


def selftest_gate(doc, out):
    """Re-derive a recorded run under named in-memory mutations and require each to change the
    verdict. Nothing is launched and nothing on disk is read except the harness's own hash."""
    shapes = {e['kind'] for e in doc['profiles'].values()}
    have_started = any(e['kind'] == 'start' and e['go']['listener_started']
                       for e in doc['profiles'].values())
    have_redis = bool((doc.get('redis') or {}).get('owned'))
    missing = [n for n in ('start', 'refusal') if n not in shapes]
    if not have_started:
        missing.append('a start row that bound a WebTransport listener')
    if have_redis:
        if 'wt-valid-registers-node' not in doc['profiles']:
            missing.append('the node-registration row')
    else:
        missing.append('an owned redis (run without --skip-redis)')
    if missing:
        raise SystemExit(json.dumps({'error': 'receipt_cannot_falsify_every_gate',
                                     'missing_shapes': missing,
                                     'why': 'run the full profile set before the gate sweep'}))

    def with_doc(mutate):
        d = json.loads(json.dumps(doc))
        mutate(d)
        return d

    def recompute(d):
        return finish(d, out, live=False, write=False)

    def inj_set(profile, side, key, value):
        def mutate(d):
            d['profiles'][profile][side][key] = value
        return mutate

    def inj_log_relocate(profile, sides, needle, where):
        """Move every line naming `needle` to one end of the log, keeping it present, so an ordering
        field changes without the message simply disappearing."""
        def mutate(d):
            for side in sides:
                rec = d['profiles'][profile][side]
                lines = rec['log'].splitlines()
                moved = [l for l in lines if needle in l]
                rest = [l for l in lines if needle not in l]
                rec['log'] = '\n'.join(moved + rest if where == 'first' else rest + moved)
        return mutate

    def inj_log_strip(profile, sides, needle):
        """Delete the lines, which is what a start that never bound would have logged."""
        def mutate(d):
            for side in sides:
                rec = d['profiles'][profile][side]
                rec['log'] = '\n'.join(l for l in rec['log'].splitlines() if needle not in l)
        return mutate

    def inj_pem_same(d):
        src = d['profiles']['wt-empty-cert-file']['rust']['stdout']
        d['profiles']['wt-junk-key-file']['rust']['stdout'] = src

    def inj_registration_same(d):
        d['profiles']['wt-valid-registers-node']['rust']['nodes_while_alive']['count'] = 0

    mutators = {
        'listener_never_bound': inj_log_strip('wt-valid-material', ('go', 'rust'), 'webtransport'),
        'listener_after_readiness': inj_log_relocate(
            'wt-valid-material', ('go', 'rust'), WT_STARTED, 'last'),
        'dev_warning_missing': inj_log_strip(
            'wt-dev-selfsigned', ('go', 'rust'), 'self-signed'),
        'port_left_occupied': inj_set('wt-valid-material', 'rust', 'wt_udp_occupied_while_alive', False),
        'port_leaked_after_exit': inj_set('wt-valid-material', 'rust', 'wt_udp_free_after_exit', False),
        'killed_not_exited': inj_set('wt-valid-material', 'rust', 'sigkill_required', True),
        'never_reaches_readiness': inj_set('wt-valid-material', 'rust', 'ready', False),
        'refusal_survives_the_wait': inj_set('wt-cert-only-no-key', 'rust', 'self_exited', False),
        'refusal_prints_nothing': inj_set('wt-junk-key-file', 'rust', 'stdout', ''),
        'refusal_leaves_a_listener': inj_set('wt-cert-key-mismatch', 'go', 'http_accepts_after', True),
        'refusal_leaks_the_port': inj_set('wt-cert-key-mismatch', 'go', 'wt_udp_free_after', False),
        'pem_text_not_specific': inj_pem_same,
        'message_order_swapped': inj_log_relocate('wt-valid-material', ('rust',), WT_STOPPED, 'first'),
        'registration_ordering_split': inj_registration_same,
        'binary_identity_drifted': lambda d: d['binaries']['rust'].__setitem__(
            'sha256', 'stale-hash-from-an-older-build'),
        'harness_identity_drifted': lambda d: d.__setitem__(
            'harness_sha256', 'stale-hash-from-an-older-harness'),
        'oracle_moved_off_commit': lambda d: d['reference'].__setitem__(
            'go_checkout_commit', '0' * 40),
    }
    rows = []
    receipt = out / 'execution.json'
    receipt_before = sha(receipt) if receipt.exists() else None
    base = recompute(json.loads(json.dumps(doc)))
    rows.append({'case': 'untouched_measurement', 'injected': 'the recorded run as filed',
                 'expect': 'clean', 'discriminated': base['all_ok'],
                 'detail': json.dumps(base['summary'])[:240]})
    for name, expect, target, field, why in GATE_INJECTIONS:
        result = recompute(with_doc(mutators[name]))
        if expect == 'IDENTITY_DRIFT':
            hit = target in result['summary']['unmet_gates']
        elif expect == 'FAILED_PROFILE':
            hit = target in result['summary']['failed_profiles']
        else:
            hit = target in result['summary']['failed_controls']
        rows.append({'case': name, 'injected': why, 'expect': f'{expect}:{target}',
                     'mutated_field': field, 'discriminated': hit,
                     'detail': json.dumps({'failed_profiles': result['summary']['failed_profiles'],
                                           'failed_controls': result['summary']['failed_controls'],
                                           'unmet_gates': result['summary']['unmet_gates']})[:240]})
    receipt_after = sha(receipt) if receipt.exists() else None
    rows.append({'case': 'sweep_writes_no_receipt',
                 'injected': 'the sweep must audit the copy in memory, not edit the evidence',
                 'expect': 'clean',
                 'discriminated': receipt_before is not None and receipt_before == receipt_after,
                 'detail': json.dumps({'before': (receipt_before or '')[:16],
                                       'after': (receipt_after or '')[:16]})})
    all_ok = all(r['discriminated'] for r in rows)
    (out / 'gate-falsification.json').write_text(
        json.dumps({'cases': rows, 'all_discriminated': all_ok,
                    'receipt_sha256_before': receipt_before,
                    'receipt_sha256_after': receipt_after,
                    'notes': 'recompute calls finish() with write=False on an in-memory copy of the '
                             'receipt, so this sweep launches nothing and changes no receipt'},
                   indent=1) + '\n', encoding='utf-8')
    print(json.dumps({'all_discriminated': all_ok,
                      'cases': {r['case']: r['discriminated'] for r in rows}}, indent=1))
    return 0 if all_ok else 1


def offline_main():
    src = a.report or a.selftest_gate
    doc = json.loads(src.read_text(encoding='utf-8'))
    if a.selftest_gate:
        return selftest_gate(doc, a.output)
    result = finish(doc, a.output, live=False)
    print(json.dumps(result['summary'], indent=1))
    return 0 if result['all_ok'] else 1


def main():
    if OFFLINE:
        return offline_main()
    work = a.output / 'work'
    work.mkdir()
    fx, fixture_log, openssl_version = make_fixtures(work)
    for child in fx.iterdir():
        if child.is_file():
            fixture_log[child.name] = {'sha256': sha(child), 'bytes': child.stat().st_size}

    redis_proc = None
    redis_port = None
    if not a.skip_redis:
        redis_port = free_port()
        redis_conf = work / 'redis.conf'
        redis_conf.write_text(f'port {redis_port}\nbind 127.0.0.1\nsave ""\n'
                              f'dir {work}\ndbfilename owned-c99.rdb\n'
                              f'logfile {work}/redis.log\n')
        redis_proc = subprocess.Popen(['redis-server', str(redis_conf)],
                                      stdout=subprocess.DEVNULL,
                                      stderr=subprocess.DEVNULL,
                                      start_new_session=True)
        deadline = time.monotonic() + 15
        while time.monotonic() < deadline:
            if subprocess.run(['redis-cli', '-p', str(redis_port), 'PING'],
                              capture_output=True).stdout.strip() == b'PONG':
                break
            time.sleep(0.1)
        else:
            raise SystemExit('owned redis never answered PING')
        global REDIS_PORT
        REDIS_PORT = redis_port

    manifest = {
        'status': 'INCOMPLETE',
        'scope': ('launched-MAIN WebTransport TLS material: the both-names gate, the '
                  'certificate-before-key read order, the unwrapped os.ReadFile error '
                  'text, the PEM/mismatch refusals, and — for the rows that do start — '
                  'the UDP listener occupancy, the ordered listener/readiness lines and '
                  'the node-registration ordering. Loopback only; no claim about a real '
                  'WebTransport session, a public CA chain or client certificate pinning.'),
        'binaries': {},
        'reference': {},
        'fixtures': fixture_log,
        'openssl_version': openssl_version,
        'redis': {'owned': bool(redis_port), 'port': redis_port},
        'harness_sha256_at_start': sha(pathlib.Path(__file__).resolve()),
        'driven_items': COVERS,
        'settle_s': a.settle,
        'profiles': {},
    }
    go = a.go.resolve()
    rust = a.rust.resolve()
    for side, binary in (('go', go), ('rust', rust)):
        manifest['binaries'][side] = {'path': str(binary), 'sha256': sha(binary)}
    manifest['binaries']['go']['is_fixed_oracle'] = (
        manifest['binaries']['go']['sha256'] == FIXED_GO_SHA256)
    ref = a.reference.resolve()
    manifest['reference'] = {
        'go_checkout': str(ref / 'references/livekit-stable'),
        'go_checkout_commit': subprocess.run(
            ['git', '-C', str(ref / 'references/livekit-stable'), 'rev-parse', 'HEAD'],
            capture_output=True, text=True).stdout.strip(),
        'rust_head': subprocess.run(['git', '-C', str(ref), 'rev-parse', 'HEAD'],
                                    capture_output=True, text=True).stdout.strip(),
        'rust_dirty_paths': subprocess.run(
            ['git', '-C', str(ref), 'status', '--short'],
            capture_output=True, text=True).stdout.splitlines(),
    }
    path = a.output / 'execution.json'

    def save():
        write_receipt(manifest, a.output)

    save()
    rows = list(PROFILES) + (REDIS_PROFILES if redis_port else [])

    def flush():
        if redis_port:
            redis_command(redis_port, 'FLUSHDB')

    for profile in rows:
        name = profile['name']
        entry = {'profile': name, 'kind': profile['kind'], 'covers': COVERS}
        use_redis = redis_port is not None and name in {r['name'] for r in REDIS_PROFILES}
        for side, binary in (('go', go), ('rust', rust)):
            if use_redis:
                flush()
            d = work / f'{name}-{side}'
            d.mkdir()
            ports = {'http': free_port(), 'rtc_udp': free_udp_port(),
                     'rtc_udp2': free_udp_port(),
                     'wt': free_udp_port() if profile.get('wt_port') != 'zero' else 0}
            if ports['wt'] == 0:
                ports['wt'] = free_udp_port()
            cfg_text = config_for(profile, fx, ports, redis_port if use_redis else None)
            profile['_http_port'] = ports['http']
            entry[side + '_ports'] = ports
            entry[side + '_config'] = cfg_text
            if profile['kind'] == 'refusal':
                entry[side] = run_refusal(binary, profile, d, cfg_text, ports['wt'])
                if use_redis:
                    entry[side]['nodes'] = node_records()
            else:
                entry[side] = run_start(binary, profile, d, cfg_text, ports['http'],
                                        ports['wt'], profile.get('dev', False),
                                        redis_port if use_redis else None)
            for stream in OUT_NAMES[profile['kind']]:
                src = d / stream
                if src.exists():
                    shutil.copyfile(src, a.output / f'{name}-{side}.{stream.split(".")[-1]}')
        if profile['kind'] == 'refusal':
            expected = profile.get('expect_stdout')
            if expected is None:
                expected = expect_missing(profile, fx)
            if profile.get('material') == 'dir-cert':
                # Derived from the Go side, recorded rather than assumed: the exact
                # op name Go's os.ReadFile reports for a directory differs per OS.
                expected = entry['go']['stdout']
            entry['expected_stdout'] = expected
        derive_profile(entry, manifest)
        (a.output / f'{name}.log').write_text(json.dumps(
            {k: v for k, v in entry.items() if k not in ('go', 'rust')}, indent=2) + '\n')
        manifest['profiles'][name] = entry
        save()
        print(f'{profile["kind"]:8s} {name}: match={entry["match"]} '
              f'go_ok={entry["go"]["contract_ok"]} rust_ok={entry["rust"]["contract_ok"]}',
              flush=True)

    result = finish(manifest, a.output, live=True)
    if redis_proc is not None:
        os.killpg(redis_proc.pid, signal.SIGTERM)
        try:
            redis_proc.wait(timeout=15)
        except subprocess.TimeoutExpired:
            os.killpg(redis_proc.pid, signal.SIGKILL)
            redis_proc.wait()
    for stale in (work / 'owned-c99.rdb', work / 'redis.log'):
        if stale.exists():
            stale.unlink()
    print(json.dumps(result['summary']))
    return 0 if result['all_ok'] else 1


if __name__ == '__main__':
    sys.exit(main())
