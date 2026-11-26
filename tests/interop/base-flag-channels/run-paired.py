#!/usr/bin/env python3
"""C102 lane: the nine remaining `base_flags:` channels, paired on launched daemons.

SPEC-0077 rows driven here (always through the *base* flag or its env spelling, never a
generated `--<path>` flag and never a YAML scalar of the same name):

    base_flags:--region          references/livekit-stable cmd/server/main.go:57-61, pkg/config/config.go:1117-1119
    base_flags:--node-ip         cmd/server/main.go:62-66, config.go:1132-1134 (conf.RTC.NodeIP.UnmarshalString)
    base_flags:--udp-port        cmd/server/main.go:67-71, config.go:1135-1137 (conf.RTC.UDPPort.UnmarshalString)
    base_flags:--redis-host      cmd/server/main.go:72-76, config.go:1120-1122 (conf.Redis.Address)
    base_flags:--redis-password  cmd/server/main.go:77-81, config.go:1123-1125 (conf.Redis.Password)
    base_flags:--turn-cert       cmd/server/main.go:82-86, config.go:1126-1128 -> pkg/service/turn.go:222-231
    base_flags:--turn-key        cmd/server/main.go:87-91, config.go:1129-1131 -> pkg/service/turn.go:225
    base_flags:--cpuprofile      cmd/server/main.go:267-279 (after ValidateKeys/LoadTURNSecrets)
    base_flags:--memprofile      cmd/server/main.go:282-294

Every upstream guard is `c.IsSet(name)`, so "passed an empty string" and "not passed" are
different inputs, and the Rust side mirrors it with `Option<String>`
(`crates/lk-config/src/cli.rs:92-133`, applied at `cli.rs:685-705`). The lanes' proofs are the
effects a *running* node produces, because none of these flags has a printable CLI projection:

  --region          the value is the Region column of the node row the daemon wrote to the store,
                    read back by the CLI of the other product too
  --node-ip         the IP column of that same row, plus the `using explicit node IP for
                    NAT1To1Ips` line the daemon logs
  --udp-port        the UDP sockets the daemon actually holds (`lsof -a -p PID -i`)
  --redis-host      which address the daemon connects to: the YAML names a port nobody listens on
  --redis-password  a store that refuses unauthenticated clients
  --turn-cert/key   a TLS handshake on turn.tls_port while the daemon is up, compared against the
                    SHA-256 of the fixture certificate
  --cpu/memprofile  the files the process created, with their stream type and size

Modes:
  --record        run every profile against both pinned binaries, then compare and write evidence.
  --report        re-derive the verdict from a recorded execution.json without launching anything.
  --selftest-gate apply one injected defect at a time to a recorded manifest and require the gate
                  to redden; writes gate-falsification.json.
  --only          development subset; it reports PARTIAL_SUBSET_NOT_ACCEPTANCE, never a pass.

Raw exit codes and raw stream bytes are kept before any masking: no pipe, no shell, and a refusal
must end its own process before the harness touches it, otherwise the row compares two empty
streams and agrees.
"""
from __future__ import annotations

import argparse
import collections
import hashlib
import json
import os
import re
import shlex
import signal
import socket
import ssl
import subprocess
import sys
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
REPO = HERE.parents[2]

BATCH = 'C102-20261127-base-flag-channels'
IP_SALT = 'c102-flags'
ORACLE_SHA256 = '4bc2378e636740b29093061036249ffc9ebbef91f4ece1de12177c8dfee1e7dc'
ACCEPTANCE_SHA256 = 'f6fe7f032536c10f075e3a540dff49111d0f2f7afb03e587f2b36be3081a21f0'
FIXED_GO_COMMIT = 'e5655ac20267fa5005dccaf3f0e3bfc3ea688c07'
REFERENCE = REPO / 'references/livekit-stable'
OFF_LIMITS = (7881, 7882)          # the developer host's own server; this lane never binds it
HOST_STORE_PORT = 6379             # and never connects to it either

# Fixture vocabulary. The API secret and the store password are written into configs and argv, so
# they must be substituted everywhere in the receipt; the loopback addresses are documentation-safe.
API_KEY = 'c102p'
API_SECRET = 'c102fixturesecret0123456789abcdef'
STORE_PASS = 'c102storepass'
WRONG_PASS = 'c102wrongpass'
REGION_A = 'c102regionA'
REGION_B = 'c102regionB'

BASE_ENV_KEYS = ('PATH', 'HOME', 'LANG', 'TMPDIR')
BASE_ENV = {k: os.environ[k] for k in BASE_ENV_KEYS if k in os.environ}


# -------------------------------------------------------------------------------------------- mask
def ip_tag(addr: str) -> str:
    """Keyed and letter-only: collapsing metric digits inside a table row must not merge two tags,
    the same address must tag the same way on both sides, and the address never reaches the file."""
    digest = hashlib.sha256((IP_SALT + addr).encode()).digest()
    return 'IP#' + ''.join(chr(65 + b % 26) for b in digest[:8])


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def sha256_file(path: Path) -> str:
    return sha256_bytes(Path(path).read_bytes())


IPV4 = re.compile(r'\b(?:\d{1,3}\.){3}\d{1,3}\b')
IPV6 = re.compile(r'\b(?:[0-9a-fA-F]{1,4}:){2,7}[0-9a-fA-F]{1,4}(?:%\w+)?\b|\bfe80::[0-9a-fA-F:%]*\b')
LOOPBACK = {'127.0.0.1', '::1', '0.0.0.0', '255.255.255.255'}
NODE_TOKEN = re.compile(r'\bND_[0-9A-Za-z]{6,16}\b')
TIME_TOKENS = (
    re.compile(r'\b\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:[+-]\d{4}|Z)?\b'),
    re.compile(r'\b\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2}\b'),
    re.compile(r'\b\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\b'),
)
MS = re.compile(r'\b\d+ms\b')
US = re.compile(r'\b\d+(?:\.\d+)?(?:ms|µs|us|ns)\b')


def forbidden_addresses(text: str) -> list:
    """Anything that looks like a real machine address, excluding the fixture vocabulary."""
    found = []
    for addr in set(IPV4.findall(text)) | set(IPV6.findall(text)):
        if addr in LOOPBACK or addr.startswith(('192.0.2.', '198.51.100.', '203.0.113.', '2001:db8:')):
            continue
        if re.fullmatch(r'0+(:0+)*', addr):
            continue
        found.append(addr)
    return sorted(found)


class Masker:
    """Role tokens for the ports this lane allocated, keyed tags for addresses it did not choose.

    The harness knows which port plays which role, so an allocated port becomes `<HTTP>` rather
    than a digit-collapsed number: that keeps "the daemon connected to the YAML address" and "the
    daemon connected to the flag address" distinguishable in the stored log line.
    """

    def __init__(self, port_roles: dict, path_aliases: dict | None = None):
        self.port_roles = dict(port_roles)
        self.path_aliases = dict(path_aliases or {})

    def paths(self, text: str) -> str:
        for real, alias in sorted(self.path_aliases.items(), key=lambda kv: -len(kv[0])):
            text = text.replace(real, alias)
        return text

    def ports(self, text: str) -> str:
        for port, role in sorted(self.port_roles.items(), key=lambda kv: -len(str(kv[0]))):
            text = text.replace(str(port), f'<{role}>')
        return text


    def secrets(self, text: str) -> str:
        text = text.replace(STORE_PASS, '<PASS>').replace(WRONG_PASS, '<WRONG_PASS>')
        text = text.replace(API_SECRET, '<SECRET>')
        return text

    def text(self, text: str, keep_node_ids: bool = True) -> str:
        text = self.paths(text)
        text = self.secrets(text)
        text = self.ports(text)
        for rx in TIME_TOKENS:
            text = rx.sub('<TS>', text)
        text = US.sub('<DUR>', text)
        spans = []
        if keep_node_ids:
            def hold(match):
                spans.append(match.group(0))
                return '\x00%d\x00' % (len(spans) - 1)
            text = NODE_TOKEN.sub(hold, text)

        def tag(match):
            addr = match.group(0)
            return addr if addr in LOOPBACK else f'<{ip_tag(addr)}>'
        text = IPV6.sub(tag, text)
        text = IPV4.sub(tag, text)
        if keep_node_ids:
            for index, value in enumerate(spans):
                text = text.replace(f'\x00{index}\x00', value)
        return text


# ------------------------------------------------------------------------------- ports/fixtures
def bind_free_port():
    sock = socket.socket()
    sock.bind(('127.0.0.1', 0))
    return sock, int(sock.getsockname()[1])


def alloc_port(used: set) -> int:
    for _ in range(64):
        sock, port = bind_free_port()
        if port in OFF_LIMITS or port == HOST_STORE_PORT or port in used:
            sock.close()
            continue
        sock.close()
        used.add(port)
        return port
    raise RuntimeError('no free port')


def listeners_on(port: int) -> int:
    out = subprocess.run(['lsof', '-nP', f'-iTCP:{port}', '-sTCP:LISTEN'],
                         capture_output=True, text=True)
    return len([l for l in out.stdout.splitlines()[1:] if l.strip()])


def process_sockets(pid: int) -> dict:
    """TCP listeners and UDP sockets held by one pid - the only honest --udp-port observation."""
    out = subprocess.run(['lsof', '-nP', '-a', '-p', str(pid), '-i'], capture_output=True, text=True)
    tcp, udp = set(), set()
    for line in out.stdout.splitlines()[1:]:
        fields = line.split()
        if len(fields) < 9:
            continue
        port = int(fields[8].rsplit(':', 1)[1])
        if '(LISTEN)' in line:
            tcp.add(port)
        elif fields[7] == 'UDP':
            udp.add(port)
    return {'tcp': sorted(tcp), 'udp': sorted(udp)}


def redis_cli(port, *args, password=None, timeout=15):
    cmd = ['redis-cli', '-p', str(port)]
    if password:
        cmd += ['-a', password, '--no-auth-warning']
    out = subprocess.run(cmd + [str(a) for a in args], capture_output=True, text=True,
                         timeout=timeout)
    return out.stdout.strip()


def start_store(port, password=None) -> subprocess.Popen:
    cmd = ['redis-server', '--port', str(port), '--bind', '127.0.0.1', '--save', '']
    if password:
        cmd += ['--requirepass', password]
    return subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                            start_new_session=True)


def stop_store(proc, port):
    try:
        os.killpg(proc.pid, signal.SIGTERM)
        proc.wait(timeout=8)
    except Exception:  # noqa: BLE001
        try:
            os.killpg(proc.pid, signal.SIGKILL)
        except Exception:  # noqa: BLE001
            pass
    return {'pid': proc.pid, 'port': port, 'listeners_after': listeners_on(port),
            'killed_by_harness': True}


def make_cert(work: Path) -> dict:
    """A throwaway loopback fixture: TURN/TLS refuses anything else at this port."""
    cert = work / 'turn-cert.pem'
    key = work / 'turn-key.pem'
    if not (cert.is_file() and key.is_file()):
        subprocess.run(['openssl', 'req', '-x509', '-newkey', 'rsa:2048', '-nodes',
                        '-keyout', str(key), '-out', str(cert), '-days', '2',
                        '-subj', '/CN=turn.c102.test'], capture_output=True, timeout=120,
                       check=True)
    other = work / 'turn-other-key.pem'
    if not other.is_file():
        subprocess.run(['openssl', 'req', '-x509', '-newkey', 'rsa:2048', '-nodes',
                        '-keyout', str(other), '-out', str(other) + '.pub.pem', '-days', '2',
                        '-subj', '/CN=other.c102.test'], capture_output=True, timeout=120,
                       check=True)
    fingerprint = sha256_file(cert)
    return {'cert': cert, 'key': key, 'other_key': other, 'cert_sha256': fingerprint}


def tls_cert_digest(port: int, timeout: float = 5.0) -> dict:
    """Presented leaf certificate, hashed. Proves the flag's file is the one being served."""
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        try:
            ctx = ssl._create_unverified_context()
            with socket.create_connection(('127.0.0.1', port), timeout=2) as sock:
                with ctx.wrap_socket(sock, server_hostname='localhost') as tls:
                    der = tls.getpeercert(True)
                    return {'connected': True, 'leaf_sha256_der': sha256_bytes(der),
                            'protocol': tls.version(), 'bytes': len(der)}
        except Exception as exc:  # noqa: BLE001
            last = f'{type(exc).__name__}: {exc}'
            time.sleep(0.2)
    return {'connected': False, 'error': (last or 'unknown')[:160]}


def pem_sha256(path: Path) -> str:
    """SHA-256 over the DER the PEM decodes to, so a Go and a Rust handshake are comparable."""
    from base64 import b64decode
    text = Path(path).read_text()
    body = ''.join(l for l in text.splitlines()
                   if 'BEGIN' not in l and 'END' not in l)
    return sha256_bytes(b64decode(body))


# ------------------------------------------------------------------------- the launched profile
def launch_pair(binary: Path, argv: list, env_extra: dict, cfg_text: str, work: Path, label: str,
                side: str, masker: Masker, ready_port: int | None, hold: float,
                refuse_wait: float | None, on_alive=None) -> dict:
    """One daemon launch, observed while it is alive, then shut down by this harness.

    `on_alive(proc)` runs between the readiness wait and the teardown, so anything the row claims
    about a *running* node - the registry row, the UDP socket set, the TLS handshake - is captured
    before the process is gone. A claim read after shutdown cannot tell the flag's effect from an
    empty store.
    """
    cfg_dir = work / 'configs'
    cfg_dir.mkdir(parents=True, exist_ok=True)
    cfg_path = cfg_dir / f'{side}-{label}.yaml'
    cfg_path.write_text(cfg_text, encoding='utf-8')
    log_path = work / 'logs' / f'{side}-{label}.log'
    log_path.parent.mkdir(parents=True, exist_ok=True)
    started = time.monotonic()
    with open(log_path, 'wb') as log:
        proc = subprocess.Popen([str(binary), '--config', str(cfg_path), *argv], stdout=log,
                                stderr=subprocess.STDOUT, stdin=subprocess.DEVNULL,
                                env={**BASE_ENV, **env_extra}, start_new_session=True)
        ready, alive_at_probe = False, False
        sockets = {'tcp': [], 'udp': []}
        observed = {}
        if ready_port is not None:
            while time.monotonic() < started + 25:
                try:
                    with socket.create_connection(('127.0.0.1', ready_port), timeout=0.4):
                        ready = True
                        break
                except OSError:
                    if proc.poll() is not None:
                        break
                    time.sleep(0.12)
            if ready:
                time.sleep(hold)
            alive_at_probe = proc.poll() is None
            if alive_at_probe:
                sockets = process_sockets(proc.pid)
                if on_alive is not None:
                    observed = on_alive(proc, cfg_path) or {}
        observed = {'ready': ready, 'alive_at_probe': alive_at_probe, 'sockets': sockets,
                    'ready_ms': int((time.monotonic() - started) * 1000), **observed}
        self_exited, exit_wait_ms = None, None
        if refuse_wait is not None:
            self_exited = proc.poll() is not None
            if not self_exited:
                try:
                    proc.wait(timeout=refuse_wait)
                    self_exited = True
                except subprocess.TimeoutExpired:
                    self_exited = False
            exit_wait_ms = int((time.monotonic() - started) * 1000)
        signals = []
        if proc.poll() is None:
            for sig in (signal.SIGINT, signal.SIGINT, signal.SIGKILL):
                os.killpg(proc.pid, sig)
                signals.append(signal.Signals(sig).name)
                try:
                    proc.wait(timeout=10)
                    break
                except subprocess.TimeoutExpired:
                    continue
        else:
            signals.append('already-exited')
        exit_code = proc.returncode
    text = log_path.read_text(errors='replace')
    data = log_path.read_bytes()
    raw = {'stdout_sha256': sha256_bytes(data), 'bytes': len(data),
           'lines': len(text.splitlines())}
    masked_log = masker.text(text)
    return {'argv_masked': [masker.text(a) for a in argv], 'argv_items': len(argv),

            'env_added_masked': {k: masker.text(v) for k, v in env_extra.items()},
            'exit': exit_code, 'signals': signals, 'self_exited': self_exited,
            'exit_wait_ms': exit_wait_ms, 'ready': ready, 'alive_at_probe': alive_at_probe,
            'sockets_masked': {'tcp': [masker.ports(str(p)) for p in sockets['tcp']],
                               'udp': [masker.ports(str(p)) for p in sockets['udp']]},
            'raw': raw, 'log_masked': masked_log, 'log_lines': len(text.splitlines()),
            'log_tail_masked': masked_log.splitlines()[-12:], 'cfg_masked': masker.text(cfg_text),
            'observed': observed}



# ------------------------------------------------------------------------------- config shapes
def cfg_body(http_port, store_port, rtc_extra='', turn=''):
    return (f'port: {http_port}\n'
            f'bind_addresses: [127.0.0.1]\n'
            f'redis:\n  address: 127.0.0.1:{store_port}\n'
            f'keys:\n  {API_KEY}: {API_SECRET}\n'
            f'rtc:\n  tcp_port: 0\n{rtc_extra}'
            f'{turn}')


def profiles(ctx) -> list:
    """One entry per argv/env shape. Ports appear in the claims as the role tokens the Masker
    produces, because every claim is evaluated against the masked record - the same text the receipt
    keeps and the same text `--report` re-derives from.
    """
    http, store, decoy, auth = (ctx['roles']['HTTP'], ctx['roles']['STORE'], ctx['roles']['DECOY'],
                                ctx['roles']['AUTH'])
    udp1, udp2, udp3, yra, yrb, tls = (ctx['roles']['UDP1'], ctx['roles']['UDP2'], ctx['roles']['UDP3'],
                                       ctx['roles']['YRA'], ctx['roles']['YRB'], ctx['roles']['TLS'])
    cert, key = ctx['cert_paths']
    absent = ctx['work'] / 'absent.pem'
    base = cfg_body(ctx['http_port'], ctx['store_port'])
    yaml_decoy = cfg_body(ctx['http_port'], ctx['decoy_port'])
    yaml_auth = cfg_body(ctx['http_port'], ctx['auth_port'])
    yaml_range = cfg_body(ctx['http_port'], ctx['store_port'],
                          rtc_extra=f'  udp_port: {ctx["yra_port"]}-{ctx["yrb_port"]}\n')
    no_keys = (f'port: {ctx["http_port"]}\nbind_addresses: [127.0.0.1]\n'
               f'redis:\n  address: 127.0.0.1:{ctx["store_port"]}\nrtc:\n  tcp_port: 0\n')
    turn_on = (f'turn:\n  enabled: true\n  domain: turn.c102.test\n  tls_port: {ctx["tls_port"]}\n'
               f'  relay_range_start: {ctx["relay_lo"]}\n  relay_range_end: {ctx["relay_hi"]}\n')
    turn_off = turn_on.replace('enabled: true', 'enabled: false')
    connect = lambda role: f'"addr": "127.0.0.1:{role}"'  # noqa: E731
    refused = lambda role: f'dial tcp 127.0.0.1:{role}: connect: connection refused'  # noqa: E731
    out = []

    def add(**kwargs):
        out.append(kwargs)

    def prof_files(side, pid):
        return profile_paths(ctx['profile_dir'], pid, side)

    # ---- base_flags:--region ----------------------------------------------------------------
    add(id='region-flag', rows=['base_flags:--region'], argv=['--region', REGION_A], cfg=base,
        ready='http', nodes=True, expect_exit=0,
        claims={'region_col': REGION_A, 'node_rows': 1, 'store_row_lifecycle': True,
                'log_all': [f'"region": "{REGION_A}"', connect(store)]},
        note='the value reaches both the startup line and the registry row the CLI reads back')
    add(id='region-env', rows=['base_flags:--region'], env={'LIVEKIT_REGION': REGION_B}, cfg=base,
        ready='http', nodes=True, expect_exit=0,
        claims={'region_col': REGION_B, 'node_rows': 1, 'log_all': [f'"region": "{REGION_B}"']},
        note='urfave/cli v3 binds Sources: cli.EnvVars("LIVEKIT_REGION") on the same flag '
             '(cmd/server/main.go:57-61), so the env spelling must reach the same column')
    add(id='region-flag-beats-env', rows=['base_flags:--region'], argv=['--region', REGION_A],
        env={'LIVEKIT_REGION': REGION_B}, cfg=base, ready='http', nodes=True, expect_exit=0,
        claims={'region_col': REGION_A, 'node_rows': 1},
        note='the argv channel is consulted before the env source '
             '(crates/lk-config/src/cli.rs:685-695)')
    add(id='region-unset', rows=['base_flags:--region'], cfg=base, ready='http', nodes=True,
        expect_exit=0, claims={'region_col': '', 'node_rows': 1,
                                'log_not': [f'"region": "{REGION_A}"']},
        note='the negative half of the pair: with nothing set the column stays empty and the startup '
             'line omits the field, so region-empty below cannot pass by accident')
    add(id='region-empty', rows=['base_flags:--region'], argv=['--region', ''], cfg=base,
        ready='http', nodes=True, expect_exit=0,
        claims={'region_col': '', 'node_rows': 1, 'log_not': [f'"region": "{REGION_B}"'],
                'differs_from': 'region-flag'},
        note='IsSet("") is true, so the flag still takes the channel; the column ends up empty by the '
             'same route on both sides and the row differs from region-flag above')

    # ---- base_flags:--node-ip ---------------------------------------------------------------
    add(id='node-ip-v4', rows=['base_flags:--node-ip'], argv=['--node-ip', '127.0.0.1'], cfg=base,
        ready='http', nodes=True, expect_exit=0,
        claims={'ip_col': '127.0.0.1', 'node_rows': 1, 'store_row_lifecycle': True,
                'log_all': ['using explicit node IP for NAT1To1Ips',
                            '"ip": {"V4":"127.0.0.1","V6":""}', '"nodeIP": "127.0.0.1"'],
                'log_not': ['ignoring --node-ip']},
        note='the flag feeds the rtcconfig NodeIP, which is what the registry row advertises')
    add(id='node-ip-v6', rows=['base_flags:--node-ip'], argv=['--node-ip', '::1'], cfg=base,
        ready='http', nodes=True, expect_exit=0,
        claims={'ip_col': '::1', 'node_rows': 1,
                'log_all': ['using explicit node IP for NAT1To1Ips',
                            '"ip": {"V4":"","V6":"::1"}', '"nodeIP": "::1"']},
        note='the v6 half of NodeIP.UnmarshalString')
    add(id='node-ip-env', rows=['base_flags:--node-ip'], env={'NODE_IP': '127.0.0.1'}, cfg=base,
        ready='http', nodes=True, expect_exit=0,
        claims={'ip_col': '127.0.0.1', 'node_rows': 1}, note='NODE_IP is the env spelling')
    add(id='node-ip-flag-beats-env', rows=['base_flags:--node-ip'], argv=['--node-ip', '::1'],
        env={'NODE_IP': '127.0.0.1'}, cfg=base, ready='http', nodes=True, expect_exit=0,
        claims={'ip_col': '::1', 'node_rows': 1}, note='argv first, then the env source')
    add(id='node-ip-garbage', rows=['base_flags:--node-ip'], argv=['--node-ip', 'not-an-ip'],
        cfg=base, ready='http', nodes=True, expect_exit=0,
        claims={'ip_cell_same': True, 'node_rows': 1, 'log_all': ['starting LiveKit server'],
                'log_not': ['using explicit node IP'], 'differs_from': 'node-ip-v4'},
        claims_rust={'log_all': ['ignoring --node-ip/NODE_IP value "not-an-ip"']},
        divergence='GO: config.go:1133 discards the UnmarshalString error and says nothing | RUST: '
                   'crates/lk-config/src/load.rs:180 pushes an "ignoring --node-ip/NODE_IP value" '
                   'warning that the oracle never prints; both end on the same auto-detected address',
        note='the parity claim is the resulting IP cell, not the wording')
    add(id='node-ip-empty', rows=['base_flags:--node-ip'], argv=['--node-ip', ''], cfg=base,
        ready='http', nodes=True, expect_exit=0,
        claims={'ip_cell_same': True, 'node_rows': 1,
                'log_not': ['using explicit node IP for NAT1To1Ips'],
                'differs_from': 'node-ip-v4'},
        note='IsSet("") reaches the channel but decodes to no IP, so this is auto-detection rather '
             'than a refusal, and it must not read the same as node-ip-v4')

    # ---- base_flags:--udp-port --------------------------------------------------------------
    add(id='udp-single', rows=['base_flags:--udp-port'], argv=['--udp-port', str(ctx['udp1_port'])],
        cfg=base, ready='http', expect_exit=0,
        claims={'udp_exact': [udp1], 'decoy_ports_unbound': [yra, yrb, udp2, udp3],
                'log_all': ['starting LiveKit server',
                            f'"rtc.portUDP": {{"Start":{udp1},"End":0}}'],
                'log_not': ['"rtc.portICERange"']},
        note='the named port is the only ICE UDP socket the daemon holds, and the startup line switches '
             'from the default range to the single port (server.go:288-298 prints rtc.portUDP only when '
             'RTC.UDPPort is valid); the range the losing channel would name stays unbound, which is the '
             'control the C100 base lane lacked')
    add(id='udp-env', rows=['base_flags:--udp-port'], env={'UDP_PORT': str(ctx['udp1_port'])},
        cfg=base, ready='http', expect_exit=0,
        claims={'udp_exact': [udp1], 'decoy_ports_unbound': [yra, yrb]},
        note='UDP_PORT is the env spelling')
    add(id='udp-range', rows=['base_flags:--udp-port'],
        argv=['--udp-port', f'{ctx["udp2_port"]}-{ctx["udp3_port"]}'], cfg=base, ready='http',
        expect_exit=0,
        claims={'udp_exact': [udp2, udp3], 'decoy_ports_unbound': [yra, yrb, udp1],
                'log_all': [f'"rtc.portUDP": {{"Start":{udp2},"End":{udp3}}}'],
                'log_not': ['"rtc.portICERange"']},
        note='a range binds every port in it and nothing outside it, and the startup line carries both '
             'ends of the range rather than the config default')
    add(id='udp-flag-merges-into-yaml-range', rows=['base_flags:--udp-port'],
        argv=['--udp-port', str(ctx['udp1_port'])], cfg=yaml_range, ready='http', expect_exit=0,
        claims={'udp_exact': [udp1, udp2, udp3, yra, yrb],
                'log_all': [f'"rtc.portUDP": {{"Start":{udp1},"End":{yrb}}}'],
                'log_not': ['"rtc.portICERange"']},
        note='measured, and it kills the clean-override reading of this flag: the single-port spelling '
             'only writes PortRange.Start, because PortRange.UnmarshalString returns after that '
             'assignment when the string has no "-" (mediatransportutil@v0.0.0-20260821083140-f234b534b095'
             '/pkg/rtcconfig/config.go). The YAML End survives, so the daemon binds flag_start..yaml_end '
             '- the five role ports allocated here are contiguous inside that range. Both runtimes '
             'render and bind identically, so the parity claim is the merge, not an override')
    add(id='udp-garbage', rows=['base_flags:--udp-port'], argv=['--udp-port', 'abc'], cfg=base,
        ready='http', expect_exit=0,
        claims={'udp_exact': [], 'decoy_ports_unbound': [yra, yrb, udp1, udp2, udp3],
                'log_all': ['"rtc.portICERange": [50000, 60000]'],
                'log_not': ['"rtc.portUDP"']},
        note='config.go:1135-1137 discards the UnmarshalString error, so the value is dropped rather '
             'than fatal: the startup line falls back to the default range and no ICE UDP socket is '
             'bound at startup on either side')

    # ---- base_flags:--redis-host ------------------------------------------------------------
    add(id='redis-host-flag-beats-yaml', rows=['base_flags:--redis-host'],
        argv=['--redis-host', f'127.0.0.1:{ctx["store_port"]}'], cfg=yaml_decoy, ready='http',
        nodes=True, expect_exit=0, cli_argv=['--redis-host', f'127.0.0.1:{ctx["store_port"]}'],
        nodes_bare=True,
        claims={'node_rows': 1, 'log_all': [connect(store)], 'log_not': [connect(decoy)],
                'store_row_lifecycle': True, 'cli_bare_refuses': [refused(decoy)]},
        note='the YAML address names a port nobody listens on, so a node that starts and registers can '
             'only have used the flag; `list-nodes` needs the same flag to read it back, which shows '
             'the value reaches the shared resolver rather than only the server start path')
    add(id='redis-host-env', rows=['base_flags:--redis-host'],
        env={'REDIS_HOST': f'127.0.0.1:{ctx["store_port"]}'}, cfg=yaml_decoy, ready='http',
        nodes=True, expect_exit=0, cli_env={'REDIS_HOST': f'127.0.0.1:{ctx["store_port"]}'},
        nodes_bare=True,
        claims={'node_rows': 1, 'log_all': [connect(store)], 'log_not': [connect(decoy)],
                'cli_bare_refuses': [refused(decoy)]},
        note='REDIS_HOST is the env spelling, and the CLI reads the registry through the same env '
             'channel while the config-only read still aims at the dead YAML port')
    add(id='redis-host-flag-broken', rows=['base_flags:--redis-host'],
        argv=['--redis-host', f'127.0.0.1:{ctx["decoy_port"]}'], cfg=base, ready=None,
        refuse_wait=25.0, expect_exit=1,
        claims={'log_all': [connect(decoy), refused(decoy), 'unable to connect to redis'],
                'log_not': [connect(store)], 'store_never_registers': True},
        claims_go={'log_all': ['failed to dial after 5 attempts', 'pool.go']},
        claims_rust={'log_not': ['failed to dial after 5 attempts', 'pool.go']},
        divergence='GO: the oracle prints four pool.go retry lines before the final sentence | '
                   'RUST: it prints only that final sentence',
        note='the YAML store must stay untouched: an empty registry is what proves the flag won '
             'instead of the daemon falling back')

    # ---- base_flags:--redis-password --------------------------------------------------------
    add(id='redis-password-absent', rows=['base_flags:--redis-password'], cfg=yaml_auth, ready=None,
        refuse_wait=15.0, expect_exit=1,
        claims={'log_all': [connect(auth), 'unable to connect to redis'],
                'store_never_registers': True},
        claims_go={'log_all': ['unable to connect to redis: NOAUTH Authentication required.']},
        claims_rust={'log_all': ['unable to connect to redis: NOAUTH HELLO must be called']},
        divergence='GO: "unable to connect to redis: NOAUTH Authentication required." | RUST: the '
                   'longer RESP3 NOAUTH sentence, because the Rust client negotiates HELLO first',
        note='a requirepass store with no password on any channel')
    add(id='redis-password-flag', rows=['base_flags:--redis-password'],
        argv=['--redis-password', STORE_PASS], cfg=yaml_auth, ready='http', nodes=True,
        expect_exit=0, cli_argv=['--redis-password', STORE_PASS], nodes_bare=True,
        claims={'node_rows': 1, 'log_all': [connect(auth)], 'store_row_lifecycle': True,
                'cli_bare_refuses': ['unable to connect to redis: NOAUTH Authentication required.']},
        note='the same store starts once the flag supplies the password, which is the positive control '
             'that makes the refusal above mean something; the config-only `list-nodes` read still '
             'refuses, so the table the flag unlocked is a registry read and not a shared default')
    add(id='redis-password-env', rows=['base_flags:--redis-password'],
        env={'REDIS_PASSWORD': STORE_PASS}, cfg=yaml_auth, ready='http', nodes=True, expect_exit=0,
        cli_env={'REDIS_PASSWORD': STORE_PASS}, nodes_bare=True,
        claims={'node_rows': 1, 'log_all': [connect(auth)], 'store_row_lifecycle': True,
                'cli_bare_refuses': ['unable to connect to redis: NOAUTH Authentication required.']},
        note='REDIS_PASSWORD is the env spelling, and the CLI reaches the store through the same env '
             'channel')
    add(id='redis-password-wrong', rows=['base_flags:--redis-password'],
        argv=['--redis-password', WRONG_PASS], cfg=yaml_auth, ready=None, refuse_wait=15.0,
        expect_exit=1,
        claims={'log_all': ['unable to connect to redis: WRONGPASS invalid username-password pair'],
                'store_never_registers': True},
        note='the identical upstream sentence on both sides, and the store still holds no node')
    add(id='redis-password-on-open-store', rows=['base_flags:--redis-password'],
        argv=['--redis-password', STORE_PASS], cfg=base, ready='http', nodes=True, expect_exit=0,
        claims={'node_rows': 1, 'log_all': [connect(store)], 'store_row_lifecycle': True},
        note='a password against an open store is accepted, so the flag must not turn a working '
             'configuration into a refusal')
    add(id='redis-password-empty', rows=['base_flags:--redis-password'],
        argv=['--redis-password', ''], cfg=yaml_auth, ready=None, refuse_wait=15.0, expect_exit=1,
        claims={'log_all': ['unable to connect to redis', connect(auth)],
                'log_any': ['NOAUTH', 'Authentication required'], 'store_never_registers': True},
        note='IsSet("") puts an empty password on the channel and the store rejects it, so this row '
             'carries the empty-vs-unset guard on a channel that can be driven safely; the same row '
             'is deliberately not run for --redis-host, whose empty value resolves to the developer '
             'host store (see README)')

    # ---- base_flags:--turn-cert / --turn-key ------------------------------------------------
    turn_cfg = cfg_body(ctx['http_port'], ctx['store_port'], turn=turn_on)
    add(id='turn-cert-key-flags', rows=['base_flags:--turn-cert', 'base_flags:--turn-key'],
        argv=['--turn-cert', str(cert), '--turn-key', str(key)], cfg=turn_cfg, ready='http',
        tls_probe=True, expect_exit=0,
        claims={'tls_listener': True, 'tls_matches_cert': True, 'log_all': ['Starting TURN server'],
                'log_not': ['TURN tls cert required', 'could not configure TURN/TLS'],
                'store_key_lifecycle': True,
                'decoy_ports_unbound': [yra, yrb, udp1]},
        claims_go={'log_all': [f'"turn.portTLS": {tls}']},
        claims_rust={'log_all': [f'"tls_port": "Some({tls})"']},
        note='the handshake on turn.tls_port presents the fixture certificate named only by the two '
             'flags, and its DER digest equals the file the flag opened')
    add(id='turn-cert-key-flags-when-disabled', rows=['base_flags:--turn-cert',
                                                      'base_flags:--turn-key'],
        argv=['--turn-cert', str(cert), '--turn-key', str(key)],
        cfg=cfg_body(ctx['http_port'], ctx['store_port'], turn=turn_off), ready='http',
        tls_probe=True, expect_exit=0,
        claims={'tls_listener': False, 'log_not': ['Starting TURN server'],
                'decoy_ports_unbound': [tls, yra, yrb, udp1]},
        note='real material plus turn.enabled false must produce no listener: the flag channel cannot '
             'switch the service on by itself')

    def turn_fail(pid, argv, needles_go, needles_rust, shared, note):
        add(id=pid, rows=['base_flags:--turn-cert', 'base_flags:--turn-key'], argv=argv,
            cfg=turn_cfg, ready=None, refuse_wait=15.0, expect_exit=1, tls_probe=True,
            claims=dict(shared, store_never_registers=True),
            claims_go={'log_all': needles_go}, claims_rust={'log_all': needles_rust},
            divergence='GO: refuses under the upstream "TURN tls cert required" prefix | RUST: '
                       'refuses under "could not configure TURN/TLS"; both exit 1 on the same class of '
                       'TLS material problem. Store ordering: GO leaves the store untouched because the '
                       'TURN server is built while the wire graph is constructed '
                       '(references/livekit-stable/pkg/service/wire.go:300 -> turn.go:222-228), which '
                       'runs before RedisStore.Start() (pkg/service/redisstore.go:93-113) | RUST starts '
                       'the store graph first (crates/livecloud/src/server.rs:604-624) and calls '
                       'turn::start after (:650), so the livekit_version marker survives the refusal',
            note=note)

    turn_fail('turn-cert-without-key', ['--turn-cert', str(cert)],
              ['TURN tls cert required'],
              ['could not configure TURN/TLS: turn.key_file is required'],
              {'log_all': [connect(store)], 'log_not': ['Starting TURN server']},
              'a certificate with no key is fatal once turn.tls_port is configured; Go reaches it '
              'through tls.LoadX509KeyPair("", "") and Rust through crates/livecloud/src/turn.rs:30')
    turn_fail('turn-swapped-pair', ['--turn-cert', str(key), '--turn-key', str(cert)],
              ['TURN tls cert required: tls: failed to find certificate PEM data in certificate '
               'input, but did find a private key'],
              ['could not configure TURN/TLS: invalid TLS PEM: certificate chain is empty'],
              {'log_not': ['Starting TURN server']},
              'a swapped pair refuses with a PEM-shaped cause on both sides, and neither side borrows '
              'the other sentence')
    turn_fail('turn-missing-file', ['--turn-cert', str(absent), '--turn-key', str(key)],
              ['TURN tls cert required', 'no such file or directory'],
              ['could not configure TURN/TLS', 'No such file or directory'],
              {'log_not': ['Starting TURN server']},
              'a configured certificate that does not exist is fatal, and the refusal names the '
              'missing path (masked to the lane work dir)')

    add(id='turn-key-without-cert', rows=['base_flags:--turn-key', 'base_flags:--turn-cert'],
        argv=['--turn-key', str(key)], cfg=turn_cfg, ready=None, refuse_wait=15.0, expect_exit=1,
        claims={'log_all': [connect(store)], 'log_not': ['Starting TURN server'],
                'store_never_registers': True},
        claims_go={'log_all': ['TURN tls cert required', 'no such file or directory']},
        claims_rust={'log_all': ['could not configure TURN/TLS: turn.cert_file is required']},
        divergence='GO: LoadX509KeyPair("", "") reports a missing file | RUST: the explicit check at '
                   'crates/livecloud/src/turn.rs:27-29 names turn.cert_file',
        note='the mirror image of turn-cert-without-key: a key with no certificate is fatal too')

    # ---- base_flags:--cpuprofile / --memprofile ---------------------------------------------
    for pid, rows, want, note in (
        ('profiles-both', ['base_flags:--cpuprofile', 'base_flags:--memprofile'],
         {'cpu': True, 'heap': True},
         'main.go:267-294 opens both files only after ValidateKeys and writes them on the way out; '
         'the sizes are runtime-specific, the stream type and the fact that both exist are not'),
        ('profiles-cpu-only', ['base_flags:--cpuprofile'], {'cpu': True},
         'memprofile is not implied by cpuprofile'),
        ('profiles-mem-only', ['base_flags:--memprofile'], {'heap': True},
         'cpuprofile is not implied by memprofile'),
    ):
        def make_argv(want=want):
            def fn(side, pid):
                paths = prof_files(side, pid)
                argv = []
                if want.get('cpu'):
                    argv += ['--cpuprofile', str(paths['cpu'])]
                if want.get('heap'):
                    argv += ['--memprofile', str(paths['heap'])]
                return argv
            return fn

        claims = {'cpu_file': bool(want.get('cpu')), 'heap_file': bool(want.get('heap'))}
        if want.get('cpu'):
            claims['cpu_gzip'] = True
        if want.get('heap'):
            claims['heap_gzip'] = True
        add(id=pid, rows=rows, argv_fn=make_argv(), cfg=base, ready='http', files=True,
            expect_exit=0, claims=dict(claims, ready=True), note=note)

    add(id='profiles-mem-after-refusal', rows=['base_flags:--memprofile'],
        argv_fn=lambda side, pid: ['--memprofile', str(prof_files(side, pid)['heap'])], cfg=no_keys,
        ready=None,
        refuse_wait=15.0, expect_exit=1, files=True,
        claims={'cpu_file': False, 'heap_file': False,
                'log_all': ['one of key-file or keys must be provided']},
        note='the heap profile is opened after the keys check too, so a refused start leaves neither '
             'file')

    add(id='profiles-empty-values', rows=['base_flags:--cpuprofile', 'base_flags:--memprofile'],
        argv=['--cpuprofile=', '--memprofile='], cfg=base, ready='http', files=True, expect_exit=0,
        claims={'cpu_file': False, 'heap_file': False, 'ready': True},
        note='an empty value is not a path: no file may appear and the daemon must still come up, so '
             'the row keeps the file probes and the readiness probe together')
    add(id='profiles-after-refusal', rows=['base_flags:--cpuprofile'],
        argv_fn=lambda side, pid: ['--cpuprofile', str(prof_files(side, pid)['cpu'])], cfg=no_keys,
        ready=None,
        refuse_wait=15.0, expect_exit=1, files=True,
        claims={'cpu_file': False, 'heap_file': False,
                'log_all': ['one of key-file or keys must be provided']},
        note='the keys check runs before the profile is created (main.go:257-267), so a refused start '
             'leaves no file behind - an ordering claim, measured on both binaries')
    return out


COVERS = ['base_flags:--region', 'base_flags:--node-ip', 'base_flags:--udp-port',
          'base_flags:--redis-host', 'base_flags:--redis-password', 'base_flags:--turn-cert',
          'base_flags:--turn-key', 'base_flags:--cpuprofile', 'base_flags:--memprofile']


# ------------------------------------------------------------------------------- observations
# The 14 primary column labels `list-nodes` prints, i.e. the text before each `\n` in the header cells
# declared by references/livekit-stable/cmd/server/commands.go (table.Header) and crates/livecloud/src/
# cli_nodes.rs (const HEADERS: [&str; 14]). Both binaries render the secondary labels on the following
# physical line, which is why a header row is read from the first box line only.
TABLE_HEADER = ['ID', 'IP Address', 'Region', 'CPUs', 'CPU Usage', 'Memory Used/Total', 'Rooms',
                'Clients', 'Bytes/s In/Out', 'Packets/s In/Out', 'System Dropped Pkts/s', 'Nack/s',
                'Retrans/s', 'Started At']
TABLE_WIDTH = len(TABLE_HEADER)


def parse_table(text: str) -> dict:
    """Read the box table `list-nodes` prints. Column names come from the header, never from a
    hardcoded index, so a column moving reddens the row instead of silently reading the wrong cell.
    """
    lines = [l for l in text.splitlines() if l.strip().startswith('│')]
    if not lines:
        return {'tables': 0, 'rows': 0, 'header': [], 'cols': 0, 'region': None, 'ip': None}
    header = [c.strip() for c in lines[0].strip('│').split('│')]
    out = {'tables': 1, 'header': header, 'cols': len(header), 'rows': 0, 'region': None,
           'ip': None}
    region_col = [i for i, h in enumerate(header) if h.upper() == 'REGION']
    ip_col = [i for i, h in enumerate(header) if 'IP' in h.upper()]
    if not region_col or not ip_col:
        out['header_missing_column'] = True
        return out
    data = [l for l in lines[1:] if 'ND_' in l]
    out['rows'] = len(data)
    if data:
        cells = [c.strip() for c in data[0].strip('│').split('│')]
        out['cells'] = len(cells)
        out['region'] = cells[region_col[0]] if len(cells) > region_col[0] else None
        out['ip'] = cells[ip_col[0]] if len(cells) > ip_col[0] else None
        out['ids'] = sorted({m for line in data for m in re.findall(r'ND_[0-9A-Za-z]+', line)})
    return out


def profile_paths(profile_dir: Path, pid: str, side: str) -> dict:
    """The one place profile file names are built. `profiles()` writes the flag values from this and
    `run_entry()` probes with it, so a row cannot pass by probing a path it never asked for."""
    out = Path(profile_dir) / pid
    return {'cpu': out / f'{side}-cpu.out', 'heap': out / f'{side}-heap.out'}


def cli_read(binary: Path, cfg_path: Path, label: str, work: Path, extra_argv=(),
             extra_env=None) -> dict:
    """`list-nodes` resolves the store through the same root-flag channels as the server, which was
    measured rather than assumed: with `--redis-host`/`REDIS_HOST` or `--redis-password`/
    `REDIS_PASSWORD` on the subcommand both binaries print the table, and with only the config file
    both refuse. Passing the channel here is therefore the check that the flag reaches the shared
    resolver, not just the server start path.
    """
    log = work / 'logs' / f'cli-{label}.log'
    argv = [str(binary), 'list-nodes', '--config', str(cfg_path), *extra_argv]
    with open(log, 'wb') as handle:
        proc = subprocess.run(argv, stdout=handle, stderr=subprocess.STDOUT, stdin=subprocess.DEVNULL,
                              env={**BASE_ENV, **(extra_env or {})}, timeout=40)
    return {'exit': proc.returncode, 'text': log.read_text(errors='replace'),
            'argv_items': len(argv)}


def file_probe(path: Path) -> dict:
    path = Path(path)
    if not path.is_file():
        return {'exists': False}
    data = path.read_bytes()
    out = {'exists': True, 'bytes': len(data), 'magic': data[:4].hex(),
           'gzip_magic': data[:2] == b'\x1f\x8b'}
    if out['gzip_magic']:
        import gzip
        try:
            out['inflated_bytes'] = len(gzip.decompress(data))
            out['gzip_ok'] = True
        except OSError as exc:
            out['gzip_ok'] = False
            out['gzip_error'] = type(exc).__name__
    else:
        out['gzip_ok'] = False
    return out


# ------------------------------------------------------------------------------- allocation
def build_ctx(out_dir: Path, hold: float) -> dict:
    used = set()
    names = ('http', 'store', 'decoy', 'auth', 'udp1', 'udp2', 'udp3', 'yra', 'yrb', 'tls')
    ports = {name: alloc_port(used) for name in names}
    relay_lo = alloc_port(used)
    roles = {name.upper(): f'<{name.upper()}>' for name in names}
    port_roles = {ports[name]: name.upper() for name in names}
    work = out_dir / 'run'
    for sub in ('configs', 'logs', 'profiles', 'fixtures'):
        (work / sub).mkdir(parents=True, exist_ok=True)
    cert_info = make_cert(work / 'fixtures')
    return dict(work=work, hold=hold, profile_dir=work / 'profiles', used=used, roles=roles,
                port_roles=port_roles, relay_lo=relay_lo, relay_hi=relay_lo + 4,
                cert_paths=(cert_info['cert'], cert_info['key']),
                cert_der_sha=pem_sha256(cert_info['cert']), **{f'{k}_port': v for k, v in ports.items()})


# ------------------------------------------------------------------------------- claims
REFUSAL_TOKENS = ['unable to connect to redis', 'TURN tls cert required',
                  'could not configure TURN/TLS', 'one of key-file or keys must be provided',
                  'panic:', 'invalid']


def leg(name, side, kind, why='', **fields):
    item = {'leg': name, 'side': side, 'kind': kind, 'why': why}
    item.update(fields)
    return item


def claims_for(entry, side) -> dict:
    """Side-specific needles *add to* the shared ones. Replacing them would let a row quietly drop the
    shared assertion for one side, which is how a pair claim stops being a pair claim."""
    merged = {k: (list(v) if isinstance(v, list) else v) for k, v in entry['claims'].items()}
    for key, val in (entry.get('claims_' + side) or {}).items():
        if isinstance(val, list) and isinstance(merged.get(key), list):
            merged[key] = merged[key] + val
        else:
            merged[key] = list(val) if isinstance(val, list) else val
    return merged


def evaluate(entry) -> list:
    """Pure over the receipt: `--record`, `--report` and `--selftest-gate` all call this, so a filed
    verdict can be re-derived offline without launching anything."""
    legs = []
    for side in ('go', 'rust'):
        rec = entry['runs'][side]
        log = rec.get('log_masked') or ''
        claims = claims_for(entry, side)
        sockets = rec.get('sockets_masked') or {}
        held = set(sockets.get('tcp') or []) | set(sockets.get('udp') or [])
        nodes = rec.get('nodes_own') or {}
        other = rec.get('nodes_other') or {}
        files = rec.get('files') or {}
        tls_probe = rec.get('tls') or {}
        store = rec.get('store') or {}

        for needle in claims.get('log_all', []):
            legs.append(leg(f"{entry['profile']}::{side}:has", side, 'has', needles=[needle],
                            haystack=log, why='the flag effect has to be visible in the daemon log'))
        for needle in claims.get('log_not', []):
            legs.append(leg(f"{entry['profile']}::{side}:not", side, 'hasnt', needles=[needle],
                            haystack=log, why='a channel that lost must stay silent'))
        if claims.get('log_any'):
            legs.append(leg(f"{entry['profile']}::{side}:any", side, 'any',
                            needles=claims['log_any'], haystack=log,
                            why='a refusal has to name at least one plausible reason'))
        want = entry.get('expect_exit_' + side, entry.get('expect_exit'))
        if want == 'nonzero':
            legs.append(leg(f"{entry['profile']}::{side}:exit", side, 'nonzero', actual=rec['exit']))
        elif want is not None:
            legs.append(leg(f"{entry['profile']}::{side}:exit", side, 'eq', expect=want,
                            actual=rec['exit'], why='the raw exit code of the launch'))
        if 'node_rows' in claims:
            legs.append(leg(f"{entry['profile']}::{side}:rows", side, 'eq', expect=claims['node_rows'],
                            actual=nodes.get('rows'), why='the registry table read back while alive'))
        # The claim is spelled `region_col`/`ip_col` and the parsed cell is `region`/`ip`. The first
        # draft tested and read the wrong half of that pair on both sides, so the leg never existed and
        # the absolute cell value was only carried by the region_pair_discriminated control.
        for key, column in (('region_col', 'region'), ('ip_col', 'ip')):
            if key in claims:
                legs.append(leg(f"{entry['profile']}::{side}:{column}", side, 'eq',
                                expect=claims[key], actual=nodes.get(column),
                                why='the column the daemon wrote for this flag'))
        if claims.get('udp_exact') is not None:
            legs.append(leg(f"{entry['profile']}::{side}:udp", side, 'eq',
                            expect=sorted(claims['udp_exact']), actual=sorted(sockets.get('udp') or []),
                            why='the UDP sockets the process actually holds'))
        for role in claims.get('decoy_ports_unbound', []):
            legs.append(leg(f"{entry['profile']}::{side}:unbound-{role}", side, 'hasnt',
                            needles=[role], haystack=' '.join(sorted(held)),
                            why='a port the losing channel names must stay unbound'))
        if 'tls_listener' in claims:
            legs.append(leg(f"{entry['profile']}::{side}:tls", side, 'eq', expect=claims['tls_listener'],
                            actual=bool(tls_probe.get('connected')),
                            why='the TURN/TLS listener either answers or does not'))
        if claims.get('tls_matches_cert'):
            legs.append(leg(f"{entry['profile']}::{side}:tls-cert", side, 'eq',
                            expect=entry['cert_der_sha'], actual=tls_probe.get('leaf_sha256_der'),
                            why='the presented leaf must be the file the flag named'))
        for key, which in (('cpu_file', 'cpu'), ('heap_file', 'heap')):
            if key in claims:
                probe = files.get(which) or {}
                legs.append(leg(f"{entry['profile']}::{side}:{key}", side, 'eq', expect=claims[key],
                                actual=bool(probe.get('exists')) and probe.get('bytes', 0) > 0,
                                why='the profile file the flag named'))
        for key, which in (('cpu_gzip', 'cpu'), ('heap_gzip', 'heap')):
            if claims.get(key):
                legs.append(leg(f"{entry['profile']}::{side}:{key}", side, 'eq', expect=True,
                                actual=(files.get(which) or {}).get('gzip_ok'),
                                why='pprof output is a gzip stream on either runtime'))
        if claims.get('ready'):
            legs.append(leg(f"{entry['profile']}::{side}:ready", side, 'eq', expect=True,
                            actual=rec.get('ready'), why='the daemon has to answer its own port'))
        own = (nodes.get('ids') or [None])[0]
        alive_store = store.get('alive') or {}
        after_store = store.get('after') or {}
        if claims.get('store_key_lifecycle'):
            legs.append(leg(f"{entry['profile']}::{side}:nodes-key-alive", side, 'eq', expect=True,
                            actual=bool(alive_store.get('nodes_key_present')),
                            why='the registry key has to exist while the daemon runs'))
            legs.append(leg(f"{entry['profile']}::{side}:nodes-key-gone", side, 'eq', expect=True,
                            actual=not bool(after_store.get('nodes_key_present')),
                            why='SIGINT has to drop the registry rather than leave a dead node behind'))
            legs.append(leg(f"{entry['profile']}::{side}:marker-retained", side, 'eq',
                            expect=['livekit_version'], actual=after_store.get('keys'),
                            why='the version marker is the only key either runtime keeps after a clean '
                                'shutdown - a leaked node hash reddens this instead of hiding under a '
                                'DBSIZE count'))
        if claims.get('store_row_lifecycle'):
            legs.append(leg(f"{entry['profile']}::{side}:nodes-key-alive", side, 'eq', expect=True,
                            actual=bool(alive_store.get('nodes_key_present')),
                            why='the registry key has to exist while the daemon runs'))
            legs.append(leg(f"{entry['profile']}::{side}:node-id-registered", side, 'eq', expect=True,
                            actual=bool(own) and own in (alive_store.get('node_ids') or []),
                            why='the id the CLI table printed is the field the store actually holds'))
            legs.append(leg(f"{entry['profile']}::{side}:node-id-gone", side, 'eq', expect=True,
                            actual=bool(own) and own not in (after_store.get('node_ids') or []),
                            why='SIGINT has to drop that field rather than leave a dead node behind'))
            legs.append(leg(f"{entry['profile']}::{side}:marker-retained", side, 'eq',
                            expect=['livekit_version'], actual=after_store.get('keys'),
                            why='the version marker is the only key either runtime keeps after a clean '
                                'shutdown - a leaked node hash reddens this instead of hiding under a '
                                'DBSIZE count'))
        if claims.get('store_never_registers'):
            legs.append(leg(f"{entry['profile']}::{side}:no-node-reg", side, 'eq', expect=False,
                            actual=bool(after_store.get('nodes_key_present'))
                            or bool(alive_store.get('nodes_key_present')),
                            why='a start that refused, or that was aimed at another store, must leave no '
                                'node registry here'))
        if claims.get('cli_bare_refuses'):
            bare = rec.get('nodes_bare') or {}
            bare_text = bare.get('text_masked') or ''
            legs.append(leg(f"{entry['profile']}::{side}:cli-bare-exit", side, 'eq', expect=1,
                            actual=bare.get('exit'),
                            why='the same subcommand without the flag channel must fail, or the '
                                'channel-carrying read proves nothing'))
            legs.append(leg(f"{entry['profile']}::{side}:cli-bare-rows", side, 'eq', expect=0,
                            actual=(bare.get('table') or {}).get('rows'),
                            why='a read that could not reach the registry prints no node row'))
            for needle in claims['cli_bare_refuses']:
                legs.append(leg(f"{entry['profile']}::{side}:cli-bare-needle", side, 'has',
                                needles=[needle], haystack=bare_text,
                                why='the refusal names the address the config file really holds'))
        if entry.get('refuse_wait') is not None:
            legs.append(leg(f"{entry['profile']}::{side}:refusal-printed", side, 'any',
                            needles=REFUSAL_TOKENS, haystack=log,
                            why='a refusal with no output would make the needle rows pass by accident'))
            legs.append(leg(f"{entry['profile']}::{side}:self-ended", side, 'eq', expect=True,
                            actual=rec.get('self_exited'),
                            why='the exit code is only the daemon\'s own if it ended before the harness '
                                'signalled it'))
        else:
            legs.append(leg(f"{entry['profile']}::{side}:alive", side, 'eq', expect=True,
                            actual=rec.get('alive_at_probe'),
                            why='every observation on this row was taken while the daemon ran'))
        if nodes and 'rows' in nodes:
            legs.append(leg(f"{entry['profile']}::{side}:cross-cli-rows", side, 'eq',
                            expect=nodes.get('rows'), actual=other.get('rows'),
                            why='the other product\'s CLI counts the same rows'))
            for key, label in (('region', 'cross-cli-region'), ('ip', 'cross-cli-ip')):
                if nodes.get(key) is not None:
                    legs.append(leg(f"{entry['profile']}::{side}:{label}", side, 'eq',
                                    expect=nodes.get(key), actual=other.get(key),
                                    why='the stored wire row is readable by the other product'))
    go, rust = entry['runs']['go'], entry['runs']['rust']
    if entry['claims'].get('ip_cell_same'):
        legs.append(leg(f"{entry['profile']}:ip-cell-same", 'pair', 'same',
                        go=(go.get('nodes_own') or {}).get('ip'),
                        rust=(rust.get('nodes_own') or {}).get('ip'),
                        why='both runtimes land on the same auto-detected address'))
    for key in ('region_col', 'ip_col'):
        if key in entry['claims'] and entry['claims'][key] != '':
            legs.append(leg(f"{entry['profile']}:non-empty-{key}", 'pair', 'same',
                            go=(go.get('nodes_own') or {}).get('region' if key == 'region_col' else 'ip'),
                            rust=(rust.get('nodes_own') or {}).get(
                                'region' if key == 'region_col' else 'ip'),
                            why='the claimed cell has to exist on both sides, not just be equal'))
    if not entry.get('divergence'):
        legs.append(leg(f"{entry['profile']}:exit-parity", 'pair', 'same', go=go['exit'],
                        rust=rust['exit'], why='outside a pinned divergence the pair agrees on the exit'))
    if any(entry['claims'].get(k) for k in ('store_row_lifecycle', 'store_key_lifecycle')):
        for when in ('alive', 'after'):
            legs.append(leg(f"{entry['profile']}:store-keys-{when}", 'pair', 'same',
                            go=(go.get('store') or {}).get(when, {}).get('keys'),
                            rust=(rust.get('store') or {}).get(when, {}).get('keys'),
                            why='both runtimes leave the same key set behind, so the registry claim is '
                                'not one side leaking a key the other cleans'))
    for side in ('go', 'rust'):
        raw = entry['runs'][side].get('raw') or {}
        legs.append(leg(f"{entry['profile']}::{side}:raw-stream", side, 'nonzero',
                        actual=raw.get('bytes') or 0,
                        why='a launch that produced no bytes at all cannot be compared'))
    # A log_all claim with several needles yields several legs of the same name, so the name alone cannot
    # identify a verdict: the map the re-derivation control compares would keep the last one and hide a
    # failing sibling. Only repeats get a suffix, so a target written for the first leg still names it.
    seen = {}
    for item in legs:
        seen[item['leg']] = seen.get(item['leg'], 0) + 1
        if seen[item['leg']] > 1:
            item['leg'] = f"{item['leg']}#{seen[item['leg']]}"
    return legs


def rederive_leg(item) -> bool:
    kind = item.get('kind')
    if kind == 'eq':
        return item['actual'] == item['expect']
    if kind == 'same':
        return item['go'] == item['rust'] and item['go'] not in (None, '', ())
    if kind == 'has':
        return bool(item['needles']) and bool(item['haystack']) and all(
            n in item['haystack'] for n in item['needles'])
    if kind == 'hasnt':
        return bool(item['needles']) and all(n not in item['haystack'] for n in item['needles'])
    if kind == 'any':
        return bool(item['needles']) and bool(item['haystack']) and any(
            n in item['haystack'] for n in item['needles'])
    if kind == 'gt':
        return isinstance(item['actual'], int) and item['actual'] > item['expect']
    if kind == 'lt':
        return isinstance(item['actual'], int) and item['actual'] < item['expect']
    if kind == 'nonzero':
        return isinstance(item['actual'], int) and item['actual'] != 0
    raise ValueError(f'unknown leg kind {kind!r}')


# ------------------------------------------------------------------------------- the run
def dbsize(port, password=None):
    out = redis_cli(port, 'DBSIZE', password=password)
    match = re.search(r'-?\d+', out)
    return int(match.group(0)) if match else None


def store_snapshot(port, password=None) -> dict:
    """Key *names*, not just a count. DBSIZE==0 was the C100 claim and it is wrong: both runtimes
    leave the `livekit_version` marker behind after a clean shutdown, so a count cannot tell a
    deregistered node from a leaked one.
    """
    keys = [l.strip() for l in redis_cli(port, 'KEYS', '*', password=password).splitlines()
            if l.strip()]
    out = {'keys': sorted(keys), 'count': len(keys)}
    out['node_ids'] = sorted(f.strip() for f in redis_cli(port, 'HKEYS', 'nodes',
                                                           password=password).splitlines()
                             if f.strip().startswith('ND_')) if 'nodes' in keys else []
    out['nodes_key_present'] = 'nodes' in keys
    return out


def store_kind_of(prof, ctx) -> str:
    return 'auth' if f'127.0.0.1:{ctx["auth_port"]}' in prof['cfg'] else 'plain'


def run_entry(ctx, prof, masker, binaries) -> dict:
    entry = {'profile': prof['id'], 'rows': prof['rows'], 'claims': prof['claims'],
             'note': prof.get('note', ''), 'expect_exit': prof.get('expect_exit'),
             'refuse_wait': prof.get('refuse_wait'), 'cert_der_sha': ctx['cert_der_sha'],
             'runs': {}}
    for key in ('claims_go', 'claims_rust', 'divergence'):
        if key in prof:
            entry[key] = prof[key]
    entry['store_kind'] = store_kind_of(prof, ctx)
    plain, auth = ctx['store_port'], ctx['auth_port']
    store_port = auth if entry['store_kind'] == 'auth' else plain
    store_pw = STORE_PASS if entry['store_kind'] == 'auth' else None
    for side in ('go', 'rust'):
        binary, other = binaries[side], binaries['rust' if side == 'go' else 'go']
        argv = prof['argv_fn'](side, prof['id']) if 'argv_fn' in prof else list(prof.get('argv', []))
        paths = profile_paths(ctx['profile_dir'], prof['id'], side)
        if prof.get('files'):
            paths['cpu'].parent.mkdir(parents=True, exist_ok=True)
            for path in paths.values():
                path.unlink(missing_ok=True)
        redis_cli(plain, 'FLUSHALL')
        redis_cli(auth, 'FLUSHALL', password=STORE_PASS)
        before = {'plain': dbsize(plain), 'auth': dbsize(auth, password=STORE_PASS)}
        if before != {'plain': 0, 'auth': 0}:
            raise RuntimeError(f'stores not clean entering {prof["id"]}/{side}: {before}')
        ready_port = ctx['http_port'] if prof.get('ready') == 'http' else None
        nodes, tls_probe = bool(prof.get('nodes')), bool(prof.get('tls_probe'))

        def on_alive(proc, cfg_path, side=side, binary=binary, other=other, nodes=nodes,
                     tls_probe=tls_probe):
            out = {'alive_store': store_snapshot(store_port, store_pw)}
            if nodes:
                for key, tool in (('nodes_own', binary), ('nodes_other', other)):
                    read = cli_read(tool, cfg_path, f'{side}-{prof["id"]}-{key}', ctx['work'],
                                    prof.get('cli_argv') or (), prof.get('cli_env'))
                    out[key] = {'exit': read['exit'],
                                'table': parse_table(masker.text(read['text'])),
                                'raw_sha': sha256_bytes(read['text'].encode())}
                if prof.get('nodes_bare'):
                    read = cli_read(binary, cfg_path, f'{side}-{prof["id"]}-nodes_bare', ctx['work'])
                    out['nodes_bare'] = {'exit': read['exit'],
                                         'table': parse_table(masker.text(read['text'])),
                                         'text_masked': masker.text(read['text'])}
            if tls_probe:
                out['tls'] = tls_cert_digest(ctx['tls_port'])
            return out

        rec = launch_pair(binary, argv, prof.get('env') or {}, prof['cfg'], ctx['work'],
                          f'{side}-{prof["id"]}', side, masker, ready_port, ctx['hold'],
                          prof.get('refuse_wait'), on_alive=on_alive)
        obs = rec.pop('observed', {}) or {}
        rec['store'] = {'before_dbsize': before, 'alive': obs.get('alive_store') or {},
                        'after': store_snapshot(store_port, store_pw)}
        for key in ('nodes_own', 'nodes_other'):
            if key in obs:
                rec[key] = obs[key]['table']
                rec[key + '_cli_exit'] = obs[key]['exit']
        if prof.get('cli_argv') or prof.get('cli_env'):
            rec['cli_channel_masked'] = {
                'argv': [masker.text(a) for a in (prof.get('cli_argv') or ())],
                'env': {k: masker.text(v) for k, v in (prof.get('cli_env') or {}).items()}}
        if 'nodes_bare' in obs:
            rec['nodes_bare'] = {'exit': obs['nodes_bare']['exit'],
                                 'table': obs['nodes_bare']['table'],
                                 'text_masked': obs['nodes_bare']['text_masked']}
        if tls_probe:
            rec['tls'] = obs.get('tls') or {'connected': False, 'error': 'not-probed'}
        if prof.get('files'):
            rec['files'] = {k: file_probe(v) for k, v in paths.items()}
            rec['files_paths_masked'] = [masker.text(str(p)) for p in paths.values()]
        entry['runs'][side] = rec
    return entry


# ------------------------------------------------------------------------------- controls
def build_controls(doc, live=False) -> dict:
    entries = {e['profile']: e for e in doc['profiles']}
    runs = [(e['profile'], side, e['runs'][side]) for e in doc['profiles'] for side in ('go', 'rust')]
    C = {}
    C['no_development_subset'] = not doc.get('subset')
    C['every_profile_ran_both_sides'] = all(
        set(e['runs']) == {'go', 'rust'} and all(r.get('exit') is not None for r in e['runs'].values())
        for e in doc['profiles'])
    positives = [e for e in doc['profiles'] if e.get('refuse_wait') is None]
    refusals = [e for e in doc['profiles'] if e.get('refuse_wait') is not None]
    C['positive_rows_observed_alive'] = bool(positives) and len(positives) >= 20 and all(
        e['runs'][s]['alive_at_probe'] for e in positives for s in ('go', 'rust'))
    C['refusals_self_ended'] = bool(refusals) and all(
        e['runs'][s]['self_exited'] for e in refusals for s in ('go', 'rust'))
    C['refusal_rows_present'] = len(refusals) >= 5
    C['sigkill_never_used'] = not any('SIGKILL' in r.get('signals', []) for _, _, r in runs)
    C['raw_streams_recorded'] = all(
        re.fullmatch(r'[0-9a-f]{64}', r['raw']['stdout_sha256']) and r['raw']['bytes'] > 0
        for _, _, r in runs)
    C['argv_never_raw'] = not any('argv' in r for _, _, r in runs)
    profile_blob = json.dumps(doc['profiles'], ensure_ascii=False, sort_keys=True)
    C['off_limits_never_named'] = not re.search(r'\b788[12]\b', profile_blob)
    C['host_store_never_named'] = ':6379' not in profile_blob
    bounds = doc.get('listeners_bounds') or {}
    C['off_limit_ports_untouched'] = bool(bounds) and all(
        v['before'] == v['after'] for v in bounds.values())
    node_rows = [(pid, s, r) for pid, s, r in runs if r.get('nodes_own')]
    # TABLE_WIDTH is re-derived from each read's own header, so the control asks "did the data row line
    # up with the header this run printed" rather than "did it match a number I typed"; TABLE_HEADER
    # separately pins which columns those are. The first draft of this control hard-coded a width of 15
    # taken from an assumption, and every registry read in the sweep reported 14.
    def table_ok(n):
        return (not n.get('header_missing_column') and n.get('cols') == len(n.get('header') or [])
                and n.get('cols') == TABLE_WIDTH and n.get('cells') == n.get('cols'))

    C['node_columns_from_header'] = bool(node_rows) and all(
        table_ok(r['nodes_own']) for _, _, r in node_rows)
    C['cli_table_schema_paired'] = bool(node_rows) and all(
        r['nodes_own'].get('header') == TABLE_HEADER for _, _, r in node_rows)
    C['cross_cli_registry_reads'] = bool(node_rows) and all(
        r['nodes_own'].get('region') == r['nodes_other'].get('region')
        and r['nodes_own'].get('ip') == r['nodes_other'].get('ip')
        and r['nodes_own'].get('rows') == r['nodes_other'].get('rows')
        for _, _, r in node_rows)

    def cell(pid, side, key):
        # A development subset has no such entry; the pair controls then read False, which is the
        # honest answer - only a full sweep can claim them.
        entry = entries.get(pid)
        if not entry:
            return None
        return (entry['runs'][side].get('nodes_own') or {}).get(key)

    C['region_pair_discriminated'] = all(
        cell('region-flag', s, 'region') == REGION_A and cell('region-env', s, 'region') == REGION_B
        and cell('region-unset', s, 'region') == '' and cell('region-empty', s, 'region') == ''
        for s in ('go', 'rust'))
    C['node_ip_pair_discriminated'] = all(
        cell('node-ip-v4', s, 'ip') == '127.0.0.1' and cell('node-ip-v6', s, 'ip') == '::1'
        and cell('node-ip-garbage', s, 'ip') not in (None, '', '127.0.0.1', '::1')
        and cell('node-ip-empty', s, 'ip') == cell('node-ip-garbage', s, 'ip') for s in ('go', 'rust'))
    udp = [e for e in doc['profiles'] if 'udp_exact' in e['claims']]
    merge = entries.get('udp-flag-merges-into-yaml-range')
    clean = [e for e in udp if e is not merge]
    C['udp_exact_rows_present'] = len(udp) >= 5 and sum(1 for e in udp if e['claims']['udp_exact']) >= 4
    C['udp_default_range_left_unbound'] = bool(clean) and all(
        sorted(r['sockets_masked']['udp']) == sorted(claims_for(e, s).get('udp_exact', []))
        and not {'<YRA>', '<YRB>', '<TLS>'} & set(r['sockets_masked']['udp'])
        for e in clean for s in ('go', 'rust') for r in [e['runs'][s]])
    # Four rows read the registry twice: once with the flag/env channel the daemon used, once with only
    # the config file. The second read has to fail, otherwise the first one proves nothing about the
    # channel - it could have been the config all along.
    chan = [e for e in doc['profiles'] if e['claims'].get('cli_bare_refuses')]
    C['registry_read_needs_the_same_channel'] = len(chan) == 4 and all(
        (e['runs'][s].get('nodes_own') or {}).get('rows') == 1
        and (e['runs'][s].get('nodes_bare') or {}).get('exit') == 1
        and not (e['runs'][s].get('nodes_bare') or {}).get('table', {}).get('rows')
        and e['runs'][s].get('cli_channel_masked')
        for e in chan for s in ('go', 'rust'))
    # Measured contract rather than an assumption: the flag channel only writes PortRange.Start when
    # the string has no "-", so the YAML End survives and the daemon binds the union of both channels.
    # If a later change turns that into a clean override, this control reddens and the row plus the
    # --udp-port ledger note have to be re-read instead of ageing quietly.
    C['udp_flag_merge_pinned'] = bool(merge) and all(
        sorted(merge['runs'][s]['sockets_masked']['udp']) == sorted(
            ['<UDP1>', '<UDP2>', '<UDP3>', '<YRA>', '<YRB>'])
        and f'"rtc.portUDP": {{"Start":<UDP1>,"End":<YRB>}}' in merge['runs'][s]['log_masked']
        for s in ('go', 'rust'))
    enabled, disabled = entries.get('turn-cert-key-flags'), entries.get('turn-cert-key-flags-when-disabled')
    C['tls_material_reaches_handshake'] = bool(enabled) and bool(disabled) and all(
        r['tls'].get('leaf_sha256_der') == enabled['cert_der_sha']
        for r in (enabled['runs']['go'], enabled['runs']['rust'])) and all(
        not r['tls'].get('connected') for r in (disabled['runs']['go'], disabled['runs']['rust']))
    C['turn_refusals_present_on_all_four_material_shapes'] = sum(
        1 for e in doc['profiles'] if e.get('refuse_wait') is not None and any(
            'turn' in r for r in e['rows'])) == 4
    # Measured split, not a guess: Go builds the TURN server while the wire graph is constructed
    # (references/livekit-stable/pkg/service/wire.go:300 -> NewTurnServer -> turn.go:222-228), so a bad
    # PEM aborts before RedisStore.Start() writes the marker (pkg/service/redisstore.go:93-113). Rust
    # starts the store graph first (crates/livecloud/src/server.rs:604-624) and calls turn::start after
    # (:650), so `livekit_version` survives the refusal. Keeping this as a control means the ordering
    # claim has to be re-audited if either side converges, instead of a stale divergence ageing quietly.
    turn_refused = [e for e in doc['profiles'] if e.get('refuse_wait') is not None
                    and any('turn' in r for r in e['rows'])]
    C['turn_refusal_marker_ordering_pinned'] = len(turn_refused) == 4 and all(
        e['runs']['go']['store']['after']['keys'] == []
        and e['runs']['rust']['store']['after']['keys'] == ['livekit_version']
        and not e['runs']['go']['store']['after']['nodes_key_present']
        and not e['runs']['rust']['store']['after']['nodes_key_present']
        for e in turn_refused)
    prof_rows = [e for e in doc['profiles'] if e['runs']['go'].get('files')]
    C['profile_files_under_work_only'] = bool(prof_rows) and all(
        p.startswith('<WORK>/profiles/') for e in prof_rows for s in ('go', 'rust')
        for p in e['runs'][s].get('files_paths_masked', []))
    divergent = [e for e in doc['profiles'] if e.get('divergence')]
    C['divergences_pinned'] = bool(divergent) and all(
        'GO:' in e['divergence'] and 'RUST:' in e['divergence']
        and (e.get('claims_go') or e.get('claims_rust'))
        and e['runs']['go']['exit'] is not None and e['runs']['rust']['exit'] is not None
        for e in divergent)
    C['divergence_rows_present'] = len(divergent) >= 4
    C['rows_cover_every_claimed_row'] = all(
        sum(1 for e in doc['profiles'] if row in e['rows']) >= 3 for row in doc['covers'])
    for row in doc['covers']:
        subset = [e for e in doc['profiles'] if row in e['rows']]
        shapes = {json.dumps([e['claims'], e['runs']['go']['exit'], e['runs']['rust']['exit']],
                             sort_keys=True) for e in subset}
        C[f'row_has_contrast_pair::{row}'] = len(shapes) >= 2
    text = json.dumps(doc, ensure_ascii=False, sort_keys=True)
    C['no_machine_address_in_receipt'] = not forbidden_addresses(
        re.sub(r'<IP#[A-Z]{8}>', '', text))
    C['fixture_credentials_not_in_receipt'] = not any(
        secret in text for secret in (API_SECRET, STORE_PASS, WRONG_PASS))
    C['store_flushed_before_each_profile'] = bool(doc.get('store_flush_ok'))
    final = doc.get('redis_final') or {}
    C['owned_stores_left_clean'] = len(final) == 2 and all(v == 0 for v in final.values())
    bins = doc['binaries']
    now = {side: (sha256_file(Path(bins[side]['path'])) if Path(bins[side].get('path', '')).is_file()
                  else None) for side in ('go', 'rust')}
    doc['binary_hash_now'] = now
    C['binaries_unchanged'] = all(now[s] is not None and now[s] == bins[s]['sha256']
                                 for s in ('go', 'rust'))
    bins['go']['is_fixed_oracle'] = now['go'] == ORACLE_SHA256
    bins['rust']['is_acceptance_candidate'] = now['rust'] == ACCEPTANCE_SHA256
    C['go_is_fixed_oracle'] = bool(bins['go']['is_fixed_oracle'])
    C['rust_is_acceptance_candidate'] = bool(bins['rust']['is_acceptance_candidate'])
    # The two controls above are re-derived from the files on disk, so they cannot see a receipt that
    # *records* a different digest than the pinned image. These two compare the recorded field.
    C['recorded_oracle_matches_pinned'] = bins['go']['sha256'] == ORACLE_SHA256
    C['recorded_candidate_matches_pinned'] = bins['rust']['sha256'] == ACCEPTANCE_SHA256
    C['reference_commit_matches'] = doc['reference']['go_checkout_commit'] == FIXED_GO_COMMIT
    harness_now = sha256_file(Path(__file__).resolve())
    doc['harness_sha256_now'] = harness_now
    C['harness_unchanged_during_run'] = (
        harness_now == doc['harness_sha256_at_start'] if live
        else doc['harness_sha256'] == doc['harness_sha256_at_start'])
    C['harness_unchanged_since_run'] = doc['harness_sha256'] == harness_now
    return C


# ------------------------------------------------------------------------------- verdict
def finish(doc, out=None, live=False, write=True) -> dict:
    for entry in doc['profiles']:
        legs = evaluate(entry)
        for item in legs:
            item['ok'] = rederive_leg(item)
        entry['legs'] = legs
        entry['problems'] = sorted(l['leg'] for l in legs if not l['ok'])
        entry['match'] = not entry['problems']
    doc['legs'] = [l for e in doc['profiles'] for l in e['legs']]
    doc['leg_count'] = len(doc['legs'])
    verdicts = {l['leg']: l['ok'] for l in doc['legs']}
    if live or 'leg_verdicts_at_record' not in doc:
        doc['leg_verdicts_at_record'] = verdicts
    controls = build_controls(doc, live=live)
    controls['verdict_stable_across_rederivation'] = (verdicts == doc['leg_verdicts_at_record'])
    doc['cross_profile_controls'] = controls
    doc['failed_profiles'] = sorted(e['profile'] for e in doc['profiles'] if not e['match'])
    doc['failed_legs'] = sorted(l['leg'] for l in doc['legs'] if not l['ok'])
    doc['failed_controls'] = sorted(k for k, v in controls.items() if not v)
    doc['status_gates'] = {
        'binaries_unchanged': controls['binaries_unchanged'],
        'go_is_fixed_oracle': controls['go_is_fixed_oracle'],
        'rust_is_acceptance_candidate': controls['rust_is_acceptance_candidate'],
        'reference_commit_matches': controls['reference_commit_matches'],
        'harness_unchanged_during_run': controls['harness_unchanged_during_run'],
        'harness_unchanged_since_run': controls['harness_unchanged_since_run'],
        'no_failed_profiles': not doc['failed_profiles'],
        'no_failed_controls': not doc['failed_controls'],
        'no_failed_legs': not doc['failed_legs'],
    }
    doc['unmet_gates'] = sorted(k for k, v in doc['status_gates'].items() if not v)
    doc['live_run'] = live
    doc['status'] = ('PARTIAL_OFFLINE_CLI_ONLY_NOT_ACCEPTANCE' if doc.get('skip_launch')
                     else 'PARTIAL_SUBSET_NOT_ACCEPTANCE' if doc.get('subset')
                     else 'PASS_LAUNCHED_BASE_FLAG_CHANNELS' if not doc['unmet_gates'] else 'FAIL')
    if write and out is not None:
        (Path(out) / 'execution.json').write_text(
            json.dumps(doc, indent=2, ensure_ascii=False, sort_keys=True) + '\n')
        batch = {'status': doc['status'], 'batch': doc['batch'], 'covers': doc['covers'],
                 'profiles': len(doc['profiles']), 'legs': doc['leg_count'],
                 'controls': len(controls), 'failed_profiles': doc['failed_profiles'],
                 'failed_legs': doc['failed_legs'], 'failed_controls': doc['failed_controls'],
                 'unmet_gates': doc['unmet_gates'], 'status_gates': doc['status_gates'],
                 'profile_verdicts': {e['profile']: {'match': e['match'], 'rows': e['rows'],
                                                     'go_exit': e['runs']['go']['exit'],
                                                     'rust_exit': e['runs']['rust']['exit'],
                                                     'divergence': bool(e.get('divergence')),
                                                     'problems': e['problems']}
                                      for e in doc['profiles']},
                 'controls': controls, 'binary_hash_now': doc['binary_hash_now'],
                 'harness_sha256_now': doc['harness_sha256_now'], 'binaries': doc['binaries'],
                 'reference': doc['reference'], 'harness_sha256': doc['harness_sha256']}
        (Path(out) / 'BATCH.json').write_text(json.dumps(batch, indent=2, ensure_ascii=False,
                                                        sort_keys=True) + '\n')
    return {'all_ok': not doc['unmet_gates'],
            'summary': {k: doc[k] for k in ('status', 'leg_count', 'failed_profiles',
                                            'failed_controls', 'failed_legs', 'unmet_gates')}}


# ------------------------------------------------------------------------------- gate sweep
def en(doc, pid):
    for entry in doc['profiles']:
        if entry['profile'] == pid:
            return entry
    raise KeyError(pid)


def pf(doc, pid, side):
    return en(doc, pid)['runs'][side]


def blank_log(rec):
    rec['log_masked'] = ''
    return rec


def append_log(doc, pid, side, text):
    rec = pf(doc, pid, side)
    rec['log_masked'] = rec['log_masked'] + text
    return rec


GATE_INJECTIONS = (
    {'case': 'exit_drift_on_go', 'expect': 'FAILED_LEG', 'target': 'region-flag::go:exit',
     'apply': lambda d: pf(d, 'region-flag', 'go').update(exit=1),
     'why': 'a launched row whose exit moved no longer matches the contract'},
    {'case': 'exit_split_unpinned', 'expect': 'FAILED_LEG', 'target': 'udp-env:exit-parity',
     'apply': lambda d: pf(d, 'udp-env', 'rust').update(exit=1),
     'why': 'outside a pinned divergence the pair must agree on the exit code'},
    {'case': 'refusal_now_succeeds', 'expect': 'FAILED_LEG', 'target': 'turn-missing-file::go:exit',
     'apply': lambda d: pf(d, 'turn-missing-file', 'go').update(exit=0),
     'why': 'the missing-certificate row has to stay a refusal'},
    {'case': 'region_cell_drift', 'expect': 'FAILED_LEG', 'target': 'region-flag::go:region',
     'apply': lambda d: pf(d, 'region-flag', 'go')['nodes_own'].update(region='other'),
     'why': 'the Region column is the only projection --region has'},
    {'case': 'region_pair_collapsed', 'expect': 'FAILED_CONTROL', 'target': 'region_pair_discriminated',
     'apply': lambda d: pf(d, 'region-unset', 'go')['nodes_own'].update(region=REGION_A),
     'why': 'if the unset row reads like the set row, the empty-string row proves nothing'},
    {'case': 'node_ip_cell_drift', 'expect': 'FAILED_LEG', 'target': 'node-ip-v4::rust:ip',
     'apply': lambda d: pf(d, 'node-ip-v4', 'rust')['nodes_own'].update(ip='::2'),
     'why': 'the advertised IP cell is what --node-ip is claimed to control'},
    {'case': 'node_ip_pair_collapsed', 'expect': 'FAILED_CONTROL',
     'target': 'node_ip_pair_discriminated',
     'apply': lambda d: pf(d, 'node-ip-empty', 'go')['nodes_own'].update(ip='127.0.0.1'),
     'why': 'an empty node IP has to stay distinguishable from an explicit loopback one'},
    {'case': 'garbage_ip_differs_across_sides', 'expect': 'FAILED_LEG',
     'target': 'node-ip-garbage:ip-cell-same',
     'apply': lambda d: pf(d, 'node-ip-garbage', 'rust')['nodes_own'].update(ip='<IP#BBBBBBBB>'),
     'why': 'the auto-detected address must be the same address on both products'},
    {'case': 'table_header_column_count', 'expect': 'FAILED_CONTROL',
     'target': 'node_columns_from_header',
     'apply': lambda d: pf(d, 'region-flag', 'go')['nodes_own'].update(cells=13),
     'why': 'the data row has to line up with the header this run printed, or the cells the row reads '
            'are positions rather than names'},
    {'case': 'header_column_unlocatable', 'expect': 'FAILED_CONTROL',
     'target': 'node_columns_from_header',
     'apply': lambda d: pf(d, 'region-flag', 'go')['nodes_own'].update(header_missing_column=True),
     'why': 'a table whose header no longer names the columns cannot support a column claim'},
    {'case': 'cli_table_schema_split', 'expect': 'FAILED_CONTROL',
     'target': 'cli_table_schema_paired',
     'apply': lambda d: pf(d, 'region-flag', 'go')['nodes_own'].update(
         header=[('Zone' if h == 'Region' else h) for h in TABLE_HEADER]),
     'why': 'the two CLIs are supposed to publish one schema; a renamed label is a wire-format break '
            'that a column-count claim would miss'},
    {'case': 'cross_cli_region_drift', 'expect': 'FAILED_CONTROL', 'target': 'cross_cli_registry_reads',
     'apply': lambda d: pf(d, 'region-flag', 'go')['nodes_other'].update(region=''),
     'why': 'the other product reading a different Region is a wire-format break'},
    {'case': 'cross_cli_ip_leg', 'expect': 'FAILED_LEG', 'target': 'region-flag::go:cross-cli-ip',
     'apply': lambda d: pf(d, 'region-flag', 'go')['nodes_other'].update(ip='elsewhere'),
     'why': 'the stored row must survive being read by the other CLI'},
    {'case': 'needle_log_blank', 'expect': 'FAILED_LEG', 'target': 'region-flag::rust:has',
     'apply': lambda d: blank_log(pf(d, 'region-flag', 'rust')),
     'why': 'an empty haystack may not satisfy a has-claim'},
    {'case': 'silent_channel_noise', 'expect': 'FAILED_LEG', 'target': 'node-ip-v4::rust:not',
     'apply': lambda d: append_log(d, 'node-ip-v4', 'rust', 'ignoring --node-ip'),
     'why': 'the row claims the warning path stays quiet for a valid address'},
    {'case': 'refusal_silent', 'expect': 'FAILED_LEG',
     'target': 'turn-swapped-pair::go:refusal-printed',
     'apply': lambda d: blank_log(pf(d, 'turn-swapped-pair', 'go')),
     'why': 'a refusal that prints nothing would make the per-side needles pass by accident'},
    {'case': 'udp_extra_socket', 'expect': 'FAILED_LEG', 'target': 'udp-single::go:udp',
     'apply': lambda d: pf(d, 'udp-single', 'go')['sockets_masked']['udp'].append('<YRA>'),
     'why': 'the YAML range the flag beat must stay unbound'},
    {'case': 'udp_socket_missing', 'expect': 'FAILED_LEG', 'target': 'udp-range::rust:udp',
     'apply': lambda d: pf(d, 'udp-range', 'rust')['sockets_masked']['udp'].pop(),
     'why': 'a range must bind every port in it'},
    {'case': 'udp_default_range_bound', 'expect': 'FAILED_CONTROL',
     'target': 'udp_default_range_left_unbound',
     'apply': lambda d: pf(d, 'udp-range', 'go')['sockets_masked']['udp'].append('<YRB>'),
     'why': 'a channel that lost must not be partly in effect'},
    {'case': 'udp_flag_override_converged', 'expect': 'FAILED_CONTROL',
     'target': 'udp_flag_merge_pinned',
     'apply': lambda d: pf(d, 'udp-flag-merges-into-yaml-range', 'go')['sockets_masked'].update(
         udp=['<UDP1>']),
     'why': 'a run where the flag really replaces the YAML range is a different contract than the merge '
            'this row pins, so the row and the ledger note have to be re-read'},
    {'case': 'tls_leaf_swapped', 'expect': 'FAILED_LEG',
     'target': 'turn-cert-key-flags::rust:tls-cert',
     'apply': lambda d: pf(d, 'turn-cert-key-flags', 'rust')['tls'].update(leaf_sha256_der='deadbeef'),
     'why': 'the handshake has to present the file the flag named, not another certificate'},
    {'case': 'tls_listener_when_disabled', 'expect': 'FAILED_CONTROL',
     'target': 'tls_material_reaches_handshake',
     'apply': lambda d: pf(d, 'turn-cert-key-flags-when-disabled', 'go')['tls'].update(
         connected=True, leaf_sha256_der=en(d, 'turn-cert-key-flags')['cert_der_sha']),
     'why': 'material plus turn.enabled false may not open a listener'},
    {'case': 'store_row_never_appeared', 'expect': 'FAILED_LEG',
     'target': 'region-flag::go:nodes-key-alive',
     'apply': lambda d: pf(d, 'region-flag', 'go')['store']['alive'].update(
         nodes_key_present=False, keys=['livekit_version']),
     'why': 'without a registry key the CLI table could have come from anywhere'},
    {'case': 'cli_id_not_in_store', 'expect': 'FAILED_LEG',
     'target': 'region-flag::rust:node-id-registered',
     'apply': lambda d: pf(d, 'region-flag', 'rust')['store']['alive'].update(node_ids=['ND_other']),
     'why': 'the id the CLI printed must be the field the store holds, not merely a field count'},
    {'case': 'shutdown_leaves_the_row', 'expect': 'FAILED_LEG',
     'target': 'node-ip-v4::go:marker-retained',
     'apply': lambda d: pf(d, 'node-ip-v4', 'go')['store']['after'].update(
         keys=['livekit_version', 'nodes']),
     'why': 'a dead node left in the registry is exactly what a DBSIZE count could not see'},
    {'case': 'sides_deregister_differently', 'expect': 'FAILED_LEG',
     'target': 'region-flag:store-keys-after',
     'apply': lambda d: pf(d, 'region-flag', 'rust')['store']['after'].update(keys=[]),
     'why': 'the retained-marker claim is a pair claim, not one side behaviour'},
    {'case': 'cli_bare_read_succeeds', 'expect': 'FAILED_LEG',
     'target': 'redis-host-flag-beats-yaml::go:cli-bare-exit',
     'apply': lambda d: pf(d, 'redis-host-flag-beats-yaml', 'go')['nodes_bare'].update(exit=0),
     'why': 'if the config-only read works too, the flag channel claim on that row is empty'},
    {'case': 'cli_bare_read_prints_a_row', 'expect': 'FAILED_LEG',
     'target': 'redis-password-flag::rust:cli-bare-rows',
     'apply': lambda d: pf(d, 'redis-password-flag', 'rust')['nodes_bare']['table'].update(rows=1),
     'why': 'a registry the password unlocked cannot also be readable without it'},
    {'case': 'cli_bare_needle_missing', 'expect': 'FAILED_LEG',
     'target': 'redis-host-env::go:cli-bare-needle',
     'apply': lambda d: pf(d, 'redis-host-env', 'go')['nodes_bare'].update(text_masked='no output'),
     'why': 'the refusal has to name the dead address the config file holds'},
    {'case': 'channel_read_unrecorded', 'expect': 'FAILED_CONTROL',
     'target': 'registry_read_needs_the_same_channel',
     'apply': lambda d: pf(d, 'redis-password-env', 'rust').pop('cli_channel_masked'),
     'why': 'an auditor cannot tell which channel the registry read was made through'},
    {'case': 'refusal_registers_a_node', 'expect': 'FAILED_LEG',
     'target': 'turn-missing-file::rust:no-node-reg',
     'apply': lambda d: pf(d, 'turn-missing-file', 'rust')['store']['after'].update(
         nodes_key_present=True, node_ids=['ND_x'], keys=['livekit_version', 'nodes']),
     'why': 'a refused start must leave no node registry'},
    {'case': 'turn_refusal_marker_ordering_converged', 'expect': 'FAILED_CONTROL',
     'target': 'turn_refusal_marker_ordering_pinned',
     'apply': lambda d: pf(d, 'turn-swapped-pair', 'go')['store'].update(
         after={'keys': ['livekit_version'], 'count': 1, 'node_ids': [], 'nodes_key_present': False}),
     'why': 'the store-marker ordering is a pinned divergence, so a run where both sides mark is a new '
            'contract the rows have to be re-read against rather than an unchanged pass'},
    {'case': 'profile_file_absent', 'expect': 'FAILED_LEG', 'target': 'profiles-both::rust:cpu_file',
     'apply': lambda d: pf(d, 'profiles-both', 'rust')['files']['cpu'].update(exists=False, bytes=0),
     'why': 'the row claims the file the flag named is created'},
    {'case': 'profile_not_gzip', 'expect': 'FAILED_LEG', 'target': 'profiles-mem-only::go:heap_gzip',
     'apply': lambda d: pf(d, 'profiles-mem-only', 'go')['files']['heap'].update(gzip_ok=False),
     'why': 'a heap profile that is not a decompressable pprof stream is not the same contract'},
    {'case': 'profile_written_elsewhere', 'expect': 'FAILED_CONTROL',
     'target': 'profile_files_under_work_only',
     'apply': lambda d: pf(d, 'profiles-both', 'go')['files_paths_masked'].__setitem__(
         0, '/elsewhere/cpu.out'),
     'why': 'the lane may only write inside its own work directory'},
    {'case': 'refusal_needs_force', 'expect': 'FAILED_CONTROL', 'target': 'sigkill_never_used',
     'apply': lambda d: pf(d, 'turn-missing-file', 'go').update(signals=['SIGINT', 'SIGKILL']),
     'why': 'a daemon that ignores SIGINT is not a clean refusal'},
    {'case': 'refusal_killed_by_harness', 'expect': 'FAILED_LEG',
     'target': 'redis-host-flag-broken::rust:self-ended',
     'apply': lambda d: pf(d, 'redis-host-flag-broken', 'rust').update(self_exited=False),
     'why': 'the exit code only belongs to the daemon if it ended on its own'},
    {'case': 'positive_row_not_alive', 'expect': 'FAILED_LEG', 'target': 'region-flag::go:alive',
     'apply': lambda d: pf(d, 'region-flag', 'go').update(alive_at_probe=False),
     'why': 'every observation on that row was taken after the process was gone'},
    {'case': 'off_limit_port_named', 'expect': 'FAILED_CONTROL', 'target': 'off_limits_never_named',
     'apply': lambda d: append_log(d, 'udp-env', 'go', ' listening on 7882'),
     'why': 'the developer host owns 7881/7882 and this lane must never name them'},
    {'case': 'host_store_named', 'expect': 'FAILED_CONTROL', 'target': 'host_store_never_named',
     'apply': lambda d: pf(d, 'udp-env', 'go').update(cfg_masked='redis:\n  address: 127.0.0.1:6379\n'),
     'why': 'the host store belongs to another product'},
    {'case': 'machine_address_leaks', 'expect': 'FAILED_CONTROL',
     'target': 'no_machine_address_in_receipt',
     'apply': lambda d: append_log(d, 'udp-env', 'go', ' peer 10.113.4.21 answered'),
     'why': 'the receipt is committed, so a machine address in it is a privacy break'},
    {'case': 'host_lan_address_leaks', 'expect': 'FAILED_CONTROL',
     'target': 'no_machine_address_in_receipt',
     'apply': lambda d: append_log(d, 'udp-env', 'rust', ' iface 192.168.9.44 up'),
     'why': 'the first version of this case leaked 192.0.2.55, which is RFC 5737 documentation space and '
            'therefore allowed - a gate case that cannot redden is not a gate case'},
    {'case': 'fixture_credential_leaks', 'expect': 'FAILED_CONTROL',
     'target': 'fixture_credentials_not_in_receipt',
     'apply': lambda d: append_log(d, 'udp-env', 'go', ' secret ' + API_SECRET),
     'why': 'the fixture secret appears in argv and configs and must never reach the file'},
    {'case': 'raw_argv_recorded', 'expect': 'FAILED_CONTROL', 'target': 'argv_never_raw',
     'apply': lambda d: pf(d, 'udp-env', 'go').update(argv=['--udp-port', '50123', STORE_PASS]),
     'why': 'only the masked argv belongs in the receipt'},
    {'case': 'divergence_wording_lost', 'expect': 'FAILED_CONTROL', 'target': 'divergences_pinned',
     'apply': lambda d: en(d, 'turn-swapped-pair').update(divergence='both refuse'),
     'why': 'a divergence row has to say what each side does'},
    {'case': 'divergence_claim_dropped', 'expect': 'FAILED_CONTROL', 'target': 'divergences_pinned',
     'apply': lambda d: en(d, 'node-ip-garbage').pop('claims_rust'),
     'why': 'the split has to be pinned by a needle on the side that prints it'},
    {'case': 'row_without_contrast', 'expect': 'FAILED_CONTROL',
     'target': 'row_has_contrast_pair::base_flags:--region',
     'apply': lambda d: [en(d, pid).update(
         claims=json.loads(json.dumps(en(d, 'region-flag')['claims'])))
         for pid in ('region-env', 'region-flag-beats-env', 'region-unset', 'region-empty')],
     'why': 'the row is only a claim about --region if its five shapes differ; copying one row onto the '
            'rest leaves nothing to discriminate - the first draft of this case flattened one profile '
            'and the control stayed green'},
    {'case': 'binary_hash_drift', 'expect': 'FAILED_CONTROL', 'target': 'binaries_unchanged',
     'apply': lambda d: d['binaries']['go'].update(sha256='0' * 64),
     'why': 'the oracle image is fixed; a moved hash means a different binary was scored'},
    {'case': 'candidate_hash_drift', 'expect': 'FAILED_CONTROL',
     'target': 'recorded_candidate_matches_pinned',
     'apply': lambda d: d['binaries']['rust'].update(sha256='1' * 64),
     'why': 'a receipt that records a digest other than the pinned candidate cannot be cited by the '
            'ledger'},
    {'case': 'oracle_hash_drift', 'expect': 'FAILED_CONTROL',
     'target': 'recorded_oracle_matches_pinned',
     'apply': lambda d: d['binaries']['go'].update(sha256='1' * 64),
     'why': 'the same half for the oracle: the recorded identity is what a reviewer re-checks'},
    {'case': 'reference_commit_drift', 'expect': 'FAILED_CONTROL', 'target': 'reference_commit_matches',
     'apply': lambda d: d['reference'].update(go_checkout_commit='deadbeef'),
     'why': 'the source the sentences were read from is part of the claim'},
    {'case': 'harness_drift', 'expect': 'FAILED_CONTROL', 'target': 'harness_unchanged_since_run',
     'apply': lambda d: d.update(harness_sha256='2' * 64),
     'why': 'the filed harness has to be the one that produced the receipt'},
    {'case': 'store_left_dirty', 'expect': 'FAILED_CONTROL', 'target': 'owned_stores_left_clean',
     'apply': lambda d: d['redis_final'].update(plain=3),
     'why': 'the lane owns those stores and must leave them empty'},
    {'case': 'off_limit_listeners_moved', 'expect': 'FAILED_CONTROL',
     'target': 'off_limit_ports_untouched',
     'apply': lambda d: d['listeners_bounds'].update({'7881': {'before': 0, 'after': 1}}),
     'why': 'nothing in this lane may take or drop a listener on the host ports'},
    {'case': 'development_subset', 'expect': 'STATUS_NOT_PASS', 'target': 'status',
     'apply': lambda d: d.update(subset=True),
     'why': 'a --only run scores a subset and is never an acceptance'},
    {'case': 'empty_profile_set', 'expect': 'STATUS_NOT_PASS', 'target': 'status',
     'apply': lambda d: d.update(profiles=[]),
     'why': 'zero rows cannot produce a pass'},
    {'case': 'missing_side', 'expect': 'HARNESS_REFUSAL', 'target': 'evaluate',
     'apply': lambda d: en(d, 'udp-single')['runs'].pop('rust'),
     'why': 'a profile without both sides is not a pair and must not be scored silently'},
    {'case': 'missing_raw_stream', 'expect': 'HARNESS_REFUSAL', 'target': 'build_controls',
     'apply': lambda d: pf(d, 'udp-single', 'go').pop('raw'),
     'why': 'the provenance of the captured stream is part of every leg'},
    {'case': 'rederive_rejects_unknown_kind', 'expect': 'DIRECT_RAISE', 'target': 'rederive_leg',
     'apply': None, 'direct': lambda: rederive_leg({'kind': 'not-a-kind', 'actual': 1, 'expect': 1}),
     'why': 'the re-deriver raises on a kind it cannot check instead of returning true'},
    {'case': 'rederive_rejects_empty_pair', 'expect': 'DIRECT_FALSE', 'target': 'rederive_leg',
     'apply': None, 'direct': lambda: rederive_leg({'kind': 'same', 'go': None, 'rust': None}),
     'why': 'two missing values are not agreement'},
)


def selftest_gate(doc, out, receipt=None):
    import copy as _copy
    # The sweep mutates deep copies and calls finish(write=False); hashing the file it read proves the
    # audit is read-only rather than asserting it. C101 learned this the hard way: its first gate mode
    # overwrote the receipt it was auditing.
    before = sha256_file(Path(receipt)) if receipt else None
    results = []
    for inj in GATE_INJECTIONS:
        hit, entry_ok, detail = False, True, ''
        if inj['expect'] in ('DIRECT_RAISE', 'DIRECT_FALSE'):
            try:
                value = inj['direct']()
                hit = (inj['expect'] == 'DIRECT_FALSE' and value is False)
                detail = f'returned {value!r}'
            except Exception as exc:  # noqa: BLE001
                hit = inj['expect'] == 'DIRECT_RAISE'
                detail = f'{type(exc).__name__}: {exc}'
            results.append({'case': inj['case'], 'expect': inj['expect'], 'target': inj['target'],
                            'discriminated': hit, 'why': inj['why'], 'detail': detail})
            continue
        mutated = _copy.deepcopy(doc)
        try:
            inj['apply'](mutated)
            finish(mutated, out=None, live=False, write=False)
        except Exception as exc:  # noqa: BLE001 - the sweep records refusals as verdicts
            entry_ok = False
            detail = f'{type(exc).__name__}: {exc}'
        if inj['expect'] == 'FAILED_LEG':
            hit = inj['target'] in mutated.get('failed_legs', [])
        elif inj['expect'] == 'FAILED_CONTROL':
            hit = inj['target'] in mutated.get('failed_controls', [])
        elif inj['expect'] == 'HARNESS_REFUSAL':
            hit = not entry_ok
        elif inj['expect'] == 'STATUS_NOT_PASS':
            hit = mutated.get('status') != 'PASS_LAUNCHED_BASE_FLAG_CHANNELS'
        else:
            raise ValueError(f'unknown expectation {inj["expect"]}')
        results.append({'case': inj['case'], 'expect': inj['expect'], 'target': inj['target'],
                        'discriminated': hit, 'why': inj['why'], 'detail': detail or None,
                        'failed_legs_under_case': len(mutated.get('failed_legs', [])) if entry_ok
                        else None})
    after = sha256_file(Path(receipt)) if receipt else None
    results.append({'case': 'sweep_writes_no_receipt', 'expect': 'RECEIPT_HASH_UNCHANGED',
                    'target': receipt and str(receipt),
                    'discriminated': bool(before) and before == after,
                    'why': 'the audit runs on the filed artifact, so it may not alter it',
                    'detail': f'{before} -> {after}'})
    report = {'batch': doc['batch'], 'cases': len(results),
              'receipt_sha256_before': before, 'receipt_sha256_after': after,
              'all_discriminated': all(r['discriminated'] for r in results),
              'undiscriminated': [r['case'] for r in results if not r['discriminated']],
              'results': results}
    Path(out).mkdir(parents=True, exist_ok=True)
    (Path(out) / 'gate-falsification.json').write_text(
        json.dumps(report, indent=2, ensure_ascii=False, sort_keys=True) + '\n')
    return report


# ------------------------------------------------------------------------------- manifest
def git_output(cwd, *args) -> str:
    out = subprocess.run(['git', '-C', str(cwd), *args], capture_output=True, text=True)
    return out.stdout.strip()


def new_doc(bins, ctx, harness_sha, bounds, hold, subset) -> dict:
    return {'schema': 'spec0077-c102-base-flag-channels/1', 'batch': BATCH, 'covers': COVERS,
            'tier': 5, 'live_run': True, 'skip_launch': False, 'subset': bool(subset),
            'platform': {'system': sys.platform, 'arch': os.uname().machine,
                         'python': sys.version.split()[0]},
            'binaries': bins,
            'reference': {'go_checkout': 'references/livekit-stable',
                          'go_commit': FIXED_GO_COMMIT,
                          'go_checkout_commit': git_output(REFERENCE, 'rev-parse', 'HEAD'),
                          'upstream_reads': {
                              'cmd/server/main.go': 'base flags at lines 52-101, profile creation at '
                                                    '267-294 after ValidateKeys/LoadTURNSecrets',
                              'pkg/config/config.go': 'IsSet-guarded overrides at 1106-1141',
                              'pkg/service/turn.go': 'tls.LoadX509KeyPair at 222-231',
                              'crates/lk-config/src/cli.rs': 'flag/env table 92-133, application '
                                                             '685-705',
                              'crates/lk-config/src/load.rs': 'node-ip warning at 180',
                              'crates/livecloud/src/turn.rs': 'TLS material checks 22-44'}},
            'harness': str(HERE / 'run-paired.py'), 'harness_sha256_at_start': harness_sha,
            'harness_sha256': harness_sha, 'ports': {k[:-5]: v for k, v in ctx.items()
                                                     if k.endswith('_port')},
            'port_roles': {str(k): v for k, v in ctx['port_roles'].items()},
            'ip_salt': IP_SALT, 'off_limits': list(OFF_LIMITS), 'host_store_port': HOST_STORE_PORT,
            'hold_seconds': hold, 'listeners_bounds': bounds, 'store_flush_ok': True,
            'redis': {'plain_role': 'STORE', 'auth_role': 'AUTH', 'auth_password_role': '<PASS>'},
            'fixtures': {'api_key': API_KEY, 'api_secret': '<SECRET>', 'store_password': '<PASS>',
                         'wrong_password': '<WRONG_PASS>', 'regions': [REGION_A, REGION_B],
                         'cert_subject': 'turn.c102.test', 'cert_der_sha256': ctx['cert_der_sha'],
                         'relay_range_roles': ['<RELAY_LO>', '<RELAY_HI>']},
            'profiles': []}


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument('--record', action='store_true')
    ap.add_argument('--report', help='re-derive the verdict from a filed execution.json')
    ap.add_argument('--selftest-gate', help='apply named mutations to an execution.json')
    ap.add_argument('--out', default='/tmp/c102-lane')
    ap.add_argument('--go', default='/tmp/c99-build/livekit-server-go')
    ap.add_argument('--rust', default=str(REPO / 'target/release/livecloud'))
    ap.add_argument('--only', help='comma-separated profile ids; a subset is never an acceptance')
    ap.add_argument('--hold', type=float, default=0.8)
    a = ap.parse_args()
    out = Path(a.out)
    (out / 'logs').mkdir(parents=True, exist_ok=True)
    harness_sha = sha256_file(Path(__file__).resolve())
    if a.report or a.selftest_gate:
        src = Path(a.report or a.selftest_gate)
        if src.is_dir():
            src = src / 'execution.json'
        doc = json.loads(src.read_text())
        if a.report:
            result = finish(doc, out=None, live=False, write=False)
            (out / 'report-rederivation.txt').write_text(json.dumps(result, indent=2) + '\n')
            print(json.dumps(result['summary'], indent=2))
            return 0 if result['all_ok'] else 1
        report = selftest_gate(doc, out, src)
        print(json.dumps({k: report[k] for k in ('cases', 'all_discriminated', 'undiscriminated')},
                         indent=2))
        return 0 if report['all_discriminated'] else 1
    if not a.record:
        ap.error('choose --record, --report or --selftest-gate')
    bins = {}
    for side, path in (('go', a.go), ('rust', a.rust)):
        p = Path(path)
        if not p.is_file():
            print(f'missing {side} binary: {p}', file=sys.stderr)
            return 2
        bins[side] = {'path': str(p), 'sha256': sha256_file(p)}
    for side, pinned in (('go', ORACLE_SHA256), ('rust', ACCEPTANCE_SHA256)):
        if bins[side]['sha256'] != pinned:
            print(f'refusing: the {side} image is {bins[side]["sha256"]}, pinned {pinned}',
                  file=sys.stderr)
            return 2
    bounds = {str(port): {'before': listeners_on(port), 'after': None}
              for port in list(OFF_LIMITS) + [HOST_STORE_PORT]}
    stores = {}
    ctx = None
    try:
        ctx = build_ctx(out, a.hold)
        masker = Masker(ctx['port_roles'], {str(ctx['work']): '<WORK>', str(REPO): '<REPO>',
                                            str(Path.home()): '<HOME>'})
        profs = profiles(ctx)
        stores['plain'] = start_store(ctx['store_port'])
        stores['auth'] = start_store(ctx['auth_port'], password=STORE_PASS)
        for name, port in (('plain', ctx['store_port']), ('auth', ctx['auth_port'])):
            deadline = time.monotonic() + 15
            while time.monotonic() < deadline and redis_cli(
                    port, 'PING', password=STORE_PASS if name == 'auth' else None) != 'PONG':
                time.sleep(0.2)
        if a.only:
            wanted = set(a.only.split(','))
            profs = [p for p in profs if p['id'] in wanted]
            print(f'development subset: {sorted(wanted)}', flush=True)
        doc = new_doc(bins, ctx, harness_sha, bounds, a.hold, a.only)
        binaries = {side: Path(info['path']) for side, info in bins.items()}
        for prof in profs:
            doc['profiles'].append(run_entry(ctx, prof, masker, binaries))
            print(f"  ran {prof['id']}", flush=True)
        doc['redis_final'] = {'plain': dbsize(ctx['store_port']),
                              'auth': dbsize(ctx['auth_port'], password=STORE_PASS)}
        for key in list(bounds):
            bounds[key]['after'] = listeners_on(int(key))
        result = finish(doc, out=out, live=True)
        (out / 'harness-sha256.txt').write_text(harness_sha + '\n')
        for entry in doc['profiles']:
            for side in ('go', 'rust'):
                log = entry['runs'][side].get('log_masked') or ''
                (out / 'logs' / f"{side}-{entry['profile']}.log").write_text(log)
        (out / 'profiles.json').write_text(json.dumps(
            [{'profile': e['profile'], 'rows': e['rows'], 'claims': e['claims'],
              'claims_go': e.get('claims_go'), 'claims_rust': e.get('claims_rust'),
              'expect_exit': e['expect_exit'], 'refuse_wait': e['refuse_wait'],
              'divergence': e.get('divergence'), 'note': e['note'],
              'argv': [r['argv_masked'] for r in (e['runs']['go'], e['runs']['rust'])]}
             for e in doc['profiles']], indent=2, ensure_ascii=False, sort_keys=True) + '\n')
        print(json.dumps(result['summary'], indent=2))
        return 0 if result['all_ok'] else 1
    finally:
        for proc in stores.values():
            try:
                stop_store(proc, 0)
            except Exception:  # noqa: BLE001
                pass
        if ctx is not None:
            for port in (ctx['store_port'], ctx['auth_port']):
                if listeners_on(port):
                    print(f'warning: port {port} still has a listener after teardown', file=sys.stderr)


if __name__ == '__main__':
    sys.exit(main())
