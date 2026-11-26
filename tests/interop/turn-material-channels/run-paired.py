#!/usr/bin/env python3
"""C104 lane: the embedded-TURN material, port, domain, CIDR and key-map rows, paired on launched
daemons and judged by what the running node's TURN listeners and auth answers actually do.

SPEC-0077 rows driven here (upstream anchors, references/livekit-stable @ e5655ac):

    YAML:turn.tls_port / .udp_port       pkg/service/turn.go:65-80 (NewTurnServer), turn.go:215 log
    YAML:turn.cert_file / .key_file      pkg/service/turn.go newTURNTCPListener (cert loaded only
                                         when !ExternalTLS), cmd/server/main.go `turn-cert`/`turn-key`
    YAML:turn.domain                     pkg/service/turn.go:73-80 + pkg/service/utils.go:156-159
    YAML:turn.external_tls               turn.go (TLS acceptor skipped, raw STUN on the TCP port)
    YAML:turn.relay_range_start / _end / per_user_relay_allocation_limit
                                         pkg/config/config.go:714-721 (defaults) and the values the
                                         live TURN server prints for itself at turn.go:215
    YAML:turn.deny_peer_cidrs / allow_restricted_peer_cidrs
                                         turn.go:84-95 parsePeerCIDRs - "parse ... once at startup so
                                         a malformed entry fails loudly instead of being silently
                                         skipped on every permission decision"
    YAML:keys                            pkg/config/config.go:1135-1158 (unmarshalKeys) - the map the
                                         running node signs and validates tokens against
    generated_flags:--turn.*             pkg/config/config.go:940-1035 GenerateCLIFlags: the flag name
                                         is the YAML path and the env name is LIVEKIT_ + that path,
                                         so `--turn.tls_port` / LIVEKIT_TURN_TLS_PORT are the derived
                                         spellings of the same field the YAML block writes
    CONFIG_BEHAVIOR:Turn.DisabledSkipsInactiveRuntimeConsumers
                                         turn.go:66-69 (`if !turnConf.Enabled { return nil, nil }`)
                                         returns before the port, domain, CIDR and cert consumers

Rust mirrors these in crates/livecloud/src/turn.rs:100-160 (listener list, the same startup line) and
crates/lk-config/src/normalize.rs:893-931 (validate_turn, is_valid_domain) + normalize.rs:712-780
(the `t.enabled` gates on ports, proxy CIDRs and peer CIDRs) reached from boot through
validate_server_wiring.

Nothing here reads a config struct. A channel is claimed by an effect the live daemon produced:

  ports            the TCP listener and the UDP endpoint the process owns while it is alive (lsof),
                   the STUN Binding Success Response that comes back on the UDP port, and the fact
                   that a losing port - the YAML one a flag beat, or a 0 the flag supplied - is bound
                   in neither socket family.
  cert/key         the leaf certificate the TLS port presents during a real handshake, hashed over
                   its DER, compared against the two fixture certificates. A log line saying a file
                   was loaded proves nothing; the digest the server hands a client does.
  external_tls     the same plaintext STUN Binding sent to the same TCP port: under internal TLS the
                   port is a TLS listener (Go goes silent, Rust answers a 7-byte TLS alert) and with
                   external_tls both servers answer the Binding, which is what makes that row a
                   listener behaviour rather than a printed flag.
  domain / cidrs   the refusal sentence, the raw exit code, and that nothing was bound afterwards.
  keys             which secret the running node accepts on POST /rtc/v1/validate: a token the key
                   set signs reaches the handler (400 "join_request is required"), one it does not is
                   refused (401 "invalid API key"). Both cells per row are required.
  enabled          the socket table again: the flag has to remove and restore the same two listeners
                   the YAML field does, in both directions.

Two things this lane expected one way and measured the other, so they are rows rather than notes:

  `--turn.enabled false` (space, not equals) does not switch TURN off on either binary - a bool flag
  does not consume the following word, so both daemons keep both listeners. The equals form does.
  Feeding the space form over a config with no domain looked like "the flag was ignored, then the
  domain gate fired", which is how the earlier draft of these profiles got the order wrong.

  Bad bytes do not produce the same sentence on the two sides. With the same broken TURN block Go
  reports the domain and Rust reports the proxy-CIDR requirement: both exit non-zero and bind
  nothing, but the first gate to speak differs, so the sentences are pinned as a divergence instead
  of averaged into "startup validation rejects a bad TURN block".

Modes:
  --record        run every profile against both pinned binaries and write the receipt.
  --report        re-derive the verdict from a filed execution.json without launching anything.
  --selftest-gate apply one named mutation to a recorded manifest and require the gate to redden.
  --only          development subset; it reports PARTIAL_SUBSET_NOT_ACCEPTANCE, never a pass.

Primitives are imported, not copied: the launcher, port allocator, socket observers, owned store and
address masker come from the accepted C102 lane, and the certificate/key generators, the auth probe
and the sentence reader from the accepted C103 lane. Importing leaves both files byte-identical, so
their filed receipts stay re-derivable.
"""
from __future__ import annotations

import argparse
import importlib.util
import json
import os
import re
import socket
import struct
import sys
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
REPO = HERE.parents[2]
C102_DIR = REPO / 'tests/interop/base-flag-channels'
C103_DIR = REPO / 'tests/interop/generated-flag-channels'


def _load(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, str(path))
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


C102 = _load('c102_harness', C102_DIR / 'run-paired.py')
C103 = _load('c103_harness', C103_DIR / 'run-paired.py')

BATCH = 'C104-20261127-turn-material-channels'
SCHEMA = 'spec0077-c104-turn-material-channels/1'
IP_SALT = 'c104-turn'
ORACLE_SHA256 = C102.ORACLE_SHA256
ACCEPTANCE_SHA256 = C102.ACCEPTANCE_SHA256
FIXED_GO_COMMIT = C102.FIXED_GO_COMMIT
REFERENCE = C102.REFERENCE
OFF_LIMITS = C102.OFF_LIMITS
HOST_STORE_PORT = C102.HOST_STORE_PORT
C102_FILED_HARNESS_SHA = 'fff0615af856f95fc329a5aee3bbafcae2ce795474edde88176c27ca10086a22'
C103_FILED_HARNESS_SHA = '93abcdd655d8638b9d4a698e6858018f6b70eeb1a83d8e67fb5c0e98836b3f37'
STATUS_PASS = 'PASS_LAUNCHED_TURN_MATERIAL_CHANNELS'
PROFILE_COUNT = 34
COVERS = [
    'YAML:turn.tls_port', 'YAML:turn.udp_port', 'YAML:turn.cert_file', 'YAML:turn.key_file',
    'YAML:turn.domain', 'YAML:turn.external_tls', 'YAML:turn.relay_range_start',
    'YAML:turn.relay_range_end', 'YAML:turn.per_user_relay_allocation_limit',
    'YAML:turn.deny_peer_cidrs', 'YAML:turn.allow_restricted_peer_cidrs', 'YAML:keys',
    'generated_flags:--turn.tls_port', 'generated_flags:--turn.udp_port',
    'generated_flags:--turn.cert_file', 'generated_flags:--turn.key_file',
    'generated_flags:--turn.domain', 'generated_flags:--turn.external_tls',
    'generated_flags:--turn.relay_range_start', 'generated_flags:--turn.relay_range_end',
    'generated_flags:--turn.per_user_relay_allocation_limit',
    'generated_flags:--turn.proxy_protocol', 'generated_flags:--turn.enabled',
    'generated_flags:--turn.ttl_seconds', 'YAML:turn.ttl_seconds',
    'CONFIG_BEHAVIOR:Turn.DisabledSkipsInactiveRuntimeConsumers',
]

# Fixture vocabulary. Every value is this lane's own invention; the secrets are masked, the key ids
# stay readable because they are the claim ("which kid the running node accepts").
DOMAIN = 'turn.c104.test'
BAD_DOMAIN = 'turn_bad'
YAML_KEYS = {'k104y': 'c104yamlinekeysecret000123456789ab'}
ENV_KEYS = {'k104e': 'c104envlinekeysecret0000123456789abc'}
NUMERIC_KID = 'k104n'
FOREIGN_KID = 'c103y'          # a C103-vocabulary kid that this lane's configs never carry
KEYS_SECRET_ALIASES = [(YAML_KEYS['k104y'], 'SEC-Y'), (ENV_KEYS['k104e'], 'SEC-E')]
SIGNABLE = dict(YAML_KEYS) | dict(ENV_KEYS) | {FOREIGN_KID: C103.YAML_KEYS[FOREIGN_KID],
                                               NUMERIC_KID: '12345'}
ACCEPTED = C103.ACCEPTED       # 'handler'
REJECTED = C103.REJECTED       # 'bad-key'

# Relay-range numbers are configuration, not sockets this lane binds, so they are written verbatim
# into the YAML and read back verbatim from the running server's own startup line.
RELAY_DEFAULT = (30000, 40000, 12)
RELAY_EXPLICIT = (45000, 45002, 3)
RELAY_FLAGGED = (46100, 46102, 5)

# Needles, all in the masked vocabulary the receipt keeps (`<TLS1>` ... `<WORK>`).
SERVER_READY = C103.SERVER_READY
TURN_START = 'Starting TURN server'
TURN_STOP_RUST = 'TURN server stopped'
KEY_MISMATCH = C103.KEY_MISMATCH
DOMAIN_REQUIRED = 'TURN domain required'
DOMAIN_INCORRECT = 'TURN domain is not correct'
INVALID_PORTS = 'invalid TURN ports'
PROXY_NEEDS_CIDRS = ('turn.proxy_protocol requires at least one entry in '
                     'turn.proxy_protocol_trusted_cidrs')
CERT_LOADED_RUST = 'could not configure TURN/TLS: could not read TLS certificate/key'
NO_SUCH_FILE = C103.NO_SUCH_FILE
NO_SUCH_FILE_RUST = 'No such file or directory'   # the same errno, capitalised by the stdlib
TTL_WARN_RUST = 'turn.ttl_seconds out of range, using safe value'
AUTH_LOG_NEEDLE = C103.AUTH_LOG_NEEDLE
DENY_CIDR = 'invalid CIDR "not-a-cidr" in turn.deny_peer_cidrs: invalid CIDR address: not-a-cidr'
ALLOW_CIDR = ('invalid CIDR "not-a-cidr" in turn.allow_restricted_peer_cidrs: '
              'invalid CIDR address: not-a-cidr')
PORT_RANGE_RUST = 'invalid configuration "turn.tls_port": 70000 is not a valid port'
PORT_RANGE_GO = 'could not listen on TURN TCP port: listen tcp: address 70000: invalid port'


class Masker(C103.Masker):
    """C103's masker, plus this lane's key secrets and a token for the unallocated source ports its
    own probes bind to. Every allocated port is already a role token by the time this runs, so any
    digit left behind after it is a socket number no claim reads; it still gets a token so the
    stored streams carry nothing that varies per connection."""

    def secrets(self, text: str) -> str:
        text = super().secrets(text)
        for real, alias in KEYS_SECRET_ALIASES:
            text = text.replace(real, f'<{alias}>')
        return text

    def text(self, text: str, keep_node_ids: bool = True) -> str:
        text = super().text(text, keep_node_ids=keep_node_ids)
        text = re.sub(r'127\.0\.0\.1:(\d{2,5})', '127.0.0.1:<EPHEM>', text)
        text = re.sub(r'\[::1\]:(\d{2,5})', '[::1]:<EPHEM>', text)
        return text


# --------------------------------------------------------------------------- STUN wire probes
STUN_MAGIC = 0x2112A442


def binding_packet() -> tuple[bytes, bytes]:
    tid = os.urandom(12)
    return struct.pack('>HHI', 0x0001, 0, STUN_MAGIC) + tid, tid


def _attr_types(data: bytes) -> list:
    out, off = [], 20
    while off + 4 <= len(data):
        atype, alen = struct.unpack('>HH', data[off:off + 4])
        out.append(atype)
        off += 4 + alen + (alen % 4 and (4 - alen % 4) or 0)
    return out


def err_kind(exc: OSError) -> str:
    """A stable name for what the socket did, so a probe failure reads as an observation rather than
    as a Python class name. errno 54/53/32 are the loopback reset/abort/broken-pipe trio on macOS."""
    if isinstance(exc, ConnectionRefusedError):
        return 'refused'
    if isinstance(exc, TimeoutError):
        return 'timeout'
    if getattr(exc, 'errno', None) in (32, 53, 54):
        return 'reset'
    return f'errno-{getattr(exc, "errno", None)}'


def stun_udp(port: int, src: int, timeout: float = 2.0) -> dict:
    """A Binding Request to the UDP listener, sent from a port this lane allocated so the peer address Both servers answer it as TURN-over-STUN, so the
    the daemon logs carries a role token. Both servers answer it as TURN-over-STUN, so the reply is a
    live-consumer effect; only the message type, the attribute *types* and the length are kept - the
    XOR-MAPPED-ADDRESS value names the client socket and is never recorded."""
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.settimeout(timeout)
    pkt, tid = binding_packet()
    try:
        sock.bind(('127.0.0.1', src))
        sock.sendto(pkt, ('127.0.0.1', port))
        data, _ = sock.recvfrom(2048)
    except OSError as exc:
        return {'kind': err_kind(exc), 'bytes': 0, 'stage': 'probe'}
    finally:
        sock.close()
    if len(data) < 20:
        return {'kind': 'short', 'bytes': len(data)}
    mtype = struct.unpack('>H', data[:2])[0]
    if mtype != 0x0101:
        return {'kind': 'other', 'type': hex(mtype), 'bytes': len(data)}
    return {'kind': 'answered', 'type': '0x101', 'tid_ok': data[8:20] == tid,
            'attrs': sorted(hex(t) for t in _attr_types(data)), 'bytes': len(data)}


def stun_tcp(port: int, timeout: float = 2.5) -> dict:
    """The same Binding over the TCP port. Against a TLS listener this never reaches STUN, and the
    two servers react differently in a way that is itself the finding; against a raw-TURN listener
    (external_tls) both answer, which is what separates that row from a printed flag. The source port
    is deliberately left to the kernel: pinning it leaves the socket in TIME_WAIT and the next probe
    fails with EADDRINUSE, which would be a harness artefact rather than a server behaviour. The
    address that number reaches the daemon log in is normalised to <EPHEM> by the masker."""
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(timeout)
    try:
        sock.connect(('127.0.0.1', port))
        pkt, tid = binding_packet()
        sock.sendall(pkt)
        data = sock.recv(2048)
    except (ConnectionRefusedError, TimeoutError, OSError) as exc:
        return {'kind': err_kind(exc), 'bytes': 0, 'stage': 'connect-or-send'}
    except Exception as exc:  # noqa: BLE001
        return {'kind': f'{type(exc).__name__}'.lower(), 'bytes': 0, 'stage': 'other'}
    finally:
        sock.close()
    if not data:
        return {'kind': 'closed', 'bytes': 0}
    if data[0] == 0x15:
        return {'kind': 'tls-alert', 'bytes': len(data), 'alert': data[-2:].hex()}
    if len(data) < 20:
        return {'kind': 'short', 'bytes': len(data)}
    mtype = struct.unpack('>H', data[:2])[0]
    return {'kind': 'stun', 'type': hex(mtype), 'tid_ok': data[8:20] == tid,
            'attrs': sorted(hex(t) for t in _attr_types(data)), 'bytes': len(data)}


def leaf_class(probe: dict, certs: dict) -> str:
    """Name the certificate the port actually presented, from its DER digest."""
    digest = (probe or {}).get('leaf_sha256_der')
    if not (probe or {}).get('connected'):
        return 'none'
    for name, info in certs.items():
        if info['cert_der'] == digest:
            return name
    return 'other'


def auth_probe104(port: int, kids: list, masker: Masker) -> dict:
    out = {}
    for kid in kids:
        probe = C103.validate_probe(port, C103.jwt(kid, SIGNABLE[kid]))
        out[kid] = {'verdict': C103.classify(probe), 'status': probe['status'],
                    'body_masked': masker.text(probe['body'])[:120]}
    return out


# ------------------------------------------------------------------------------- fixtures
def build_ctx(out_dir: Path, hold: float) -> dict:
    used = set()
    names = ('http', 'store', 'tls1', 'tls2', 'tu1', 'tu2', 'pu', 'pt')
    ports = {name: C102.alloc_port(used) for name in names}
    port_roles = {ports[name]: name.upper() for name in names}
    work = out_dir / 'run'
    for sub in ('configs', 'logs', 'fixtures'):
        (work / sub).mkdir(parents=True, exist_ok=True)
    certs = {'A': C103.gen_cert(work / 'fixtures', 'c104a'),
             'B': C103.gen_cert(work / 'fixtures', 'c104b')}
    return dict(work=work, fx=work / 'fixtures', hold=hold, port_roles=port_roles, used=used,
                certs=certs, **{f'{k}_port': v for k, v in ports.items()})


def base_cfg(ctx) -> str:
    body = (f'port: {ctx["http_port"]}\n'
            f'bind_addresses: [127.0.0.1]\n'
            f'redis:\n  address: 127.0.0.1:{ctx["store_port"]}\n')
    body += 'keys:\n' + ''.join(f'  {k}: {v}\n' for k, v in YAML_KEYS.items())
    body += 'rtc:\n  tcp_port: 0\n'
    return body


ROLES = (('tls1', 'TLS1'), ('tu1', 'TU1'), ('tls2', 'TLS2'), ('tu2', 'TU2'))


def port_of(ctx, value):
    """A profile may name a role ('TLS1') or a literal port (0, 70000) for the YAML line."""
    if isinstance(value, str):
        return ctx[f'{value.lower()}_port']
    return value


def turn_cfg(ctx, enabled=True, domain=DOMAIN, tls='TLS1', tu='TU1', material='A',
             extra='') -> str:
    """The YAML block under test. `tls`/`tu`/`domain`/`material` take a value or None to omit the
    line, so a profile can put the losing number in the YAML and the winning one on argv."""
    body = base_cfg(ctx)
    lines = ['turn:\n', f'  enabled: {str(enabled).lower()}\n']
    if domain is not None:
        lines.append(f'  domain: {domain}\n')
    if tls is not None:
        lines.append(f'  tls_port: {port_of(ctx, tls)}\n')
    if tu is not None:
        lines.append(f'  udp_port: {port_of(ctx, tu)}\n')
    if material is not None:
        cert = ctx['certs'][material]
        lines.append(f'  cert_file: {cert["cert"]}\n  key_file: {cert["key"]}\n')
    return body + ''.join(lines) + extra


def garbage_turn(ctx) -> str:
    """TURN bytes that are invalid on every axis this lane can see: a proxy gate with no trusted
    CIDRs, an unparseable peer CIDR and a certificate file that does not exist."""
    return ('  proxy_protocol: true\n  proxy_protocol_trusted_cidrs: []\n'
            '  deny_peer_cidrs: ["not-a-cidr"]\n'
            f'  cert_file: {ctx["fx"] / "absent-cert.pem"}\n'
            f'  key_file: {ctx["fx"] / "absent-key.pem"}\n')


# ------------------------------------------------------------------------------- profiles
def profiles(ctx) -> list:
    out = []
    tls1, tls2 = ctx['tls1_port'], ctx['tls2_port']
    tu1, tu2 = ctx['tu1_port'], ctx['tu2_port']
    r_tls1, r_tls2 = '<TLS1>', '<TLS2>'
    r_tu1, r_tu2 = '<TU1>', '<TU2>'
    certs = ctx['certs']
    all_turn_ports = [('tls1', ctx['tls1_port']), ('tu1', ctx['tu1_port']),
                      ('tls2', ctx['tls2_port']), ('tu2', ctx['tu2_port'])]

    def add(pid, rows, claims, argv=(), env=None, cfg=None, expect_exit=0, refuse_wait=None,
            note='', ready=True):
        out.append({'id': pid, 'rows': rows, 'claims': claims, 'argv': list(argv),
                    'env': dict(env or {}), 'cfg': cfg if cfg is not None else turn_cfg(ctx),
                    'expect_exit': expect_exit, 'refuse_wait': refuse_wait,
                    'probe_ports': all_turn_ports, 'note': note, 'ready': ready})

    def listener_claims(tls_role=r_tls1, tu_role=r_tu1, leaf='A', external=False,
                        stun_tcp_go='timeout', stun_tcp_rust='tls-alert', with_auth=True):
        """The claim set a *started* TURN profile has to satisfy: both listeners owned by the live
        process, the other two ports bound in neither family, the leaf the port actually presented,
        a Binding answered on UDP, and the TCP port not answering plaintext STUN."""
        claims = {'log_all': [TURN_START, SERVER_READY],
                  'log_not': [DOMAIN_REQUIRED, DOMAIN_INCORRECT, INVALID_PORTS],
                  'tcp_has': [tls_role], 'udp_has': [tu_role],
                  'tcp_not': [r for r in (r_tls1, r_tls2) if r != tls_role],
                  'udp_not': [r for r in (r_tu1, r_tu2) if r != tu_role],
                  'release_ports': True, 'node_alive': 1, 'leaf': leaf,
                  'stun_udp': 'answered', 'stun_tcp_go': stun_tcp_go,
                  'stun_tcp_rust': stun_tcp_rust}
        if with_auth:
            claims['auth'] = {'k104y': ACCEPTED, FOREIGN_KID: REJECTED}
        if external:
            claims['stun_tcp_go'] = 'stun'
            claims['stun_tcp_rust'] = 'stun'
            claims['tls_connected'] = False
        else:
            claims['tls_connected'] = leaf != 'none'
        return claims

    # -------------------------------------------------------------- the two listeners and the leaf
    add('turn-yaml-ports-and-leaf',
        ['YAML:turn.tls_port', 'YAML:turn.udp_port', 'YAML:turn.cert_file', 'YAML:turn.key_file',
         'YAML:turn.domain', 'YAML:keys'],
        listener_claims(),
        note='the YAML pair is what the live listeners are: TLS1/TU1 bound, TLS2/TU2 nowhere, leaf A')

    add('turn-yaml-material-b',
        ['YAML:turn.cert_file', 'YAML:turn.key_file'],
        listener_claims(leaf='B'),
        cfg=turn_cfg(ctx, material='B'),
        note='the same channel with the other certificate: the digest is what discriminates')

    # ------------------------------------------------------------- generated flags beat the YAML
    add('turn-flag-ports-beat-yaml',
        ['generated_flags:--turn.tls_port', 'generated_flags:--turn.udp_port'],
        listener_claims(),
        argv=['--turn.tls_port', str(tls1), '--turn.udp_port', str(tu1)],
        cfg=turn_cfg(ctx, tls=tls2, tu=tu2),
        note='YAML names the losing ports, so neither may appear in the socket table')

    add('turn-env-ports-beat-yaml',
        ['generated_flags:--turn.tls_port', 'generated_flags:--turn.udp_port'],
        listener_claims(),
        env={'LIVEKIT_TURN_TLS_PORT': str(tls1), 'LIVEKIT_TURN_UDP_PORT': str(tu1)},
        cfg=turn_cfg(ctx, tls=tls2, tu=tu2),
        note='the env spelling GenerateCLIFlags derived (LIVEKIT_ + the YAML path) reaches the same '
             'listeners')

    add('turn-flag-zero-port-beats-yaml',
        ['generated_flags:--turn.tls_port', 'YAML:turn.tls_port'],
        {'log_all': [TURN_START, SERVER_READY], 'udp_has': [r_tu1], 'udp_not': [r_tu2],
         'tcp_not': [r_tls1, r_tls2],
         'leaf': 'none', 'tls_connected': False, 'stun_udp': 'answered',
         'stun_tcp_go': 'refused', 'stun_tcp_rust': 'refused', 'release_ports': True,
         'node_alive': 1, 'auth': {'k104y': ACCEPTED}},
        argv=['--turn.tls_port', '0'],
        cfg=turn_cfg(ctx, tls=tls2),
        note='0 is a value rather than an absence: the flag 0 removes the TLS listener the YAML asked '
             'for')

    add('turn-udp-only-no-tls-listener',
        ['YAML:turn.tls_port', 'YAML:turn.udp_port', 'YAML:turn.cert_file', 'YAML:turn.domain'],
        {'log_all': [TURN_START, SERVER_READY], 'udp_has': [r_tu1], 'udp_not': [r_tu2],
         'tcp_not': [r_tls1, r_tls2],
         'leaf': 'none', 'tls_connected': False, 'stun_udp': 'answered',
         'stun_tcp_go': 'refused', 'stun_tcp_rust': 'refused', 'release_ports': True,
         'node_alive': 1, 'auth': {'k104y': ACCEPTED}},
        cfg=turn_cfg(ctx, tls=0, domain=None, material=None),
        note='tls_port 0 with no domain and no certificate still starts: both consumers are gated on '
             'a TLS listener existing')

    add('turn-invalid-ports-refusal', ['YAML:turn.tls_port', 'YAML:turn.udp_port'],
        {'log_all': [INVALID_PORTS], 'log_not': [TURN_START, SERVER_READY], 'node_alive': 0,
         'store_no_node_after': True},
        expect_exit='nonzero', refuse_wait=10.0,
        cfg=turn_cfg(ctx, tls=0, tu=0),
        note='NewTurnServer returns before any listener when neither port is usable')

    # ------------------------------------------------------------------- the material channels
    add('turn-flag-material-beats-yaml',
        ['generated_flags:--turn.cert_file', 'generated_flags:--turn.key_file'],
        listener_claims(leaf='A'),
        argv=['--turn.cert_file', str(certs['A']['cert']), '--turn.key_file', str(certs['A']['key'])],
        cfg=turn_cfg(ctx, material='B'),
        note='YAML carries a valid B pair, argv carries A: the leaf the port presents names the winner')

    add('turn-flag-material-mismatch',
        ['generated_flags:--turn.cert_file', 'generated_flags:--turn.key_file'],
        {'log_any': [KEY_MISMATCH, 'keys may not be consistent', CERT_LOADED_RUST],
         'log_not': [TURN_START, SERVER_READY], 'node_alive': 0, 'store_no_node_after': True},
        expect_exit='nonzero', refuse_wait=10.0,
        argv=['--turn.cert_file', str(certs['B']['cert']), '--turn.key_file', str(certs['A']['key'])],
        cfg=turn_cfg(ctx, material='A'),
        note='a crossed flag pair must not start a listener on either spelling; the two sides word the '
             'same refusal differently, so the needle set covers both')

    add('turn-flag-missing-files', ['generated_flags:--turn.cert_file',
                                    'generated_flags:--turn.key_file'],
        {'log_any': [NO_SUCH_FILE, NO_SUCH_FILE_RUST], 'log_not': [TURN_START, SERVER_READY],
         'node_alive': 0, 'store_no_node_after': True},
        expect_exit='nonzero', refuse_wait=10.0,
        argv=['--turn.cert_file', str(ctx['fx'] / 'absent-cert.pem'),
              '--turn.key_file', str(ctx['fx'] / 'absent-key.pem')],
        cfg=turn_cfg(ctx, material=None),
        note='the missing-file path: the refusal has to name a file and bind nothing')

    # ----------------------------------------------------------------------- external_tls
    add('turn-external-tls-yaml', ['YAML:turn.external_tls', 'YAML:turn.cert_file'],
        listener_claims(leaf='none', external=True),
        cfg=turn_cfg(ctx, material=None, extra='  external_tls: true\n'),
        note='no certificate is loaded and the TCP port answers a plaintext Binding instead of a '
             'handshake - that is the external_tls behaviour, not a log line')

    add('turn-external-tls-flag', ['generated_flags:--turn.external_tls',
                                   'YAML:turn.external_tls'],
        listener_claims(leaf='none', external=True),
        argv=['--turn.external_tls', 'true'],
        cfg=turn_cfg(ctx, material=None),
        note='the generated spelling of the same field produces the same raw-TURN TCP listener')

    add('turn-external-tls-with-cert', ['YAML:turn.external_tls'],
        listener_claims(leaf='none', external=True),
        cfg=turn_cfg(ctx, extra='  external_tls: true\n'),
        note='material present but external_tls true: the listener stays raw, so the digest must not '
             'appear on the wire')

    # ----------------------------------------------------------------------- the domain gate
    add('turn-domain-required-refusal', ['YAML:turn.domain'],
        {'log_all': [DOMAIN_REQUIRED], 'log_not': [TURN_START, SERVER_READY], 'node_alive': 0,
         'store_no_node_after': True},
        expect_exit='nonzero', refuse_wait=10.0,
        cfg=turn_cfg(ctx, domain=None),
        note='tls_port > 0 with no domain is refused by NewTurnServer before a listener exists')

    add('turn-domain-incorrect-refusal', ['YAML:turn.domain'],
        {'log_all': [DOMAIN_INCORRECT], 'log_not': [TURN_START, SERVER_READY], 'node_alive': 0,
         'store_no_node_after': True},
        expect_exit='nonzero', refuse_wait=10.0,
        cfg=turn_cfg(ctx, domain=BAD_DOMAIN),
        note='utils.go:156 IsValidDomain is the predicate; the sentence is the observable')

    add('turn-flag-empty-domain-beats-yaml', ['generated_flags:--turn.domain', 'YAML:turn.domain'],
        {'log_all': [DOMAIN_REQUIRED], 'log_not': [TURN_START], 'node_alive': 0,
         'store_no_node_after': True},
        expect_exit='nonzero', refuse_wait=10.0,
        argv=['--turn.domain', ''],
        cfg=turn_cfg(ctx, domain=DOMAIN),
        note='an empty flag value over a valid YAML domain: the flag is applied, not ignored')

    # ------------------------------------------------------------------------- peer CIDR lists
    add('turn-deny-cidr-refusal', ['YAML:turn.deny_peer_cidrs'],
        {'log_all': [DENY_CIDR], 'log_not': [TURN_START, SERVER_READY], 'node_alive': 0,
         'store_no_node_after': True},
        expect_exit='nonzero', refuse_wait=10.0,
        cfg=turn_cfg(ctx, extra='  deny_peer_cidrs: ["not-a-cidr"]\n'),
        note='turn.go:84-95 parses the lists at startup so a bad entry fails loudly')

    add('turn-allow-cidr-refusal', ['YAML:turn.allow_restricted_peer_cidrs'],
        {'log_all': [ALLOW_CIDR], 'log_not': [TURN_START, SERVER_READY], 'node_alive': 0,
         'store_no_node_after': True},
        expect_exit='nonzero', refuse_wait=10.0,
        cfg=turn_cfg(ctx, extra='  allow_restricted_peer_cidrs: ["not-a-cidr"]\n'),
        note='the same parse for the allow list')

    add('turn-cidr-lists-valid', ['YAML:turn.deny_peer_cidrs',
                                  'YAML:turn.allow_restricted_peer_cidrs'],
        listener_claims(),
        cfg=turn_cfg(ctx, extra='  allow_restricted_peer_cidrs: ["198.51.100.0/24"]\n'
                                '  deny_peer_cidrs: ["192.0.2.0/24"]\n'),
        note='the control that keeps the two refusal rows from meaning "any list fails"')

    # --------------------------------------------------------------------- relay range and limit
    def echo(key, value):
        return f'{key}": {value}'

    add('turn-relay-default-numbers',
        ['YAML:turn.relay_range_start', 'YAML:turn.relay_range_end',
         'YAML:turn.per_user_relay_allocation_limit'],
        dict(listener_claims(), log_all=[TURN_START, SERVER_READY,
                                         echo('relay_range_start', RELAY_DEFAULT[0]),
                                         echo('relay_range_end', RELAY_DEFAULT[1]),
                                         echo('per_user_relay_allocation_limit', RELAY_DEFAULT[2])]),
        note='no relay keys in the config: the running TURN server prints the range it was built with')

    add('turn-relay-explicit-numbers',
        ['YAML:turn.relay_range_start', 'YAML:turn.relay_range_end',
         'YAML:turn.per_user_relay_allocation_limit'],
        dict(listener_claims(), log_all=[echo('relay_range_start', RELAY_EXPLICIT[0]),
                                         echo('relay_range_end', RELAY_EXPLICIT[1]),
                                         echo('per_user_relay_allocation_limit', RELAY_EXPLICIT[2])],
             log_not=[echo('relay_range_start', RELAY_DEFAULT[0]),
                      DOMAIN_REQUIRED, INVALID_PORTS]),
        cfg=turn_cfg(ctx, extra=f'  relay_range_start: {RELAY_EXPLICIT[0]}\n'
                                f'  relay_range_end: {RELAY_EXPLICIT[1]}\n'
                                f'  per_user_relay_allocation_limit: {RELAY_EXPLICIT[2]}\n'),
        note='the same three keys move with the config, so the echo is not a constant')

    add('turn-relay-flag-numbers',
        ['generated_flags:--turn.relay_range_start', 'generated_flags:--turn.relay_range_end',
         'generated_flags:--turn.per_user_relay_allocation_limit'],
        dict(listener_claims(), log_all=[echo('relay_range_start', RELAY_FLAGGED[0]),
                                         echo('relay_range_end', RELAY_FLAGGED[1]),
                                         echo('per_user_relay_allocation_limit', RELAY_FLAGGED[2])],
             log_not=[echo('relay_range_start', RELAY_EXPLICIT[0])]),
        argv=['--turn.relay_range_start', str(RELAY_FLAGGED[0]),
              '--turn.relay_range_end', str(RELAY_FLAGGED[1]),
              '--turn.per_user_relay_allocation_limit', str(RELAY_FLAGGED[2])],
        cfg=turn_cfg(ctx, extra=f'  relay_range_start: {RELAY_EXPLICIT[0]}\n'
                                f'  relay_range_end: {RELAY_EXPLICIT[1]}\n'
                                f'  per_user_relay_allocation_limit: {RELAY_EXPLICIT[2]}\n'),
        note='the generated flag spellings reach the same three numbers on the live server')

    # ---------------------------------------------------------------------------------- keys
    add('turn-keys-yaml-baseline', ['YAML:keys'],
        dict(listener_claims(with_auth=False),
             auth={'k104y': ACCEPTED, FOREIGN_KID: REJECTED, 'k104e': REJECTED}),
        note='the YAML map is the key set the node validates against')

    add('turn-keys-env-replaces-yaml', ['YAML:keys'],
        dict(listener_claims(with_auth=False),
             auth={'k104y': REJECTED, 'k104e': ACCEPTED, FOREIGN_KID: REJECTED}),
        env={'LIVEKIT_KEYS': f'k104e: {ENV_KEYS["k104e"]}'},
        note='config.go:1135-1158 unmarshals the string over the map and reassigns conf.Keys, so the '
             'YAML kid is gone rather than merged')

    add('turn-keys-env-numeric-secret', ['YAML:keys'],
        dict(listener_claims(with_auth=False),
             auth={'k104e': ACCEPTED, NUMERIC_KID: REJECTED, 'k104y': REJECTED}),
        env={'LIVEKIT_KEYS': f'k104e: {ENV_KEYS["k104e"]}\nk104n: 12345\n'},
        note='config.go:1145-1157 unmarshals the env string into map[string]any and keeps only the '
             'string-valued entries, so a numeric secret is dropped rather than stringified')

    # ------------------------------------------------------------------- the disabled short-circuit
    garbage = garbage_turn(ctx)
    add('turn-disabled-skips-consumers',
        ['CONFIG_BEHAVIOR:Turn.DisabledSkipsInactiveRuntimeConsumers', 'YAML:turn.tls_port'],
        {'log_all': [SERVER_READY], 'log_not': [TURN_START, DENY_CIDR, DOMAIN_REQUIRED],
         'tcp_not': [r_tls1, r_tls2], 'udp_not': [r_tu1, r_tu2], 'node_alive': 1,
         'auth': {'k104y': ACCEPTED}, 'release_ports': True, 'leaf': 'none',
         'tls_connected': False, 'stun_udp': 'timeout', 'stun_tcp_go': 'refused',
         'stun_tcp_rust': 'refused'},
        cfg=turn_cfg(ctx, enabled=False, tls=tls1, tu=tu1, domain=None, material=None,
                     extra=garbage),
        note='every invalid value is present but enabled is false, so NewTurnServer returns nil nil')

    add('turn-enabled-same-garbage-refuses',
        ['CONFIG_BEHAVIOR:Turn.DisabledSkipsInactiveRuntimeConsumers'],
        {'log_any': [DENY_CIDR, DOMAIN_REQUIRED, PROXY_NEEDS_CIDRS],
         'log_not': [SERVER_READY], 'node_alive': 0, 'store_no_node_after': True},
        expect_exit='nonzero', refuse_wait=10.0,
        cfg=turn_cfg(ctx, enabled=True, tls=tls1, tu=tu1, domain=None, material=None,
                     extra=garbage),
        note='the pair that makes the row above falsifiable: flip enabled and the same bytes refuse. '
             'Which gate speaks first is the divergence the control below pins.')

    add('turn-flag-disabled-equals-skips-consumers',
        ['generated_flags:--turn.enabled',
         'CONFIG_BEHAVIOR:Turn.DisabledSkipsInactiveRuntimeConsumers'],
        {'log_all': [SERVER_READY], 'log_not': [TURN_START],
         'tcp_not': [r_tls1, r_tls2], 'udp_not': [r_tu1, r_tu2], 'node_alive': 1,
         'auth': {'k104y': ACCEPTED}, 'release_ports': True, 'leaf': 'none',
         'tls_connected': False, 'stun_udp': 'timeout', 'stun_tcp_go': 'refused',
         'stun_tcp_rust': 'refused'},
        argv=['--turn.enabled=false'],
        cfg=turn_cfg(ctx),
        note='a valid TURN block switched off by the flag: neither configured listener exists, so the '
             'flag reaches the field the consumer reads')

    add('turn-flag-enabled-true-beats-yaml-false',
        ['generated_flags:--turn.enabled',
         'CONFIG_BEHAVIOR:Turn.DisabledSkipsInactiveRuntimeConsumers'],
        listener_claims(),
        argv=['--turn.enabled=true'],
        cfg=turn_cfg(ctx, enabled=False),
        note='the opposite direction over the same bytes: the flag switches the consumers back on, so '
             'the pair measures precedence rather than a default')

    add('turn-flag-space-value-keeps-turn-on',
        ['generated_flags:--turn.enabled'],
        listener_claims(),
        argv=['--turn.enabled', 'false'],
        cfg=turn_cfg(ctx),
        note='measured, not assumed: a bool flag written with a space does not consume the word, so '
             'both daemons keep both listeners - the equals form above is what applies')

    # --------------------------------------------------------------------------- out-of-range
    add('turn-port-out-of-range', ['YAML:turn.tls_port'],
        {'log_not': [TURN_START], 'node_alive': 0, 'store_no_node_after': True},
        expect_exit='nonzero', refuse_wait=10.0,
        cfg=turn_cfg(ctx, tls=70000),
        note='the two sides refuse at different stages and in different words, so the sentence is '
             'recorded rather than compared')

    # ---------------------------------------------------------------------------------- ttl
    add('turn-ttl-clamped-flag', ['generated_flags:--turn.ttl_seconds', 'YAML:turn.ttl_seconds'],
        dict(listener_claims(), log_all_rust=[TTL_WARN_RUST], log_not_go=[TTL_WARN_RUST]),
        argv=['--turn.ttl_seconds', '999999'],
        note='both daemons start with the clamped value; only Rust still has a logger attached, which '
             'is the divergence this row records instead of claiming')

    add('turn-ttl-nonpositive-yaml', ['YAML:turn.ttl_seconds'],
        dict(listener_claims(), log_all_rust=[TTL_WARN_RUST], log_not_go=[TTL_WARN_RUST]),
        cfg=turn_cfg(ctx, extra='  ttl_seconds: -5\n'),
        note='the lower half of the same clamp; no credential expiry was driven, so the row stops at '
             'component behaviour')

    # ------------------------------------------------------------------- proxy on the flag path
    add('turn-proxy-flag-needs-cidrs', ['generated_flags:--turn.proxy_protocol'],
        {'log_all': [PROXY_NEEDS_CIDRS], 'log_not': [TURN_START, SERVER_READY], 'node_alive': 0,
         'store_no_node_after': True},
        expect_exit='nonzero', refuse_wait=10.0,
        argv=['--turn.proxy_protocol', 'true'],
        cfg=turn_cfg(ctx, extra='  proxy_protocol_trusted_cidrs: []\n'),
        note='the generated spelling reaches the same trusted-CIDR requirement the YAML row has')

    return out


# ------------------------------------------------------------------------------- one launch
def run_entry(ctx, prof, masker, binaries) -> dict:
    entry = {'profile': prof['id'], 'rows': prof['rows'], 'claims': prof['claims'],
             'note': prof['note'], 'expect_exit': prof['expect_exit'],
             'refuse_wait': prof['refuse_wait'], 'ready': prof['ready'],
             'probe_kids': sorted(prof['claims'].get('auth') or {}),
             'probe_ports': [name for name, _ in prof['probe_ports']], 'runs': {}}
    plain = ctx['store_port']
    for side in ('go', 'rust'):
        C102.redis_cli(plain, 'FLUSHALL')
        before = C102.dbsize(plain)
        if before != 0:
            raise RuntimeError(f'store not clean entering {prof["id"]}/{side}: {before}')

        def on_alive(proc, cfg_path, prof=prof):
            observed = {'store': C102.store_snapshot(plain),
                        'tls': C102.tls_cert_digest(ctx['tls1_port'], timeout=2.5),
                        'stun_tcp': stun_tcp(ctx['tls1_port']),
                        'stun_udp': stun_udp(ctx['tu1_port'], ctx['pu_port'])}
            kids = sorted(prof['claims'].get('auth') or {})
            if kids and prof['ready']:
                observed['auth'] = auth_probe104(ctx['http_port'], kids, masker)
            return observed

        rec = C102.launch_pair(Path(binaries[side]), list(prof['argv']), prof['env'], prof['cfg'],
                               ctx['work'], f'{side}-{prof["id"]}', side, masker,
                               ctx['http_port'] if prof['ready'] else None, ctx['hold'],
                               prof['refuse_wait'], on_alive=on_alive)
        obs = rec.pop('observed', {}) or {}
        # A refusal never reaches the probes, so "nothing bound" is only honest read after the process
        # is gone; a start reads the socket table while it is alive and every port table again after.
        rec['tls'] = obs.get('tls') or {}
        rec['leaf_class'] = leaf_class(rec['tls'], ctx['certs'])
        rec['stun_tcp'] = obs.get('stun_tcp') or {}
        rec['stun_udp'] = obs.get('stun_udp') or {}
        rec['auth'] = obs.get('auth') or {}
        rec['store'] = {'before_dbsize': before, 'alive': obs.get('store') or {},
                        'after': C102.store_snapshot(plain)}
        rec['post_ports'] = {name: {'tcp': C103.tcp_listeners(port), 'udp': C103.udp_holders(port)}
                             for name, port in prof['probe_ports']}
        entry['runs'][side] = rec
    return entry


# ------------------------------------------------------------------------------- claims/legs
# The one vocabulary evaluate understands. A claim key outside this set would be recorded and then
# silently ignored, which is how a receipt grows a field that asserts nothing.
CLAIM_KEYS = {'log_all', 'log_all_go', 'log_all_rust', 'log_not', 'log_not_go', 'log_not_rust',
              'log_any', 'tcp_has', 'tcp_not', 'udp_has', 'udp_not', 'leaf', 'tls_connected',
              'stun_udp', 'stun_tcp_go', 'stun_tcp_rust', 'node_alive', 'auth', 'store_left_clean',
              'store_no_node_after', 'release_ports'}


def needle_slug(needle: str) -> str:
    """Readable and process-stable. `hash()` is randomised per interpreter, and a leg id that moved
    between --record and --report would make verdict_stable_across_rederivation meaningless."""
    return re.sub(r'[^0-9a-z]', '', str(needle).lower())[:16] or 'x'


def L(pid, side, suffix) -> str:
    return f'{pid}::{side}:{suffix}'


def leg(name, side, kind, why='', **fields):
    item = {'leg': name, 'side': side, 'kind': kind, 'why': why}
    item.update(fields)
    return item


def rederive_leg(item) -> bool:
    kind = item['kind']
    if kind == 'has':
        return all(needle in item['haystack'] for needle in item['needles'])
    if kind == 'hasnt':
        return not any(needle in item['haystack'] for needle in item['needles'])
    if kind == 'any':
        return bool(item['needles']) and any(needle in item['haystack'] for needle in item['needles'])
    if kind == 'nonzero':
        return bool(item['actual'])
    if kind == 'eq':
        return item['expect'] == item['actual']
    raise RuntimeError(f'leg {item["leg"]} has kind {kind!r}, which this re-deriver cannot check')


def evaluate(entry) -> list:
    """Pure over the receipt, so --record, --report and --selftest-gate grade the same legs."""
    unknown = set(entry['claims']) - CLAIM_KEYS
    if unknown:
        raise RuntimeError(f'{entry["profile"]} carries claims nothing evaluates: {sorted(unknown)}')
    legs = []
    claims = entry['claims']
    pid = entry['profile']
    for side in ('go', 'rust'):
        rec = entry['runs'][side]
        log = rec.get('log_masked') or ''
        sockets = rec.get('sockets_masked') or {}
        tcp = sockets.get('tcp') or []
        udp = sockets.get('udp') or []
        held = ' '.join(tcp + udp)

        def emit(suffix, kind, **fields):
            legs.append(leg(f'{pid}::{side}:{suffix}', side, kind, **fields))

        for group, sides in (('log_all', ('go', 'rust')), ('log_all_go', ('go',)),
                             ('log_all_rust', ('rust',))):
            if side in sides:
                for needle in claims.get(group, []):
                    emit(f'{group}-{needle_slug(needle)}', 'has', needles=[needle], haystack=log,
                         why='the channel effect has to be visible in the daemon log')
        for group, sides in (('log_not', ('go', 'rust')), ('log_not_go', ('go',)),
                             ('log_not_rust', ('rust',))):
            if side in sides:
                for needle in claims.get(group, []):
                    emit(f'{group}-{needle_slug(needle)}', 'hasnt', needles=[needle], haystack=log,
                         why='a channel that lost, or a step that was skipped, must stay silent')
        if claims.get('log_any'):
            emit('any', 'any', needles=claims['log_any'], haystack=log,
                 why='a refusal has to name at least one plausible reason')
        want = entry.get('expect_exit')
        if want == 'nonzero':
            emit('exit', 'nonzero', actual=rec['exit'], why='the raw exit code, not a wrapper verdict')
        elif want is not None:
            emit('exit', 'eq', expect=want, actual=rec['exit'], why='the raw exit code of the launch')
        for role in claims.get('tcp_has', []):
            emit(f'tcp-{role}', 'eq', expect=True, actual=role in tcp,
                 why='the TCP listener the config named is one the live process owns')
        for role in claims.get('udp_has', []):
            emit(f'udp-{role}', 'eq', expect=True, actual=role in udp,
                 why='the UDP endpoint the config named is one the live process owns')
        for role in claims.get('tcp_not', []) + claims.get('udp_not', []):
            emit(f'not-{role}', 'eq', expect=False, actual=role in held,
                 why='a port that lost, or that the feature never opened, must be bound in neither '
                      'family')
        if 'leaf' in claims:
            emit('leaf', 'eq', expect=claims['leaf'], actual=rec.get('leaf_class'),
                 why='the leaf the TLS port presented is named by its DER digest, not by a config '
                      'field')
        if 'tls_connected' in claims:
            emit('tls-hs', 'eq', expect=claims['tls_connected'],
                 actual=bool((rec.get('tls') or {}).get('connected')),
                 why='whether a handshake completes is the external_tls discriminator')
        if 'stun_udp' in claims:
            emit('stun-udp', 'eq', expect=claims['stun_udp'],
                 actual=(rec.get('stun_udp') or {}).get('kind'),
                 why='a Binding Answer means the UDP listener is serving TURN, not just bound')
        for key, field in (('stun_tcp_go', 'stun_tcp'), ('stun_tcp_rust', 'stun_tcp')):
            if key in claims and ((key == 'stun_tcp_go') == (side == 'go')):
                emit(key, 'eq', expect=claims[key], actual=(rec.get(field) or {}).get('kind'),
                     why='the TCP port reaction to plaintext STUN is the measured split between an '
                          'internal TLS listener and a raw external one')
        if 'node_alive' in claims:
            alive = (rec.get('store') or {}).get('alive') or {}
            emit('node', 'eq', expect=claims['node_alive'], actual=len(alive.get('node_ids') or []),
                 why='the registry row exists only while the daemon is alive')
        for kid, verdict in (claims.get('auth') or {}).items():
            cell = ((rec.get('auth') or {}).get(kid) or {}).get('verdict')
            emit(f'auth-{kid}', 'eq', expect=verdict, actual=cell,
                 why='the running node either signs this key in or keeps it out')
        if claims.get('store_left_clean'):
            emit('store-clean', 'eq', expect=[],
                 actual=((rec.get('store') or {}).get('after') or {}).get('keys'),
                 why='a refused start writes nothing it then has to unwind')
        if claims.get('store_no_node_after'):
            emit('node-gone', 'eq', expect=[],
                 actual=((rec.get('store') or {}).get('after') or {}).get('node_ids'),
                 why='a refused start never registers a node')
        if claims.get('release_ports'):
            post = rec.get('post_ports') or {}
            for name in sorted(post):
                emit(f'release-{name}', 'eq', expect={'tcp': 0, 'udp': 0}, actual=post[name],
                     why='the socket is released when the daemon is gone')
    seen = {}
    for item in legs:
        seen[item['leg']] = seen.get(item['leg'], 0) + 1
        if seen[item['leg']] > 1:
            item['leg'] = f"{item['leg']}#{seen[item['leg']] - 1}"
    return legs


# ------------------------------------------------------------------------------- controls
START_PROFILES = ('turn-yaml-ports-and-leaf', 'turn-yaml-material-b', 'turn-flag-ports-beat-yaml',
                  'turn-env-ports-beat-yaml', 'turn-flag-zero-port-beats-yaml',
                  'turn-udp-only-no-tls-listener', 'turn-flag-material-beats-yaml',
                  'turn-external-tls-yaml', 'turn-external-tls-flag', 'turn-external-tls-with-cert',
                  'turn-cidr-lists-valid', 'turn-relay-default-numbers',
                  'turn-relay-explicit-numbers', 'turn-relay-flag-numbers',
                  'turn-keys-yaml-baseline', 'turn-keys-env-replaces-yaml',
                  'turn-keys-env-numeric-secret', 'turn-ttl-clamped-flag',
                  'turn-ttl-nonpositive-yaml', 'turn-disabled-skips-consumers',
                  'turn-flag-disabled-equals-skips-consumers',
                  'turn-flag-enabled-true-beats-yaml-false',
                  'turn-flag-space-value-keeps-turn-on')
EXTERNAL_PROFILES = ('turn-external-tls-yaml', 'turn-external-tls-flag',
                     'turn-external-tls-with-cert')
QUIET_PROFILES = ('turn-flag-zero-port-beats-yaml', 'turn-udp-only-no-tls-listener',
                  'turn-disabled-skips-consumers', 'turn-flag-disabled-equals-skips-consumers')
TLS_LISTENER_PROFILES = tuple(pid for pid in START_PROFILES if pid not in EXTERNAL_PROFILES
                              and pid not in QUIET_PROFILES)
DOMAIN_REFUSALS = ('turn-domain-required-refusal', 'turn-domain-incorrect-refusal',
                   'turn-flag-empty-domain-beats-yaml')
CIDR_REFUSALS = ('turn-deny-cidr-refusal', 'turn-allow-cidr-refusal')
DISABLED_PROFILES = ('turn-disabled-skips-consumers', 'turn-flag-disabled-equals-skips-consumers')
ENABLED_FLAG_PROFILES = ('turn-flag-disabled-equals-skips-consumers',
                         'turn-flag-enabled-true-beats-yaml-false',
                         'turn-flag-space-value-keeps-turn-on')
RELAY_PROFILES = {'turn-relay-default-numbers': RELAY_DEFAULT,
                  'turn-relay-explicit-numbers': RELAY_EXPLICIT,
                  'turn-relay-flag-numbers': RELAY_FLAGGED}


def _held(rec) -> str:
    """Both socket families of one run as one masked string."""
    socks = rec.get('sockets_masked') or {}
    return ' '.join((socks.get('tcp') or []) + (socks.get('udp') or []))


def by_side(entry, side) -> dict:
    return entry['runs'][side]


def auth_cells(rec) -> dict:
    return {kid: cell['verdict'] for kid, cell in (rec.get('auth') or {}).items()}


def build_controls(doc, live=False) -> dict:
    entries = doc['profiles']
    by_id = {e['profile']: e for e in entries}
    runs = [(e['profile'], side, e['runs'][side]) for e in entries for side in ('go', 'rust')]

    def rows(*pids):
        """Only the profiles present in this receipt. On a --only subset the missing ones are absent,
        and an absent row must read False rather than raise: only a full sweep may claim them."""
        return [by_id[pid] for pid in pids if pid in by_id]
    C = {}
    C['no_development_subset'] = not doc.get('subset')
    C['every_profile_ran_both_sides'] = all(
        set(e['runs']) == {'go', 'rust'} and all(r.get('exit') is not None for r in e['runs'].values())
        for e in entries)
    C['profile_inventory_complete'] = len(entries) == PROFILE_COUNT
    C['every_profile_names_a_row'] = all(
        e['rows'] and set(e['rows']) <= set(COVERS) for e in entries)
    named = {row for e in entries for row in e['rows']}
    C['every_row_is_named_by_a_profile'] = named == set(COVERS)
    C['claims_vocabulary_closed'] = all(not (set(e['claims']) - CLAIM_KEYS) for e in entries)
    C['start_profile_inventory_pinned'] = sorted(
        e['profile'] for e in entries if e['refuse_wait'] is None) == sorted(START_PROFILES)

    # ---- the material channel: the digest has to be able to say two different things
    certs = doc['fixtures']['certs']
    C['fixture_certificates_differ'] = certs['A']['cert_der_sha256'] != \
        certs['B']['cert_der_sha256']
    leaf_rows = [e for e in entries if 'leaf' in e['claims']]
    pair = rows('turn-yaml-ports-and-leaf', 'turn-yaml-material-b')
    C['leaf_channel_discriminates'] = len(pair) == 2 and all(
        e['runs']['go']['leaf_class'] == want and e['runs']['rust']['leaf_class'] == want
        for e, want in zip(pair, ('A', 'B')))
    C['leaf_cells_recorded_for_every_claim'] = all(
        (by_side(e, side).get('tls') or {}) for e in leaf_rows for side in ('go', 'rust'))
    C['handshake_leaves_der_on_the_wire'] = bool(rows(*TLS_LISTENER_PROFILES)) and all(
        e['runs'][side]['tls'].get('bytes', 0) > 100
        for e in rows(*TLS_LISTENER_PROFILES) for side in ('go', 'rust')
        if e['claims'].get('tls_connected'))

    # ---- the port channel
    bound = [e['profile'] for e in entries if e['claims'].get('tcp_has')]
    unbound = [e['profile'] for e in entries
               if '<TLS1>' in (e['claims'].get('tcp_not') or []) and e['refuse_wait'] is None]
    C['port_channel_split_between_profiles'] = bool(bound) and bool(unbound)
    C['losing_ports_never_bound'] = all(
        not any(role in ' '.join((r.get('sockets_masked') or {}).get('tcp', []) +
                                 (r.get('sockets_masked') or {}).get('udp', []))
                for role in (e['claims'].get('tcp_not') or []) + (e['claims'].get('udp_not') or []))
        for e in entries for r in e['runs'].values())

    # ---- the STUN channels, and the split that makes external_tls a behaviour
    C['stun_udp_answered_on_every_listener'] = all(
        (by_side(e, side).get('stun_udp') or {}).get('kind') == 'answered'
        for e in entries if e['claims'].get('stun_udp') == 'answered' for side in ('go', 'rust'))
    def kind(e, side, field):
        return ((by_side(e, side).get(field) or {}) if field != 'tls'
                else (by_side(e, side).get('tls') or {})).get('kind')

    shape = rows(*(tuple(TLS_LISTENER_PROFILES) + tuple(EXTERNAL_PROFILES)))
    C['stun_answer_shape_pinned'] = len(shape) == len(TLS_LISTENER_PROFILES) + len(EXTERNAL_PROFILES) \
        and all(
        '0x20' in (by_side(e, side).get('stun_udp') or {}).get('attrs', []) and
        '0x8028' in (by_side(e, side).get('stun_udp') or {}).get('attrs', []) and
        (by_side(e, side).get('stun_udp') or {}).get('tid_ok') is True and
        '0x8022' in (by_side(e, 'rust').get('stun_udp') or {}).get('attrs', []) and
        '0x8022' not in (by_side(e, 'go').get('stun_udp') or {}).get('attrs', [])
        for e in shape for side in ('go', 'rust'))
    tls_rows = rows(*TLS_LISTENER_PROFILES)
    C['plaintext_stun_over_tls_split_pinned'] = len(tls_rows) == len(TLS_LISTENER_PROFILES) and all(
        kind(e, 'go', 'stun_tcp') == 'timeout' and kind(e, 'rust', 'stun_tcp') == 'tls-alert'
        for e in tls_rows)
    ext = rows(*EXTERNAL_PROFILES)
    C['external_tls_answers_stun_over_tcp'] = len(ext) == len(EXTERNAL_PROFILES) and all(
        kind(e, side, 'stun_tcp') == 'stun' and not e['runs'][side]['tls'].get('connected')
        for e in ext for side in ('go', 'rust'))
    quiet = rows(*QUIET_PROFILES)
    C['no_tls_listener_means_nothing_listens'] = len(quiet) == len(QUIET_PROFILES) and all(
        kind(e, side, 'stun_tcp') == 'refused' for e in quiet for side in ('go', 'rust'))

    # ---- refusals: self-ended, silent afterwards, and worded the same
    refusals = [e for e in entries if e.get('refuse_wait') is not None]
    C['refusal_rows_present'] = len(refusals) >= 9
    C['refusal_rows_self_ended'] = bool(refusals) and all(
        r.get('self_exited') for e in refusals for r in e['runs'].values())
    C['sigkill_never_used'] = not any('SIGKILL' in (r.get('signals') or []) for _, _, r in runs)
    C['refusal_leaves_no_listener'] = bool(refusals) and all(
        (r.get('post_ports') or {}) and all(v == {'tcp': 0, 'udp': 0}
                                            for v in (r.get('post_ports') or {}).values())
        for e in refusals for r in e['runs'].values())
    C['refusal_rows_leave_no_node'] = all(
        ((r.get('store') or {}).get('after') or {}).get('node_ids') == []
        for e in refusals if e['claims'].get('store_no_node_after') for r in e['runs'].values())
    C['domain_gate_sentences_identical_across_sides'] = bool(rows(*DOMAIN_REFUSALS)) and all(
        C103.sentence_after(e['runs']['go']['log_masked'], needle) ==
        C103.sentence_after(e['runs']['rust']['log_masked'], needle) and
        needle in e['runs']['go']['log_masked']
        for e in rows(*DOMAIN_REFUSALS) for needle in (e['claims'].get('log_all') or [])[:1])
    C['cidr_gate_sentences_identical_across_sides'] = bool(rows(*CIDR_REFUSALS)) and all(
        C103.sentence_after(e['runs']['go']['log_masked'], needle) ==
        C103.sentence_after(e['runs']['rust']['log_masked'], needle) and
        needle in e['runs']['go']['log_masked']
        for e in rows(*CIDR_REFUSALS) for needle in (e['claims'].get('log_all') or [])[:1])
    cond = rows('turn-udp-only-no-tls-listener')
    C['domain_gate_is_conditional'] = bool(cond) and all(
        e['runs'][side]['exit'] == 0 and TURN_START in e['runs'][side]['log_masked']
        for e in cond for side in ('go', 'rust'))

    # ---- the disabled short-circuit and its enabled twin
    twin = rows('turn-enabled-same-garbage-refuses')
    C['disabled_pair_pinned'] = bool(rows(*DISABLED_PROFILES)) and bool(twin) and all(
        TURN_START not in e['runs'][side]['log_masked'] and e['runs'][side]['exit'] == 0
        for e in rows(*DISABLED_PROFILES) for side in ('go', 'rust')) and all(
        twin[0]['runs'][side]['exit'] not in (0, None) for side in ('go', 'rust'))
    # The flag spelling has to move the same listener the YAML spelling moves, in both directions, or
    # `--turn.enabled` is only a parsed string. Reusing the listener claim keeps this off log words.
    off_by_flag = rows('turn-flag-disabled-equals-skips-consumers')
    on_by_flag = rows('turn-flag-enabled-true-beats-yaml-false')
    C['flag_enabled_moves_the_listeners'] = bool(off_by_flag) and bool(on_by_flag) and all(
        not any(named in _held(r) for named in ('<TLS1>', '<TU1>'))
        for r in (off_by_flag[0]['runs']['go'], off_by_flag[0]['runs']['rust'])) and all(
        '<TLS1>' in (r.get('sockets_masked') or {}).get('tcp', [])
        and '<TU1>' in (r.get('sockets_masked') or {}).get('udp', [])
        for r in (on_by_flag[0]['runs']['go'], on_by_flag[0]['runs']['rust']))
    # Measured in this lane, not assumed: a bool flag written with a space does not consume the word,
    # so `--turn.enabled false` leaves TURN on. If a future parser accepts the space form this control
    # fails and the row's note has to be re-read.
    space = rows('turn-flag-space-value-keeps-turn-on')
    C['flag_bool_space_value_pinned'] = bool(space) and all(
        '<TLS1>' in (r.get('sockets_masked') or {}).get('tcp', [])
        and '<TU1>' in (r.get('sockets_masked') or {}).get('udp', [])
        and TURN_START in r.get('log_masked', '')
        for r in (space[0]['runs']['go'], space[0]['runs']['rust']))

    # ---- the startup validation gate runs in a different order on the two sides
    # Both refuse the same bytes with a non-zero exit; which gate speaks first is the divergence, and
    # pinning the sentences stops a later reader from calling the row "both fail, therefore equal".
    gate = rows('turn-enabled-same-garbage-refuses')
    C['garbage_gate_first_sentence_pinned'] = bool(gate) and (
        DOMAIN_REQUIRED in gate[0]['runs']['go']['log_masked'] and
        PROXY_NEEDS_CIDRS in gate[0]['runs']['rust']['log_masked'] and
        PROXY_NEEDS_CIDRS not in gate[0]['runs']['go']['log_masked'] and
        DOMAIN_REQUIRED not in gate[0]['runs']['rust']['log_masked'] and
        gate[0]['runs']['go']['exit'] == gate[0]['runs']['rust']['exit'] != 0)

    # ---- relay numbers reach the running server and are not constants
    relay = [(e, RELAY_PROFILES[e['profile']]) for e in rows(*RELAY_PROFILES)]
    C['relay_numbers_move_with_config'] = len(relay) == len(RELAY_PROFILES) and all(
        all(f'{key}": {value}' in e['runs'][side]['log_masked']
            for key, value in zip(('relay_range_start', 'relay_range_end',
                                   'per_user_relay_allocation_limit'), nums))
        for e, nums in relay for side in ('go', 'rust')) and not any(
        f'relay_range_start": {RELAY_DEFAULT[0]}' in e['runs'][side]['log_masked']
        for e, nums in relay if nums != RELAY_DEFAULT for side in ('go', 'rust'))

    # ---- keys: the running node is the authority
    probed = [e for e in entries if (e['claims'].get('auth') or {})]
    C['auth_rows_present'] = len(probed) >= 14
    C['auth_cells_recorded_not_missing'] = all(
        (by_side(e, side).get('auth') or {}).get(kid) is not None
        for e in probed for side in ('go', 'rust') for kid in e['claims']['auth'])
    C['auth_pair_identical_across_sides'] = all(
        auth_cells(by_id[e['profile']]['runs']['go']) ==
        auth_cells(by_id[e['profile']]['runs']['rust']) for e in probed)
    keys_pair = rows('turn-keys-env-replaces-yaml', 'turn-keys-yaml-baseline')
    C['keys_env_replaces_yaml_map'] = len(keys_pair) == 2 and all(
        auth_cells(e['runs'][side]) == want
        for e, want in zip(keys_pair, ({'k104y': REJECTED, 'k104e': ACCEPTED,
                                        FOREIGN_KID: REJECTED},
                                       {'k104y': ACCEPTED, 'k104e': REJECTED,
                                        FOREIGN_KID: REJECTED})) for side in ('go', 'rust'))
    numeric = rows('turn-keys-env-numeric-secret')
    C['keys_numeric_secret_dropped'] = bool(numeric) and all(
        auth_cells(e['runs'][side]).get(NUMERIC_KID) == REJECTED and
        auth_cells(e['runs'][side]).get('k104e') == ACCEPTED
        for e in numeric for side in ('go', 'rust'))
    # Measured, not assumed: the oracle logs one `error handling request` WARN per /rtc/v1/validate
    # call that ends in an HTTP error status, while the Rust node answers the same statuses and logs
    # nothing. Counting lines against probes keeps this from ageing quietly if either side changes.
    with_auth = [e for e in entries if (e['runs']['go'].get('auth') or {})]
    C['auth_rejection_logging_pinned'] = len(with_auth) >= 6 and all(
        e['runs']['go']['log_masked'].count(AUTH_LOG_NEEDLE) == len(e['runs']['go']['auth']) and
        e['runs']['rust']['log_masked'].count(AUTH_LOG_NEEDLE) == 0 for e in with_auth)

    # ---- lifecycle and the two recorded stage divergences
    starts = [e for e in entries if e.get('refuse_wait') is None]
    C['started_rows_register_one_node'] = all(
        len((((e['runs'][side].get('store') or {}).get('alive') or {}).get('node_ids') or [])) == 1
        for e in starts if 'node_alive' in e['claims'] for side in ('go', 'rust'))
    C['node_row_gone_after_exit'] = all(
        ((e['runs'][side].get('store') or {}).get('after') or {}).get('node_ids') == []
        for e in starts for side in ('go', 'rust'))
    C['version_marker_survives_clean_shutdown'] = all(
        'livekit_version' in ((e['runs'][side].get('store') or {}).get('after') or {}).get('keys', [])
        for e in starts for side in ('go', 'rust'))
    ttl_rows = rows('turn-ttl-clamped-flag', 'turn-ttl-nonpositive-yaml')
    C['ttl_warning_visibility_pinned'] = bool(ttl_rows) and all(
        TTL_WARN_RUST in e['runs']['rust']['log_masked'] and
        TTL_WARN_RUST not in e['runs']['go']['log_masked'] and
        e['runs']['go']['exit'] == 0 and e['runs']['rust']['exit'] == 0 for e in ttl_rows)
    oor = rows('turn-port-out-of-range')
    C['out_of_range_port_stage_pinned'] = bool(oor) and (
        PORT_RANGE_GO in oor[0]['runs']['go']['log_masked'] and
        PORT_RANGE_RUST in oor[0]['runs']['rust']['log_masked'] and
        oor[0]['runs']['go']['exit'] == oor[0]['runs']['rust']['exit'] != 0)
    tear = rows(*(tuple(TLS_LISTENER_PROFILES) + tuple(EXTERNAL_PROFILES)))
    C['turn_teardown_line_pinned'] = bool(tear) and all(
        (TURN_STOP_RUST in e['runs']['rust']['log_masked']) !=
        (TURN_STOP_RUST in e['runs']['go']['log_masked']) for e in tear)
    C['store_flushed_before_every_launch'] = all(
        (r.get('store') or {}).get('before_dbsize') == 0 for _, _, r in runs)
    C['ready_rows_reach_the_daemon'] = all(r.get('ready') for e in starts for r in e['runs'].values())
    return C


# ---------------------------------------------------------- provenance/privacy controls
def sha256_file_now(path: str):
    p = Path(path)
    return C102.sha256_file(p) if p.is_file() else None


def build_provenance_controls(doc, live=False) -> dict:
    bins = doc['binaries']
    C = {}
    now = {side: (sha256_file_now(bins[side].get('path', '')) if bins[side].get('path') else None)
           for side in ('go', 'rust')}
    doc['binary_hash_now'] = now
    C['binaries_unchanged'] = all(now[s] is not None and now[s] == bins[s]['sha256']
                                 for s in ('go', 'rust'))
    bins['go']['is_fixed_oracle'] = now['go'] == ORACLE_SHA256
    bins['rust']['is_acceptance_candidate'] = now['rust'] == ACCEPTANCE_SHA256
    C['go_is_fixed_oracle'] = bool(bins['go']['is_fixed_oracle'])
    C['rust_is_acceptance_candidate'] = bool(bins['rust']['is_acceptance_candidate'])
    C['recorded_oracle_matches_pinned'] = bins['go']['sha256'] == ORACLE_SHA256
    C['recorded_candidate_matches_pinned'] = bins['rust']['sha256'] == ACCEPTANCE_SHA256
    C['reference_commit_matches'] = doc['reference']['go_checkout_commit'] == FIXED_GO_COMMIT
    C['imported_primitives_are_the_accepted_lanes'] = (
        doc['c102_harness_sha256'] == C102_FILED_HARNESS_SHA and
        doc['c103_harness_sha256'] == C103_FILED_HARNESS_SHA)
    harness_now = C102.sha256_file(Path(__file__).resolve())
    doc['harness_sha256_now'] = harness_now
    C['harness_unchanged_during_run'] = (
        harness_now == doc['harness_sha256_at_start'] if live
        else doc['harness_sha256'] == doc['harness_sha256_at_start'])
    C['harness_unchanged_since_run'] = doc['harness_sha256'] == harness_now

    blob = json.dumps(doc['profiles'], ensure_ascii=False, sort_keys=True)
    whole = json.dumps(doc, ensure_ascii=False, sort_keys=True)
    C['off_limits_never_named'] = not re.search(r'\b788[12]\b', blob)
    C['host_store_never_named'] = ':6379' not in blob
    C['no_machine_address_in_receipt'] = not C102.forbidden_addresses(whole)
    C['no_unmasked_loopback_ports'] = not re.search(r'127\.0\.0\.1:\d', blob)
    C['no_working_paths_in_profile_records'] = not any(
        needle in blob for needle in (str(REPO), str(Path.home()),
                                      str(Path(doc.get('work_dir') or '\x00')), '/private/tmp/'))
    C['fixture_secrets_never_raw'] = not any(real in whole for real, _ in KEYS_SECRET_ALIASES)
    C['argv_never_raw'] = not any('argv_raw' in r or ('argv' in r and 'argv_masked' not in r)
                                  for _, _, r in runs_of(doc))
    C['raw_streams_recorded'] = all(
        re.fullmatch(r'[0-9a-f]{64}', r['raw']['stdout_sha256']) and r['raw']['bytes'] > 0
        for _, _, r in runs_of(doc))
    bounds = doc.get('listeners_bounds') or {}
    C['off_limit_ports_untouched'] = bool(bounds) and all(
        v['before'] == v['after'] for v in bounds.values())
    C['owned_store_stopped_cleanly'] = bool(doc.get('store_final')) and \
        doc['store_final'].get('listeners_after_stop') == 0
    return C


def runs_of(doc):
    return [(e['profile'], side, e['runs'][side]) for e in doc['profiles']
            for side in ('go', 'rust')]


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
    controls.update(build_provenance_controls(doc, live=live))
    controls['verdict_stable_across_rederivation'] = (verdicts == doc['leg_verdicts_at_record'])
    doc['cross_profile_controls'] = controls
    doc['failed_profiles'] = sorted(e['profile'] for e in doc['profiles'] if not e['match'])
    doc['failed_legs'] = sorted(l['leg'] for l in doc['legs'] if not l['ok'])
    doc['failed_controls'] = sorted(k for k, v in controls.items() if not v)
    doc['row_profile_map'] = {row: sorted(e['profile'] for e in doc['profiles']
                                          if row in e['rows']) for row in doc['covers']}
    doc['status_gates'] = {
        'binaries_unchanged': controls['binaries_unchanged'],
        'go_is_fixed_oracle': controls['go_is_fixed_oracle'],
        'rust_is_acceptance_candidate': controls['rust_is_acceptance_candidate'],
        'reference_commit_matches': controls['reference_commit_matches'],
        'imported_primitives_are_the_accepted_lanes':
            controls['imported_primitives_are_the_accepted_lanes'],
        'harness_unchanged_during_run': controls['harness_unchanged_during_run'],
        'harness_unchanged_since_run': controls['harness_unchanged_since_run'],
        'no_failed_profiles': not doc['failed_profiles'],
        'no_failed_controls': not doc['failed_controls'],
        'no_failed_legs': not doc['failed_legs'],
    }
    doc['unmet_gates'] = sorted(k for k, v in doc['status_gates'].items() if not v)
    doc['live_run'] = live
    doc['status'] = ('PARTIAL_SUBSET_NOT_ACCEPTANCE' if doc.get('subset')
                     else STATUS_PASS if not doc['unmet_gates'] else 'FAIL')
    if write and out is not None:
        (Path(out) / 'execution.json').write_text(
            json.dumps(doc, indent=2, ensure_ascii=False, sort_keys=True) + '\n')
        batch = {'status': doc['status'], 'batch': doc['batch'], 'covers': doc['covers'],
                 'profiles': len(doc['profiles']), 'legs': doc['leg_count'],
                 'controls': len(controls), 'failed_profiles': doc['failed_profiles'],
                 'failed_legs': doc['failed_legs'], 'failed_controls': doc['failed_controls'],
                 'unmet_gates': doc['unmet_gates'], 'status_gates': doc['status_gates'],
                 'row_profile_map': doc['row_profile_map'],
                 'profile_verdicts': {e['profile']: {'match': e['match'], 'rows': e['rows'],
                                                     'go_exit': e['runs']['go']['exit'],
                                                     'rust_exit': e['runs']['rust']['exit'],
                                                     'problems': e['problems']}
                                      for e in doc['profiles']},
                 'control_values': controls, 'binary_hash_now': doc['binary_hash_now'],
                 'harness_sha256_now': doc['harness_sha256_now'], 'binaries': doc['binaries'],
                 'reference': doc['reference'], 'harness_sha256': doc['harness_sha256'],
                 'c102_harness_sha256': doc['c102_harness_sha256'],
                 'c103_harness_sha256': doc['c103_harness_sha256']}
        (Path(out) / 'BATCH.json').write_text(json.dumps(batch, indent=2, ensure_ascii=False,
                                                        sort_keys=True) + '\n')
    return {'all_ok': not doc['unmet_gates'],
            'summary': {k: doc[k] for k in ('status', 'leg_count', 'failed_profiles',
                                            'failed_controls', 'failed_legs', 'unmet_gates')}}


# ------------------------------------------------------------------------------- falsification
# Every gate in this file is an assertion about data the harness could have recorded wrongly. A case
# below is a named, plausible misrecord - a listener dropped from the socket table, a leaf digest
# swapped, a refusal sentence rewritten to match the other side - applied to a deep copy of a real
# receipt, with the expectation that a specific leg or control reddens. An injection that changes
# nothing, or that targets a leg the receipt does not contain, is reported as undiscriminated.
def _e(doc, pid):
    for e in doc['profiles']:
        if e['profile'] == pid:
            return e
    raise KeyError(pid)


def _r(doc, pid, side):
    return _e(doc, pid)['runs'][side]


def _socks(rec, fam):
    return rec.setdefault('sockets_masked', {}).setdefault(fam, [])


def _log(rec):
    return rec.get('log_masked') or ''


def _say(rec, line):
    rec['log_masked'] = _log(rec).rstrip('\n') + '\n' + line + '\n'


def _mute(rec, needle):
    body = _log(rec)
    if needle not in body:
        raise RuntimeError(f'refusing to remove {needle!r}: it is not in the recorded log')
    rec['log_masked'] = '\n'.join(l for l in body.splitlines() if needle not in l) + '\n'


def _turn_line(doc, pid, side):
    """The real startup line of a recorded profile, so an injection edits what was measured."""
    for line in _log(_r(doc, pid, side)).splitlines():
        if TURN_START in line:
            return line
    raise RuntimeError(f'{pid}/{side} recorded no {TURN_START!r} line')


def _auth(rec, kid, verdict):
    cell = (rec.get('auth') or {}).get(kid)
    if cell is None:
        raise RuntimeError(f'no auth cell recorded for {kid}')
    cell['verdict'] = verdict


GATE_INJECTIONS = (
    # ---- the listener and port channel
    {'case': 'tls_listener_unrecorded', 'expect': 'FAILED_LEG',
     'target': 'turn-yaml-ports-and-leaf::go:tcp-<TLS1>',
     'apply': lambda d: _socks(_r(d, 'turn-yaml-ports-and-leaf', 'go'), 'tcp').remove('<TLS1>'),
     'why': 'if the socket table says the TLS port is not held, the row cannot claim the listener'},
    {'case': 'udp_listener_unrecorded', 'expect': 'FAILED_LEG',
     'target': 'turn-yaml-ports-and-leaf::rust:udp-<TU1>',
     'apply': lambda d: _socks(_r(d, 'turn-yaml-ports-and-leaf', 'rust'), 'udp').remove('<TU1>'),
     'why': 'the UDP endpoint is a separate claim from the TCP one and has to fail separately'},
    {'case': 'losing_tls_port_rebound', 'expect': 'FAILED_LEG',
     'target': 'turn-flag-ports-beat-yaml::go:not-<TLS2>',
     'apply': lambda d: _socks(_r(d, 'turn-flag-ports-beat-yaml', 'go'), 'tcp').append('<TLS2>'),
     'why': 'a flag that wins must leave the YAML port bound in neither family'},
    {'case': 'losing_udp_port_rebound', 'expect': 'FAILED_LEG',
     'target': 'turn-env-ports-beat-yaml::rust:not-<TU2>',
     'apply': lambda d: _socks(_r(d, 'turn-env-ports-beat-yaml', 'rust'), 'udp').append('<TU2>'),
     'why': 'the env spelling is claimed to reach the same listeners; the loser must stay free'},
    {'case': 'zero_port_still_listens', 'expect': 'FAILED_LEG',
     'target': 'turn-flag-zero-port-beats-yaml::go:not-<TLS1>',
     'apply': lambda d: _socks(_r(d, 'turn-flag-zero-port-beats-yaml', 'go'), 'tcp').append('<TLS1>'),
     'why': '0 has to remove the listener the YAML asked for, not merely be recorded'},
    {'case': 'losing_ports_bound_everywhere', 'expect': 'FAILED_CONTROL',
     'target': 'losing_ports_never_bound',
     'apply': lambda d: [(_socks(r, 'tcp') if '<TLS2>' not in _socks(r, 'tcp') else _socks(r, 'udp'))
                         .append('<TLS2>') for _, _, r in runs_of(d)],
     'why': 'the cross-profile guard has to notice a losing port appearing anywhere, not only in the '
            'profile whose leg cites it'},
    {'case': 'port_channel_not_split', 'expect': 'FAILED_CONTROL',
     'target': 'port_channel_split_between_profiles',
     'apply': lambda d: [e['claims'].pop('tcp_not', None) for e in d['profiles']],
     'why': 'if no profile ever names a losing port, the port row proves precedence by accident'},
    {'case': 'port_never_released', 'expect': 'FAILED_LEG',
     'target': 'turn-external-tls-flag::rust:release-tls1',
     'apply': lambda d: _r(d, 'turn-external-tls-flag', 'rust')['post_ports'].update(
         {'tls1': {'tcp': 1, 'udp': 0}}),
     'why': 'a listener the config opened must be gone once the daemon is, or the harness leaked it'},

    # ---- the certificate channel
    {'case': 'leaf_is_the_other_fixture', 'expect': 'FAILED_LEG',
     'target': 'turn-yaml-material-b::rust:leaf',
     'apply': lambda d: _r(d, 'turn-yaml-material-b', 'rust').update({'leaf_class': 'A'}),
     'why': 'the material row is only about the digest the port actually presented'},
    {'case': 'flag_material_loses_to_yaml', 'expect': 'FAILED_LEG',
     'target': 'turn-flag-material-beats-yaml::go:leaf',
     'apply': lambda d: _r(d, 'turn-flag-material-beats-yaml', 'go').update({'leaf_class': 'B'}),
     'why': 'argv over YAML has to be decided by the wire, not by the config text'},
    {'case': 'fixtures_are_the_same_cert', 'expect': 'FAILED_CONTROL',
     'target': 'fixture_certificates_differ',
     'apply': lambda d: d['fixtures']['certs']['B'].update(
         {'cert_der_sha256': d['fixtures']['certs']['A']['cert_der_sha256']}),
     'why': 'if A and B are one certificate the whole material channel says nothing'},
    {'case': 'leaf_channel_degenerate', 'expect': 'FAILED_CONTROL',
     'target': 'leaf_channel_discriminates',
     'apply': lambda d: [_r(d, pid, s).update({'leaf_class': 'A'})
                         for pid in ('turn-yaml-ports-and-leaf', 'turn-yaml-material-b')
                         for s in ('go', 'rust')],
     'why': 'the pair must show two different digests, otherwise nothing distinguished the configs'},
    {'case': 'leaf_cell_never_recorded', 'expect': 'FAILED_CONTROL',
     'target': 'leaf_cells_recorded_for_every_claim',
     'apply': lambda d: _r(d, 'turn-yaml-ports-and-leaf', 'go').update({'tls': {}}),
     'why': 'a row that claims a leaf but records no handshake is a hole, not a pass'},
    {'case': 'handshake_bytes_zeroed', 'expect': 'FAILED_CONTROL',
     'target': 'handshake_leaves_der_on_the_wire',
     'apply': lambda d: _r(d, 'turn-yaml-ports-and-leaf', 'go')['tls'].update({'bytes': 0}),
     'why': 'a TLS cell with no bytes is not a handshake, whatever it claims about the leaf'},
    {'case': 'material_mismatch_needle_gone', 'expect': 'FAILED_LEG',
     'target': 'turn-flag-material-mismatch::rust:any',
     'apply': lambda d: _mute(_r(d, 'turn-flag-material-mismatch', 'rust'), 'keys may not be consistent'),
     'why': 'the crossed pair has to say why it refused'},
    {'case': 'missing_file_needle_gone', 'expect': 'FAILED_LEG',
     'target': 'turn-flag-missing-files::go:any',
     'apply': lambda d: _mute(_r(d, 'turn-flag-missing-files', 'go'), 'no such file or directory'),
     'why': 'the refusal must still name the missing file, not just exit non-zero'},

    # ---- the STUN channels
    {'case': 'udp_binding_unanswered', 'expect': 'FAILED_LEG',
     'target': 'turn-yaml-ports-and-leaf::rust:stun-udp',
     'apply': lambda d: _r(d, 'turn-yaml-ports-and-leaf', 'rust')['stun_udp'].update({'kind': 'timeout'}),
     'why': 'a bound UDP port that never answers is not a TURN service'},
    {'case': 'go_answers_software_attr', 'expect': 'FAILED_CONTROL',
     'target': 'stun_answer_shape_pinned',
     'apply': lambda d: _r(d, 'turn-yaml-ports-and-leaf', 'go')['stun_udp']['attrs'].append('0x8022'),
     'why': 'the SOFTWARE attribute is the measured Rust-only addition; erasing it erases the finding'},
    {'case': 'rust_loses_software_attr', 'expect': 'FAILED_CONTROL',
     'target': 'stun_answer_shape_pinned',
     'apply': lambda d: _r(d, 'turn-yaml-ports-and-leaf', 'rust')['stun_udp']['attrs'].remove('0x8022'),
     'why': 'the same split read from the other side'},
    {'case': 'tid_never_checked', 'expect': 'FAILED_CONTROL',
     'target': 'stun_answer_shape_pinned',
     'apply': lambda d: _r(d, 'turn-yaml-ports-and-leaf', 'go')['stun_udp'].update({'tid_ok': False}),
     'why': 'an answer with the wrong transaction id is not an answer to this probe'},
    {'case': 'answer_attributes_emptied', 'expect': 'FAILED_CONTROL',
     'target': 'stun_answer_shape_pinned',
     'apply': lambda d: [_r(d, pid, s)['stun_udp'].update({'attrs': []})
                         for pid in TLS_LISTENER_PROFILES for s in ('go', 'rust')],
     'why': 'without the attribute list the xor-mapped-only claim has no referent'},
    {'case': 'tls_port_answers_plaintext_stun', 'expect': 'FAILED_LEG',
     'target': 'turn-cidr-lists-valid::go:stun_tcp_go',
     'apply': lambda d: _r(d, 'turn-cidr-lists-valid', 'go')['stun_tcp'].update({'kind': 'stun'}),
     'why': 'an internal TLS listener must not hand back a Binding Answer'},
    {'case': 'rust_stops_alerting', 'expect': 'FAILED_LEG',
     'target': 'turn-cidr-lists-valid::rust:stun_tcp_rust',
     'apply': lambda d: _r(d, 'turn-cidr-lists-valid', 'rust')['stun_tcp'].update({'kind': 'timeout'}),
     'why': 'the 7-byte alert is the Rust half of the measured split'},
    {'case': 'tls_split_averaged', 'expect': 'FAILED_CONTROL',
     'target': 'plaintext_stun_over_tls_split_pinned',
     'apply': lambda d: [_r(d, pid, s)['stun_tcp'].update({'kind': 'stun'})
                         for pid in TLS_LISTENER_PROFILES for s in ('go', 'rust')],
     'why': 'if both sides behave the same on the TLS port the split is not a finding'},
    {'case': 'external_tls_stops_answering', 'expect': 'FAILED_CONTROL',
     'target': 'external_tls_answers_stun_over_tcp',
     'apply': lambda d: _r(d, 'turn-external-tls-yaml', 'go')['stun_tcp'].update({'kind': 'timeout'}),
     'why': 'external_tls is claimed as a raw listener; a silent port disproves it'},
    {'case': 'external_tls_handshakes', 'expect': 'FAILED_LEG',
     'target': 'turn-external-tls-flag::rust:tls-hs',
     'apply': lambda d: _r(d, 'turn-external-tls-flag', 'rust')['tls'].update({'connected': True}),
     'why': 'if the raw port completes a TLS handshake the field did not skip the acceptor'},
    {'case': 'quiet_port_answers_anyway', 'expect': 'FAILED_CONTROL',
     'target': 'no_tls_listener_means_nothing_listens',
     'apply': lambda d: _r(d, 'turn-udp-only-no-tls-listener', 'go')['stun_tcp'].update(
         {'kind': 'stun'}),
     'why': 'with tls_port 0 nothing may answer on that port in either spelling'},
    {'case': 'stun_udp_control_dropped', 'expect': 'FAILED_CONTROL',
     'target': 'stun_udp_answered_on_every_listener',
     'apply': lambda d: [_r(d, pid, s)['stun_udp'].update({'kind': 'refused'})
                         for pid in TLS_LISTENER_PROFILES for s in ('go', 'rust')],
     'why': 'every profile that claims an answered Binding has to have one'},

    # ---- the domain and CIDR gates
    {'case': 'refusal_exit_masked', 'expect': 'FAILED_LEG',
     'target': 'turn-domain-required-refusal::go:exit',
     'apply': lambda d: _r(d, 'turn-domain-required-refusal', 'go').update({'exit': 0}),
     'why': 'a row read as a refusal cannot record a clean exit'},
    {'case': 'refusal_node_row_survives', 'expect': 'FAILED_LEG',
     'target': 'turn-deny-cidr-refusal::rust:node-gone',
     'apply': lambda d: _r(d, 'turn-deny-cidr-refusal', 'rust')['store']['after'].update(
         {'node_ids': ['ND_injected']}),
     'why': 'a start that was refused must not leave a registry row behind'},
    {'case': 'refusal_leaves_a_listener', 'expect': 'FAILED_CONTROL',
     'target': 'refusal_leaves_no_listener',
     'apply': lambda d: _r(d, 'turn-domain-incorrect-refusal', 'go')['post_ports'].update(
         {'tls1': {'tcp': 1, 'udp': 0}}),
     'why': 'the claim is that the gate fires before anything binds'},
    {'case': 'refusal_needed_a_kill', 'expect': 'FAILED_CONTROL',
     'target': 'refusal_rows_self_ended',
     'apply': lambda d: _r(d, 'turn-invalid-ports-refusal', 'rust').update({'self_exited': False}),
     'why': 'a refusal the harness had to terminate is a different behaviour'},
    {'case': 'refusal_was_sigkilled', 'expect': 'FAILED_CONTROL',
     'target': 'sigkill_never_used',
     'apply': lambda d: _r(d, 'turn-port-out-of-range', 'go').update({'signals': ['SIGKILL']}),
     'why': 'a killed process invalidates the exit code and the teardown claims'},
    {'case': 'turn_line_in_a_refusal', 'expect': 'FAILED_LEG',
     'target': 'turn-proxy-flag-needs-cidrs::go:log_not-' + needle_slug(TURN_START),
     'apply': lambda d: _say(_r(d, 'turn-proxy-flag-needs-cidrs', 'go'),
                             _turn_line(d, 'turn-yaml-ports-and-leaf', 'go')),
     'why': 'the refusal rows are only refusals if the consumer never started'},
    {'case': 'ready_line_in_a_refusal', 'expect': 'FAILED_LEG',
     'target': 'turn-domain-required-refusal::rust:log_not-' + needle_slug(SERVER_READY),
     'apply': lambda d: _say(_r(d, 'turn-domain-required-refusal', 'rust'), SERVER_READY),
     'why': 'a refusal never reaches readiness, so a daemon line in its log is a rewritten stream'},
    {'case': 'domain_gate_words_diverge', 'expect': 'FAILED_CONTROL',
     'target': 'domain_gate_sentences_identical_across_sides',
     'apply': lambda d: _r(d, 'turn-domain-required-refusal', 'rust').update(
         {'log_masked': _log(_r(d, 'turn-domain-required-refusal', 'rust')).replace(
             DOMAIN_REQUIRED, 'TURN domain must be set')}),
     'why': 'the row is claimed as parity on the strength of the sentence, so the sentence is checked'},
    {'case': 'cidr_gate_words_diverge', 'expect': 'FAILED_CONTROL',
     'target': 'cidr_gate_sentences_identical_across_sides',
     'apply': lambda d: _r(d, 'turn-deny-cidr-refusal', 'rust').update(
         {'log_masked': _log(_r(d, 'turn-deny-cidr-refusal', 'rust')).replace('not-a-cidr', 'bogus')}),
     'why': 'the parsed-entry message is part of what the parity claim rests on'},
    {'case': 'domain_gate_always_on', 'expect': 'FAILED_CONTROL',
     'target': 'domain_gate_is_conditional',
     'apply': lambda d: _mute(_r(d, 'turn-udp-only-no-tls-listener', 'go'), TURN_START),
     'why': 'without a TLS listener the missing domain is allowed; that condition is the row'},
    {'case': 'gate_order_averaged', 'expect': 'FAILED_CONTROL',
     'target': 'garbage_gate_first_sentence_pinned',
     'apply': lambda d: _r(d, 'turn-enabled-same-garbage-refuses', 'rust').update(
         {'log_masked': _log(_r(d, 'turn-enabled-same-garbage-refuses', 'rust')).replace(
             PROXY_NEEDS_CIDRS, DOMAIN_REQUIRED)}),
     'why': 'the two sides refuse different gates first; rewriting one to match would hide it'},

    # ---- keys and the auth channel
    {'case': 'yaml_key_refused', 'expect': 'FAILED_LEG',
     'target': 'turn-keys-yaml-baseline::go:auth-k104y',
     'apply': lambda d: _auth(_r(d, 'turn-keys-yaml-baseline', 'go'), 'k104y', REJECTED),
     'why': 'the YAML map is claimed to be the key set the node validates against'},
    {'case': 'foreign_key_accepted', 'expect': 'FAILED_LEG',
     'target': 'turn-keys-env-replaces-yaml::rust:auth-c103y',
     'apply': lambda d: _auth(_r(d, 'turn-keys-env-replaces-yaml', 'rust'), FOREIGN_KID, ACCEPTED),
     'why': 'a kid no config in this lane carries must stay refused'},
    {'case': 'env_key_merged_not_replaced', 'expect': 'FAILED_CONTROL',
     'target': 'keys_env_replaces_yaml_map',
     'apply': lambda d: _auth(_r(d, 'turn-keys-env-replaces-yaml', 'go'), 'k104y', ACCEPTED),
     'why': 'the env string replaces conf.Keys; if both kids sign, the YAML map was merged'},
    {'case': 'numeric_secret_stringified', 'expect': 'FAILED_CONTROL',
     'target': 'keys_numeric_secret_dropped',
     'apply': lambda d: _auth(_r(d, 'turn-keys-env-numeric-secret', 'rust'), NUMERIC_KID, ACCEPTED),
     'why': 'unmarshalKeys keeps only string values; accepting the numeric one would be a different '
            'reader'},
    {'case': 'auth_cell_missing', 'expect': 'FAILED_CONTROL',
     'target': 'auth_cells_recorded_not_missing',
     'apply': lambda d: _r(d, 'turn-keys-yaml-baseline', 'go')['auth'].pop('k104y'),
     'why': 'a missing cell must not read as the verdict the row wanted'},
    {'case': 'auth_pair_differs', 'expect': 'FAILED_CONTROL',
     'target': 'auth_pair_identical_across_sides',
     'apply': lambda d: _auth(_r(d, 'turn-ttl-clamped-flag', 'rust'), 'k104y', REJECTED),
     'why': 'the differential is the two nodes agreeing on which keys sign in'},
    {'case': 'auth_rejection_logging_drift', 'expect': 'FAILED_CONTROL',
     'target': 'auth_rejection_logging_pinned',
     'apply': lambda d: _say(_r(d, 'turn-keys-yaml-baseline', 'rust'), AUTH_LOG_NEEDLE),
     'why': 'the measured asymmetry is that only the oracle logs refused validates; if that changes the '
            'row needs re-reading'},

    # ---- enabled, relay numbers, lifecycle
    {'case': 'turn_starts_while_disabled', 'expect': 'FAILED_CONTROL',
     'target': 'disabled_pair_pinned',
     'apply': lambda d: _say(_r(d, 'turn-disabled-skips-consumers', 'rust'),
                             _turn_line(d, 'turn-yaml-ports-and-leaf', 'rust')),
     'why': 'the row says the disabled short-circuit returns before the consumer runs'},
    {'case': 'flag_enabled_inert', 'expect': 'FAILED_CONTROL',
     'target': 'flag_enabled_moves_the_listeners',
     'apply': lambda d: [_socks(_r(d, 'turn-flag-disabled-equals-skips-consumers', s), 'tcp')
                         .append('<TLS1>') for s in ('go', 'rust')],
     'why': 'the flag spelling has to move the same listeners the YAML field moves'},
    {'case': 'flag_true_cannot_re_enable', 'expect': 'FAILED_CONTROL',
     'target': 'flag_enabled_moves_the_listeners',
     'apply': lambda d: [(_socks(_r(d, 'turn-flag-enabled-true-beats-yaml-false', s), 'tcp').remove(
         '<TLS1>'), _socks(_r(d, 'turn-flag-enabled-true-beats-yaml-false', s), 'udp').remove('<TU1>'))
                         for s in ('go', 'rust')],
     'why': 'the other direction of the same precedence claim'},
    {'case': 'space_value_consumed', 'expect': 'FAILED_CONTROL',
     'target': 'flag_bool_space_value_pinned',
     'apply': lambda d: _mute(_r(d, 'turn-flag-space-value-keeps-turn-on', 'go'), TURN_START),
     'why': 'the note records that a space-separated bool value is not applied; if it were, the row '
            'has to be re-measured'},
    {'case': 'relay_numbers_are_constants', 'expect': 'FAILED_CONTROL',
     'target': 'relay_numbers_move_with_config',
     'apply': lambda d: _r(d, 'turn-relay-explicit-numbers', 'go').update(
         {'log_masked': _log(_r(d, 'turn-relay-explicit-numbers', 'go')).replace(
             str(RELAY_EXPLICIT[0]), str(RELAY_DEFAULT[0]))}),
     'why': 'the echo is evidence only if it tracks the config'},
    {'case': 'flag_relay_numbers_static', 'expect': 'FAILED_CONTROL',
     'target': 'relay_numbers_move_with_config',
     'apply': lambda d: _r(d, 'turn-relay-flag-numbers', 'rust').update(
         {'log_masked': _log(_r(d, 'turn-relay-flag-numbers', 'rust')).replace(
             str(RELAY_FLAGGED[2]), str(RELAY_DEFAULT[2]))}),
     'why': 'the generated flag spellings are claimed to reach the same three numbers'},
    {'case': 'node_row_absent_while_alive', 'expect': 'FAILED_LEG',
     'target': 'turn-relay-default-numbers::go:node',
     'apply': lambda d: _r(d, 'turn-relay-default-numbers', 'go')['store']['alive'].update(
         {'node_ids': []}),
     'why': 'the started daemon has to be the registry row it claims to be'},
    {'case': 'started_rows_register_nothing', 'expect': 'FAILED_CONTROL',
     'target': 'started_rows_register_one_node',
     'apply': lambda d: _r(d, 'turn-yaml-ports-and-leaf', 'rust')['store']['alive'].update(
         {'node_ids': ['ND_a', 'ND_b']}),
     'why': 'one daemon, one node: a different count means the store was not the harness-owned one'},
    {'case': 'node_row_survives_shutdown', 'expect': 'FAILED_CONTROL',
     'target': 'node_row_gone_after_exit',
     'apply': lambda d: _r(d, 'turn-cidr-lists-valid', 'go')['store']['after'].update(
         {'node_ids': ['ND_stale']}),
     'why': 'clean shutdown deregisters; a surviving row would make every later row suspect'},
    {'case': 'version_marker_lost', 'expect': 'FAILED_CONTROL',
     'target': 'version_marker_survives_clean_shutdown',
     'apply': lambda d: _r(d, 'turn-yaml-ports-and-leaf', 'go')['store']['after'].update(
         {'keys': []}),
     'why': 'the store the harness owns is proved by what the daemon left in it'},
    {'case': 'launch_without_flush', 'expect': 'FAILED_CONTROL',
     'target': 'store_flushed_before_every_launch',
     'apply': lambda d: _r(d, 'turn-yaml-material-b', 'rust').update({'store': {
         'before_dbsize': 4, 'alive': _r(d, 'turn-yaml-material-b', 'rust')['store']['alive'],
         'after': _r(d, 'turn-yaml-material-b', 'rust')['store']['after']}}),
     'why': 'every row is read against a clean store; a dirty start would make the counts meaningless'},
    {'case': 'daemon_never_ready', 'expect': 'FAILED_CONTROL',
     'target': 'ready_rows_reach_the_daemon',
     'apply': lambda d: _r(d, 'turn-yaml-ports-and-leaf', 'go').update({'ready': False}),
     'why': 'a probe against a daemon that never accepted is not launched behaviour'},
    {'case': 'ttl_warning_fabricated', 'expect': 'FAILED_LEG',
     'target': 'turn-ttl-clamped-flag::go:log_not_go-' + needle_slug(TTL_WARN_RUST),
     'apply': lambda d: _say(_r(d, 'turn-ttl-clamped-flag', 'go'), TTL_WARN_RUST),
     'why': 'the clamp message is Rust-only because the oracle clamps before its logger exists'},
    {'case': 'ttl_warning_lost', 'expect': 'FAILED_CONTROL',
     'target': 'ttl_warning_visibility_pinned',
     'apply': lambda d: _mute(_r(d, 'turn-ttl-nonpositive-yaml', 'rust'), TTL_WARN_RUST),
     'why': 'the row keeps the visibility split visible instead of claiming parity'},
    {'case': 'out_of_range_stage_gone', 'expect': 'FAILED_CONTROL',
     'target': 'out_of_range_port_stage_pinned',
     'apply': lambda d: _mute(_r(d, 'turn-port-out-of-range', 'go'), 'address 70000'),
     'why': 'the two sides refuse at different stages; the sentences are what record that'},
    {'case': 'teardown_line_shared', 'expect': 'FAILED_CONTROL',
     'target': 'turn_teardown_line_pinned',
     'apply': lambda d: _say(_r(d, 'turn-yaml-ports-and-leaf', 'go'),
                             'TURN server stopped {"allocations": 0}'),
     'why': 'only the Rust build logs the TURN teardown; a shared line would be a rewritten log'},

    # ---- inventory, privacy and provenance
    {'case': 'profile_missing_from_inventory', 'expect': 'FAILED_CONTROL',
     'target': 'profile_inventory_complete',
     'apply': lambda d: d['profiles'].remove(_e(d, 'turn-ttl-nonpositive-yaml')),
     'why': 'a short receipt cannot certify the rows the batch claims to cover'},
    {'case': 'subset_declared_as_complete', 'expect': 'FAILED_CONTROL',
     'target': 'no_development_subset',
     'apply': lambda d: d.update({'subset': 'injected'}),
     'why': 'the subset marker is what stops a development run being filed as an acceptance'},
    {'case': 'row_loses_its_profile', 'expect': 'FAILED_CONTROL',
     'target': 'every_row_is_named_by_a_profile',
     'apply': lambda d: [e.__setitem__('rows', [r for r in e['rows']
                                                if r != 'YAML:turn.external_tls'])
                         for e in d['profiles']],
     'why': 'a ledger row with no profile behind it is promoted on the strength of nothing'},
    {'case': 'profile_names_nothing', 'expect': 'FAILED_CONTROL',
     'target': 'every_profile_names_a_row',
     'apply': lambda d: _e(d, 'turn-yaml-ports-and-leaf').update({'rows': []}),
     'why': 'each recorded profile has to be evidence for a row the batch declares'},
    {'case': 'start_inventory_wrong', 'expect': 'FAILED_CONTROL',
     'target': 'start_profile_inventory_pinned',
     'apply': lambda d: _e(d, 'turn-domain-incorrect-refusal').update({'refuse_wait': None}),
     'why': 'which rows are starts and which are refusals is a claim about the harness, not the data'},
    {'case': 'unknown_claim_vocabulary', 'expect': 'HARNESS_REFUSAL',
     'target': 'claims_vocabulary_closed',
     'apply': lambda d: _e(d, 'turn-yaml-ports-and-leaf')['claims'].update({'leaf_sha_hint': 'A'}),
     'why': 'a claim nothing evaluates would be recorded, counted and assert nothing'},
    {'case': 'both_sides_not_recorded', 'expect': 'HARNESS_REFUSAL',
     'target': 'every_profile_ran_both_sides',
     'apply': lambda d: _e(d, 'turn-yaml-ports-and-leaf')['runs'].pop('rust'),
     'why': 'a paired lane without one side is not a pair'},
    {'case': 'expectation_moved_after_record', 'expect': 'FAILED_CONTROL',
     'target': 'verdict_stable_across_rederivation',
     'apply': lambda d: _e(d, 'turn-yaml-ports-and-leaf').update({'expect_exit': 7}),
     'why': 'the re-derivation must disagree with the recorded verdicts if the claim moved'},
    {'case': 'loopback_port_left_raw', 'expect': 'FAILED_CONTROL',
     'target': 'no_unmasked_loopback_ports',
     'apply': lambda d: _e(d, 'turn-yaml-ports-and-leaf').update(
         {'note': _e(d, 'turn-yaml-ports-and-leaf')['note'] + ' saw 127.0.0.1:45123'}),
     'why': 'ephemeral ports are machine-specific and must not travel with the receipt'},
    {'case': 'machine_address_in_receipt', 'expect': 'FAILED_CONTROL',
     'target': 'no_machine_address_in_receipt',
     'apply': lambda d: _e(d, 'turn-yaml-ports-and-leaf').update(
         {'note': _e(d, 'turn-yaml-ports-and-leaf')['note'] + ' via 10.113.4.21'}),
     'why': 'a real interface address identifies the host even when the port is masked'},
    {'case': 'off_limit_port_named', 'expect': 'FAILED_CONTROL',
     'target': 'off_limits_never_named',
     'apply': lambda d: _e(d, 'turn-keys-yaml-baseline').update(
         {'note': _e(d, 'turn-keys-yaml-baseline')['note'] + ' (7881 was busy)'}),
     'why': 'the lane never touches the user-facing ports, and the receipt must not suggest it did'},
    {'case': 'host_store_named', 'expect': 'FAILED_CONTROL',
     'target': 'host_store_never_named',
     'apply': lambda d: _e(d, 'turn-keys-yaml-baseline').update(
         {'note': _e(d, 'turn-keys-yaml-baseline')['note'] + ' peer 127.0.0.1:6379'}),
     'why': 'the host store is not this lane to name or use'},
    {'case': 'key_secret_recorded_raw', 'expect': 'FAILED_CONTROL',
     'target': 'fixture_secrets_never_raw',
     'apply': lambda d: _e(d, 'turn-keys-yaml-baseline').update(
         {'note': _e(d, 'turn-keys-yaml-baseline')['note'] + ' ' + YAML_KEYS['k104y']}),
     'why': 'the signing secret is the one thing a committed receipt may never carry'},
    {'case': 'argv_recorded_raw', 'expect': 'FAILED_CONTROL',
     'target': 'argv_never_raw',
     'apply': lambda d: _r(d, 'turn-flag-ports-beat-yaml', 'go').update({'argv_raw': []}),
     'why': 'argv holds absolute fixture paths, so only the masked spelling may be filed'},
    {'case': 'raw_stream_truncated', 'expect': 'FAILED_CONTROL',
     'target': 'raw_streams_recorded',
     'apply': lambda d: _r(d, 'turn-yaml-ports-and-leaf', 'go')['raw'].update({'bytes': 0}),
     'why': 'the digest of the untouched log is what makes the masked copy checkable'},
    {'case': 'working_path_in_receipt', 'expect': 'FAILED_CONTROL',
     'target': 'no_working_paths_in_profile_records',
     'apply': lambda d: _r(d, 'turn-yaml-ports-and-leaf', 'go').update(
         {'cfg_masked': _r(d, 'turn-yaml-ports-and-leaf', 'go')['cfg_masked'] + f'# {REPO}\n'}),
     'why': 'a home or repo path makes the evidence unportable and names the machine'},
    {'case': 'bounds_show_a_touched_port', 'expect': 'FAILED_CONTROL',
     'target': 'off_limit_ports_untouched',
     'apply': lambda d: d['listeners_bounds'][list(d['listeners_bounds'])[0]].update(
         {'after': 1}),
     'why': 'the off-limit ports are sampled before and after; a change is a violation'},
    {'case': 'store_outlived_the_run', 'expect': 'FAILED_CONTROL',
     'target': 'owned_store_stopped_cleanly',
     'apply': lambda d: d.update({'store_final': dict(d['store_final'], listeners_after_stop=1)}),
     'why': 'the lane owns the store it started and must leave nothing listening'},
    {'case': 'binary_identity_drifted', 'expect': 'FAILED_CONTROL',
     'target': 'binaries_unchanged',
     'apply': lambda d: d['binaries']['go'].update({'sha256': '0' * 64}),
     'why': 'the whole comparison is worthless if the images are not the pinned ones'},
    {'case': 'pinned_oracle_recorded_wrong', 'expect': 'FAILED_CONTROL',
     'target': 'recorded_oracle_matches_pinned',
     'apply': lambda d: d['binaries']['go'].update(
         {'sha256': d['binaries']['rust']['sha256']}),
     'why': 'swapping the two identities would make every differential read as agreement'},
    {'case': 'reference_commit_moved', 'expect': 'FAILED_CONTROL',
     'target': 'reference_commit_matches',
     'apply': lambda d: d['reference'].update({'go_checkout_commit': 'deadbeef'}),
     'why': 'the source anchors are quotes from a fixed checkout, not from whatever is on disk'},
    {'case': 'imported_primitives_replaced', 'expect': 'FAILED_CONTROL',
     'target': 'imported_primitives_are_the_accepted_lanes',
     'apply': lambda d: d.update({'c102_harness_sha256': '1' * 64}),
     'why': 'this lane reuses the accepted launcher and probes; a different file is a different lane'},
    {'case': 'harness_edited_after_run', 'expect': 'FAILED_CONTROL',
     'target': 'harness_unchanged_since_run',
     'apply': lambda d: d.update({'harness_sha256': '2' * 64}),
     'why': 'the gate must describe the harness that produced the data, not a later edit of it'},
)


# Fields a --record run leaves in execution.json. A working copy must not carry them: otherwise a
# subset receipt that recorded its own failing verdicts would make an injection that never ran look
# discriminated. `leg_verdicts_at_record` stays - it is the ground truth the stability control reads.
STALE_VERDICT_FIELDS = ('legs', 'leg_count', 'failed_profiles', 'failed_legs', 'failed_controls',
                        'cross_profile_controls', 'status', 'unmet_gates', 'status_gates',
                        'row_profile_map', 'live_run', 'binary_hash_now', 'harness_sha256_now')


def _clean(doc):
    for key in STALE_VERDICT_FIELDS:
        doc.pop(key, None)
    for e in doc['profiles']:
        for key in ('legs', 'problems', 'match'):
            e.pop(key, None)
    return doc


def selftest_gate(doc, out, receipt=None):
    """Apply each named mutation to a deep copy of a real receipt and require the gate to redden."""
    import copy as _copy
    out.mkdir(parents=True, exist_ok=True)
    # The sweep reads the filed artifact and must not alter it: hashing before and after proves that
    # instead of asserting it.
    before = C102.sha256_file(Path(receipt)) if receipt else None
    baseline = _clean(_copy.deepcopy(doc))
    finish(baseline, out=None, live=False, write=False)
    leg_ids = {l['leg'] for l in baseline['legs']}
    control_ids = set(baseline['cross_profile_controls'])
    results = []
    for inj in GATE_INJECTIONS:
        target = inj['target']
        anchor = (target in leg_ids) if inj['expect'] == 'FAILED_LEG' else target in control_ids
        if not anchor:
            results.append({'case': inj['case'], 'expect': inj['expect'], 'target': target,
                            'discriminated': False, 'why': inj['why'],
                            'detail': 'the target does not exist in the unmutated receipt, so this '
                                      'case could only ever appear to pass'})
            continue
        mutated = _clean(_copy.deepcopy(doc))
        detail = ''
        try:
            inj['apply'](mutated)
            finish(mutated, out=None, live=False, write=False)
        except Exception as exc:  # noqa: BLE001 - a harness refusal is itself a verdict
            detail = f'{type(exc).__name__}: {exc}'
        if inj['expect'] == 'FAILED_LEG':
            hit = target in mutated.get('failed_legs', [])
        elif inj['expect'] == 'FAILED_CONTROL':
            hit = target in mutated.get('failed_controls', [])
        elif inj['expect'] == 'HARNESS_REFUSAL':
            hit = bool(detail)
        else:
            raise ValueError(f'unknown expectation {inj["expect"]}')
        results.append({'case': inj['case'], 'expect': inj['expect'], 'target': target,
                        'discriminated': bool(hit), 'why': inj['why'], 'detail': detail or None,
                        'failed_legs_under_case': len(mutated.get('failed_legs', []) or []),
                        'failed_controls_under_case': len(mutated.get('failed_controls', []) or [])})
    after = C102.sha256_file(Path(receipt)) if receipt else None
    results.append({'case': 'sweep_writes_no_receipt', 'expect': 'RECEIPT_HASH_UNCHANGED',
                    'target': str(receipt) if receipt else None,
                    'discriminated': bool(before) and before == after,
                    'why': 'the audit runs against the filed artifact and may not alter it',
                    'detail': f'{before} -> {after}'})
    report = {'batch': doc['batch'], 'cases': len(results),
              'gate_injections': len(GATE_INJECTIONS),
              'receipt_sha256_before': before, 'receipt_sha256_after': after,
              'baseline_legs': len(leg_ids), 'baseline_controls': len(control_ids),
              'all_discriminated': all(r['discriminated'] for r in results),
              'undiscriminated': [r['case'] for r in results if not r['discriminated']],
              'results': results}
    (Path(out) / f'{BATCH}-gate-falsification.json').write_text(
        json.dumps(report, indent=2, ensure_ascii=False, sort_keys=True) + '\n')
    return report


# ------------------------------------------------------------------------------- manifest
def git_output(cwd, *args) -> str:
    import subprocess
    out = subprocess.run(['git', '-C', str(cwd), *args], capture_output=True, text=True)
    return out.stdout.strip()


def new_doc(bins, ctx, harness_sha, c102_sha, c103_sha, bounds, hold, subset) -> dict:
    # Tier 5: both sides are the launched production binaries, paired on identical YAML bytes, and the
    # effect is read back from the live listeners (socket table, TLS handshake, STUN Binding) rather
    # than from a config struct.
    return {'schema': SCHEMA, 'batch': BATCH, 'covers': COVERS, 'tier': 5, 'live_run': True,
            'skip_launch': False, 'subset': bool(subset),
            'platform': {'system': sys.platform, 'arch': os.uname().machine,
                         'python': sys.version.split()[0]},
            'binaries': bins,
            'reference': {'go_checkout': 'references/livekit-stable',
                          'go_commit': FIXED_GO_COMMIT,
                          'go_checkout_commit': git_output(REFERENCE, 'rev-parse', 'HEAD'),
                          'upstream_reads': {
                              'pkg/service/turn.go:65-80': 'NewTurnServer returns nil,nil when the '
                                                           'block is disabled, then "invalid TURN '
                                                           'ports", "TURN domain required" and "TURN '
                                                           'domain is not correct" in that order',
                              'pkg/service/turn.go:84-95': 'parsePeerCIDRs runs once at startup for '
                                                           'turn.allow_restricted_peer_cidrs and '
                                                           'turn.deny_peer_cidrs',
                              'pkg/service/turn.go:215': 'the single "Starting TURN server" line, '
                                                         'after the binds, carrying the relay range, '
                                                         'the per-user limit and the port keys',
                              'pkg/service/utils.go:156-159': 'IsValidDomain is the predicate behind '
                                                              '"TURN domain is not correct"',
                              'pkg/config/config.go:714-721': 'relay_range defaults (30000-40000 off, '
                                                              '30000-30002 in development) when either '
                                                              'bound is zero',
                              'pkg/config/config.go:940-1035': 'GenerateCLIFlags names each flag after '
                                                               'the YAML path and each env var after '
                                                               'LIVEKIT_ + that path',
                              'pkg/config/config.go:1135-1158': 'unmarshalKeys re-parses the keys '
                                                                'string as YAML and keeps only the '
                                                                'string-valued entries',
                              'cmd/server/main.go:56-64': 'the base keys/turn-cert/turn-key flags; '
                                                          '"keys" carries Sources: cli.EnvVars('
                                                          '"LIVEKIT_KEYS")',
                              'crates/livecloud/src/turn.rs:100-160': 'the Rust listener list, the '
                                                                      'tls acceptor and the same '
                                                                      'startup keys',
                              'crates/lk-config/src/normalize.rs:712-780': 'the t.enabled gates on '
                                                                           'ports, proxy CIDRs and '
                                                                           'peer CIDRs',
                              'crates/lk-config/src/normalize.rs:893-931': 'validate_turn and '
                                                                           'is_valid_domain reproduce '
                                                                           'the upstream sentences',
                              'crates/lk-config/src/cli.rs:87-91': 'the base "keys" flag with '
                                                                   'LIVEKIT_KEYS'}},
            'harness': str(HERE / 'run-paired.py'),
            'harness_sha256_at_start': harness_sha, 'harness_sha256': harness_sha,
            'imports_from': [str(C102_DIR / 'run-paired.py'), str(C103_DIR / 'run-paired.py')],
            'c102_harness_sha256_at_start': c102_sha, 'c102_harness_sha256': c102_sha,
            'c103_harness_sha256_at_start': c103_sha, 'c103_harness_sha256': c103_sha,
            'imports_reason': 'the launcher, port allocator, socket observers, owned store and address '
                              'masker are the accepted C102 primitives; the certificate generator, the '
                              'auth probe and the sentence reader are the accepted C103 ones. Importing '
                              '(not copying) leaves both files byte-identical, so their filed receipts '
                              'stay re-derivable',
            'work_dir': str(ctx['work']),
            'ports': {k[:-5]: v for k, v in ctx.items() if k.endswith('_port')},
            'port_roles': {str(k): v for k, v in ctx['port_roles'].items()},
            'probe_sources': {'udp': '<PU>', 'tcp': '<PT>'},
            'ip_salt': IP_SALT, 'off_limits': list(OFF_LIMITS), 'host_store_port': HOST_STORE_PORT,
            'hold_seconds': hold, 'listeners_bounds': bounds, 'store_flush_ok': True,
            'redis': {'plain_role': 'STORE'},
            'fixtures': {'key_ids': sorted(SIGNABLE), 'secrets': ['<SEC-Y>', '<SEC-E>'],
                         'domain': DOMAIN, 'bad_domain': BAD_DOMAIN,
                         'relay_numbers': {'default': list(RELAY_DEFAULT),
                                           'explicit': list(RELAY_EXPLICIT),
                                           'flagged': list(RELAY_FLAGGED)},
                         'certs': {name: {'cert_der_sha256': info['cert_der'],
                                          'cert_masked': str(info['cert']).replace(
                                              str(ctx['work']), '<WORK>')}
                                   for name, info in ctx['certs'].items()}},
            'profiles': []}


# ------------------------------------------------------------------------------- driver
def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument('--record', action='store_true')
    ap.add_argument('--report', help='re-derive the verdict from a filed execution.json')
    ap.add_argument('--selftest-gate', help='apply named mutations to a recorded execution.json')
    ap.add_argument('--out', default='/tmp/c104-lane')
    ap.add_argument('--go', default='/tmp/c99-build/livekit-server-go')
    ap.add_argument('--rust', default=str(REPO / 'target/release/livecloud'))
    ap.add_argument('--only', help='comma-separated profile ids; a subset is never an acceptance')
    ap.add_argument('--hold', type=float, default=0.6)
    a = ap.parse_args()
    out = Path(a.out)
    (out / 'logs').mkdir(parents=True, exist_ok=True)
    harness_sha = C102.sha256_file(Path(__file__).resolve())
    c102_sha = C102.sha256_file(C102_DIR / 'run-paired.py')
    c103_sha = C102.sha256_file(C103_DIR / 'run-paired.py')
    if a.report or a.selftest_gate:
        src = Path(a.report or a.selftest_gate)
        if src.is_dir():
            src = src / 'execution.json'
        doc = json.loads(src.read_text())
        if a.only:
            wanted = set(a.only.split(','))
            doc['profiles'] = [e for e in doc['profiles'] if e['profile'] in wanted]
            doc['subset'] = a.only
            print(f'development subset: {sorted(wanted)}', flush=True)
        if a.report:
            result = finish(doc, out=None, live=False, write=False)
            (out / 'report-rederivation.txt').write_text(json.dumps(result, indent=2) + '\n')
            print(json.dumps(result['summary'], indent=2))
            return 0 if result['all_ok'] else 1
        report = selftest_gate(doc, out, src)
        print(json.dumps({k: report[k] for k in ('cases', 'baseline_legs', 'baseline_controls',
                                                 'all_discriminated', 'undiscriminated')}, indent=2))
        return 0 if report['all_discriminated'] else 1
    if not a.record:
        ap.error('choose --record, --report or --selftest-gate')
    bins = {}
    for side, path in (('go', a.go), ('rust', a.rust)):
        pth = Path(path)
        if not pth.is_file():
            print(f'missing {side} binary: {pth}', file=sys.stderr)
            return 2
        bins[side] = {'path': str(pth), 'sha256': C102.sha256_file(pth)}
    for side, pinned in (('go', ORACLE_SHA256), ('rust', ACCEPTANCE_SHA256)):
        if bins[side]['sha256'] != pinned:
            print(f'refusing: the {side} image is {bins[side]["sha256"]}, pinned {pinned}',
                  file=sys.stderr)
            return 2
    bounds = {str(port): {'before': C102.listeners_on(port), 'after': None}
              for port in list(OFF_LIMITS) + [HOST_STORE_PORT]}
    ctx = build_ctx(out, a.hold)
    masker = Masker(ctx['port_roles'], {str(ctx['work']): '<WORK>', str(REPO): '<REPO>',
                                        str(Path.home()): '<HOME>'})
    store = C102.start_store(ctx['store_port'])
    doc = None
    try:
        deadline = time.monotonic() + 15
        while time.monotonic() < deadline and C102.redis_cli(ctx['store_port'], 'PING') != 'PONG':
            time.sleep(0.2)
        if C102.dbsize(ctx['store_port']) != 0:
            print('owned store is not empty at startup', file=sys.stderr)
            return 2
        profs = profiles(ctx)
        if a.only:
            wanted = set(a.only.split(','))
            profs = [p for p in profs if p['id'] in wanted]
            print(f'development subset: {sorted(wanted)}', flush=True)
        doc = new_doc(bins, ctx, harness_sha, c102_sha, c103_sha, bounds, a.hold, a.only)
        binaries = {side: info['path'] for side, info in bins.items()}
        for prof in profs:
            doc['profiles'].append(run_entry(ctx, prof, masker, binaries))
            print(f"  ran {prof['id']}", flush=True)
        doc['redis_final'] = {'plain': C102.dbsize(ctx['store_port'])}
        C102.stop_store(store, 0)
        doc['store_final'] = {'dbsize_after_stop': C102.dbsize(ctx['store_port']),
                              'listeners_after_stop': C102.listeners_on(ctx['store_port'])}
        for key in list(bounds):
            bounds[key]['after'] = C102.listeners_on(int(key))
        result = finish(doc, out=out, live=True)
        (out / 'harness-sha256.txt').write_text(harness_sha + '\n')
        for entry in doc['profiles']:
            for side in ('go', 'rust'):
                log = entry['runs'][side].get('log_masked') or ''
                (out / 'logs' / f"{side}-{entry['profile']}.log").write_text(log)
        (out / 'profiles.json').write_text(json.dumps(
            [{'profile': e['profile'], 'rows': e['rows'], 'claims': e['claims'], 'note': e['note'],
              'expect_exit': e['expect_exit'], 'refuse_wait': e['refuse_wait'],
              'probe_ports': e['probe_ports'], 'match': e['match'], 'problems': e['problems'],
              'argv': [r['argv_masked'] for r in (e['runs']['go'], e['runs']['rust'])],
              'auth': {side: auth_cells(e['runs'][side]) for side in ('go', 'rust')},
              'leaf': {side: e['runs'][side].get('leaf_class') for side in ('go', 'rust')},
              'stun': {side: {'udp': (e['runs'][side].get('stun_udp') or {}).get('kind'),
                              'tcp': (e['runs'][side].get('stun_tcp') or {}).get('kind')}
                       for side in ('go', 'rust')}}
             for e in doc['profiles']], indent=2, ensure_ascii=False, sort_keys=True) + '\n')
        print(json.dumps(result['summary'], indent=2))
        return 0 if result['all_ok'] else 1
    finally:
        if doc is not None:
            for entry in doc.get('profiles') or []:
                for side in ('go', 'rust'):
                    log = entry['runs'][side].get('log_masked') or ''
                    if log:
                        (out / 'logs' / f"{side}-{entry['profile']}.log").write_text(log)
        try:
            C102.stop_store(store, 0)
        except Exception:  # noqa: BLE001
            pass
        if C102.listeners_on(ctx['store_port']):
            print('warning: the owned store port still has a listener', file=sys.stderr)


if __name__ == '__main__':
    raise SystemExit(main())
