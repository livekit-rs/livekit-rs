#!/usr/bin/env python3
"""C105 lane: the TURN credential material, judged on the ice_servers a real join returns.

SPEC-0077 rows driven here (upstream anchors, references/livekit-stable @ e5655ac):

    YAML:rtc.turn_servers[].secret      pkg/config/config.go:205-208 (the field), config.go:857-898
                                        (LoadTURNSecrets) and pkg/service/roommanager.go:1101-1136
                                        (username "<expiry>:<pID>", credential base64(HMAC-SHA1))
    YAML:rtc.turn_servers[].secret_file config.go:210-211 + config.go:867-883: os.Stat, the
                                        other-permission filter 0o007, ReadFile, TrimSpace, then
                                        ErrTURNSecretEmpty - reached from the daemon at
                                        cmd/server/main.go:263
    YAML:rtc.turn_servers[].username    config.go:889-894 (has_static needs BOTH username and
                                        credential) and roommanager.go:1134-1136 (static passthrough)
    YAML:rtc.turn_servers[].credential  the same union; ErrTURNServerNoCredentials config.go:75
    YAML:rtc.turn_servers[].host/.port  roommanager.go:1138-1142 ("%s:%s:%d?transport=%s")
    YAML:rtc.turn_servers[].protocol    roommanager.go:1101-1108 ("tls" -> turns, "udp" -> udp,
                                        anything else -> turn/tcp)
    YAML:rtc.turn_servers[].ttl         roommanager.go:1110-1120 (0 -> DefaultExternalTURNTTLSeconds
                                        14400, then ClampTURNTTLSeconds config.go:903-913) and
                                        config.go:926-939 (NormalizeTURNTTLs caps the upper bound at
                                        load and says "turn_servers ttl out of range")
    YAML:turn.ttl_seconds               pkg/service/turn.go:292-297 CreateUsername clamps the same
                                        way, and roommanager.go:1080-1084 feeds the result to
                                        CreatePassword -> base62(sha256("secret|pID|expiry"))
    generated_flags:--turn.ttl_seconds  pkg/config/config.go:940-1035: the flag name is the YAML path

Rust mirrors all of it: crates/lk-config/src/load.rs:384-420 (load_turn_secrets, the same four
sentences), load.rs:221-245 (normalize_turn_ttls), crates/lk-compat/src/signal/ice.rs:62-122
(embedded + external URL shapes and the credential split), ice.rs:145-161 (rest_credentials) and
crates/lk-compat/src/turn_auth.rs:43-80 (create_credentials, the base62 username and password).

Nothing here reads a config struct or a log line about a secret. The channel is a live websocket
join - `GET /rtc/v1?join_request=<wrapped JoinRequest>` with a bearer token, the RFC 6455 handshake
and the protobuf frames read with nothing but the CPython standard library - and the claim is what
the running node put into `SignalResponse.join.ice_servers`:

  secret        the credential on the wire equals base64(HMAC-SHA1(configured secret, username))
                recomputed here from the fixture bytes, and a foreign secret is recorded as NOT
                matching on the same row. A secret the node never read cannot produce that digest.
  secret_file   the same test with the file's bytes as the key, against a config whose inline secret
                is a different value, so the digest says which of the two the daemon opened.
  username      a static pair comes back byte-identical (compared by digest), and a dynamic one comes
                back as `<expiry>:<participant SID>` with this join's own SID in it.
  ttl           the expiry inside the username, minus the second the join happened: 14400 for the
                external default, 600 for a configured value, 300 for a negative one, 86400 for one
                past the cap, and the same four readings again for the embedded server.
  host/port     the URL string, with the role token standing in for the port.
  protocol      turns vs turn and transport=tcp vs udp, including what an unlisted value does.
  keys          the embedded password is base62(sha256("<key secret>|<SID>|<expiry>")) - it only
                reproduces for the secret of the key the token was signed with, which is a stronger
                statement about the key map than the 401/400 answer C104 graded.

Three things were assumed one way before this lane ran and measured the other:

  C104 recorded "ttl_seconds has no launched channel: neither binary exposes GetTURNCredentials". The
  expiry is inside the credential the join hands out, so both ttl rows do have one, and they are
  promoted here.

  An external `ttl` above the cap does not look like a clamp from the outside: both binaries cap it at
  load time (config.go:926-939, load.rs:236-242), so the wire shows 86400 on both, and the only trace
  is the warning Go prints before its logger is attached and Rust prints as a normal line.

  `--turn.ttl_seconds 240` over a YAML 120 does move the expiry, but a *negative* external ttl does
  not go through the load-time clamp on either side - it survives to credential generation, where Go
  and Rust both fall back to the 300 s default rather than the 14400 that 0 gets.

  The embedded password is not a fixed-width string. The compact base62 codec both sides use packs
  the digest's bits, so a digest whose top byte is small encodes to 43 characters and one whose top
  byte is large encodes to 44 - and the digest covers the participant SID, which differs between two
  launches of the same config. The first subset run showed go=43, rust=44 for one profile and read
  as a divergence. The width claim is therefore a band per side, and the pair comparison is made on
  the bucket the width falls in, not on the raw number.

Modes:
  --record        run every profile against both pinned binaries and write the receipt.
  --report        re-derive the verdict from a filed execution.json without launching anything.
  --selftest-gate apply one named mutation to a recorded receipt and require the gate to redden.
  --only          development subset; it reports PARTIAL_SUBSET_NOT_ACCEPTANCE, never a pass.

Primitives are imported, not copied: the launcher, port allocator, socket observers, owned store and
address masker from the accepted C102 lane; the JWT builder, sentence reader, key-file writer and
port listeners from C103; the masker subclass with its loopback-port masking from C104. Importing
leaves all three files byte-identical, so their filed receipts stay re-derivable.
"""
from __future__ import annotations

import argparse
import base64
import hashlib
import hmac
import importlib.util
import json
import os
import re
import socket
import struct
import subprocess
import sys
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
REPO = HERE.parents[2]
C102_DIR = REPO / 'tests/interop/base-flag-channels'
C103_DIR = REPO / 'tests/interop/generated-flag-channels'
C104_DIR = REPO / 'tests/interop/turn-material-channels'


def _load(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, str(path))
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


C102 = _load('c102_harness', C102_DIR / 'run-paired.py')
C103 = _load('c103_harness', C103_DIR / 'run-paired.py')
C104 = _load('c104_harness', C104_DIR / 'run-paired.py')

BATCH = 'C105-20261127-turn-credential-join'
SCHEMA = 'spec0077-c105-turn-credential-join/1'
IP_SALT = 'c105-creds'
ORACLE_SHA256 = C102.ORACLE_SHA256
ACCEPTANCE_SHA256 = C102.ACCEPTANCE_SHA256
FIXED_GO_COMMIT = C102.FIXED_GO_COMMIT
REFERENCE = C102.REFERENCE
OFF_LIMITS = C102.OFF_LIMITS
HOST_STORE_PORT = C102.HOST_STORE_PORT
C102_FILED_HARNESS_SHA = 'fff0615af856f95fc329a5aee3bbafcae2ce795474edde88176c27ca10086a22'
C103_FILED_HARNESS_SHA = '93abcdd655d8638b9d4a698e6858018f6b70eeb1a83d8e67fb5c0e98836b3f37'
C104_FILED_HARNESS_SHA = 'a4b5eacb2f38f7fd3c6dc1bb3cb2e853e6537ff8729d48f3e86ddb4733c5d130'
STATUS_PASS = 'PASS_LAUNCHED_TURN_CREDENTIAL_JOIN'
PROFILE_COUNT = 33
# The eleven rows this lane is trying to move, then the rows it only corroborates: those already
# carry a verdict from C103/C104 and a second, independent channel (a credential on the wire instead
# of a listener or a 401) is added to their evidence, not to their state.
COVERS = [
    'YAML:rtc.turn_servers', 'YAML:rtc.turn_servers[].host', 'YAML:rtc.turn_servers[].port',
    'YAML:rtc.turn_servers[].protocol', 'YAML:rtc.turn_servers[].username',
    'YAML:rtc.turn_servers[].credential', 'YAML:rtc.turn_servers[].secret',
    'YAML:rtc.turn_servers[].secret_file', 'YAML:rtc.turn_servers[].ttl',
    'YAML:turn.ttl_seconds', 'generated_flags:--turn.ttl_seconds',
    'YAML:turn.domain', 'YAML:turn.udp_port', 'YAML:turn.tls_port', 'YAML:keys',
    'YAML:turn.enabled', 'YAML:rtc.node_ip', 'YAML:rtc.stun_servers',
]

# Fixture vocabulary. The four secrets are this lane's own invention and only ever appear as letter
# aliases in the receipt; the digests that prove them are recomputed from these bytes at evaluate
# time, so a receipt can be re-graded without the daemon.
KID = 'c105k'
KSEC = 'c105keysecret0123456789abcdef0'
KID2 = 'c105m'
KSEC2 = 'c105secondkeysecret0123456789abcde'
# The candidate names join_probe tries each secret under, and the reverse map so a username's API key
# can be compared with the secret that actually signed its password.
KEY_ALIAS = {KID: 'k', KID2: 'm'}
TWO_KEY_PROBES = 12
EXT_SECRET = 'c105externalsecret0123456789ab'
FILE_SECRET = 'c105filesecret0000123456789abcdef'
STATIC_USER = 'c105staticuser'
STATIC_CRED = 'c105staticcredential'
FOREIGN_SECRET = 'c105foreignsecret00000000000000ab'
# The TURN ports this lane writes as literals into its YAML. They must stay outside the allocated
# role range or the masker would rewrite a fixture port into a role token and the URL claim would be
# comparing two different things on each run.
LITERAL_TURN_PORTS = {3478, 3479, 443, 5349, 7887}
DOMAIN = 'turn.c105.test'
HOST = 'ext.c105.test'
ROOM = 'c105room'
SECRET_ALIASES = [(KSEC, 'SEC-K'), (KSEC2, 'SEC-M'), (EXT_SECRET, 'SEC-X'),
                  (FILE_SECRET, 'SEC-F'), (FOREIGN_SECRET, 'SEC-G')]
STATIC_ALIASES = [(STATIC_USER, 'USR'), (STATIC_CRED, 'CRD')]
# A secret file whose bytes are not text: config.go:877 ReadFile + TrimSpace has no UTF-8 rule, so
# the raw bytes become the HMAC key. The harness needs the same bytes to recompute the digest.
BIN_SECRET = b'\xff\xfe\x00\x41\x7f\x80\x1bBINc105\x00\xc3\x28'

# The four TTL numbers the upstream constants define. Every ttl leg is a band around one of them.
TTL_DEFAULT_EMBEDDED = 300       # config.DefaultTURNTTLSeconds
TTL_DEFAULT_EXTERNAL = 14400     # config.DefaultExternalTURNTTLSeconds
TTL_MAX = 86400                  # config.TURNMaxTTLSeconds
TTL_MIN = 120                    # config.TURNMinTTLSeconds, the floor of a clamp

# Sentences both binaries print (references/livekit-stable/pkg/config/config.go:71-75, :864, :881,
# :932 and crates/lk-config/src/load.rs:239, :394, :415, keys.rs:117, :133). Each is quoted only by
# its stable middle, because Go wraps it in `turn server "host" secret file "path": %w` and Rust
# formats the same words with its own quoting.
NO_CREDENTIALS = 'turn server has no usable credentials'
SECRET_EMPTY = 'turn secret is empty'
PERMS_NOT_ZERO = 'turn secret file others permissions must be set to 0'
SECRET_PRECEDENCE = 'the hardcoded secret will be used'
TS_TTL_CLAMPED = 'turn_servers ttl out of range, using safe value'
# config.go:917-926, verbatim. Rust emits the same sentence after its logger exists; Go calls
# NormalizeTURNTTLs from NewConfig (main.go:204) before InitLoggerFromConfig (main.go:208), so the
# Go clamp is only visible on the credential, never in the log.
EMB_TTL_CLAMPED = 'turn.ttl_seconds out of range, using safe value'
NO_SUCH_FILE = C103.NO_SUCH_FILE                    # go:  'no such file or directory'
NO_SUCH_FILE_RUST = 'No such file or directory'     # rust: same errno, capitalised by std::io
IS_A_DIRECTORY = 'is a directory'                   # go:  read <path>: is a directory
IS_A_DIRECTORY_RUST = 'Is a directory'              # rust: the same errno, capitalised
TURN_START = C104.TURN_START                        # 'Starting TURN server'
SERVER_READY = C103.SERVER_READY                    # 'starting LiveKit server'
TLS_CERT_REQUIRED = 'TURN tls cert required'

# --------------------------------------------------------------------------- protobuf wire
# A generated stub would only prove the codec that generated it. This is the smallest reader and
# writer that can speak the field numbers pinned in crates/lk-protocol/protos/livekit_rtc.proto:
# SignalResponse.join = 1, JoinResponse.ice_servers = 5, ICEServer{urls=1, username=2, credential=3}.


def _varint(n: int) -> bytes:
    out = bytearray()
    while True:
        b = n & 0x7F
        n >>= 7
        out.append(b | (0x80 if n else 0))
        if not n:
            return bytes(out)


def _tag(field: int, wire: int) -> bytes:
    return _varint((field << 3) | wire)


def _len_field(field: int, payload: bytes) -> bytes:
    return _tag(field, 2) + _varint(len(payload)) + payload


def pb_decode(buf: bytes) -> dict:
    """field number -> ordered list of (wire type, value) for one protobuf message."""
    out: dict = {}
    i, n = 0, len(buf)
    while i < n:
        key, shift = 0, 0
        while True:
            if i >= n:
                raise ValueError('truncated field key')
            b = buf[i]
            i += 1
            key |= (b & 0x7F) << shift
            shift += 7
            if not b & 0x80:
                break
        field, wire = key >> 3, key & 7
        if wire == 0:
            v, shift = 0, 0
            while True:
                if i >= n:
                    raise ValueError('truncated varint')
                b = buf[i]
                i += 1
                v |= (b & 0x7F) << shift
                shift += 7
                if not b & 0x80:
                    break
        elif wire == 2:
            ln, shift = 0, 0
            while True:
                if i >= n:
                    raise ValueError('truncated length')
                b = buf[i]
                i += 1
                ln |= (b & 0x7F) << shift
                shift += 7
                if not b & 0x80:
                    break
            if i + ln > n:
                raise ValueError('truncated payload')
            v = buf[i:i + ln]
            i += ln
        elif wire == 5:
            v = buf[i:i + 4]
            i += 4
        elif wire == 1:
            v = buf[i:i + 8]
            i += 8
        else:
            raise ValueError('unsupported wire type %d' % wire)
        out.setdefault(field, []).append((wire, v))
    return out


def pb_strings(buf: bytes, field: int) -> list:
    return [v.decode('utf-8', 'replace') for w, v in pb_decode(buf).get(field, []) if w == 2]


# WrappedJoinRequest{compression: NONE, join_request: <empty JoinRequest>} is `12 00`: the token
# carries the room, identity and grants, so the message itself needs no field.
JOIN_REQUEST_PARAM = base64.urlsafe_b64encode(bytes([0x12, 0x00])).decode()


# --------------------------------------------------------------------------- base62
# jxskiss/base62 v2, the alphabet and the 0x1E compaction rule Go's protocol auth uses; the Rust
# port documents the same layout in crates/rtc-core/src/util.rs:11-46 and turn_auth.rs:169-195.
B62 = b'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789'
COMPACT_MASK = 0x1E


def _get6bits(src: bytes, pos: int) -> int:
    r = pos & 7
    i = pos >> 3
    if r == 0:
        i -= 1
        r = 8
    b = src[i] >> (8 - r)
    if r < 6 and i > 0:
        b |= src[i - 1] << r
    return b & 0x3F


def b62_encode(src: bytes) -> str:
    out = []
    pos = len(src) * 8
    while pos > 0:
        size = 6
        b = _get6bits(src, pos)
        if b & COMPACT_MASK == COMPACT_MASK:
            if pos > 6 or b > 0x1F:
                size = 5
            b &= 0x1F
        out.append(B62[b])
        pos = max(pos - size, 0)
    return bytes(out).decode()


def _b62_symbol(c: int) -> int:
    if 65 <= c <= 90:
        return c - 65
    if 97 <= c <= 122:
        return c - 97 + 26
    if 48 <= c <= 57:
        return c - 48 + 52
    raise ValueError('illegal base62 character')


def b62_decode(src: str) -> bytes:
    bs = src.encode()
    out = bytearray()
    pos = 0
    acc = 0
    for i, c in enumerate(bs):
        x = _b62_symbol(c)
        acc |= x << pos
        if i == len(bs) - 1:
            pos += x.bit_length()
        elif x & COMPACT_MASK == COMPACT_MASK:
            pos += 5
        else:
            pos += 6
        if pos >= 8:
            out.append(acc & 0xFF)
            pos %= 8
            acc >>= 8
    if pos > 0:
        out.append(acc & 0xFF)
    out.reverse()
    return bytes(out)


# --------------------------------------------------------------------------- websocket client
# RFC 6455 with no dependencies: the join needs one masked text-less binary request (nothing at all,
# the request rides in the query string) and then reads unmasked server frames.


def ws_open(port: int, path: str, headers: dict, timeout: float = 6.0):
    key = base64.b64encode(os.urandom(16)).decode()
    lines = [f'GET {path} HTTP/1.1', f'Host: 127.0.0.1:{port}', 'Upgrade: websocket',
             'Connection: Upgrade', f'Sec-WebSocket-Key: {key}', 'Sec-WebSocket-Version: 13']
    lines += [f'{k}: {v}' for k, v in headers.items()]
    sock = socket.create_connection(('127.0.0.1', port), timeout=timeout)
    sock.settimeout(timeout)
    sock.sendall(('\r\n'.join(lines) + '\r\n\r\n').encode())
    buf = b''
    while b'\r\n\r\n' not in buf:
        chunk = sock.recv(4096)
        if not chunk:
            break
        buf += chunk
    head, _, rest = buf.partition(b'\r\n\r\n')
    first = head.split(b'\r\n', 1)[0].decode('latin1')
    return sock, first, rest


def ws_read_frame(sock, state: dict):
    """(opcode, payload) using bytes already read in state['buf']."""

    def need(n: int) -> bool:
        while len(state['buf']) < n:
            try:
                chunk = sock.recv(65536)
            except OSError:
                return False
            if not chunk:
                return False
            state['buf'] += chunk
        return True

    if not need(2):
        raise ConnectionError('socket closed before a frame header')
    b0, b1 = state['buf'][0], state['buf'][1]
    op = b0 & 0x0F
    ln = b1 & 0x7F
    off = 2
    if ln == 126:
        if not need(4):
            raise ConnectionError('socket closed inside a 16-bit length')
        ln = struct.unpack('>H', state['buf'][2:4])[0]
        off = 4
    elif ln == 127:
        if not need(10):
            raise ConnectionError('socket closed inside a 64-bit length')
        ln = struct.unpack('>Q', state['buf'][2:10])[0]
        off = 10
    if not need(off + ln):
        raise ConnectionError('socket closed inside a frame body')
    payload = state['buf'][off:off + ln]
    state['buf'] = state['buf'][off + ln:]
    return op, payload


def token(kid: str, secret: str, room: str = ROOM, sub: str = 'c105probe', ttl: int = 300) -> str:
    """HS256 access token with the grants a join needs (roomJoin plus the room name)."""
    now = int(time.time())
    def b64u(obj):
        return C103.b64u(json.dumps(obj, separators=(',', ':')).encode())
    head = b64u({'alg': 'HS256', 'typ': 'JWT'})
    body = b64u({'iss': kid, 'sub': sub, 'nbf': now - 5, 'exp': now + ttl,
                 'video': {'room': room, 'roomJoin': True}})
    sig = base64.urlsafe_b64encode(
        hmac.new(secret.encode(), f'{head}.{body}'.encode(), hashlib.sha256).digest()).rstrip(b'=').decode()
    return f'{head}.{body}.{sig}'


def err_kind(exc: OSError) -> str:
    if isinstance(exc, ConnectionRefusedError):
        return 'refused'
    if isinstance(exc, socket.timeout):
        return 'timeout'
    if isinstance(exc, ConnectionResetError):
        return 'reset'
    return f'errno-{abs(exc.errno or 0)}'


# --------------------------------------------------------------------------- credential maths
def rest_credential(secret: bytes, username: str) -> str:
    """roommanager.go:1125-1129: base64(HMAC-SHA1(secret, username))."""
    return base64.b64encode(hmac.new(secret, username.encode(), hashlib.sha1).digest()).decode()


def embedded_password(secret: str, sid: str, expiry: int) -> str:
    """turn.go:325-334: base62(sha256("secret|participantID|expiry"))."""
    return b62_encode(hashlib.sha256(f'{secret}|{sid}|{expiry}'.encode()).digest())


REST_RE = re.compile(r'^(\d{9,11}):(PA_[0-9A-Za-z]{6,16})$')


def row_class(username: str) -> dict:
    """What shape the credential pair on one ICE row has, before any secret is tried against it."""
    m = REST_RE.match(username)
    if m:
        return {'class': 'rest', 'expiry': int(m.group(1)), 'sid': m.group(2)}
    if username == '':
        return {'class': 'empty', 'expiry': None, 'sid': None}
    try:
        decoded = b62_decode(username).decode('utf-8')
    except Exception:
        # A literal username is not a base62 blob: "c105staticuser" decodes to bytes that are not
        # UTF-8, so the codec failing is the normal answer for the static pair, not an anomaly.
        return {'class': 'static', 'expiry': None, 'sid': None, 'text': username}
    parts = decoded.split('|')
    if len(parts) == 3 and re.fullmatch(r'\d{9,11}', parts[2]) and parts[1].startswith('PA_'):
        return {'class': 'embedded', 'expiry': int(parts[2]), 'sid': parts[1], 'kid': parts[0]}
    return {'class': 'static', 'expiry': None, 'sid': None, 'text': username}
