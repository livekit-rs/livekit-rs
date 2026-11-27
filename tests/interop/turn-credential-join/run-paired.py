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




class Masker(C104.Masker):
    """C104's masker (paths, ports, store password, key secrets, timestamps, letter addresses, and the
    unallocated `127.0.0.1:<port>` -> `<EPHEM>` sweep) plus the two vocabularies this lane puts on the
    wire: the participant SIDs a join generates, and the static credential pair from the YAML."""

    def secrets(self, text: str) -> str:
        text = super().secrets(text)
        for real, alias in SECRET_ALIASES:
            text = text.replace(real, f'<{alias}>')
        return text

    def text(self, text: str, keep_node_ids: bool = True) -> str:
        out = super().text(text, keep_node_ids=keep_node_ids)
        out = re.sub(r'\bPA_[0-9A-Za-z]{6,16}\b', '<PA>', out)
        for real, alias in STATIC_ALIASES:
            out = out.replace(real, f'<{alias}>')
        return out


# --------------------------------------------------------------------------- the join probe
def join_probe(port: int, kid: str, key_secret: str, secrets: dict, timeout: float = 6.0) -> dict:
    """One real RFC 6455 join against the live daemon, decoded with the standard library only.

    `secrets` names every candidate this profile configured - the external secret(s) a REST credential
    could have been signed with, and the API-key secret(s) an embedded password could have been derived
    from. Each is tried and its own verdict stored, so "matched nothing" stays distinguishable from
    "nobody looked", and the foreign key is on the list too: a row that matches everything is a row
    whose digest was never compared to anything.
    """
    tok = token(kid, key_secret)
    path = f'/rtc/v1?join_request={JOIN_REQUEST_PARAM}&protocol=15'
    try:
        sock, first, rest = ws_open(port, path, {'Authorization': 'Bearer ' + tok}, timeout=timeout)
    except OSError as exc:
        return {'kind': err_kind(exc), 'status': '', 'frames': 0, 'rows': [], 'join_ms': 0,
                'sid': None, 'room': None}
    started = time.time()
    state = {'buf': rest}
    out = {'kind': 'no-join-frame', 'status': first.split(' ', 1)[1] if ' ' in first else first,
           'frames': 0, 'rows': [], 'sid': None, 'room': None, 'version': None}
    try:
        if ' 101 ' not in first:
            out['kind'] = 'upgrade-refused'
            return out
        for _ in range(8):
            try:
                op, payload = ws_read_frame(sock, state)
            except (ConnectionError, OSError):
                break
            out['frames'] += 1
            if op != 2:
                continue
            try:
                outer = pb_decode(payload)
            except ValueError:
                continue
            for _, join in outer.get(1, []):
                try:
                    d = pb_decode(join)
                except ValueError:
                    continue
                if 5 not in d:
                    continue
                now = int(time.time())
                sid = (pb_strings(join, 2) and pb_strings(d[2][0][1], 1) or [None])[0] \
                    if 2 in d else None
                out['sid'] = sid
                out['room'] = (pb_strings(d[1][0][1], 1) or [None])[0] if 1 in d else None
                out['version'] = (pb_strings(d[4][0][1], 1) or [None])[0] if 4 in d else None
                for _, ice in d[5]:
                    urls = pb_strings(ice, 1)
                    username = (pb_strings(ice, 2) or [''])[0]
                    credential = (pb_strings(ice, 3) or [''])[0]
                    shape = row_class(username)
                    row = {'urls': urls, 'class': shape['class'],
                           'username_sha256': hashlib.sha256(username.encode()).hexdigest()[:16],
                           'cred_sha256': hashlib.sha256(credential.encode()).hexdigest()[:16],
                           'cred_len': len(credential), 'ttl_delta': None, 'sid_ok': None,
                           'kid': shape.get('kid'), 'url_count': len(urls)}
                    if shape['class'] == 'static':
                        # A literal username is the only one that is not derived from something the
                        # node knows, so it can be carried in the clear and still be a claim: the
                        # masker turns the fixture value into <USR> before the row is stored.
                        row['static_text'] = shape['text']
                    if shape['expiry'] is not None:
                        row['ttl_delta'] = shape['expiry'] - now
                        row['sid_ok'] = (shape['sid'] == sid)
                    for name, candidate in secrets.items():
                        data = candidate.encode() if isinstance(candidate, str) else candidate
                        if shape['class'] == 'rest':
                            row[f'hmac_{name}'] = 'match' if rest_credential(
                                data, username) == credential else 'no'
                        elif shape['class'] == 'embedded':
                            row[f'pw_{name}'] = 'match' if embedded_password(
                                candidate, shape['sid'], shape['expiry']) == credential else 'no'
                    out['rows'].append(row)
                out['kind'] = 'joined'
                return out
        return out
    finally:
        out['join_ms'] = int((time.time() - started) * 1000)
        try:
            sock.close()
        except OSError:
            pass


# --------------------------------------------------------------------------- context and configs
def build_ctx(out_dir: Path, hold: float) -> dict:
    work = out_dir / 'work'
    fx = work / 'fixtures'
    (work / 'configs').mkdir(parents=True, exist_ok=True)
    (work / 'logs').mkdir(parents=True, exist_ok=True)
    fx.mkdir(parents=True, exist_ok=True)
    used: set = set()
    ctx = {}
    port_roles = {}
    for role in ('http', 'store', 'tu1', 'tu2', 'tls1'):
        port = C102.alloc_port(used)
        ctx[f'{role}_port'] = port
        port_roles[port] = role.upper()
    certs = {'A': C103.gen_cert(fx, 'c105a')}
    return dict(work=work, fx=fx, hold=hold, port_roles=port_roles, used=used, certs=certs, **ctx)


def secret_file(ctx, name: str, data: bytes, mode: int) -> str:
    path = ctx['fx'] / name
    path.write_bytes(data)
    os.chmod(path, mode)
    return str(path)


def ts_item(host=HOST, port=3478, protocol='udp', username=None, credential=None, secret=None,
            secret_file_path=None, ttl=None) -> str:
    """One `rtc.turn_servers` list entry. A None field is left out of the YAML entirely, which is how
    the credential-mode union gets tested instead of assumed: `secret: ""` and an omitted `secret` are
    different YAML, and only the second one is the blank the upstream branch actually checks."""
    out = f'    - host: {host}\n'
    out += f'      port: {port}\n'
    if protocol is not None:
        out += f'      protocol: {protocol}\n'
    if username is not None:
        out += f'      username: "{username}"\n'
    if credential is not None:
        out += f'      credential: "{credential}"\n'
    if secret is not None:
        out += f'      secret: "{secret}"\n'
    if secret_file_path is not None:
        out += f'      secret_file: {secret_file_path}\n'
    if ttl is not None:
        out += f'      ttl: {ttl}\n'
    return out


def ts_block(*items: str) -> str:
    return '  turn_servers:\n' + ''.join(items)


def cfg(ctx, keys: dict | None = None, ts_items: str = '', turn: str = '',
        node_ip: str = '127.0.0.1', stun: str = '') -> str:
    """The whole YAML: fixed loopback node, owned store, this lane's key map, and whichever of the two
    TURN surfaces the profile is about. `rtc.tcp_port: 0` keeps the TCP mux off, so the only listener
    the lane owns besides HTTP is the one a profile names."""
    ks = keys if keys is not None else {KID: KSEC}
    body = (f'port: {ctx["http_port"]}\n'
            f'bind_addresses: [127.0.0.1]\n'
            f'redis:\n  address: 127.0.0.1:{ctx["store_port"]}\n')
    body += 'keys:\n' + ''.join(f'  {k}: {v}\n' for k, v in ks.items())
    body += 'rtc:\n  tcp_port: 0\n'
    if node_ip:
        body += f'  node_ip: {node_ip}\n'
    if stun:
        body += f'  stun_servers:\n{stun}'
    if ts_items:
        body += ts_items
    if turn:
        body += turn
    return body


def turn_block(ctx, enabled=True, domain=DOMAIN, tls=None, tu=None, ttl_seconds=None,
               material='A') -> str:
    """The embedded `turn:` section - top level, not under `rtc:`. Ports are named by role, so the
    receipt carries `<TU1>`/`<TLS1>` instead of a number that would collide with the masker."""
    out = 'turn:\n'
    out += f'  enabled: {"true" if enabled else "false"}\n'
    if domain is not None:
        out += f'  domain: {domain}\n'
    if tls is not None:
        out += f'  tls_port: {ctx[f"{tls.lower()}_port"] if isinstance(tls, str) else tls}\n'
    if tu is not None:
        out += f'  udp_port: {ctx[f"{tu.lower()}_port"] if isinstance(tu, str) else tu}\n'
    if ttl_seconds is not None:
        out += f'  ttl_seconds: {ttl_seconds}\n'
    if material is not None and tls is not None:
        out += f'  cert_file: {ctx["certs"][material]["cert"]}\n'
        out += f'  key_file: {ctx["certs"][material]["key"]}\n'
    return out




# --------------------------------------------------------------------------- row expectations
def mask_join(raw: dict, masker) -> dict:
    """The join record before it can be stored: the URLs carry the node's own loopback port, and a
    static credential pair is a literal from the YAML, so both go through the masker. Digests,
    lengths and verdicts stay - they are the claims."""
    out = {k: v for k, v in raw.items() if k not in ('rows', 'sid')}
    out['sid'] = masker.text(raw['sid']) if raw.get('sid') else raw.get('sid')
    rows = []
    for r in raw['rows']:
        m = dict(r)
        m['urls'] = [masker.text(u) for u in r['urls']]
        if m.get('kid') is None:
            m.pop('kid', None)
        if 'static_text' in m:
            m['static_text'] = masker.text(m['static_text'])
        rows.append(m)
    out['rows'] = rows
    return out


def band(value: int, slack: int = 3) -> list:
    """A TTL is a clock difference, so every ttl claim is a band. Three seconds either side still
    separates 120 from 300 from 600 from 86400, which is the whole discrimination being claimed."""
    return [value - slack, value + slack]


DEFAULT_STUN_URLS = ['stun:global.stun.twilio.com:3478', 'stun:stun.l.google.com:19302',
                     'stun:stun1.l.google.com:19302']


def row(url=None, urls=None, cls='rest', ttl=None, hmac=None, pw=None, sid_ok=None, kid=None,
        url_count=None, static_text=None, cred_len=None, cred_band=None, cred_sha256=None,
        kid_signs=None) -> dict:
    r = {'class': cls}
    if url is not None:
        r['url'] = url
    if urls is not None:
        r['urls'] = list(urls)
    if ttl is not None:
        r['ttl'] = band(ttl)
    if cred_band is not None:
        r['cred_band'] = cred_band
    if cred_sha256 is not None:
        r['cred_sha256'] = cred_sha256
    if hmac is not None:
        r['hmac'] = dict(hmac)
    if pw is not None:
        r['pw'] = dict(pw)
    if sid_ok is not None:
        r['sid_ok'] = sid_ok
    if kid is not None:
        r['kid'] = kid
    if kid_signs is not None:
        r['kid_signs'] = kid_signs
    if url_count is not None:
        r['url_count'] = url_count
    if static_text is not None:
        r['static_text'] = static_text
    if cred_len is not None:
        r['cred_len'] = cred_len
    return r


# --------------------------------------------------------------------------- profiles




# --------------------------------------------------------------------------- runner
def mask_store(snap: dict, masker) -> dict:
    """Store keys are names the daemon chose, and a participant key carries the SID the credential
    embeds. The snapshot goes through the same masker as the logs, or the receipt is a SID list."""
    if not snap:
        return {}
    return {'keys': [masker.text(k) for k in snap.get('keys') or []],
            'count': snap.get('count'),
            'node_ids': [masker.text(n) for n in snap.get('node_ids') or []],
            'nodes_key_present': snap.get('nodes_key_present')}


def run_entry(ctx, prof, masker, binaries) -> dict:
    claimed_ports = set(prof['claims'].get('tcp_has', [])) | set(prof['claims'].get('tcp_not', [])) \
        | set(prof['claims'].get('udp_has', [])) | set(prof['claims'].get('udp_not', []))
    unknown_roles = claimed_ports - set(ctx['port_roles'].values())
    if unknown_roles:
        raise RuntimeError(f'{prof["id"]} claims ports never allocated: {sorted(unknown_roles)}')
    entry = {'profile': prof['id'], 'rows': prof['rows'], 'claims': prof['claims'],
             'note': prof['note'], 'expect_exit': prof['expect_exit'],
             'refuse_wait': prof['refuse_wait'], 'ready': prof['ready'],
             'ice': prof['ice'], 'runs': {}}
    plain = ctx['store_port']
    for side in ('go', 'rust'):
        C102.redis_cli(plain, 'FLUSHALL')
        before = C102.dbsize(plain)
        if before != 0:
            raise RuntimeError(f'store not clean entering {prof["id"]}/{side}: {before}')

        def on_alive(proc, cfg_path, prof=prof):
            observed = {}
            if prof['ready']:
                # The credential is read while the node is up, and the registry row is sampled after
                # the join returns: a participant that reached the room is what makes the row real.
                joins = [mask_join(
                    join_probe(ctx['http_port'], prof['join_kid'], prof['join_secret'],
                               prof['secrets']), masker)
                    for _ in range(max(1, int(prof['claims'].get('join_repeat') or 1)))]
                observed['join'] = joins[0]
                observed['extra_joins'] = joins[1:]
            observed['store'] = mask_store(C102.store_snapshot(plain), masker)
            return observed

        rec = C102.launch_pair(Path(binaries[side]), list(prof['argv']), prof['env'], prof['cfg'],
                               ctx['work'], f'{side}-{prof["id"]}', side, masker,
                               ctx['http_port'] if prof['ready'] else None, ctx['hold'],
                               prof['refuse_wait'], on_alive=on_alive)
        obs = rec.pop('observed', {}) or {}
        rec['join'] = obs.get('join') or {'kind': 'not-attempted', 'status': '', 'frames': 0,
                                          'rows': [], 'sid': None, 'room': None, 'version': None,
                                          'join_ms': 0}
        rec['extra_joins'] = obs.get('extra_joins') or []
        rec['store'] = {'before_dbsize': before, 'alive': obs.get('store') or {},
                        'after': mask_store(C102.store_snapshot(plain), masker)}
        rec['post_ports'] = {name: {'tcp': C103.tcp_listeners(port), 'udp': C103.udp_holders(port)}
                             for name, port in prof['probe_ports']}
        entry['runs'][side] = rec
    return entry


# ------------------------------------------------------------------------------- claims/legs
# The one vocabulary evaluate understands. A claim key outside this set would be recorded and then
# silently ignored, which is how a receipt grows a field that asserts nothing.
CLAIM_KEYS = {'log_all', 'log_all_go', 'log_all_rust', 'log_not', 'log_not_go', 'log_not_rust',
              'log_any', 'tcp_has', 'tcp_not', 'udp_has', 'udp_not', 'node_alive',
              'store_left_clean', 'store_no_node_after', 'release_ports', 'room_sid_present',
              'join_repeat', 'kids_pinned', 'kids_allowed'}

# Which of the per-row fields an expectation is allowed to name, so a typo in a profile cannot make a
# leg vanish: an expectation key outside this set raises instead of being skipped.
ROW_KEYS = {'class', 'url', 'urls', 'ttl', 'hmac', 'pw', 'sid_ok', 'kid', 'url_count',
            'static_text', 'cred_len', 'cred_band', 'cred_sha256', 'kid_signs'}


def needle_slug(needle: str) -> str:
    return re.sub(r'[^0-9a-z]', '', str(needle).lower())[:16] or 'x'


def sock_token(role: str) -> str:
    """An allocated port reaches the receipt as the masker's own <ROLE> token, never as the bare
    role name the claim uses. Comparing the bare name against the masked list makes every tcp_has
    leg fail and every tcp_not leg pass, which is the shape of a vacuous socket claim."""
    return role if role.startswith('<') else f'<{role}>'


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
    if kind == 'band':
        return (isinstance(item['actual'], int) and not isinstance(item['actual'], bool)
                and item['expect'][0] <= item['actual'] <= item['expect'][1])
    if kind == 'same':
        vals = item['values']
        return len(vals) == 2 and vals[0] is not None and vals[0] == vals[1]
    raise RuntimeError(f'leg {item["leg"]} has kind {kind!r}, which this re-deriver cannot check')


def ttl_minute(delta):
    """Nearest-minute bucket for a pair comparison. Every fixture TTL is a multiple of 60 s, so the
    bucket is stable across the sub-second skew between two joins while 120/300/600/14400/86400 stay
    five different values."""
    return None if delta is None else int(round(delta / 60.0))


def evaluate(entry) -> list:
    """Pure over the receipt, so --record, --report and --selftest-gate grade the same legs."""
    unknown = set(entry['claims']) - CLAIM_KEYS
    if unknown:
        raise RuntimeError(f'{entry["profile"]} carries claims nothing evaluates: {sorted(unknown)}')
    legs = []
    claims = entry['claims']
    pid = entry['profile']
    ice = entry['ice'] or {}
    exp_rows = ice.get('rows') or []
    for exp in exp_rows:
        stray = set(exp) - ROW_KEYS
        if stray:
            raise RuntimeError(f'{pid} row expectation uses fields evaluate ignores: {sorted(stray)}')

    for side in ('go', 'rust'):
        rec = entry['runs'][side]
        log = rec.get('log_masked') or ''
        sockets = rec.get('sockets_masked') or {}
        tcp = sockets.get('tcp') or []
        udp = sockets.get('udp') or []
        join = rec.get('join') or {}
        rows = join.get('rows') or []

        def emit(suffix, kind, **fields):
            legs.append(leg(f'{pid}::{side}:{suffix}', side, kind, **fields))

        for group, sides in (('log_all', ('go', 'rust')), ('log_all_go', ('go',)),
                             ('log_all_rust', ('rust',))):
            if side in sides:
                for needle in claims.get(group, []):
                    emit(f'{group}-{needle_slug(needle)}', 'has', needles=[needle], haystack=log,
                         why='the effect has to be visible in the daemon log')
        for group, sides in (('log_not', ('go', 'rust')), ('log_not_go', ('go',)),
                             ('log_not_rust', ('rust',))):
            if side in sides:
                for needle in claims.get(group, []):
                    emit(f'{group}-{needle_slug(needle)}', 'hasnt', needles=[needle], haystack=log,
                         why='a branch that did not run must stay silent')
        if claims.get('log_any'):
            emit('any', 'any', needles=claims['log_any'], haystack=log,
                 why='a refusal has to name at least one plausible reason')
        want = entry.get('expect_exit')
        if want == 'nonzero':
            emit('exit', 'nonzero', actual=rec['exit'], why='the raw exit code, not a wrapper verdict')
        elif want is not None:
            emit('exit', 'eq', expect=want, actual=rec['exit'], why='the raw exit code of the launch')
        for role in claims.get('tcp_has', []):
            emit(f'tcp-{role}', 'eq', expect=True, actual=sock_token(role) in tcp,
                 why='the TCP listener the config named is one the live process owns')
        for role in claims.get('udp_has', []):
            emit(f'udp-{role}', 'eq', expect=True, actual=sock_token(role) in udp,
                 why='the UDP endpoint the config named is one the live process owns')
        for family, lst in (('tcp', tcp), ('udp', udp)):
            for role in claims.get(f'{family}_not', []):
                emit(f'not-{family}-{role}', 'eq', expect=False, actual=sock_token(role) in lst,
                     why=f'no {family.upper()} endpoint was opened for a port the config never named')
        if 'node_alive' in claims:
            alive = (rec.get('store') or {}).get('alive') or {}
            emit('node', 'eq', expect=claims['node_alive'], actual=len(alive.get('node_ids') or []),
                 why='the registry row exists while the daemon is alive')
        if claims.get('room_sid_present'):
            emit('room-sid', 'nonzero', actual=join.get('room'),
                 why='the join response named a room, so a participant really was admitted')
            emit('join-sid', 'nonzero', actual=join.get('sid'),
                 why='the join response named the participant whose SID the credential carries')
        if claims.get('kids_pinned') is not None:
            kids = [((j.get('rows') or [{}])[0]).get('kid')
                    for j in [join] + list(rec.get('extra_joins') or [])]
            emit('kids-count', 'eq', expect=claims.get('join_repeat') or 0, actual=len(kids),
                 why='every repeat has to have returned its own credential row')
            emit('kids-named', 'nonzero', actual=[k for k in kids if k],
                 why='a repeat whose row named no key says nothing about which key is chosen')
            emit('kids-pinned', 'eq', expect=claims['kids_pinned'][side],
                 actual=bool(kids) and all(k == kids[0] for k in kids),
                 why='whether one launch advertises the same API key on every join')
            emit('kids-known', 'eq', expect=True,
                 actual=bool(kids) and all(k in (claims.get('kids_allowed') or []) for k in kids),
                 why='a username can only name a key this node actually has in its map')
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

        emit('join', 'eq', expect='joined' if entry['ready'] else 'not-attempted',
             actual=join.get('kind'),
             why='the websocket upgrade either produced a JoinResponse or was never attempted')
        emit('ice-count', 'eq', expect=ice.get('count'), actual=len(rows),
             why='the number of ice_servers the running node put on the wire')
        if entry['ready']:
            emit('upgrade', 'eq', expect='101 Switching Protocols', actual=join.get('status'),
                 why='the pair is only real if the handshake itself switched protocols')
            emit('frames', 'nonzero', actual=join.get('frames'),
                 why='at least one binary frame has to come back before any ICE row can exist')
        for i, exp in enumerate(exp_rows):
            r = rows[i] if i < len(rows) else {}

            def actual(field, default=None):
                return r.get(field, default)

            emit(f'ice{i}-class', 'eq', expect=exp['class'], actual=actual('class'),
                 why='whether the username is an expiry pair, a base62 blob, a literal or empty')
            if 'url' in exp:
                emit(f'ice{i}-url', 'eq', expect=exp['url'],
                     actual=(actual('urls') or [None])[0],
                     why='the scheme, host, port and transport of the advertised URL')
            if 'urls' in exp:
                emit(f'ice{i}-urls', 'eq', expect=exp['urls'], actual=actual('urls'),
                     why='the whole URL list of this ICE server, in order')
            if 'url_count' in exp:
                emit(f'ice{i}-urlcount', 'eq', expect=exp['url_count'], actual=actual('url_count'),
                     why='one embedded UDP listener plus one TLS URL is two entries in one row')
            if 'ttl' in exp:
                emit(f'ice{i}-ttl', 'band', expect=exp['ttl'], actual=actual('ttl_delta'),
                     why='the expiry inside the username, minus the second the join happened')
            if 'cred_len' in exp:
                emit(f'ice{i}-credlen', 'eq', expect=exp['cred_len'], actual=actual('cred_len'),
                     why='the credential length is a property of the algorithm, not of the input')
            if 'cred_band' in exp:
                emit(f'ice{i}-credlen', 'band', expect=exp['cred_band'], actual=actual('cred_len'),
                     why='the derived password is a fixed-width encoding of a digest')
            if 'cred_sha256' in exp:
                emit(f'ice{i}-cred', 'eq', expect=exp['cred_sha256'], actual=actual('cred_sha256'),
                     why='a static credential is passed through byte for byte')
            if 'static_text' in exp:
                emit(f'ice{i}-user', 'eq', expect=exp['static_text'], actual=actual('static_text'),
                     why='a static username comes back verbatim (masked, like everything stored)')
            if exp.get('sid_ok') is not None:
                emit(f'ice{i}-sid', 'eq', expect=True, actual=actual('sid_ok'),
                     why='the participant SID inside the credential is the one this join got')
            if 'kid' in exp:
                emit(f'ice{i}-kid', 'eq', expect=exp['kid'], actual=actual('kid'),
                     why='the base62 username names the API key the token was signed with')
            if 'kid_signs' in exp:
                signed = [name for name in ('k', 'm') if actual(f'pw_{name}') == 'match']
                named = KEY_ALIAS.get(actual('kid') or '')
                emit(f'ice{i}-kid-signs', 'eq', expect=exp['kid_signs'],
                     actual=len(signed) == 1 and signed[0] == named,
                     why='the password reproduces from exactly the secret of the key the username names')
            for name, want in sorted((exp.get('hmac') or {}).items()):
                emit(f'ice{i}-hmac-{name}', 'eq', expect=want, actual=actual(f'hmac_{name}', 'skip'),
                     why='the credential either reproduces from this configured secret or it does not')
            for name, want in sorted((exp.get('pw') or {}).items()):
                emit(f'ice{i}-pw-{name}', 'eq', expect=want, actual=actual(f'pw_{name}', 'skip'),
                     why='the derived password either reproduces from this key secret or it does not')

    if entry['ready']:
        g = (entry['runs']['go'].get('join') or {}).get('rows') or []
        r2 = (entry['runs']['rust'].get('join') or {}).get('rows') or []

        def emit_pair(suffix, **fields):
            legs.append(leg(f'{pid}::{suffix}', 'pair', **fields))

        emit_pair('count', kind='same', values=[len(g), len(r2)],
                  why='both binaries advertise the same number of ICE servers to the same join')
        for i in range(min(len(g), len(r2))):
            emit_pair(f'ice{i}-urls', kind='same', values=[g[i].get('urls'), r2[i].get('urls')],
                      why='the advertised URL list is identical modulo role-token masking')
            emit_pair(f'ice{i}-class', kind='same', values=[g[i].get('class'), r2[i].get('class')],
                      why='the username shape agrees')
            emit_pair(f'ice{i}-urlcount', kind='same',
                      values=[g[i].get('url_count'), r2[i].get('url_count')],
                      why='the number of URLs in the row agrees')
            emit_pair(f'ice{i}-ttlshape', kind='same',
                      values=[g[i].get('ttl_delta') is None, r2[i].get('ttl_delta') is None],
                      why='either both usernames carry an expiry or neither does')
            if g[i].get('ttl_delta') is not None and r2[i].get('ttl_delta') is not None:
                emit_pair(f'ice{i}-ttl', kind='same',
                          values=[ttl_minute(g[i]['ttl_delta']), ttl_minute(r2[i]['ttl_delta'])],
                          why='the credential lifetime agrees to the nearest minute')
            # The embedded password is a variable-width base62 encoding of a digest, and the digest
            # covers the participant SID, which differs between the two launches by construction. A
            # pair comparison of the raw width would grade that, so each side is checked against the
            # band the profile declares and only the non-embedded widths are compared directly.
            if g[i].get('class') == r2[i].get('class') and g[i].get('class') != 'embedded':
                emit_pair(f'ice{i}-credlen', kind='same',
                          values=[g[i].get('cred_len'), r2[i].get('cred_len')],
                          why='the credential width says both used the same derivation')
            if g[i].get('class') == 'static':
                emit_pair(f'ice{i}-cred', kind='same',
                          values=[g[i].get('cred_sha256'), r2[i].get('cred_sha256')],
                          why='a passed-through secret is byte-identical on both sides')
            exp = exp_rows[i] if i < len(exp_rows) else {}
            for key in sorted((exp.get('hmac') or {})):
                a, b = g[i].get(f'hmac_{key}', 'skip'), r2[i].get(f'hmac_{key}', 'skip')
                emit_pair(f'ice{i}-hmac-{key}', kind='same', values=[a, b],
                          why='both binaries signed the same shape against the same candidate secrets')
            for key in sorted((exp.get('pw') or {})):
                a, b = g[i].get(f'pw_{key}', 'skip'), r2[i].get(f'pw_{key}', 'skip')
                emit_pair(f'ice{i}-pw-{key}', kind='same', values=[a, b],
                          why='both derived the password from the same key secret and neither from '
                               'another')
    seen = {}
    for item in legs:
        seen[item['leg']] = seen.get(item['leg'], 0) + 1
        if seen[item['leg']] > 1:
            item['leg'] = f"{item['leg']}#{seen[item['leg']] - 1}"
    return legs




# ------------------------------------------------------------------------------- controls
ALLOWED_ROW_KEYS = {'urls', 'class', 'username_sha256', 'cred_sha256', 'cred_len', 'ttl_delta',
                    'sid_ok', 'kid', 'url_count', 'static_text'}
VERDICT_KEYS = {f'hmac_{k}' for k in ('ext', 'file', 'bin', 'foreign')} | \
               {f'pw_{k}' for k in ('k', 'm', 'x')}


def by_side(entry, side) -> dict:
    return entry['runs'][side]


def join_rows(rec) -> list:
    return (rec.get('join') or {}).get('rows') or []


def all_rows(rec) -> list:
    return [r for j in [rec.get('join') or {}] + list(rec.get('extra_joins') or [])
            for r in (j.get('rows') or [])]


def verdict_cells(rec, prefix: str) -> dict:
    out = {}
    for i, r in enumerate(join_rows(rec)):
        for k, v in r.items():
            if k.startswith(prefix):
                out[f'{i}-{k[len(prefix):]}'] = v
    return out


def first_row(rec):
    rows = join_rows(rec)
    return rows[0] if rows else {}


def profile_map(doc):
    return {e['profile']: e for e in doc['profiles']}


def build_controls(doc, live=False) -> dict:
    C = {}
    entries = doc['profiles']
    sub = bool(doc.get('subset'))

    def graded(value):
        """None means the receipt does not contain the material this control grades. A development
        subset that skipped the profile gets no verdict in either direction, which is honest; the
        full run always grades it."""
        return None if sub else value

    table_ids = doc.get('table_ids')
    rows_by_id = doc.get('table_row_claims')
    if not table_ids or not rows_by_id:
        raise RuntimeError('receipt carries no table_ids/table_row_claims, so the lane-design '
                           'controls cannot be separated from the profiles that happened to run')
    runs = [(e['profile'], s, e['runs'][s]) for e in entries for s in ('go', 'rust')]
    ready = [e for e in entries if e['ready']]
    refusals = [e for e in entries if not e['ready']]
    names = [e['profile'] for e in entries]
    if doc.get('subset'):
        C['profile_count_matches_the_table'] = 0 < len(entries) <= PROFILE_COUNT
    else:
        C['profile_count_matches_the_table'] = (len(entries) == PROFILE_COUNT ==
                                                doc['expected_profile_count'])
    C['every_profile_ran_both_binaries'] = all(set(e['runs']) == {'go', 'rust'} for e in entries)
    C['every_profile_carries_a_ledger_row'] = all(e['rows'] for e in entries)
    C['every_row_is_in_covers'] = all(r in doc['covers'] for e in entries for r in e['rows'])
    C['every_covers_row_is_claimed_by_a_profile'] = all(
        any(row in rows for rows in rows_by_id.values()) for row in doc['covers'])
    C['every_ran_profile_is_in_the_table'] = all(n in table_ids for n in names)
    C['every_profile_has_a_note'] = all(len(e['note']) > 24 for e in entries)
    C['lane_has_positive_controls'] = all(n in table_ids for n in ('no-turn-default-stun',
                                                                  'ts-stun-list-append'))
    stun_controls = ('no-turn-default-stun', 'ts-stun-list-append')
    C['both_default_stun_controls_joined'] = all(
        by_side(e, s)['join']['kind'] == 'joined' for e in entries
        if e['profile'] in stun_controls for s in ('go', 'rust')) if set(stun_controls) & set(
        names) else graded(False)
    C['default_stun_list_is_upstreams_own'] = all(
        first_row(by_side(e, s)).get('urls') == DEFAULT_STUN_URLS
        for e in entries if e['profile'] == 'no-turn-default-stun'
        for s in ('go', 'rust')) if 'no-turn-default-stun' in names else graded(False)
    C['every_join_row_carries_only_digest_fields'] = all(
        set(r) <= (ALLOWED_ROW_KEYS | VERDICT_KEYS)
        for _, _, rec in runs for r in all_rows(rec))
    C['every_row_digests_its_credential'] = all(
        re.fullmatch(r'[0-9a-f]{16}', r['cred_sha256']) and r['cred_len'] is not None
        for _, _, rec in runs for r in all_rows(rec))
    hmac_cells = {e['profile']: {s: verdict_cells(by_side(e, s), 'hmac_')
                                 for s in ('go', 'rust')} for e in entries}
    pw_cells = {e['profile']: {s: verdict_cells(by_side(e, s), 'pw_')
                               for s in ('go', 'rust')} for e in entries}
    matched = [(p, s, c) for p, cells in hmac_cells.items() for s, c in cells.items()
               if any(v == 'match' for v in c.values())]
    C['at_least_one_secret_reproduced_a_credential'] = bool(matched)
    C['every_secret_match_is_discriminated'] = all(
        sum(1 for v in c.values() if v == 'no') >= 1 for _, _, c in matched)
    pw_matched = [(p, s, c) for p, cells in pw_cells.items() for s, c in cells.items()
                  if any(v == 'match' for v in c.values())]
    C['at_least_one_derived_password_reproduced'] = bool(pw_matched)
    C['every_password_match_is_discriminated'] = all(
        sum(1 for v in c.values() if v == 'no') >= 2 for _, _, c in pw_matched)
    C['foreign_secret_never_matched'] = not any(
        v == 'match' for cells in hmac_cells.values() for side in cells.values()
        for k, v in side.items() if k.split('-', 1)[1] == 'foreign')
    def declared_hmac(e, i, name):
        rows = (e['ice'] or {}).get('rows') or []
        return (rows[i].get('hmac') or {}).get(name) if i < len(rows) else None

    C['file_secret_signs_only_the_file_rows'] = all(
        r[f'hmac_{name}'] == ('match' if declared_hmac(e, i, name) == 'match' else 'no')
        for e in ready for s in ('go', 'rust') for i, r in enumerate(join_rows(by_side(e, s)))
        for name in ('ext', 'file', 'bin', 'foreign') if f'hmac_{name}' in r)
    C['sid_ok_never_assented_for_another_participant'] = all(
        r.get('sid_ok') is not False for _, _, rec in runs for r in join_rows(rec))
    buckets = sorted({ttl_minute(r['ttl_delta']) for _, _, rec in runs for r in join_rows(rec)
                      if r.get('ttl_delta') is not None})
    C['ttl_buckets_recorded'] = buckets
    C['ttl_legs_span_five_distinct_lifetimes'] = graded(len(buckets) >= 5)
    url_seen = sorted({u for _, _, rec in runs for r in join_rows(rec) for u in r['urls']})
    C['advertised_urls_are_recorded'] = bool(url_seen)
    C['protocol_urls_are_distinguishable'] = graded(
        len({u for u in url_seen if 'c105.test' in u}) >= 3)
    C['advertised_tls_port_is_443'] = graded(
        'turns:turn.c105.test:443?transport=tcp' in url_seen)
    C['embedded_urls_use_the_loopback_node_ip'] = graded(
        any(u.startswith('turn:127.0.0.1:<TU1>?transport=udp') for u in url_seen))
    C['no_public_stun_leaked_into_a_turn_row'] = not any(
        u.startswith('stun:') and 'turn' in u for u in url_seen)
    static_profiles = ('ts-static-pair', 'ts-empty-secret-with-static', 'ts-protocol-tls-static',
                       'ts-protocol-omitted', 'ts-protocol-unknown-quic')
    statics = [e for e in entries if e['profile'] in static_profiles]
    C['static_rows_are_not_dynamic'] = all(
        r['class'] == 'static' for e in statics for s in ('go', 'rust')
        for r in join_rows(by_side(e, s))) if statics else graded(False)
    C['every_static_profile_returned_a_static_row'] = all(
        join_rows(by_side(e, s)) for e in statics for s in ('go', 'rust')) if statics else \
        graded(False)

    def one(pid):
        return profile_map(doc).get(pid)

    C['static_credential_digest_is_the_yaml_value'] = all(
        first_row(by_side(one('ts-static-pair'), s)).get('cred_sha256') ==
        hashlib.sha256(STATIC_CRED.encode()).hexdigest()[:16]
        for s in ('go', 'rust')) if one('ts-static-pair') else graded(False)
    C['static_username_comes_back_verbatim'] = all(
        first_row(by_side(one('ts-static-pair'), s)).get('static_text') == '<USR>'
        for s in ('go', 'rust')) if one('ts-static-pair') else graded(False)
    two = one('ts-two-servers-order')
    C['two_servers_keep_their_own_secrets_and_order'] = all(
        [r['class'] for r in join_rows(by_side(two, s))] == ['rest', 'rest'] and
        join_rows(by_side(two, s))[0]['hmac_ext'] == 'match' and
        join_rows(by_side(two, s))[1]['hmac_file'] == 'match' and
        join_rows(by_side(two, s))[0]['hmac_file'] == 'no' and
        join_rows(by_side(two, s))[1]['hmac_ext'] == 'no' and
        join_rows(by_side(two, s))[0]['urls'] != join_rows(by_side(two, s))[1]['urls']
        for s in ('go', 'rust')) if two else graded(False)
    def joins_of(rec):
        return [rec.get('join') or {}] + list(rec.get('extra_joins') or [])

    twokey = one('emb-two-keys')
    repeats = (twokey['claims'].get('join_repeat') or 0) if twokey else 0
    C['two_key_probe_repeats_recorded'] = all(
        len(joins_of(by_side(twokey, s))) == repeats for s in ('go', 'rust')) if twokey \
        else graded(False)
    C['every_repeat_password_follows_its_own_username_key'] = all(
        len(joins_of(by_side(twokey, s))) == repeats and
        all((signed := [n for n in ('k', 'm')
                        if ((j.get('rows') or [{}])[0]).get(f'pw_{n}') == 'match']) and
            len(signed) == 1 and KEY_ALIAS.get(((j.get('rows') or [{}])[0]).get('kid')) == signed[0]
            for j in joins_of(by_side(twokey, s)))
        for s in ('go', 'rust')) if twokey else graded(False)
    pinned = {(e['profile'], l['side']): l['actual'] for e in entries for l in e['legs']
              if l['leg'].endswith(':kids-pinned')}
    C['key_selection_divergence_is_measured'] = (
        pinned.get(('emb-two-keys', 'go')) is False and
        pinned.get(('emb-two-keys', 'rust')) is True) if ('emb-two-keys', 'go') in pinned else \
        graded(False)
    binp = one('ts-file-binary-bytes')
    C['binary_secret_file_is_the_key_on_both_sides'] = all(
        first_row(by_side(binp, s)).get('hmac_bin') == 'match' and
        first_row(by_side(binp, s)).get('hmac_file') == 'no'
        for s in ('go', 'rust')) if binp else graded(False)
    tls443 = one('emb-uri-tls-443')
    C['tls_only_join_advertises_443_then_default_stun'] = all(
        [len(join_rows(by_side(tls443, s))),
         [r['class'] for r in join_rows(by_side(tls443, s))]] == [2, ['embedded', 'empty']]
        for s in ('go', 'rust')) if tls443 else graded(False)
    C['refusals_are_only_the_refusal_profiles'] = all(
        (by_side(e, s)['exit'] != 0) == (not e['ready']) for e in entries for s in ('go', 'rust'))
    C['lane_covers_both_modes'] = (len(refusals) >= 5 and len(ready) >= 20) if not sub else None
    C['every_refusal_self_exited_before_the_timeout'] = all(
        by_side(e, s).get('self_exited') for e in refusals for s in ('go', 'rust')) if refusals \
        else graded(False)
    # log_any is a list of acceptable wordings, not a checklist: one refusal sentence may be shared
    # by both binaries and another may be phrased differently on each side. Requiring every
    # alternative to appear is what made a correct refusal read as a missing one.
    C['every_refusal_needle_is_visible_on_at_least_one_side'] = all(
        any(n in by_side(e, s)['log_masked'] for n in (e['claims'].get('log_any') or [])
            for s in ('go', 'rust'))
        for e in refusals) if refusals else graded(False)
    C['every_refusal_left_the_store_untouched'] = all(
        by_side(e, s)['store']['after']['keys'] == [] for e in refusals
        for s in ('go', 'rust')) if refusals else graded(False)
    C['every_start_claimed_a_live_node'] = all('node_alive' in e['claims']
                                                              for e in ready)
    C['store_was_empty_before_every_launch'] = all(rec['store']['before_dbsize'] == 0
                                                   for _, _, rec in runs)
    C['ready_profiles_received_frames'] = all((by_side(e, s)['join']['frames'] or 0) >= 1
                                              for e in ready for s in ('go', 'rust'))
    C['legs_are_uniquely_named'] = len(doc['legs']) == len({l['leg'] for l in doc['legs']})
    C['no_leg_compares_a_missing_value'] = not any(l['kind'] == 'eq' and l.get('expect') is None
                                                   for l in doc['legs'])
    C['no_pair_leg_has_a_missing_side'] = not any(l['kind'] == 'same' and
                                                  any(v is None for v in l['values'])
                                                  for l in doc['legs'])
    C['every_leg_kind_is_rederivable'] = all(
        l['kind'] in ('has', 'hasnt', 'any', 'nonzero', 'eq', 'band', 'same') for l in doc['legs'])
    C['leg_count_is_healthy'] = len(doc['legs']) >= 3 * len(entries)
    C['profile_verdicts_match_the_legs'] = all(
        e['match'] == all(l['ok'] for l in e['legs']) for e in entries)
    C['join_client_is_the_harness_itself'] = (
        doc['join_client']['harness_sha256'] == doc['harness_sha256'] and
        doc['join_client']['third_party_modules'] == [])
    C['sdk_substitution_is_explained'] = len(doc['join_client']['sdk_note']) > 80
    C['both_binaries_are_different_files'] = (doc['binaries']['go']['sha256'] !=
                                              doc['binaries']['rust']['sha256'])
    C['launch_labels_are_unique'] = len({f'{p}-{s}' for p, s, _ in runs}) == len(runs)
    C['every_profile_config_was_written_and_masked'] = all(by_side(e, s).get('cfg_masked')
                                                           for e in entries for s in ('go', 'rust'))
    return C


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
        doc['c103_harness_sha256'] == C103_FILED_HARNESS_SHA and
        doc['c104_harness_sha256'] == C104_FILED_HARNESS_SHA)
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
    C['no_allocated_port_leaks_into_urls'] = not any(
        f':{port}' in blob for port in doc['ports'].values())
    C['no_working_paths_in_profile_records'] = not any(
        needle in blob for needle in (str(REPO), str(Path.home()),
                                      str(Path(doc.get('work_dir') or '\x00')), '/private/tmp/'))
    C['fixture_secrets_never_raw'] = not any(real in whole for real, _ in
                                            SECRET_ALIASES + STATIC_ALIASES)
    C['binary_secret_file_bytes_never_copied_in'] = b'BINc105' not in whole.encode('utf-8',
                                                                                  'surrogateescape')
    C['participant_sids_are_tokenised'] = not re.search(r'\bPA_[0-9A-Za-z]{6,16}\b', whole)
    C['argv_never_raw'] = not any('argv_raw' in r or ('argv' in r and 'argv_masked' not in r)
                                  for _, _, r in runs_of(doc))
    C['raw_streams_recorded'] = all(
        re.fullmatch(r'[0-9a-f]{64}', r['raw']['stdout_sha256']) and r['raw']['bytes'] > 0
        for _, _, r in runs_of(doc))
    C['every_launch_produced_log_output'] = all(r['raw']['lines'] >= 1 for _, _, r in runs_of(doc))
    bounds = doc.get('listeners_bounds') or {}
    C['off_limit_ports_untouched'] = bool(bounds) and all(
        v['before'] == v['after'] for v in bounds.values())
    C['owned_store_stopped_cleanly'] = bool(doc.get('store_final')) and \
        doc['store_final'].get('listeners_after_stop') == 0
    C['store_count_recorded'] = doc.get('redis_final', {}).get('plain') is not None
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
    # None is "this receipt holds no material for that claim", which a development subset produces
    # whenever it skips the driving profile. Counting it as a failure would hide the real reds;
    # counting it as a pass would be vacuous. It is listed instead.
    doc['failed_controls'] = sorted(k for k, v in controls.items() if v is False)
    doc['unassessed_controls'] = sorted(k for k, v in controls.items() if v is None)
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
        'join_client_is_the_harness_itself': controls['join_client_is_the_harness_itself'],
        'every_secret_match_is_discriminated': controls['every_secret_match_is_discriminated'],
        'rows_store_digests_only': controls['every_join_row_carries_only_digest_fields'],
        'lane_covers_both_modes': controls['lane_covers_both_modes'],
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
                 'unassessed_controls': doc['unassessed_controls'],
                 'divergences': doc.get('divergences') or [],
                 'unmet_gates': doc['unmet_gates'], 'status_gates': doc['status_gates'],
                 'row_profile_map': doc['row_profile_map'],
                 'ttl_buckets': controls.get('ttl_buckets_recorded'),
                 'advertised_urls': sorted({u for _, _, r in runs_of(doc)
                                            for row in join_rows(r) for u in row['urls']}),
                 'profile_verdicts': {
                     e['profile']: {'match': e['match'], 'rows': e['rows'], 'ready': e['ready'],
                                    'go_exit': e['runs']['go']['exit'],
                                    'rust_exit': e['runs']['rust']['exit'],
                                    'ice_shape': {
                                        side: [{k: v for k, v in row.items()
                                                 if k in ('class', 'ttl_delta', 'cred_len',
                                                          'static_text', 'kid', 'urls')}
                                                for row in join_rows(e['runs'][side])]
                                        for side in ('go', 'rust')},
                                    'problems': e['problems']}
                     for e in doc['profiles']},
                 'control_values': controls, 'binary_hash_now': doc['binary_hash_now'],
                 'harness_sha256_now': doc['harness_sha256_now'], 'binaries': doc['binaries'],
                 'reference': doc['reference'], 'harness_sha256': doc['harness_sha256'],
                 'c102_harness_sha256': doc['c102_harness_sha256'],
                 'c103_harness_sha256': doc['c103_harness_sha256'],
                 'c104_harness_sha256': doc['c104_harness_sha256']}
        (Path(out) / 'BATCH.json').write_text(json.dumps(batch, indent=2, ensure_ascii=False,
                                                        sort_keys=True) + '\n')
    return {'all_ok': not doc['unmet_gates'],
            'summary': {k: doc[k] for k in ('status', 'leg_count', 'failed_profiles',
                                            'failed_controls', 'failed_legs', 'unmet_gates')}}




# ------------------------------------------------------------------------------- falsification
# Each case is a plausible misrecord applied to a deep copy of the filed receipt. A case that changes
# nothing, or that names a leg the receipt does not contain, is reported as undiscriminated rather
# than quietly passing.
def _r(doc, pid, side):
    for e in doc['profiles']:
        if e['profile'] == pid:
            return e['runs'][side]
    raise KeyError(pid)


def _j(doc, pid, side) -> dict:
    return _r(doc, pid, side).setdefault('join', {})


def _row(doc, pid, side, i=0) -> dict:
    rows = _j(doc, pid, side).setdefault('rows', [])
    while len(rows) <= i:
        rows.append({})
    return rows[i]


def _all_joins(doc, pid, side) -> list:
    """The primary join and its repeats. rec['join'] and rec['extra_joins'] are separate objects once
    the receipt has been through JSON, so a mutation has to walk both to be seen everywhere."""
    rec = _r(doc, pid, side)
    return [rec.setdefault('join', {})] + list(rec.setdefault('extra_joins', []))


def _socks(rec, fam):
    return rec.setdefault('sockets_masked', {}).setdefault(fam, [])


GATE_INJECTIONS = (
    # ------------------------------------------------------------- the secret -> credential channel
    {'case': 'inline_secret_never_used', 'expect': 'FAILED_LEG',
     'target': 'ts-secret-inline::go:ice0-hmac-ext',
     'apply': lambda d: _row(d, 'ts-secret-inline', 'go').update({'hmac_ext': 'no'}),
     'why': 'the row claims the credential reproduces from the configured secret; if it does not, the '
            'secret row has no evidence at all'},
    {'case': 'foreign_secret_signs_the_credential', 'expect': 'FAILED_CONTROL',
     'target': 'foreign_secret_never_matched',
     'apply': lambda d: _row(d, 'ts-secret-inline', 'go').update({'hmac_foreign': 'match'}),
     'why': 'a secret that nobody configured must not be able to produce the wire credential, or the '
            'match leg means nothing'},
    {'case': 'file_secret_signs_an_inline_row', 'expect': 'FAILED_CONTROL',
     'target': 'file_secret_signs_only_the_file_rows',
     'apply': lambda d: _row(d, 'ts-secret-inline', 'go').update({'hmac_file': 'match'}),
     'why': 'the file fixture is configured in three profiles only; a fourth match means the digest '
            'check is not tied to the config that named it'},
    {'case': 'password_derived_from_another_key', 'expect': 'FAILED_LEG',
     'target': 'emb-two-keys::rust:ice0-kid-signs',
     'apply': lambda d: _row(d, 'emb-two-keys', 'rust').update({'kid': KID}),
     'why': 'the key-map row is only proven if the password follows the key the username names'},
    {'case': 'go_key_choice_actually_stable', 'expect': 'FAILED_LEG',
     'target': 'emb-two-keys::go:kids-pinned',
     'apply': lambda d: [j['rows'][0].update({'kid': KID2}) for j in _all_joins(d, 'emb-two-keys', 'go')
                         if j.get('rows')],
     'why': 'the claim about Go is that the advertised key is not pinned; if every join named the '
            'same one, the divergence being recorded would not be there'},
    {'case': 'rust_key_choice_wanders', 'expect': 'FAILED_LEG',
     'target': 'emb-two-keys::rust:kids-pinned',
     'apply': lambda d: _all_joins(d, 'emb-two-keys', 'rust')[1]['rows'][0].update({'kid': KID}),
     'why': 'Rust is claimed to name the token key on every join of one launch'},
    {'case': 'a_repeat_returned_no_row', 'expect': 'FAILED_LEG',
     'target': 'emb-two-keys::rust:kids-count',
     'apply': lambda d: (_r(d, 'emb-two-keys', 'rust')['extra_joins'].pop()
                         if len(_r(d, 'emb-two-keys', 'rust').get('extra_joins') or []) > 1 else None),
     'why': 'a spread claim measured over one join is not a spread claim'},
    {'case': 'username_names_an_unconfigured_key', 'expect': 'FAILED_LEG',
     'target': 'emb-two-keys::go:kids-known',
     'apply': lambda d: _all_joins(d, 'emb-two-keys', 'go')[0]['rows'][0]
                         .update({'kid': 'c105nope'}),
     'why': 'a credential signed by a key nobody configured would make the key-map claim meaningless'},
    {'case': 'another_participants_sid_accepted', 'expect': 'FAILED_CONTROL',
     'target': 'sid_ok_never_assented_for_another_participant',
     'apply': lambda d: _row(d, 'ts-secret-inline', 'rust').update({'sid_ok': False}),
     'why': 'if a credential can carry someone else SID, the join proves nothing about this one'},
    {'case': 'static_pair_treated_as_dynamic', 'expect': 'FAILED_LEG',
     'target': 'ts-static-pair::go:ice0-class',
     'apply': lambda d: _row(d, 'ts-static-pair', 'go').update({'class': 'rest', 'static_text': None}),
     'why': 'the username/credential row is about pass-through; an HMAC-shaped username is a different '
            'mode'},
    {'case': 'blank_secret_treated_as_set', 'expect': 'FAILED_LEG',
     'target': 'ts-empty-secret-with-static::rust:ice0-class',
     'apply': lambda d: _row(d, 'ts-empty-secret-with-static', 'rust').update(
         {'class': 'rest', 'static_text': None}),
     'why': 'secret: "" must stay blank; if one side reads it as Some(""), the static pair is replaced '
            'by an HMAC over an empty key'},
    {'case': 'binary_secret_file_not_the_key', 'expect': 'FAILED_LEG',
     'target': 'ts-file-binary-bytes::go:ice0-hmac-bin',
     'apply': lambda d: _row(d, 'ts-file-binary-bytes', 'go').update({'hmac_bin': 'no'}),
     'why': 'this row is the non-UTF-8 file case; a miss means the bytes were transcoded, not used'},
    {'case': 'two_servers_order_swapped', 'expect': 'FAILED_LEG',
     'target': 'ts-two-servers-order::go:ice0-hmac-ext',
     'apply': lambda d: _j(d, 'ts-two-servers-order', 'go')['rows'].reverse(),
     'why': 'the list order is the wire order and each row carries its own secret'},
    {'case': 'secret_file_bytes_copied_into_receipt', 'expect': 'FAILED_CONTROL',
     'target': 'binary_secret_file_bytes_never_copied_in',
     'apply': lambda d: _r(d, 'ts-file-binary-bytes', 'go')['log_tail_masked'].append('BINc105'),
     'why': 'the file content is a secret fixture; only its digest may be stored'},
    {'case': 'raw_credential_stored', 'expect': 'FAILED_CONTROL',
     'target': 'every_join_row_carries_only_digest_fields',
     'apply': lambda d: _row(d, 'ts-secret-inline', 'go').update({'credential': 'AAAAAAAAAAAAAAAAAAAAAAAAAAAA='}),
     'why': 'a stored credential would put a usable TURN password in the committed evidence'},
    {'case': 'participant_sid_stored_raw', 'expect': 'FAILED_CONTROL',
     'target': 'participant_sids_are_tokenised',
     'apply': lambda d: _row(d, 'ts-secret-inline', 'go').update({'kid': 'PA_abcdefghijklmnop'}),
     'why': 'SIDs are per-connection identifiers and must not reach the receipt'},
    {'case': 'machine_address_in_an_advertised_url', 'expect': 'FAILED_CONTROL',
     'target': 'no_machine_address_in_receipt',
     'apply': lambda d: _row(d, 'emb-ttl-120', 'rust').update(
         {'urls': ['turn:10.113.4.21:<TU1>?transport=udp']}),
     'why': 'the node IP is the one thing a committed receipt must never carry'},
    {'case': 'allocated_port_left_unmasked', 'expect': 'FAILED_CONTROL',
     'target': 'no_allocated_port_leaks_into_urls',
     'apply': lambda d: _row(d, 'emb-ttl-default', 'go').update(
         {'urls': [f'turn:127.0.0.1:{d["ports"]["http"]}?transport=udp']}),
     'why': 'a bare port number in the receipt cannot be tied back to a role and varies per run'},

    # ------------------------------------------------------------------------- the ttl channel
    {'case': 'configured_ttl_ignored', 'expect': 'FAILED_LEG',
     'target': 'emb-ttl-120::go:ice0-ttl',
     'apply': lambda d: _row(d, 'emb-ttl-120', 'go').update({'ttl_delta': 3600}),
     'why': 'the ttl_seconds row is only about the lifetime the credential actually has'},
    {'case': 'flag_ttl_loses_to_yaml', 'expect': 'FAILED_LEG',
     'target': 'emb-ttl-flag-beats-yaml::rust:ice0-ttl',
     'apply': lambda d: _row(d, 'emb-ttl-flag-beats-yaml', 'rust').update({'ttl_delta': 120}),
     'why': 'this is the launched consumer for the generated flag; the YAML value means the flag lost'},
    {'case': 'env_ttl_loses_to_yaml', 'expect': 'FAILED_LEG',
     'target': 'emb-ttl-env-beats-yaml::go:ice0-ttl',
     'apply': lambda d: _row(d, 'emb-ttl-env-beats-yaml', 'go').update({'ttl_delta': 120}),
     'why': 'the env spelling is claimed as a separate channel and has to fail separately'},
    {'case': 'external_default_ttl_wrong', 'expect': 'FAILED_LEG',
     'target': 'ts-ttl-zero-explicit::go:ice0-ttl',
     'apply': lambda d: _row(d, 'ts-ttl-zero-explicit', 'go').update({'ttl_delta': 300}),
     'why': 'a zero external ttl means 14400, not the embedded 300 - the two defaults are the claim'},
    {'case': 'over_max_ttl_not_capped', 'expect': 'FAILED_LEG',
     'target': 'ts-ttl-over-max::rust:ice0-ttl',
     'apply': lambda d: _row(d, 'ts-ttl-over-max', 'rust').update({'ttl_delta': 999999}),
     'why': '999999 s must arrive as 86400 s, which is the cap and not the configured number'},
    {'case': 'ttl_legs_all_the_same_value', 'expect': 'FAILED_CONTROL',
     'target': 'ttl_legs_span_five_distinct_lifetimes',
     'apply': lambda d: [row.update({'ttl_delta': 300}) for _, _, rec in runs_of(d)
                         for row in join_rows(rec)],
     'why': 'if every row expired at the same second, the five ttl profiles would be one profile'},

    # --------------------------------------------------------------------- the URL shape channel
    {'case': 'tls_scheme_lost', 'expect': 'FAILED_LEG',
     'target': 'ts-protocol-tls-static::rust:ice0-url',
     'apply': lambda d: _row(d, 'ts-protocol-tls-static', 'rust').update(
         {'urls': [f'turn:{HOST}:3478?transport=tcp']}),
     'why': 'protocol tls is the only spelling that changes the scheme'},
    {'case': 'omitted_protocol_becomes_udp', 'expect': 'FAILED_LEG',
     'target': 'ts-protocol-omitted::go:ice0-url',
     'apply': lambda d: _row(d, 'ts-protocol-omitted', 'go').update(
         {'urls': [f'turn:{HOST}:3478?transport=udp']}),
     'why': 'the zero value is the fall-through turn/tcp; reading it as udp is a different default'},
    {'case': 'unknown_protocol_produces_no_row', 'expect': 'FAILED_LEG',
     'target': 'ts-protocol-unknown-quic::go:ice-count',
     'apply': lambda d: _j(d, 'ts-protocol-unknown-quic', 'go')['rows'].clear(),
     'why': 'an unlisted protocol must still advertise a row; if it vanishes, the fall-through is being '
            'asserted by absence rather than by a URL'},
    {'case': 'advertised_443_becomes_the_listener', 'expect': 'FAILED_LEG',
     'target': 'emb-uri-tls-443::ice0-urls',
     'apply': lambda d: _row(d, 'emb-uri-tls-443', 'rust').update(
         {'urls': [f'turns:{DOMAIN}:<TLS1>?transport=tcp']}),
     'why': 'the URL says 443 while the socket is on <TLS1>; flattening the two erases the quirk'},
    {'case': 'tls_only_advertises_no_stun_fallback', 'expect': 'FAILED_LEG',
     'target': 'emb-uri-tls-443::rust:ice-count',
     'apply': lambda d: _j(d, 'emb-uri-tls-443', 'rust')['rows'].__delitem__(slice(1, 2)),
     'why': 'hasSTUN stays false when only the TLS branch runs, so the default STUN row must follow'},
    {'case': 'default_stun_list_shortened', 'expect': 'FAILED_CONTROL',
     'target': 'default_stun_list_is_upstreams_own',
     'apply': lambda d: _row(d, 'no-turn-default-stun', 'go').update(
         {'urls': DEFAULT_STUN_URLS[:2], 'url_count': 2}),
     'why': 'the control is only a control if it returns the list upstream actually ships'},
    {'case': 'enabled_false_still_advertises_embedded', 'expect': 'FAILED_LEG',
     'target': 'emb-disabled-with-ts::go:ice-count',
     'apply': lambda d: _j(d, 'emb-disabled-with-ts', 'go')['rows'].append(dict(
         _row(d, 'emb-disabled-with-ts', 'go'))),
     'why': 'a disabled block must contribute no row of its own'},
    {'case': 'node_ip_missing_from_embedded_url', 'expect': 'FAILED_CONTROL',
     'target': 'embedded_urls_use_the_loopback_node_ip',
     'apply': lambda d: [row.update({'urls': [u.replace('127.0.0.1', 'other.c105.test')
                                              for u in row['urls']]})
                         for _, _, rec in runs_of(d) for row in join_rows(rec)],
     'why': 'rtc.node_ip is what puts an addressable host in the embedded URL; losing it makes the '
            'row unparseable by the client'},

    # ------------------------------------------------------------------------- the launch itself
    {'case': 'refusal_actually_started', 'expect': 'FAILED_LEG',
     'target': 'ts-file-empty-refused::rust:exit',
     'apply': lambda d: _r(d, 'ts-file-empty-refused', 'rust').update({'exit': 0}),
     'why': 'the refusal rows are claims about the raw exit code'},
    {'case': 'refusal_needle_absent_everywhere', 'expect': 'FAILED_LEG',
     'target': 'ts-username-only-refused::go:any',
     'apply': lambda d: _r(d, 'ts-username-only-refused', 'go').update({'log_masked': 'startup'}),
     'why': 'a refusal that names no reason is indistinguishable from some other failure'},
    {'case': 'precedence_warning_missing', 'expect': 'FAILED_LEG',
     'target': f'ts-secret-beats-file::go:log_all-{needle_slug(SECRET_PRECEDENCE)}',
     'apply': lambda d: _r(d, 'ts-secret-beats-file', 'go').update({'log_masked': ''}),
     'why': 'the precedence branch is claimed in the log as well as in the digest'},
    {'case': 'refusal_wrote_the_store', 'expect': 'FAILED_LEG',
     'target': 'ts-no-credentials-refused::go:store-clean',
     'apply': lambda d: _r(d, 'ts-no-credentials-refused', 'go')['store']['after'].update(
         {'keys': ['livekit_version']}),
     'why': 'a start that fails after touching the store leaves state the next launch inherits'},
    {'case': 'store_not_flushed_before_launch', 'expect': 'FAILED_CONTROL',
     'target': 'store_was_empty_before_every_launch',
     'apply': lambda d: _r(d, 'ts-static-pair', 'go')['store'].update({'before_dbsize': 4}),
     'why': 'a launch that inherited rows cannot attribute them to itself'},
    {'case': 'tls_listener_not_held', 'expect': 'FAILED_LEG',
     'target': 'emb-uri-tls-443::go:tcp-TLS1',
     'apply': lambda d: (_socks(_r(d, 'emb-uri-tls-443', 'go'), 'tcp').remove('<TLS1>')
                         if '<TLS1>' in _socks(_r(d, 'emb-uri-tls-443', 'go'), 'tcp') else None),
     'why': 'the 443 row is about a TLS listener that exists'},
    {'case': 'omitted_udp_port_bound_anyway', 'expect': 'FAILED_LEG',
     'target': 'emb-uri-tls-443::rust:not-udp-TU1',
     'apply': lambda d: _socks(_r(d, 'emb-uri-tls-443', 'rust'), 'udp').append('<TU1>'),
     'why': 'an absent udp_port must bind nothing; otherwise the two-URL shape is an accident'},
    {'case': 'no_join_frame_but_rows_claimed', 'expect': 'FAILED_LEG',
     'target': 'ts-secret-inline::rust:ice-count',
     'apply': lambda d: _j(d, 'ts-secret-inline', 'rust').update({'rows': [], 'kind': 'joined'}),
     'why': 'the count leg is what stops an empty stream reading as a pass'},
    {'case': 'join_reported_without_frames', 'expect': 'FAILED_CONTROL',
     'target': 'ready_profiles_received_frames',
     'apply': lambda d: [_j(d, e['profile'], 'go').update({'frames': 0}) for e in d['profiles']
                         if e['ready']],
     'why': 'a profile that claims a joined node but received no frame never joined'},
    {'case': 'upgrade_never_switched', 'expect': 'FAILED_LEG',
     'target': 'no-turn-default-stun::go:upgrade',
     'apply': lambda d: _j(d, 'no-turn-default-stun', 'go').update({'status': '400 Bad Request'}),
     'why': 'the RFC 6455 handshake returning 400 means the probe read nothing from a session'},
    {'case': 'room_sid_never_returned', 'expect': 'FAILED_LEG',
     'target': 'ts-ttl-600::rust:room-sid',
     'apply': lambda d: _j(d, 'ts-ttl-600', 'rust').update({'room': None}),
     'why': 'a response with no room is not a join'},
    {'case': 'refusal_survived_the_timeout', 'expect': 'FAILED_CONTROL',
     'target': 'every_refusal_self_exited_before_the_timeout',
     'apply': lambda d: _r(d, 'ts-file-missing-refused', 'go').update({'self_exited': False}),
     'why': 'a process the harness had to signal is not a startup refusal'},
    {'case': 'off_limit_port_touched', 'expect': 'FAILED_CONTROL',
     'target': 'off_limit_ports_untouched',
     'apply': lambda d: d['listeners_bounds'][str(list(d['listeners_bounds'])[0])].update(
         {'after': 1}),
     'why': 'the user-owned listeners must be exactly as found'},
    {'case': 'raw_stream_empty', 'expect': 'FAILED_CONTROL',
     'target': 'raw_streams_recorded',
     'apply': lambda d: _r(d, 'emb-ttl-120', 'go')['raw'].update({'bytes': 0}),
     'why': 'a launch with no recorded bytes cannot be re-examined later'},
    {'case': 'working_path_in_profile_record', 'expect': 'FAILED_CONTROL',
     'target': 'no_working_paths_in_profile_records',
     'apply': lambda d: _r(d, 'emb-ttl-120', 'rust').update(
         {'cfg_masked': f'secret_file: {REPO}/x.yaml'}),
     'why': 'committed evidence must not name the machine layout'},

    # ------------------------------------------------------------------------- the harness itself
    {'case': 'oracle_identity_replaced', 'expect': 'FAILED_CONTROL',
     'target': 'recorded_oracle_matches_pinned',
     'apply': lambda d: d['binaries']['go'].update({'sha256': 'f' * 64}),
     'why': 'the fixed Go e5655ac image is the differential reference'},
    {'case': 'reference_commit_drift', 'expect': 'FAILED_CONTROL',
     'target': 'reference_commit_matches',
     'apply': lambda d: d['reference'].update({'go_checkout_commit': 'e5655ac20267fa5005dccaf3f0e3bfc'
                                                                    '3ea688c06'[:-4] + 'dead'}),
     'why': 'the upstream anchors are only valid for the commit they were read from'},
    {'case': 'imported_primitive_swapped', 'expect': 'FAILED_CONTROL',
     'target': 'imported_primitives_are_the_accepted_lanes',
     'apply': lambda d: d.update({'c104_harness_sha256': '0' * 64}),
     'why': 'the launcher, masker and probes are accepted-lane code; a different file is a different '
            'harness'},
    {'case': 'harness_rewritten_after_record', 'expect': 'FAILED_CONTROL',
     'target': 'harness_unchanged_since_run',
     'apply': lambda d: d.update({'harness_sha256': '1' * 64}),
     'why': 'the legs in this receipt were derived by the bytes the record names'},
    {'case': 'profile_table_silently_short', 'expect': 'FAILED_CONTROL',
     'target': 'profile_count_matches_the_table',
     'apply': lambda d: d['profiles'].__delitem__(slice(0, 1)),
     'why': 'a run that dropped a profile is not the run the file describes'},
    {'case': 'row_claim_dropped', 'expect': 'FAILED_CONTROL',
     'target': 'every_profile_carries_a_ledger_row',
     'apply': lambda d: d['profiles'][0].update({'rows': []}),
     'why': 'a profile that maps to no ledger row cannot move evidence'},
    {'case': 'covers_row_left_unclaimed', 'expect': 'FAILED_CONTROL',
     'target': 'every_covers_row_is_claimed_by_a_profile',
     'apply': lambda d: [rows.remove('YAML:turn.ttl_seconds')
                         for rows in d['table_row_claims'].values()
                         if 'YAML:turn.ttl_seconds' in rows],
     'why': 'the batch advertises the ttl rows; nothing may quietly stop covering them'},
    {'case': 'third_party_client_claimed', 'expect': 'FAILED_CONTROL',
     'target': 'join_client_is_the_harness_itself',
     'apply': lambda d: d['join_client'].update({'third_party_modules': ['websockets']}),
     'why': 'if the probe needed a library, the wire format would be that library s claim, not ours'},
    {'case': 'unknown_claim_key_silently_ignored', 'expect': 'HARNESS_REFUSAL',
     'target': 'CLAIM_KEYS',
     'apply': lambda d: d['profiles'][0]['claims'].update({'turns_up_a_bell': True}),
     'why': 'a claim nothing evaluates is a pass that asserts nothing'},
    {'case': 'unknown_row_expectation_field', 'expect': 'HARNESS_REFUSAL',
     'target': 'ROW_KEYS',
     'apply': lambda d: d['profiles'][0]['ice']['rows'][0].update({'not_a_field': 1}),
     'why': 'an expectation naming a field evaluate ignores makes a leg disappear silently'},
    {'case': 'pair_leg_with_a_missing_side', 'expect': 'FAILED_CONTROL',
     'target': 'no_pair_leg_has_a_missing_side',
     'apply': lambda d: _row(d, 'ts-secret-inline', 'rust').update({'cred_len': None}),
     'why': 'a cross-binary comparison where one side is absent is not a comparison'},
)

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
    out = Path(out)
    out.mkdir(parents=True, exist_ok=True)
    before = C102.sha256_file(Path(receipt)) if receipt else None
    baseline = _clean(_copy.deepcopy(doc))
    finish(baseline, out=None, live=False, write=False)
    leg_ids = {l['leg'] for l in baseline['legs']}
    control_ids = set(baseline['cross_profile_controls'])
    results = []
    for inj in GATE_INJECTIONS:
        target = inj['target']
        # A refusal case names a vocabulary (CLAIM_KEYS/ROW_KEYS), not a leg or control id, so the
        # anchor check only applies to the two verdict expectations.
        anchor = True if inj['expect'] == 'HARNESS_REFUSAL' else (
            (target in leg_ids) if inj['expect'] == 'FAILED_LEG' else target in control_ids)
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
            hit = target in (mutated.get('failed_legs') or [])
        elif inj['expect'] == 'FAILED_CONTROL':
            hit = target in (mutated.get('failed_controls') or [])
        elif inj['expect'] == 'HARNESS_REFUSAL':
            hit = bool(detail)
        else:
            raise ValueError(f'unknown expectation {inj["expect"]}')
        results.append({'case': inj['case'], 'expect': inj['expect'], 'target': target,
                        'discriminated': bool(hit), 'why': inj['why'], 'detail': detail or None,
                        'failed_legs_under_case': len(mutated.get('failed_legs') or []),
                        'failed_controls_under_case': len(mutated.get('failed_controls') or [])})
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
    (out / f'{BATCH}-gate-falsification.json').write_text(
        json.dumps(report, indent=2, ensure_ascii=False, sort_keys=True) + '\n')
    return report


# ------------------------------------------------------------------------------- manifest
def git_output(cwd, *args) -> str:
    out = subprocess.run(['git', '-C', str(cwd), *args], capture_output=True, text=True)
    return out.stdout.strip()


def third_party_modules() -> list:
    """Every import this file makes that is not the standard library. The join probe has to be ours,
    or the wire format it reports is whatever that package decided to parse."""
    bad = []
    src = (HERE / 'run-paired.py').read_text()
    for match in re.finditer(r'^\s*(?:import|from)\s+([a-zA-Z_][\w.]*)', src, re.M):
        root = match.group(1).split('.')[0]
        if root in sys.stdlib_module_names or root in ('__future__',):
            continue
        bad.append(root)
    return sorted(set(bad))


def new_doc(bins, ctx, harness_sha, c102_sha, c103_sha, c104_sha, bounds, hold, subset) -> dict:
    # Tier 5: both sides are the launched production binaries, paired on identical YAML bytes, and the
    # effect is read back from the ice_servers a real websocket join returns - not from a config
    # struct, not from a log line about a secret.
    return {'schema': SCHEMA, 'batch': BATCH, 'covers': COVERS, 'tier': 5, 'live_run': True,
            'skip_launch': False, 'subset': bool(subset),
            'expected_profile_count': PROFILE_COUNT,
            'platform': {'system': sys.platform, 'arch': os.uname().machine,
                         'python': sys.version.split()[0]},
            'binaries': bins,
            'join_client': {
                'kind': 'stdlib-rfc6455-and-protobuf-codec-in-this-file',
                'harness_sha256': harness_sha,
                'third_party_modules': third_party_modules(),
                'client_note': 'the join is a raw loopback websocket upgrade with a bearer token and '
                               'the first binary SignalResponse frame decoded by the field-number '
                               'codec above; nothing between the daemon and the assertion is '
                               'third-party code',
                'sdk_note': 'the original LiveKit SDK is deliberately not the client here. ice_servers '
                            'is delivered in the first response frame, before any PeerConnection or '
                            'ICE gathering exists, so an SDK client would consume the row rather than '
                            'expose it - and the claim is about the bytes the node emitted, which the '
                            'SDK re-encodes into its own config object. #386 pairs the SDK where the '
                            'claim is candidate behaviour on a live connection; here the SDK would add '
                            'a translation layer between the daemon and the digest being compared.'},
            'divergences': [
                {'id': 'turn_embedded_key_selection', 'profiles': ['emb-two-keys'],
                 'go': 'iceServersForParticipant is handed getFirstKeyPair() (roommanager.go:324, '
                       '1203-1208), which ranges the config.Keys map: the API key inside an embedded '
                       'TURN username is an arbitrary configured key, and with two keys configured it '
                       'changes between joins of one running process.',
                 'rust': 'the username names the key the join token was signed with, the same on every '
                         'join of one launch.',
                 'measured': 'emb-two-keys::go:kids-pinned False and emb-two-keys::rust:kids-pinned '
                             'True over 12 joins each; both sides sign the password from the secret of '
                             'the key the username actually names.',
                 'ledger': 'the derivation half of YAML:keys is promoted; the selection rule stays '
                           'OPEN as a follow-up row.'}],
            'reference': {'go_checkout': 'references/livekit-stable',
                          'go_commit': FIXED_GO_COMMIT,
                          'go_checkout_commit': git_output(REFERENCE, 'rev-parse', 'HEAD'),
                          'upstream_reads': {
                              'pkg/config/config.go:201-217': 'TURNServer{host,port,protocol,username,'
                                                              'credential,secret,secret_file,ttl} is the '
                                                              'external list element; the embedded '
                                                              'TURNConfig has no secret field',
                              'pkg/config/config.go:52-75': 'DefaultTURNTTLSeconds 300, '
                                                            'DefaultExternalTURNTTLSeconds 14400, '
                                                            'TURNMaxTTLSeconds 86400 and the four '
                                                            'credential errors',
                              'pkg/config/config.go:857-898': 'LoadTURNSecrets: trim the inline secret, '
                                                              'precedence with a warning, stat then the '
                                                              '0o007 other-permission filter, read, '
                                                              'trim, ErrTURNSecretEmpty, then the '
                                                              'hasDynamic/hasStatic union',
                              'pkg/config/config.go:903-941': 'ClampTURNTTLSeconds and NormalizeTURNTTLs '
                                                              '- the embedded ttl is clamped both ways at '
                                                              'load, the external one only at the top',
                              'pkg/service/roommanager.go:1059-1155': 'iceServersForParticipant: the '
                                                                      'embedded turn:/turns: pair, '
                                                                      'CreateUsername/CreatePassword, '
                                                                      'the external HMAC-SHA1 branch, '
                                                                      'hasSTUN and the default STUN '
                                                                      'fallback',
                              'pkg/service/turn.go:292-334': 'CreateUsername clamps then base62-encodes '
                                                             '"apiKey|participantID|expiry"; '
                                                             'computePassword is base62 of the sha256 of '
                                                             '"secret|participantID|expiry" using the '
                                                             'key that signed the token',
                              'cmd/server/main.go:258-266': 'LoadTURNSecrets runs during server startup '
                                                             'and a failure aborts the process',
                              'crates/lk-config/src/load.rs:384-420': 'the Rust mirror, including the '
                                                                      'binary_secrets vector for a file '
                                                                      'that is not valid UTF-8',
                              'crates/lk-config/src/load.rs:221-245': 'normalize_turn_ttls',
                              'crates/lk-config/src/keys.rs:113-137': 'load_turn_secret_file: stat, the '
                                                                      'others-bits filter, read, trim, '
                                                                      'the empty error',
                              'crates/lk-compat/src/signal/ice.rs:62-141': 'the same three URL shapes '
                                                                          'and the Some(secret)/static '
                                                                          'split on the Rust side',
                              'crates/lk-compat/src/signal/ice.rs:145-161': 'rest_credentials: zero '
                                                                           'means 14400, then '
                                                                           'base64(HMAC-SHA1)',
                              'crates/lk-compat/src/turn_auth.rs:43-80': 'create_credentials: the base62 '
                                                                        'username and password'}},
            'harness': str(HERE / 'run-paired.py'),
            'harness_sha256_at_start': harness_sha, 'harness_sha256': harness_sha,
            'imports_from': [str(C102_DIR / 'run-paired.py'), str(C103_DIR / 'run-paired.py'),
                             str(C104_DIR / 'run-paired.py')],
            'c102_harness_sha256_at_start': c102_sha, 'c102_harness_sha256': c102_sha,
            'c103_harness_sha256_at_start': c103_sha, 'c103_harness_sha256': c103_sha,
            'c104_harness_sha256_at_start': c104_sha, 'c104_harness_sha256': c104_sha,
            'imports_reason': 'the launcher, port allocator, socket observers, owned store and address '
                              'masker are the accepted C102 primitives; the certificate generator, the '
                              'JWT builder and the port listeners are C103 s; the loopback-port masking '
                              'is C104 s. Importing (not copying) leaves all three files byte-identical, '
                              'so their filed receipts stay re-derivable',
            'work_dir': str(ctx['work']),
            'ports': {k[:-5]: v for k, v in ctx.items() if k.endswith('_port')},
            'port_roles': {str(k): v for k, v in ctx['port_roles'].items()},
            'ip_salt': IP_SALT, 'off_limits': list(OFF_LIMITS), 'host_store_port': HOST_STORE_PORT,
            'hold_seconds': hold, 'listeners_bounds': bounds, 'store_flush_ok': True,
            'redis': {'plain_role': 'STORE'},
            'fixtures': {'key_ids': [KID, KID2], 'secrets': ['<SEC-K>', '<SEC-M>', '<SEC-X>',
                                                            '<SEC-F>', '<SEC-G>'],
                         'static_pair': ['<USR>', '<CRD>'], 'domain': DOMAIN, 'host': HOST,
                         'room': ROOM, 'literal_turn_ports': sorted(LITERAL_TURN_PORTS),
                         'default_stun_urls': DEFAULT_STUN_URLS,
                         'secret_files': {'secret-600': '0600', 'secret-world': '0604',
                                          'secret-empty': '0600 (whitespace)',
                                          'secret-bin': '0600 (not UTF-8)',
                                          'secret-dir': '0700 (a directory)'}},
            'ttl_constants': {'embedded_default': TTL_DEFAULT_EMBEDDED,
                              'external_default': TTL_DEFAULT_EXTERNAL, 'max': TTL_MAX,
                              'min': TTL_MIN},
            'profiles': []}




def profiles(ctx) -> list:
    out = []
    clash = sorted(LITERAL_TURN_PORTS & set(ctx['port_roles']))
    if clash:
        raise RuntimeError(f'a fixture port collides with an allocated role port: {clash}')
    probe_ports = [('http', ctx['http_port']), ('tu1', ctx['tu1_port']),
                   ('tu2', ctx['tu2_port']), ('tls1', ctx['tls1_port'])]
    joined = {'count': 1}

    def add(pid, rows, claims, cfg_text, argv=(), env=None, expect_exit=0, refuse_wait=None,
            ready=True, join_kid=KID, join_secret=KSEC, secrets=None, note='', ice=None,
            check_store=True, extra_ports=()):
        out.append({'id': pid, 'rows': rows, 'claims': claims, 'argv': list(argv),
                    'env': dict(env or {}), 'cfg': cfg_text, 'expect_exit': expect_exit,
                    'refuse_wait': refuse_wait, 'ready': ready, 'note': note,
                    'join_kid': join_kid, 'join_secret': join_secret,
                    'secrets': dict(secrets or {}), 'ice': ice, 'check_store': check_store,
                    'probe_ports': probe_ports + list(extra_ports)})

    cand = {'ext': EXT_SECRET, 'file': FILE_SECRET, 'bin': BIN_SECRET, 'foreign': FOREIGN_SECRET}
    emb_cand = {'k': KSEC, 'm': KSEC2, 'x': EXT_SECRET}

    def ts(pid, rows, ice, items, turn='', claims=None, **kw):
        """A profile whose YAML is `rtc.turn_servers` (plus an optional embedded block) joined with
        every secret this lane configured, so each row also records which candidates did NOT match."""
        add(pid, rows, dict(claims or {}), cfg(ctx, ts_items=ts_block(*items), turn=turn),
            ice=ice, secrets=cand, **kw)

    # ------------------------------------------------------------ what a join returns with no TURN
    # The lane's positive control: the probe really does read ice_servers, and the shape it sees when
    # nothing is configured is upstream's own default STUN list with an empty credential pair.
    add('no-turn-default-stun', ['YAML:rtc.turn_servers', 'YAML:rtc.stun_servers'],
        {'log_not': [TURN_START], 'node_alive': 1, 'room_sid_present': True},
        cfg(ctx), ready=True, ice={'count': 1, 'rows': [
            row(urls=DEFAULT_STUN_URLS, cls='empty', url_count=3, cred_len=0,
                hmac={'ext': 'skip', 'file': 'skip', 'bin': 'skip', 'foreign': 'skip'})]},
        secrets=cand,
        note='the control: with no TURN config the join still carries ice_servers, and the credential '
             'pair on that row is empty on both binaries')

    add('ts-stun-list-append', ['YAML:rtc.stun_servers'],
        {'node_alive': 1, 'room_sid_present': True, 'log_not': [NO_CREDENTIALS]},
        cfg(ctx, stun='    - stun.c105.test:3478\n'), ready=True,
        ice={'count': 1, 'rows': [row(url='stun:stun.c105.test:3478', cls='empty', url_count=1,
                                      cred_len=0)]},
        note='rtc.stun_servers reaches the same list, and the default STUN fallback stays silent once '
             'a configured server sets hasSTUN')

    # --------------------------------------------------------------------- secret -> HMAC-SHA1
    ts('ts-secret-inline',
       ['YAML:rtc.turn_servers[].secret', 'YAML:rtc.turn_servers[].host',
        'YAML:rtc.turn_servers[].port', 'YAML:rtc.turn_servers[].protocol',
        'YAML:rtc.turn_servers[].ttl', 'YAML:rtc.turn_servers'],
       {'count': 1, 'rows': [
           row(url=f'turn:{HOST}:3478?transport=udp', cls='rest', ttl=TTL_DEFAULT_EXTERNAL,
               sid_ok=True, url_count=1, cred_len=28,
               hmac={'ext': 'match', 'file': 'no', 'bin': 'no', 'foreign': 'no'})]},
       [ts_item(secret=EXT_SECRET)],
       claims={'log_not': [SECRET_PRECEDENCE, NO_CREDENTIALS], 'node_alive': 1,
               'room_sid_present': True},
       note='the inline secret is the HMAC key: the credential on the wire reproduces from these bytes '
            'and from none of the other three')

    sf600 = secret_file(ctx, 'secret-600', FILE_SECRET.encode() + b'\n', 0o600)
    ts('ts-secret-file-0600',
       ['YAML:rtc.turn_servers[].secret_file', 'YAML:rtc.turn_servers[].secret'],
       {'count': 1, 'rows': [
           row(url=f'turn:{HOST}:3478?transport=udp', cls='rest', ttl=TTL_DEFAULT_EXTERNAL,
               sid_ok=True, url_count=1, cred_len=28,
               hmac={'file': 'match', 'ext': 'no', 'bin': 'no', 'foreign': 'no'})]},
       [ts_item(secret_file_path=sf600)],
       claims={'log_not': [SECRET_PRECEDENCE, PERMS_NOT_ZERO], 'node_alive': 1,
               'room_sid_present': True},
       note='the file bytes are the key and the trailing newline is trimmed: the digest says which of '
            'the two fixtures signed the credential')

    ts('ts-secret-beats-file', ['YAML:rtc.turn_servers[].secret',
                                'YAML:rtc.turn_servers[].secret_file'],
       {'count': 1, 'rows': [
           row(url=f'turn:{HOST}:3478?transport=udp', cls='rest', ttl=TTL_DEFAULT_EXTERNAL,
               sid_ok=True, url_count=1, cred_len=28,
               hmac={'ext': 'match', 'file': 'no', 'bin': 'no', 'foreign': 'no'})]},
       [ts_item(secret=EXT_SECRET, secret_file_path=sf600)],
       claims={'log_all': [SECRET_PRECEDENCE], 'node_alive': 1, 'room_sid_present': True},
       note='both fields set: the warning is printed and the inline value wins, which is the whole '
            'precedence branch and it is visible in the digest as well as the log')

    ts('ts-blank-inline-file-wins', ['YAML:rtc.turn_servers[].secret',
                                     'YAML:rtc.turn_servers[].secret_file'],
       {'count': 1, 'rows': [
           row(url=f'turn:{HOST}:3478?transport=udp', cls='rest', ttl=TTL_DEFAULT_EXTERNAL,
               sid_ok=True, url_count=1, cred_len=28,
               hmac={'file': 'match', 'ext': 'no', 'bin': 'no', 'foreign': 'no'})]},
       [ts_item(secret='   ', secret_file_path=sf600)],
       claims={'log_not': [SECRET_PRECEDENCE, NO_CREDENTIALS], 'node_alive': 1,
               'room_sid_present': True},
       note='a whitespace inline secret is trimmed before the switch, so the file branch runs and no '
            'precedence warning is printed')

    ts('ts-static-pair', ['YAML:rtc.turn_servers[].username',
                          'YAML:rtc.turn_servers[].credential',
                          'YAML:rtc.turn_servers[].host', 'YAML:rtc.turn_servers[].port'],
       {'count': 1, 'rows': [
           row(url=f'turn:{HOST}:3479?transport=tcp', cls='static', url_count=1,
               static_text='<USR>', cred_len=len(STATIC_CRED),
               cred_sha256=hashlib.sha256(STATIC_CRED.encode()).hexdigest()[:16])]},
       [ts_item(port=3479, protocol='tcp', username=STATIC_USER, credential=STATIC_CRED)],
       claims={'log_not': [NO_CREDENTIALS], 'node_alive': 1, 'room_sid_present': True},
       note='the static pair comes back byte-identical (the digest is of the YAML value itself) and the '
            'username keeps no expiry shape at all')

    ts('ts-empty-secret-with-static', ['YAML:rtc.turn_servers[].secret',
                                       'YAML:rtc.turn_servers[].username'],
       {'count': 1, 'rows': [
           row(url=f'turn:{HOST}:3478?transport=udp', cls='static', url_count=1,
               static_text='<USR>', cred_len=len(STATIC_CRED),
               cred_sha256=hashlib.sha256(STATIC_CRED.encode()).hexdigest()[:16])]},
       [ts_item(secret='', username=STATIC_USER, credential=STATIC_CRED)],
       claims={'log_not': [SECRET_PRECEDENCE, NO_CREDENTIALS], 'node_alive': 1,
               'room_sid_present': True},
       note='secret: "" is a present-but-blank field: hasDynamic is false, so the static pair wins. '
            'This is the row where a Some(\"\")/None split in the loader would show up as a dynamic '
            'credential on one side')

    ts('ts-two-servers-order',
       ['YAML:rtc.turn_servers[].host', 'YAML:rtc.turn_servers[].port',
        'YAML:rtc.turn_servers[].secret', 'YAML:rtc.turn_servers[].secret_file',
        'YAML:rtc.turn_servers'],
       {'count': 2, 'rows': [
           row(url=f'turn:{HOST}:3478?transport=udp', cls='rest', ttl=TTL_DEFAULT_EXTERNAL,
               sid_ok=True, url_count=1, cred_len=28,
               hmac={'ext': 'match', 'file': 'no', 'bin': 'no', 'foreign': 'no'}),
           row(url=f'turns:second.c105.test:443?transport=tcp', cls='rest',
               ttl=TTL_DEFAULT_EXTERNAL, sid_ok=True, url_count=1, cred_len=28,
               hmac={'file': 'match', 'ext': 'no', 'bin': 'no', 'foreign': 'no'})]},
       [ts_item(secret=EXT_SECRET),
        ts_item(host='second.c105.test', port=443, protocol='tls',
                secret_file_path=sf600)],
       claims={'log_not': [NO_CREDENTIALS], 'node_alive': 1, 'room_sid_present': True},
       note='two servers, two hosts, two ports, two protocols, two secret sources - the list order is '
            'the wire order and each row is signed by its own secret')

    ts('ts-ttl-per-row', ['YAML:rtc.turn_servers[].ttl'],
       {'count': 2, 'rows': [
           row(url=f'turn:{HOST}:3478?transport=udp', cls='rest', ttl=600, sid_ok=True,
               url_count=1, cred_len=28,
               hmac={'ext': 'match', 'file': 'no', 'bin': 'no', 'foreign': 'no'}),
           row(url=f'turn:{HOST}:3479?transport=tcp', cls='rest', ttl=TTL_DEFAULT_EXTERNAL,
               sid_ok=True, url_count=1, cred_len=28,
               hmac={'file': 'match', 'ext': 'no', 'bin': 'no', 'foreign': 'no'})]},
       [ts_item(secret=EXT_SECRET, ttl=600),
        ts_item(port=3479, protocol='tcp', secret_file_path=sf600, ttl=0)],
       claims={'node_alive': 1, 'room_sid_present': True},
       note='ttl is per entry, not per server: one row expires in 600 s and the other in the 14400 s '
            'that a zero means')

    # --------------------------------------------------------------------- protocol -> URL
    ts('ts-protocol-tls-static', ['YAML:rtc.turn_servers[].protocol'],
       {'count': 1, 'rows': [
           row(url=f'turns:{HOST}:3478?transport=tcp', cls='static', url_count=1,
               static_text='<USR>', cred_len=len(STATIC_CRED),
               cred_sha256=hashlib.sha256(STATIC_CRED.encode()).hexdigest()[:16])]},
       [ts_item(protocol='tls', username=STATIC_USER, credential=STATIC_CRED)],
       claims={'node_alive': 1, 'room_sid_present': True},
       note='"tls" is the only value that changes the scheme, and it also flips the transport to tcp')

    ts('ts-protocol-omitted', ['YAML:rtc.turn_servers[].protocol'],
       {'count': 1, 'rows': [
           row(url=f'turn:{HOST}:3478?transport=tcp', cls='static', url_count=1,
               static_text='<USR>', cred_len=len(STATIC_CRED),
               cred_sha256=hashlib.sha256(STATIC_CRED.encode()).hexdigest()[:16])]},
       [ts_item(protocol=None, username=STATIC_USER, credential=STATIC_CRED)],
       claims={'node_alive': 1, 'room_sid_present': True},
       note='no protocol key at all: the zero value is not "udp", it is the fall-through turn/tcp')

    ts('ts-protocol-unknown-quic', ['YAML:rtc.turn_servers[].protocol'],
       {'count': 1, 'rows': [
           row(url=f'turn:{HOST}:3478?transport=tcp', cls='static', url_count=1,
               static_text='<USR>', cred_len=len(STATIC_CRED),
               cred_sha256=hashlib.sha256(STATIC_CRED.encode()).hexdigest()[:16])]},
       [ts_item(protocol='quic', username=STATIC_USER, credential=STATIC_CRED)],
       claims={'log_not': [NO_CREDENTIALS], 'node_alive': 1, 'room_sid_present': True},
       note='an unlisted protocol is neither rejected nor honoured: it advertises turn/tcp, the same '
            'URL an omitted one produces')

    # ------------------------------------------------------------------------ external ttl bands
    ts('ts-ttl-zero-explicit', ['YAML:rtc.turn_servers[].ttl'],
       {'count': 1, 'rows': [
           row(url=f'turn:{HOST}:3478?transport=udp', cls='rest', ttl=TTL_DEFAULT_EXTERNAL,
               sid_ok=True, url_count=1, cred_len=28,
               hmac={'ext': 'match', 'file': 'no', 'bin': 'no', 'foreign': 'no'})]},
       [ts_item(secret=EXT_SECRET, ttl=0)],
       claims={'node_alive': 1, 'room_sid_present': True},
       note='an explicit 0 is the same as an absent ttl: the external default of 14400 s')

    ts('ts-ttl-600', ['YAML:rtc.turn_servers[].ttl'],
       {'count': 1, 'rows': [
           row(url=f'turn:{HOST}:3478?transport=udp', cls='rest', ttl=600, sid_ok=True,
               url_count=1, cred_len=28,
               hmac={'ext': 'match', 'file': 'no', 'bin': 'no', 'foreign': 'no'})]},
       [ts_item(secret=EXT_SECRET, ttl=600)],
       claims={'node_alive': 1, 'room_sid_present': True},
       note='a configured ttl inside the range is used verbatim')

    ts('ts-ttl-negative', ['YAML:rtc.turn_servers[].ttl'],
       {'count': 1, 'rows': [
           row(url=f'turn:{HOST}:3478?transport=udp', cls='rest', ttl=TTL_DEFAULT_EMBEDDED,
               sid_ok=True, url_count=1, cred_len=28,
               hmac={'ext': 'match', 'file': 'no', 'bin': 'no', 'foreign': 'no'})]},
       [ts_item(secret=EXT_SECRET, ttl=-5)],
       claims={'log_not': [TS_TTL_CLAMPED], 'node_alive': 1, 'room_sid_present': True},
       note='a negative ttl is not clamped at load (only the upper bound is), it survives to '
            'credential generation where both binaries land on the 300 s default, not the 14400 s that '
            'a zero gets')

    ts('ts-ttl-over-max', ['YAML:rtc.turn_servers[].ttl'],
       {'count': 1, 'rows': [
           row(url=f'turn:{HOST}:3478?transport=udp', cls='rest', ttl=TTL_MAX, sid_ok=True,
               url_count=1, cred_len=28,
               hmac={'ext': 'match', 'file': 'no', 'bin': 'no', 'foreign': 'no'})]},
       [ts_item(secret=EXT_SECRET, ttl=999999)],
       claims={'log_all_rust': [TS_TTL_CLAMPED], 'log_not_go': [TS_TTL_CLAMPED], 'node_alive': 1,
               'room_sid_present': True},
       note='999999 s is capped at 86400 before the credential is ever made, so the wire cannot tell '
            'a cap from a configured value; only the warning does, and Go prints it before its logger '
            'exists')

    # ------------------------------------------------------------------- embedded credentials
    def emb(pid, rows, ice, turn='', claims=None, argv=(), env=None, keys=None, note='',
            join_kid=KID, join_secret=KSEC, secrets=None, extra_claims=None):
        add(pid, rows, dict({'node_alive': 1, 'room_sid_present': True}, **(extra_claims or {})),
            cfg(ctx, keys=keys, turn=turn or turn_block(ctx, tu='tu1')), argv=argv, env=env,
            ice=ice, join_kid=join_kid, join_secret=join_secret,
            secrets=secrets or emb_cand, note=note)

    def emb_row(ttl, kid=KID, urls=None, kid_signs=None, pw=None):
        return row(urls=urls or [f'turn:127.0.0.1:<TU1>?transport=udp'], cls='embedded', ttl=ttl,
                   sid_ok=True, kid=kid, kid_signs=kid_signs, url_count=len(urls or ['x']),
                   cred_band=[40, 48], pw=pw or {'k': 'match', 'm': 'no', 'x': 'no'})

    emb('emb-ttl-default', ['YAML:turn.ttl_seconds', 'YAML:turn.udp_port', 'YAML:turn.domain',
                            'YAML:keys', 'YAML:rtc.node_ip'],
        {'count': 1, 'rows': [emb_row(TTL_DEFAULT_EMBEDDED)]},
        note='no ttl_seconds in the YAML: the base62 username carries the 300 s default and the '
             'password reproduces from the joining key\'s secret')

    emb('emb-ttl-120', ['YAML:turn.ttl_seconds'],
        {'count': 1, 'rows': [emb_row(120)]},
        turn=turn_block(ctx, tu='tu1', ttl_seconds=120),
        note='the configured value arrives verbatim')

    emb('emb-ttl-clamp-high', ['YAML:turn.ttl_seconds'],
        {'count': 1, 'rows': [emb_row(TTL_MAX)]},
        turn=turn_block(ctx, tu='tu1', ttl_seconds=999999),
        extra_claims={'log_all_rust': [EMB_TTL_CLAMPED], 'log_not_go': [EMB_TTL_CLAMPED]},
        note='999999 is clamped to 86400 at load on both sides, and the asymmetry is measured: Rust '
             'logs the upstream sentence, Go never does because NormalizeTURNTTLs runs before '
             'InitLoggerFromConfig (main.go:204 before 208)')

    emb('emb-ttl-clamp-negative', ['YAML:turn.ttl_seconds'],
        {'count': 1, 'rows': [emb_row(TTL_DEFAULT_EMBEDDED)]},
        turn=turn_block(ctx, tu='tu1', ttl_seconds=-5),
        extra_claims={'log_all_rust': [EMB_TTL_CLAMPED], 'log_not_go': [EMB_TTL_CLAMPED]},
        note='unlike the external field, a negative embedded ttl is clamped at load and the wire shows '
             'the 300 s default')

    emb('emb-ttl-flag-beats-yaml', ['generated_flags:--turn.ttl_seconds', 'YAML:turn.ttl_seconds'],
        {'count': 1, 'rows': [emb_row(240)]},
        turn=turn_block(ctx, tu='tu1', ttl_seconds=120), argv=['--turn.ttl_seconds', '240'],
        note='the flag spelling moves the expiry that the join hands out, which is the launched '
             'consumer C104 withheld for this row')

    emb('emb-ttl-env-beats-yaml', ['generated_flags:--turn.ttl_seconds'],
        {'count': 1, 'rows': [emb_row(360)]},
        turn=turn_block(ctx, tu='tu1', ttl_seconds=120),
        env={'LIVEKIT_TURN_TTL_SECONDS': '360'},
        note='the env form of the same generated flag, attested on the credential instead of the '
             'projection')

    emb('emb-two-keys', ['YAML:keys', 'YAML:turn.ttl_seconds'],
        {'count': 1, 'rows': [emb_row(TTL_DEFAULT_EMBEDDED, kid=None, kid_signs=True,
                                      pw={'x': 'no'})]},
        keys={KID: KSEC, KID2: KSEC2}, join_kid=KID2, join_secret=KSEC2,
        extra_claims={'join_repeat': TWO_KEY_PROBES, 'kids_allowed': [KID, KID2],
                      'kids_pinned': {'go': False, 'rust': True}},
        note='twelve joins with a token signed by the second key. Rust names that key every time; Go '
             'takes getFirstKeyPair() (roommanager.go:324, 1203-1208), which ranges a map, so the key '
             'it advertises is not the token\'s and is not stable across joins of one process. Either '
             'way the password reproduces from the secret of the key the username actually names, '
             'and never from the unconfigured third secret - the selection rule is the divergence, not '
             'the derivation.')

    # ---------------------------------------- the advertised 443 and the hasSTUN fallback, one launch
    ts443 = turn_block(ctx, tls='tls1', tu=None)
    add('emb-uri-tls-443',
        ['YAML:turn.tls_port', 'YAML:turn.domain', 'YAML:turn.enabled', 'YAML:keys'],
        {'log_all': [TURN_START, SERVER_READY], 'tcp_has': ['TLS1'], 'udp_not': ['TU1', 'TU2'],
         'tcp_not': ['TU1', 'TU2'], 'release_ports': True, 'node_alive': 1, 'room_sid_present': True},
        cfg(ctx, turn=ts443), ready=True,
        ice={'count': 2, 'rows': [
            row(urls=['turns:turn.c105.test:443?transport=tcp'], cls='embedded',
                ttl=TTL_DEFAULT_EMBEDDED, sid_ok=True, kid=KID, url_count=1,
                cred_band=[40, 48], pw={'k': 'match', 'm': 'no', 'x': 'no'}),
            row(urls=DEFAULT_STUN_URLS, cls='empty', url_count=3, cred_len=0)]},
        secrets=emb_cand, extra_ports=[],
        note='the listener is on <TLS1> and the URL says 443, which is the hardcoding; and because '
             'only the TLS branch ran, hasSTUN stayed false, so the default STUN row follows')

    add('emb-disabled-with-ts', ['YAML:turn.enabled', 'YAML:rtc.turn_servers[].secret'],
        {'log_not': [TURN_START], 'udp_not': ['TU1', 'TU2'], 'tcp_not': ['TLS1', 'TU1'],
         'release_ports': True, 'node_alive': 1, 'room_sid_present': True},
        cfg(ctx, ts_items=ts_block(ts_item(secret=EXT_SECRET)),
            turn=turn_block(ctx, enabled=False, tu='tu1')), ready=True,
        ice={'count': 1, 'rows': [
            row(url=f'turn:{HOST}:3478?transport=udp', cls='rest', ttl=TTL_DEFAULT_EXTERNAL,
                sid_ok=True, url_count=1, cred_len=28,
                hmac={'ext': 'match', 'file': 'no', 'bin': 'no', 'foreign': 'no'})]},
        secrets=cand,
        note='turn.enabled false removes the embedded row and nothing else; the external list is a '
             'separate consumer and still advertises')

    # ------------------------------------------------------------------------- secret-file refusals
    def refuse(pid, rows, items, needles, kind, turn=''):
        add(pid, rows, {'log_any': needles, 'store_left_clean': True, 'store_no_node_after': True,
                        'udp_not': ['TU1', 'TU2'], 'tcp_not': ['TLS1', 'HTTP', 'TU1', 'TU2'],
                        'release_ports': True},
            cfg(ctx, ts_items=ts_block(*items), turn=turn), ready=False, expect_exit='nonzero',
            refuse_wait=6.0, ice={'count': 0, 'rows': []}, secrets=cand,
            note=f'{kind}: the daemon never becomes ready, so nothing is advertised and the store is '
                 f'untouched')

    refuse('ts-username-only-refused', ['YAML:rtc.turn_servers[].username'],
           [ts_item(username=STATIC_USER)], [NO_CREDENTIALS],
           'a username without a credential is not a static pair')

    refuse('ts-no-credentials-refused',
           ['YAML:rtc.turn_servers[].credential', 'YAML:rtc.turn_servers[].secret'],
           [ts_item()], [NO_CREDENTIALS], 'host and port alone are neither mode')

    refuse('ts-file-missing-refused', ['YAML:rtc.turn_servers[].secret_file'],
           [ts_item(secret_file_path=str(ctx['fx'] / 'nowhere'))],
           [NO_SUCH_FILE, NO_SUCH_FILE_RUST], 'the stat happens before any secret is read')

    world = secret_file(ctx, 'secret-world', FILE_SECRET.encode(), 0o604)
    refuse('ts-file-world-readable-refused', ['YAML:rtc.turn_servers[].secret_file'],
           [ts_item(secret_file_path=world)], [PERMS_NOT_ZERO],
           'other-writable (0o604) trips the 0o007 filter')

    empty = secret_file(ctx, 'secret-empty', b'  \n\t\n', 0o600)
    refuse('ts-file-empty-refused', ['YAML:rtc.turn_servers[].secret_file'],
           [ts_item(secret_file_path=empty)], [SECRET_EMPTY],
           'whitespace-only content trims to nothing')

    dirpath = ctx['fx'] / 'secret-dir'
    dirpath.mkdir(exist_ok=True)
    os.chmod(dirpath, 0o700)
    refuse('ts-file-is-directory-refused', ['YAML:rtc.turn_servers[].secret_file'],
           [ts_item(secret_file_path=str(dirpath))],
           [IS_A_DIRECTORY, IS_A_DIRECTORY_RUST],
           'a directory passes os.Stat, and the mode check too at 0700, so the read is what fails')

    # ------------------------------------------------------------- a secret file that is not text
    binpath = secret_file(ctx, 'secret-bin', BIN_SECRET, 0o600)
    ts('ts-file-binary-bytes', ['YAML:rtc.turn_servers[].secret_file'],
       {'count': 1, 'rows': [
           row(url=f'turn:{HOST}:3478?transport=udp', cls='rest', ttl=TTL_DEFAULT_EXTERNAL,
               sid_ok=True, url_count=1, cred_len=28,
               hmac={'bin': 'match', 'file': 'no', 'ext': 'no', 'foreign': 'no'})]},
       [ts_item(secret_file_path=binpath)],
       claims={'log_not': [SECRET_EMPTY, PERMS_NOT_ZERO], 'node_alive': 1, 'room_sid_present': True},
       note='the file bytes are not valid UTF-8. Go uses them as the key because string([]byte) is '
            'lossless; Rust cannot put them in a String, so it carries the raw bytes in a separate '
            'vector and the digest says both arrived at the same key')
    return out



# ------------------------------------------------------------------------------- driver
def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument('--record', action='store_true')
    ap.add_argument('--report', help='re-derive the verdict from a filed execution.json')
    ap.add_argument('--selftest-gate', help='apply named mutations to a recorded execution.json')
    ap.add_argument('--out', default='/tmp/c105-lane')
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
    c104_sha = C102.sha256_file(C104_DIR / 'run-paired.py')
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
    profs = profiles(ctx)
    if len(profs) != PROFILE_COUNT:
        print(f'refusing: the profile table has {len(profs)} entries, the file claims '
              f'{PROFILE_COUNT}', file=sys.stderr)
        return 2
    full_table = list(profs)
    if a.only:
        wanted = set(a.only.split(','))
        missing = sorted(wanted - {p['id'] for p in profs})
        if missing:
            print(f'refusing: unknown profile ids {missing}', file=sys.stderr)
            return 2
        profs = [p for p in profs if p['id'] in wanted]
        print(f'development subset: {sorted(wanted)}', flush=True)
    store = C102.start_store(ctx['store_port'])
    doc = None
    try:
        deadline = time.monotonic() + 15
        while time.monotonic() < deadline and C102.redis_cli(ctx['store_port'], 'PING') != 'PONG':
            time.sleep(0.2)
        if C102.dbsize(ctx['store_port']) != 0:
            print('owned store is not empty at startup', file=sys.stderr)
            return 2
        doc = new_doc(bins, ctx, harness_sha, c102_sha, c103_sha, c104_sha, bounds, a.hold, a.only)
        # The lane-design controls grade the table, not the subset that happened to run, so a
        # development subset still gets a real answer about covers/ids instead of a false red.
        doc['table_ids'] = [p['id'] for p in full_table]
        doc['table_row_claims'] = {p['id']: list(p['rows']) for p in full_table}
        binaries = {side: info['path'] for side, info in bins.items()}
        for prof in profs:
            doc['profiles'].append(run_entry(ctx, prof, masker, binaries))
            e = doc['profiles'][-1]
            shape = {side: [(r['class'], r.get('ttl_delta'), r.get('url_count'))
                            for r in (e['runs'][side].get('join') or {}).get('rows') or []]
                     for side in ('go', 'rust')}
            print(f"  ran {prof['id']} exits={e['runs']['go']['exit']}/{e['runs']['rust']['exit']} "
                  f"ice={shape}", flush=True)
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
              'expect_exit': e['expect_exit'], 'refuse_wait': e['refuse_wait'], 'ready': e['ready'],
              'ice': e['ice'], 'match': e['match'], 'problems': e['problems'],
              'argv': [r['argv_masked'] for r in (e['runs']['go'], e['runs']['rust'])],
              'join': {side: {k: v for k, v in (e['runs'][side].get('join') or {}).items()
                              if k != 'rows'} for side in ('go', 'rust')},
              'ice_rows': {side: join_rows(e['runs'][side]) for side in ('go', 'rust')},
              'sockets': {side: e['runs'][side].get('sockets_masked') for side in ('go', 'rust')},
              'store_after': {side: (e['runs'][side].get('store') or {}).get('after')
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
