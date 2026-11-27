#!/usr/bin/env python3
"""C106 limit-gate channels: the `limit.*` numbers judged on the refusals they cause.

Assembled by ./assemble.py from part1..part7 - edit the parts, not this file.
See README.md for the profile inventory, the grading rules and how to reproduce the receipt.
"""
from __future__ import annotations



# ---- part1.py ----
#!/usr/bin/env python3
"""C106 limit-gate channels - part 1: identities, sibling primitives, request builders.

The question this lane asks of every `limit.*` channel is not whether the field parses or whether a
unit test can reach a predicate. It is: what does a *real* client get refused with, on a loopback
socket, from a daemon started with that YAML? So each leg drives an actual HTTP/Twirp request, an
actual request body, or an actual websocket frame, and records the raw status line, header set, body
bytes and frame/close sequence the running server produced.

Three things make a row here worth filing:

* a *pair* - the identical byte stream against the pinned Go oracle and against the Rust candidate,
  launched side by side by this harness, so the parity claim is about wire behaviour and not about
  two source readers' interpretations;
* a *family* - each gate is probed with an accepted case and a refused case whose verdicts must
  differ, otherwise "both sides returned X" says nothing except that the gate never spoke;
* *ordering* - several gates sit in front of each other (body limiter, Twirp decode, config limit,
  permission, room lookup), and the refusal that arrives first is the observable contract, so the
  legs deliberately violate two gates at once and record which one answered.

Nothing here binds a non-loopback address, touches the developer host's own server ports (7881/7882)
or the host redis store (6379), and no fixture payload, token or credential body is written to the
receipt: only lengths, digests and the server's own refusal text.
"""

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
import zlib
from pathlib import Path

HERE = Path(__file__).resolve().parent
REPO = HERE.parents[2]
INTEROP = REPO / 'tests' / 'interop'

BATCH = 'C106-20261127-limit-gate-channels'
ORACLE_SHA256 = '4bc2378e636740b29093061036249ffc9ebbef91f4ece1de12177c8dfee1e7dc'
ACCEPTANCE_SHA256 = 'f6fe7f032536c10f075e3a540dff49111d0f2f7afb03e587f2b36be3081a21f0'
FIXED_GO_COMMIT = 'e5655ac20267fa5005dccaf3f0e3bfc3ea688c07'
REFERENCE = REPO / 'references' / 'livekit-stable'
ORACLE = Path('/tmp/c99-build/livekit-server-go')
CANDIDATE = REPO / 'target' / 'release' / 'livecloud'

# Ports this lane must never bind or talk to: the developer host's own livekit-server and its store.
OFF_LIMITS = (7881, 7882)
HOST_STORE_PORT = 6379

API_KEY = 'c106p'
API_SECRET = 'c106fixturekeysecret0123456789ab'
STORE_PASS = 'c106storepass'
ROOM = 'c106room'
IDENT = 'c106id'

BASE_ENV_KEYS = ('PATH', 'HOME', 'LANG', 'TMPDIR')
BASE_ENV = {k: os.environ[k] for k in BASE_ENV_KEYS if k in os.environ}


def load_lane(name: str, rel: str):
    """Import a sibling lane's runner as a module so its primitives stay single-sourced."""
    path = INTEROP / rel
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    sys.modules[name] = mod
    spec.loader.exec_module(mod)
    return mod


C102 = load_lane('c102', 'base-flag-channels/run-paired.py')
C103 = load_lane('c103', 'generated-flag-channels/run-paired.py')
C105 = load_lane('c105', 'turn-credential-join/run-paired.py')

sha256_bytes = C102.sha256_bytes
sha256_file = C102.sha256_file
alloc_port = C102.alloc_port
listeners_on = C102.listeners_on
class Masker(C102.Masker):
    """The shared masker plus this lane's fixture secrets, so no C106 value reaches a receipt."""

    def secrets(self, text: str) -> str:
        return (super().secrets(text)
                .replace(API_SECRET, '<SECRET>')
                .replace(STORE_PASS, '<STORE_PASS>'))
launch_pair = C102.launch_pair
start_store = C102.start_store
stop_store = C102.stop_store
dbsize = C102.dbsize
ws_open = C105.ws_open

pb_tag = C105._tag
pb_var = C105._varint
pb_len = C105._len_field
pb_decode = C105.pb_decode


# ------------------------------------------------------------------------------- token signing
def b64u(obj) -> str:
    return base64.urlsafe_b64encode(
        json.dumps(obj, separators=(',', ':')).encode()).rstrip(b'=').decode()


def sign_token(claims: dict, key: str = API_KEY, secret: str = API_SECRET) -> str:
    """HS256 access token over an explicit claim set (the CLI-generated shape, no SDK)."""
    head = b64u({'alg': 'HS256', 'typ': 'JWT'})
    body = b64u(claims)
    sig = base64.urlsafe_b64encode(
        hmac.new(secret.encode(), f'{head}.{body}'.encode(), hashlib.sha256).digest()).rstrip(b'=').decode()
    return f'{head}.{body}.{sig}'


def grants(room=ROOM, identity=IDENT, admin=True, room_join=True, agent=False,
           own_metadata=False, room_create=True, can_update=True, room_list=True, extra=None) -> dict:
    video = {'room': room, 'roomJoin': room_join, 'roomAdmin': admin, 'roomCreate': room_create,
             'roomList': room_list, 'canUpdateMetadata': can_update, 'canPublish': True,
             'canSubscribe': True, 'canUpdateOwnMetadata': own_metadata}
    if agent:
        video['agent'] = True
    if extra:
        video.update(extra)
    return video


def access_token(video=None, name=None, metadata=None, attributes=None, identity=IDENT,
                 key=API_KEY, secret=API_SECRET, ttl=300, extra_claims=None) -> str:
    now = int(time.time())
    claims = {'iss': key, 'sub': identity, 'nbf': now - 5, 'exp': now + ttl,
              'video': grants() if video is None else video}
    if name is not None:
        claims['name'] = name
    if metadata is not None:
        claims['metadata'] = metadata
    if attributes is not None:
        claims['attributes'] = attributes
    if extra_claims:
        claims.update(extra_claims)
    return sign_token(claims, key, secret)


# ---------------------------------------------------------------------------- protobuf builders
# Field numbers come from the pinned protos (crates/lk-protocol/protos) which are the same
# definitions the Go oracle compiles against, so a byte stream is meaningful to both sides.
def create_room(name=ROOM, metadata='', agents=None, empty_timeout=None) -> bytes:
    out = pb_len(1, name.encode()) + pb_len(5, metadata.encode())
    if empty_timeout is not None:
        out += pb_tag(2, 0) + pb_var(empty_timeout)
    for agent in agents or []:
        out += pb_len(14, agent)
    return out


def room_agent(name='c106agent', metadata='', attributes=None) -> bytes:
    out = pb_len(1, name.encode()) + pb_len(3, metadata.encode())
    for k, v in (attributes or {}).items():
        out += pb_len(4, pb_len(1, k.encode()) + pb_len(2, v.encode()))
    return out


def update_room_metadata(room=ROOM, metadata='') -> bytes:
    return pb_len(1, room.encode()) + pb_len(2, metadata.encode())


def update_participant(room=ROOM, identity=IDENT, metadata=None, name=None, attributes=None) -> bytes:
    out = pb_len(1, room.encode()) + pb_len(2, identity.encode())
    if metadata is not None:
        out += pb_len(3, metadata.encode())
    if name is not None:
        out += pb_len(5, name.encode())
    for k, v in (attributes or {}).items():
        out += pb_len(6, pb_len(1, k.encode()) + pb_len(2, v.encode()))
    return out


def create_dispatch(agent='c106agent', room=ROOM, metadata='', attributes=None) -> bytes:
    """CreateAgentDispatchRequest{agent_name=1, room=2, metadata=3, attributes=6}."""
    out = pb_len(1, agent.encode()) + pb_len(2, room.encode()) + pb_len(3, metadata.encode())
    for k, v in (attributes or {}).items():
        out += pb_len(6, pb_len(1, k.encode()) + pb_len(2, v.encode()))
    return out


# SignalRequest: oneof message; ping=14 (int64, deprecated by ping_req), update_metadata=15.
# An unknown trailing field keeps the message valid on both sides while padding it to an exact size,
# which is how the size legs reach "exactly N" without changing what the message *does*.
PAD_FIELD = 999


def signal_ping(pad=0) -> bytes:
    out = pb_tag(14, 0) + pb_var(int(time.time() * 1000))
    if pad:
        out += pb_len(PAD_FIELD, b'p' * pad)
    return out


def signal_update_metadata(metadata=None, name=None, attributes=None, request_id=7) -> bytes:
    inner = b''
    if metadata is not None:
        inner += pb_len(1, metadata.encode())
    if name is not None:
        inner += pb_len(2, name.encode())
    for k, v in (attributes or {}).items():
        inner += pb_len(3, pb_len(1, k.encode()) + pb_len(2, v.encode()))
    inner += pb_tag(4, 0) + pb_var(request_id)
    return pb_len(15, inner)


def pad_to(payload: bytes, total: int, fill: bytes = b'z') -> bytes:
    """`payload` plus unknown fields so the message is exactly `total` bytes.

    A single length-delimited pad field with PAD_FIELD (a 2-byte tag) adds 3..130 bytes; a second
    one closes the gaps between those ranges. An increment of 1 or 2 is not expressible without
    breaking the message, so those totals raise instead of sending a truncated payload.
    """
    need = total - len(payload)
    if need < 0:
        raise ValueError(f'payload already {len(payload)} > {total}')

    def segment(target):
        for n in (target - 3, target - 4, target - 5, target - 6):
            if n >= 0:
                seg = pb_len(PAD_FIELD, fill * n)
                if len(seg) == target:
                    return seg
        return None

    if need == 0:
        return payload
    one = segment(need)
    if one is not None:
        return payload + one
    for first in (130, 131, 132, 6, 3):
        if first > need:
            continue
        head = segment(first)
        tail = segment(need - first) if need > first else b''
        if head is not None and tail is not None:
            return payload + head + tail
    raise ValueError(f'cannot pad {len(payload)} bytes to exactly {total}')



def frame_overhead(payload_len: int) -> int:
    """Bytes a masked client frame adds around a payload of this size (RFC 6455)."""
    return 2 + (0 if payload_len < 126 else 2 if payload_len < 65536 else 8) + 4


def largest_frame_payload(limit: int) -> int:
    """Biggest payload whose whole masked frame (header included) fits in `limit` bytes."""
    for n in (limit, limit - 6, limit - 8, limit - 14):
        if n >= 2 and n + frame_overhead(n) <= limit:
            return n
    return 0


def worker_register(pad=0) -> bytes:
    inner = (pb_tag(1, 0) + pb_var(1)                 # type = AGENT
             + pb_len(3, b'0.0.1')                    # version
             + pb_len(6, b'c106ns')                   # namespace
             + pb_len(8, b'c106worker')               # agent_name
             + pb_len(11, b'c106instance')            # instance_id
             + pb_tag(12, 0) + pb_var(1))             # endpoint_protocol = WEBSOCKET
    if pad:
        inner += pb_len(PAD_FIELD, b'w' * pad)
    return pb_len(1, inner)


# ---------------------------------------------------------------------------------- raw HTTP
def http_raw(port: int, method: str, path: str, headers: dict, body=b'',
           timeout: float | None = None) -> dict:
    """Send an exact request over loopback and return the raw response, tolerating early close.

    The read window ends at the last byte of the declared body, not at close: Go writes
    `Content-Length` and hyper writes `content-length`, so the header match is case-insensitive. A
    case-sensitive one makes every Rust response wait for the window to expire, and that wall-clock
    cost is what later reads as a dropped session rather than a refused request.

    A failed write is not treated as "no answer": a body limiter stops reading and closes while the
    client still has megabytes to send, so the status line can already be in the socket buffer.
    """
    lines = [f'{method} {path} HTTP/1.1', f'Host: 127.0.0.1:{port}']
    lines += [f'{k}: {v}' for k, v in headers.items() if v is not None]
    body = body if isinstance(body, bytes) else body.encode()
    # A ten-megabyte probe is a legitimate request at the default body limit; the window has to cover
    # writing it, or the harness reports a refusal the daemon never had a chance to make.
    if timeout is None:
        timeout = max(6.0, 2.0 + len(body) / 1_000_000.0 * 6.0)
    raw = b''
    end = ''
    send_error = ''
    try:
        sock = socket.create_connection(('127.0.0.1', port), timeout=timeout)
    except OSError as exc:
        return {'error': f'connect:{type(exc).__name__}', 'raw': b'', 'tail': 'connect-error',
                'end': 'connect-error'}
    try:
        sock.settimeout(timeout)
        try:
            sock.sendall(('\r\n'.join(lines) + '\r\n\r\n').encode())
            if body:
                sock.sendall(body)
        except OSError as exc:
            # A limiter that stops reading and closes while the body is still in flight makes the
            # write raise, but its status line was already written. A client that returns here
            # reports "no answer" for a refusal the daemon did make, so the read window still runs.
            send_error = f'{type(exc).__name__}'
        try:
            while True:
                try:
                    chunk = sock.recv(65536)
                except socket.timeout:
                    end = 'window-expired'
                    break
                except OSError as exc:
                    end = f'read-error:{type(exc).__name__}'
                    break
                if not chunk:
                    end = 'peer-eof'
                    break
                raw += chunk
                head, sep, rest = raw.partition(b'\r\n\r\n')
                if not sep:
                    continue
                low = head.lower()
                if b'transfer-encoding' in low and b'chunked' in low:
                    if raw.endswith(b'\r\n0\r\n\r\n') or b'\r\n0\r\n\r\n' in raw:
                        break
                    continue
                want = None
                for line in head.split(b'\r\n')[1:]:
                    if line.lower().startswith(b'content-length:'):
                        want = int(line.split(b':', 1)[1].strip())
                if want is not None and len(rest) >= want:
                    end = 'body-complete'
                    break
        finally:
            sock.close()
    except OSError as exc:
        return {'error': f'{type(exc).__name__}:{exc}', 'raw': b'', 'tail': 'connect-error',
                'end': 'connect-error'}
    if not raw and send_error:
        end = f'send-error:{send_error}'
    # `tail` stays the window-end sentinel, never raw response bytes: an accepted body echoes the
    # fixture payload, and the receipt must carry cells rather than verbatim probe content.
    return {'raw': raw, 'tail': end, 'end': end}


def header_case(lines) -> str:
    """How the server capitalised its header names: Go canonicalises, hyper lowercases.

    Recorded on one side each, never paired: the difference belongs to the HTTP stacks, not to a
    `limit.*` gate, but a client that reads headers case-sensitively breaks on one side only, so the
    lane pins which side does what.
    """
    names = [ln.split(b':', 1)[0] for ln in lines or [] if b':' in ln]
    if not names:
        return 'none'
    upper = sum(1 for n in names if n[:1].isupper())
    if upper == len(names):
        return 'canonical'
    return 'lower' if upper == 0 else 'mixed'


def split_response(resp: dict) -> dict:
    """Project a raw response into stable, comparable cells (no Date, no header order)."""
    raw = resp.get('raw', b'')
    head, sep, body = raw.partition(b'\r\n\r\n')
    if not sep:
        return {'parsed': False, 'status': head.decode('latin1', 'replace')[:80],
                'tail': resp.get('tail', ''), 'bytes': len(raw)}
    lines = head.split(b'\r\n')
    hdrs = {}
    for line in lines[1:]:
        if b':' in line:
            k, v = line.split(b':', 1)
            hdrs[k.strip().lower().decode('latin1')] = v.strip().decode('latin1')
    cells = {
        'parsed': True,
        'status': lines[0].decode('latin1', 'replace'),
        'ctype': hdrs.get('content-type', ''),
        'clen': hdrs.get('content-length', ''),
        'body_bytes': len(body),
        'tail': resp.get('tail', ''),
        'hdr_case': header_case(lines[1:]),
    }
    text = body.decode('latin1', 'replace')
    if 'protobuf' in hdrs.get('content-type', ''):
        # A binary success body is not comparable cell material: the RoomInfo it carries embeds a
        # freshly generated SID, so only its size and digest are kept and the pair is graded on the
        # status line, the content type and the byte count.
        cells['body_sha'] = sha256_bytes(body)[:16]
    elif text.startswith('{') or text.startswith('['):
        try:
            obj = json.loads(text)
        except ValueError:
            obj = None
        if isinstance(obj, dict):
            cells['code'] = obj.get('code', '')
            cells['msg'] = obj.get('msg', obj.get('message', ''))
            meta = obj.get('meta')
            cells['meta_keys'] = ','.join(sorted(meta)) if isinstance(meta, dict) else ''
            cells['meta_argument'] = (meta or {}).get('argument', '') if isinstance(meta, dict) else ''
            cells['json_keys'] = ','.join(sorted(obj))
        else:
            cells['json_unparsable'] = True
            cells['body_sha'] = sha256_bytes(body)[:16]
    else:
        cells['text'] = text[:120]
    return cells


def twirp(port, service, method, payload, ctype='application/protobuf', token_value=None,
          declare=None, raw_body=None, extra=None) -> dict:
    """POST a Twirp call and return its projected response cells."""
    body = raw_body if raw_body is not None else payload
    headers = {'Content-Type': ctype, 'Content-Length': str(len(body) if declare is None else declare)}
    if token_value:
        headers['Authorization'] = 'Bearer ' + token_value
    if extra:
        headers.update(extra)
    path = f'/twirp/livekit.{service}/{method}'
    return split_response(http_raw(port, 'POST', path, headers, body))


def body_text(cells: dict) -> str:
    return cells.get('msg') or cells.get('text') or ''


# ---- part2.py ----
#!/usr/bin/env python3
"""C106 limit-gate channels - part 2: RFC 6455 framing, permessage-deflate, signal sessions.

Server->client frames are *unmasked*, so a reader that assumes a mask key parses every opcode four
bytes late; that is why this lane carries its own reader instead of reusing the join reader from
C105 (which only ever expected binary protobuf frames). The reader here keeps the three properties the
size legs depend on:

* it separates the wire length from the message length, because the two-tier Go contract bounds
  compressed bytes at the transport (`conn.SetReadLimit`) and decompressed bytes at the application
  (`io.LimitReader(r, limit+1)` in `wsprotocol.go:69-90`);
* it reassembles fragments, so a message split across frames is judged as one message while the
  per-frame sizes stay visible - that is the fragmentation-evasion question;
* it records close codes and reasons verbatim, because the close code is what distinguishes the two
  guards (transport 1009 with an empty reason vs application 1000).
"""


SIGNAL_RESPONSE_FIELDS = {
    1: 'join', 2: 'answer', 3: 'offer', 4: 'trickle', 5: 'update', 6: 'track_published',
    8: 'leave', 9: 'mute', 10: 'speakers_changed', 11: 'room_update', 12: 'connection_quality',
    13: 'stream_state_update', 14: 'subscribed_quality_update', 15: 'subscription_permission_update',
    16: 'refresh_token', 18: 'pong', 19: 'reconnect', 20: 'pong_resp', 21: 'subscription_response',
    22: 'request_response', 23: 'track_subscribed', 24: 'room_moved',
}
REQUEST_RESPONSE_REASONS = {0: 'OK', 1: 'NOT_FOUND', 2: 'NOT_ALLOWED', 3: 'LIMIT_EXCEEDED',
                            4: 'QUEUED', 5: 'UNSUPPORTED_TYPE', 6: 'UNCLASSIFIED_ERROR',
                            7: 'INVALID_HANDLE', 8: 'INVALID_NAME', 9: 'DUPLICATE_HANDLE',
                            10: 'DUPLICATE_NAME', 11: 'INVALID_REQUEST'}
# livekit_agent.proto ServerMessage (server -> worker). The numbers collide with SignalResponse's,
# so a frame is only named once the socket it arrived on is known: a register answer is field 1 on
# /agent and a join on /rtc/v1.
SERVER_MESSAGE_FIELDS = {1: 'register', 2: 'availability', 3: 'assignment', 4: 'pong',
                         5: 'termination', 6: 'go_away'}
OPCODES = {0: 'cont', 1: 'text', 2: 'binary', 8: 'close', 9: 'ping', 10: 'pong'}


# ------------------------------------------------------------------------------------ framing
def ws_frame(opcode: int, payload: bytes, rsv1: bool = False, fin: bool = True) -> bytes:
    """A masked client frame (RFC 6455 requires client frames to be masked)."""
    b0 = (0x80 if fin else 0) | (0x40 if rsv1 else 0) | opcode
    ln = len(payload)
    if ln < 126:
        hdr = struct.pack('!BB', b0, 0x80 | ln)
    elif ln <= 0xFFFF:
        hdr = struct.pack('!BBH', b0, 0x80 | 126, ln)
    else:
        hdr = struct.pack('!BBQ', b0, 0x80 | 127, ln)
    mask = b'\x00\x00\x00\x00'
    return hdr + mask + payload


def df_comp(data: bytes, level: int = 9) -> bytes:
    """Raw-deflate a message the way permessage-deflate sends it (trailing empty block removed)."""
    co = zlib.compressobj(level, zlib.DEFLATED, -15)
    out = co.compress(data) + co.flush()
    return out[:-4] if out.endswith(b'\x00\x00\xff\xff') else out


class Reader:
    """Frame/message reader for server->client traffic on one connection."""

    def __init__(self, sock, buf: bytes = b'', schema: str = 'signal'):
        self.sock = sock
        self.buf = buf
        self.schema = schema
        self.infl = None
        self.started = None          # (fin, opcode, rsv1) of the message being assembled
        self.end = 'no-read'         # why the last read window stopped

    def _need(self, n: int) -> bool:
        while len(self.buf) < n:
            try:
                chunk = self.sock.recv(65536)
            except socket.timeout:
                self.end = 'window-expired'
                return False
            except OSError as exc:
                self.end = f'read-error:{type(exc).__name__}'
                return False
            if not chunk:
                self.end = 'peer-eof'
                return False
            self.buf += chunk
        return True

    def frame(self, timeout: float = 3.0):
        """One frame: (opcode, rsv1, fin, masked, wire_len, plain_bytes) or None at EOF/timeout."""
        self.sock.settimeout(timeout)
        self.end = 'no-frame'
        if not self._need(2):
            return None
        b0, b1 = self.buf[0], self.buf[1]
        fin, rsv1 = bool(b0 & 0x80), bool(b0 & 0x40)
        op = b0 & 0x0F
        masked = bool(b1 & 0x80)
        ln = b1 & 0x7F
        off = 2
        if ln == 126:
            if not self._need(4):
                return None
            ln = struct.unpack('!H', self.buf[2:4])[0]
            off = 4
        elif ln == 127:
            if not self._need(10):
                return None
            ln = struct.unpack('!Q', self.buf[2:10])[0]
            off = 10
        key = b''
        if masked:
            if not self._need(off + 4):
                return None
            key = self.buf[off:off + 4]
            off += 4
        if not self._need(off + ln):
            return None
        payload = self.buf[off:off + ln]
        self.buf = self.buf[off + ln:]
        if masked:
            payload = bytes(c ^ key[i % 4] for i, c in enumerate(payload))
        return op, rsv1, fin, masked, ln, payload

    def message(self, timeout: float = 3.0) -> dict | None:
        """Next complete message, with fragment accounting and permessage-deflate inflation."""
        wire = 0
        frags = 0
        frag_wires = []
        plain = b''
        rsv1 = False
        op = None
        while True:
            got = self.frame(timeout if op is None else 1.0)
            if got is None:
                if op is None:
                    return None
                return {'op': OPCODES.get(op, op), 'incomplete': True, 'wire': wire,
                        'fragments': frags, 'plain': len(plain)}
            fop, frsv, ffin, fmasked, flen, data = got
            if fop in (8, 9, 10):
                if fop == 8:
                    code = struct.unpack('!H', data[:2])[0] if len(data) >= 2 else None
                    return {'op': 'close', 'code': code, 'reason': data[2:].decode('latin1', 'replace')[:120],
                            'wire': flen, 'masked': fmasked}
                return {'op': OPCODES[fop], 'wire': flen, 'plain': len(data), 'masked': fmasked}
            if op is None:
                op, rsv1 = fop, frsv
                if rsv1:
                    self.infl = zlib.decompressobj(-15)
            frags += 1
            frag_wires.append(flen)
            wire += flen
            if rsv1:
                tail = b'\x00\x00\xff\xff' if ffin else b''
                try:
                    plain += self.infl.decompress(data + tail)
                except zlib.error as exc:
                    return {'op': OPCODES.get(op, op), 'inflate_error': str(exc)[:80],
                            'wire': wire, 'fragments': frags}
            else:
                plain += data
            if ffin:
                break
        cell = {'op': OPCODES.get(op, op), 'rsv1': int(rsv1), 'wire': wire, 'plain': len(plain),
                'fragments': frags, 'frag_wires': frag_wires}
        if cell['op'] == 'binary':
            cell['fields'] = _signal_fields(plain, self.schema)
            if 'request_response' in cell['fields']:
                cell['rr'] = request_response(plain) or {}
        return cell


def _signal_fields(payload: bytes, schema: str = 'signal') -> list:
    """Top-level field numbers of a protobuf message, named for the socket that carried it."""
    try:
        decoded = pb_decode(payload)
    except Exception:  # noqa: BLE001
        return ['unparsable']
    table = SERVER_MESSAGE_FIELDS if schema == 'worker' else SIGNAL_RESPONSE_FIELDS
    return [table.get(tag, f'f{tag}') for tag in sorted(decoded)]


def request_response(payload: bytes) -> dict | None:
    """Decode SignalResponse{request_response=22 -> {request_id=1, reason=2, message=3}}."""
    try:
        top = pb_decode(payload)
    except Exception:  # noqa: BLE001
        return None
    for _, inner in top.get(22, []):
        d = pb_decode(inner)
        reason = (d.get(2) or [(None, None)])[0][1]
        msg = (d.get(3) or [(None, b'')])[0][1]
        rid = (d.get(1) or [(None, None)])[0][1]
        return {'request_id': rid,
                'reason': REQUEST_RESPONSE_REASONS.get(reason, f'?{reason}'),
                'message': msg.decode('latin1', 'replace') if isinstance(msg, bytes) else str(msg),
                'has_update_metadata': 7 in d}
    return None


def pong_field(payload: bytes):
    """SignalResponse.pong = 18 (int64) or pong_resp = 20 (message)."""
    try:
        top = pb_decode(payload)
    except Exception:  # noqa: BLE001
        return 'unparsable'
    if 18 in top:
        return 'pong18'
    if 20 in top:
        return 'pong_resp20'
    return ','.join(_signal_fields(payload)) or 'empty'


# ----------------------------------------------------------------------------------- handshake
def ws_handshake(port: int, path: str, token_value: str, ext: str | None = None,
                 headers: dict | None = None, timeout: float = 6.0, schema: str = 'signal'):
    """Raw upgrade so the negotiated extension line is visible; returns (reader, cells)."""
    key = base64.b64encode(os.urandom(16)).decode()
    lines = [f'GET {path} HTTP/1.1', f'Host: 127.0.0.1:{port}', 'Upgrade: websocket',
             'Connection: Upgrade', f'Sec-WebSocket-Key: {key}', 'Sec-WebSocket-Version: 13',
             f'Authorization: Bearer {token_value}']
    if ext:
        lines.append(f'Sec-WebSocket-Extensions: {ext}')
    for k, v in (headers or {}).items():
        lines.append(f'{k}: {v}')
    try:
        sock = socket.create_connection(('127.0.0.1', port), timeout=timeout)
    except OSError as exc:
        return None, {'upgrade': f'error:{type(exc).__name__}'}
    sock.settimeout(timeout)
    try:
        sock.sendall(('\r\n'.join(lines) + '\r\n\r\n').encode())
        buf = b''
        while b'\r\n\r\n' not in buf:
            chunk = sock.recv(4096)
            if not chunk:
                break
            buf += chunk
        head, _, rest = buf.partition(b'\r\n\r\n')
        lines = head.split(b'\r\n')
        hdrs = {}
        for line in lines[1:]:
            if b':' in line:
                k, v = line.split(b':', 1)
                hdrs.setdefault(k.strip().lower().decode('latin1'), []).append(v.strip().decode('latin1'))
        status = lines[0].decode('latin1', 'replace') if lines else ''
        cells = {'upgrade': status, 'http': (status.split()[1] if len(status.split()) > 1 else ''),
                 'ext': ','.join(hdrs.get('sec-websocket-extensions', [])),
                 'server': (hdrs.get('server') or [''])[0],
                 'hdr_keys': ','.join(sorted(hdrs))}
        if cells['http'] != '101':
            body = rest[:200].decode('latin1', 'replace')
            if not body:
                sock.settimeout(1.0)
                try:
                    body = sock.recv(4096)[:200].decode('latin1', 'replace')
                except OSError:
                    body = ''
            cells['refusal'] = body[:160]
            sock.close()
            return None, cells
        return Reader(sock, rest, schema), cells
    except OSError as exc:
        cells = {'upgrade': f'error:{type(exc).__name__}'}
        try:
            sock.close()
        except OSError:
            pass
        return None, cells


JOIN_PATH = '/rtc/v1'
# WrappedRequest{request=2 SignalRequest{join=1 JoinRequest{room=1, identity=2, ...}}} as the CLI and
# SDK send it: an empty join body rides in the query string. Reused from C105 so both lanes put the
# identical bytes on the wire.
JOIN_PARAM = C105.JOIN_REQUEST_PARAM


def read_window(reader, frames: int = 4, timeout: float = 1.2, decisive: bool = True) -> list:
    """What the daemon sends after we pushed something.

    The window also runs when the push itself failed: a transport-tier refusal writes its close frame
    inside the daemon's read loop, so the socket can carry the answer even though our send raised.
    Reading only on a clean send would report `no frames` for a refusal that was in fact announced.
    """
    seen = []
    while len(seen) < frames:
        got = reader.message(timeout)
        if got is None:
            seen.append({'op': 'no-more-frames', 'end': reader.end})
            break
        seen.append(got)
        if got.get('op') == 'close':
            break
        if decisive and any(f in ('request_response', 'pong', 'pong_resp')
                            for f in (got.get('fields') or [])):
            break
    return seen


def signal_session(port: int, *, name=None, metadata=None, attributes=None, video=None,
                   pad=0, send=None, ext=None, read_limit=4, budget=2.5, ident=IDENT,
                   room=ROOM, raw_token=None):
    """Join, read the opening messages, optionally push frames, and record what came back.

    `send` is a list of (label, kind, payload) where kind is one of 'binary', 'frag-first',
    'frag-last', 'binary-deflate'. Frames are sent in order and every message the server produces
    afterwards is recorded, so a refusal is attributable to a specific label via `after`.
    """
    tok = raw_token if raw_token is not None else access_token(video=video, name=name,
                                                              metadata=metadata, attributes=attributes,
                                                              identity=ident)
    path = f'{JOIN_PATH}?join_request={JOIN_PARAM}&protocol=15'
    reader, cells = ws_handshake(port, path, tok, ext=ext)
    if reader is None:
        return cells
    msgs = []
    for _ in range(read_limit):
        got = reader.message(budget)
        if got is None:
            msgs.append({'op': 'no-more-frames', 'end': reader.end})
            break
        msgs.append(got)
        if got.get('op') == 'close':
            cells['closed_after_join'] = True
            break
    cells['join_msgs'] = msgs[:read_limit]
    if send:
        for label, kind, payload in send:
            cells.setdefault('sent', []).append([label, len(payload), kind])
            send_error = ''
            try:
                if kind == 'frag-first':
                    reader.sock.sendall(ws_frame(2, payload, fin=False))
                elif kind == 'frag-last':
                    reader.sock.sendall(ws_frame(0, payload, fin=True))
                elif kind == 'binary-deflate':
                    comp = df_comp(payload)
                    cells['sent'][-1].append(len(comp))
                    reader.sock.sendall(ws_frame(2, comp, rsv1=True, fin=True))
                else:
                    reader.sock.sendall(ws_frame(2, payload, fin=True))
            except OSError as exc:
                send_error = type(exc).__name__
                cells.setdefault('send_errors', {})[label] = send_error
            seen = read_window(reader, 4, 1.2)
            cells.setdefault('after', {})[label] = seen
            if send_error and seen[-1].get('op') == 'no-more-frames' \
                    and str(seen[-1].get('end', '')).startswith(('peer-eof', 'read-error')):
                break
    try:
        reader.sock.close()
    except OSError:
        pass
    return cells


# ---- part3.py ----
#!/usr/bin/env python3
"""C106 limit-gate channels - part 3: the HTTP body limiter and the Twirp limit gates.

Two families live on the API listener. `limit.max_api_request_body_size` is a middleware guard that
runs before routing, before authentication and before protobuf decoding; the per-field limits
(`max_metadata_size`, `max_attributes_size`, `max_room_name_length`, `max_participant_*_length`) are
handler guards that run at a documented position relative to the permission check and the room
lookup. The legs below deliberately collide those guards so the *first* refusal is the finding.
"""


BODY_ROWS = ['YAML:limit.max_api_request_body_size', 'generated_flags:--limit.max_api_request_body_size']
META_ROWS = ['YAML:limit.max_metadata_size', 'generated_flags:--limit.max_metadata_size']
ATTR_ROWS = ['YAML:limit.max_attributes_size']
NAME_ROWS = ['YAML:limit.max_participant_name_length']
ROOMNAME_ROWS = ['YAML:limit.max_room_name_length']
IDENT_ROWS = ['YAML:limit.max_participant_identity_length']
ORDER_ROWS = ['API_BEHAVIOR:RoomService.UpdateParticipant.owner_local_limit_error_echo']


def leg(leg_id, covers, cells, grade, expect=None, note=''):
    return {'id': leg_id, 'covers': covers, 'cells': cells, 'grade': list(grade),
            'expect': expect or {}, 'note': note}


def cells_of(resp: dict) -> dict:
    """Keep only stable cells; drop the status-line phrase (Go/Rust phrase the same code differently)."""
    out = {k: v for k, v in resp.items() if k not in ('raw', 'headers')}
    if resp.get('error'):
        out['conn_error'] = 1
    status = out.pop('status', '')
    parts = status.split(None, 2)
    out['http_code'] = parts[1] if len(parts) > 1 else ''
    out['phrase'] = parts[2] if len(parts) > 2 else ''
    return out


def http_body_legs(port, S):
    """`limit.max_api_request_body_size` seen through a real request body."""
    tok = access_token()
    legs = []
    base = {'method': 'UpdateRoomMetadata', 'service': 'RoomService'}

    def call(body_bytes, declare=None, token_value=tok, ctype='application/protobuf'):
        out = cells_of(twirp(port, base['service'], base['method'], b'', ctype=ctype,
                             token_value=token_value, declare=declare, raw_body=body_bytes))
        out['req_bytes'] = len(body_bytes)
        out['declared'] = len(body_bytes) if declare is None else declare
        return out

    L = S.get('body_limit')
    refuse = str(S.get('body_over_http') or '413')
    shell = update_room_metadata(ROOM, '')

    def body_at(total):
        return pad_to(shell, total)

    def chunks_of(data, parts):
        size = max(1, len(data) // parts)
        out = []
        for i in range(0, len(data), size):
            piece = data[i:i + size]
            out.append(b'%x\r\n' % len(piece) + piece + b'\r\n')
        return b''.join(out) + b'0\r\n\r\n'

    over = body_at(L + 1) if L else None
    exact = body_at(L) if L else None
    under = body_at(L - 1) if L and L > 4 else None
    big = body_at(S['body_big']) if S.get('body_big') else None

    if over is not None:
        legs.append(leg('body_over', BODY_ROWS, call(over),
                        ['http_code', 'ctype', 'clen', 'body_bytes', 'text', 'code', 'msg', 'parsed'],
                        {'http_code': refuse}))
        legs.append(leg('body_over_no_token', BODY_ROWS, call(over, token_value=None),
                        ['http_code', 'ctype', 'text', 'parsed', 'json_keys'],
                        {'http_code': refuse},
                        note='the limiter sits before the auth middleware, so an anonymous oversize body is refused'))
    if exact is not None:
        legs.append(leg('body_exact', BODY_ROWS, call(exact),
                        ['http_code', 'ctype', 'text', 'code', 'msg', 'json_keys'],
                        {'not_http_code': refuse}))
    if under is not None:
        legs.append(leg('body_under', BODY_ROWS, call(under),
                        ['http_code', 'ctype', 'code', 'msg', 'json_keys'],
                        {'not_http_code': refuse}))
    if big is not None:
        legs.append(leg('body_large_ok', BODY_ROWS, call(big),
                        ['http_code', 'ctype', 'code', 'msg', 'json_keys'],
                        {'not_http_code': refuse},
                        note='a body well past every configured number, still a valid request'))
    if over is not None:
        # chunked transfer: no declared length, so the guard must act on bytes actually read
        legs.append(leg('body_chunked_over', BODY_ROWS,
                        cells_of(twirp(port, 'RoomService', 'UpdateRoomMetadata', b'',
                                       declare=None, raw_body=chunks_of(over, 3), token_value=tok,
                                       extra={'Transfer-Encoding': 'chunked', 'Content-Length': None})),
                        ['http_code', 'ctype', 'text', 'parsed', 'json_keys']))
        legs.append(leg('body_chunked_small', BODY_ROWS,
                        cells_of(twirp(port, 'RoomService', 'UpdateRoomMetadata', b'',
                                       declare=None, raw_body=chunks_of(shell, 2), token_value=tok,
                                       extra={'Transfer-Encoding': 'chunked', 'Content-Length': None})),
                        ['http_code', 'ctype', 'code', 'msg', 'json_keys', 'parsed']))
        # declared length that lies: the transport, not the limiter, decides what the handler reads
        legs.append(leg('body_lying_length', BODY_ROWS, call(body_at(L + 500), declare=10),
                        ['http_code', 'code', 'msg', 'parsed', 'json_keys']))
        # the limiter is a listener-wide guard: an unknown route must still see it first
        legs.append(leg('body_over_unknown_route', BODY_ROWS,
                        cells_of(split_response(http_raw(port, 'POST', '/c106-nosuch-route',
                                          {'Content-Length': str(len(over)),
                                           'Content-Type': 'text/plain'}, over))),
                        ['http_code', 'ctype', 'text', 'parsed']))
    return legs


def twirp_limit_legs(port, S):
    """The per-field limit guards, each against its own positive control and ordering neighbour."""
    tok = access_token()
    adminless = access_token(video=grants(admin=False, room_join=True, room_create=False,
                                          can_update=False))
    legs = []
    if S.get('room_name_over') is not None:
        legs.append(leg('room_name_over', ROOMNAME_ROWS,
                        cells_of(twirp(port, 'RoomService', 'CreateRoom',
                                       create_room(name='r' * S['room_name_over']), token_value=tok)),
                        ['http_code', 'code', 'msg', 'meta_argument', 'ctype', 'json_keys']))
    if S.get('room_name_exact') is not None:
        legs.append(leg('room_name_exact', ROOMNAME_ROWS,
                        cells_of(twirp(port, 'RoomService', 'CreateRoom',
                                       create_room(name='r' * S['room_name_exact']), token_value=tok)),
                        ['http_code', 'code', 'msg', 'parsed']))
    for label, size, expect in (('meta_exact', 'meta_exact', None), ('meta_over', 'meta_over', None),
                                ('meta_under', 'meta_under', None)):
        if S.get(size) is None:
            continue
        legs.append(leg(f'update_room_{label}', META_ROWS,
                        cells_of(twirp(port, 'RoomService', 'UpdateRoomMetadata',
                                       update_room_metadata(ROOM, 'M' * S[size]), token_value=tok)),
                        ['http_code', 'code', 'msg', 'meta_keys', 'meta_argument', 'ctype', 'json_keys']))
    if S.get('meta_over') is not None:
        legs.append(leg('update_room_over_missing_room', META_ROWS,
                        cells_of(twirp(port, 'RoomService', 'UpdateRoomMetadata',
                                       update_room_metadata('c106nope', 'M' * S['meta_over']),
                                       token_value=tok)),
                        ['http_code', 'code', 'msg', 'meta_argument'],
                        note='limits run before the room lookup, so the limit text is what answers'))
        legs.append(leg('update_room_over_no_token', META_ROWS,
                        cells_of(twirp(port, 'RoomService', 'UpdateRoomMetadata',
                                       update_room_metadata(ROOM, 'M' * S['meta_over']),
                                       token_value=None)),
                        ['http_code', 'code', 'msg', 'meta_argument', 'ctype', 'json_keys'],
                        note='a *missing* token passes the middleware, so the limit still speaks first'))
        legs.append(leg('update_room_over_bad_token', META_ROWS + ORDER_ROWS,
                        cells_of(twirp(port, 'RoomService', 'UpdateRoomMetadata',
                                       update_room_metadata(ROOM, 'M' * S['meta_over']),
                                       token_value=tok[:-2] + 'xx')),
                        ['http_code', 'code', 'msg', 'text', 'ctype', 'json_keys'],
                        note='an *invalid* token is refused by the middleware before the handler runs'))
        legs.append(leg('update_room_over_not_admin', META_ROWS,
                        cells_of(twirp(port, 'RoomService', 'UpdateRoomMetadata',
                                       update_room_metadata(ROOM, 'M' * (S['meta_under'] or 4)),
                                       token_value=adminless)),
                        ['http_code', 'code', 'msg', 'text', 'ctype', 'json_keys'],
                        note='under-limit metadata on a non-admin token: the permission refusal must answer'))
        legs.append(leg('create_room_agents_meta_over', META_ROWS,
                        cells_of(twirp(port, 'RoomService', 'CreateRoom',
                                       create_room(name='c106ag', metadata='',
                                                   agents=[room_agent(metadata='a' * S['meta_over'])]),
                                       token_value=tok)),
                        ['http_code', 'code', 'msg', 'meta_argument', 'json_keys']))
        legs.append(leg('dispatch_meta_over', META_ROWS,
                        cells_of(twirp(port, 'AgentDispatchService', 'CreateDispatch',
                                       create_dispatch(metadata='d' * S['meta_over']), token_value=tok)),
                        ['http_code', 'code', 'msg', 'meta_argument', 'json_keys'],
                        note='the dispatch path carries its own metadata field; with no worker '
                             'registered the answer also names which guard is reached first'))
        legs.append(leg('dispatch_meta_under', META_ROWS,
                        cells_of(twirp(port, 'AgentDispatchService', 'CreateDispatch',
                                       create_dispatch(metadata='d' * (S['meta_under'] or 4)),
                                       token_value=tok)),
                        ['http_code', 'code', 'msg', 'meta_argument', 'json_keys']))
        legs.append(leg('update_participant_meta_over', META_ROWS,
                        cells_of(twirp(port, 'RoomService', 'UpdateParticipant',
                                       update_participant(metadata='M' * S['meta_over']), token_value=tok)),
                        ['http_code', 'code', 'msg', 'meta_argument', 'json_keys']))
        if S.get('attrs_over') is not None:
            legs.append(leg('update_participant_attrs_over', ATTR_ROWS,
                            cells_of(twirp(port, 'RoomService', 'UpdateParticipant',
                                           update_participant(attributes={'k': 'v' * S['attrs_over']}),
                                           token_value=tok)),
                            ['http_code', 'code', 'msg', 'meta_argument', 'json_keys']))
        if S.get('attrs_under') is not None:
            legs.append(leg('update_participant_attrs_under', ATTR_ROWS,
                            cells_of(twirp(port, 'RoomService', 'UpdateParticipant',
                                           update_participant(attributes={'k': 'v' * S['attrs_under']}),
                                           token_value=tok)),
                            ['http_code', 'code', 'msg', 'meta_argument', 'json_keys']))
        legs.append(leg('update_participant_double_violation', META_ROWS + NAME_ROWS + ORDER_ROWS,
                        cells_of(twirp(port, 'RoomService', 'UpdateParticipant',
                                       update_participant(metadata='M' * S['meta_over'],
                                                          name='n' * (S['name_over'] or 40)),
                                       token_value=tok)),
                        ['http_code', 'code', 'msg', 'meta_argument'],
                        note='two violated gates in one request: the answer names the one that runs first'))
    if S.get('name_over') is not None:
        legs.append(leg('update_participant_name_over', NAME_ROWS,
                        cells_of(twirp(port, 'RoomService', 'UpdateParticipant',
                                       update_participant(name='n' * S['name_over']), token_value=tok)),
                        ['http_code', 'code', 'msg', 'meta_argument', 'json_keys']))
        legs.append(leg('update_participant_name_exact', NAME_ROWS,
                        cells_of(twirp(port, 'RoomService', 'UpdateParticipant',
                                       update_participant(name='n' * (S['name_exact'] or 1)), token_value=tok)),
                        ['http_code', 'code', 'msg', 'meta_argument', 'json_keys']))
    if S.get('meta_over_json'):
        legs.append(leg('update_room_over_json_ctype', META_ROWS,
                        cells_of(twirp(port, 'RoomService', 'UpdateRoomMetadata', b'',
                                       ctype='application/json', token_value=tok,
                                       raw_body=json.dumps({'room': ROOM,
                                                             'metadata': 'M' * S['meta_over']}).encode())),
                        ['http_code', 'code', 'msg', 'meta_argument', 'ctype', 'json_keys'],
                        note='the same limit reached through the JSON Twirp codec'))
        legs.append(leg('update_room_wrong_ctype', ['HTTP_BEHAVIOR:twirp.content_type_negotiation'],
                        cells_of(twirp(port, 'RoomService', 'UpdateRoomMetadata',
                                       update_room_metadata(ROOM, 'M' * 4), ctype='application/proto',
                                       token_value=tok)),
                        ['http_code', 'code', 'msg', 'text', 'ctype', 'json_keys']))
        legs.append(leg('update_room_bad_proto_body', ['HTTP_BEHAVIOR:twirp.decode_before_auth'],
                        cells_of(twirp(port, 'RoomService', 'UpdateRoomMetadata', b'\xff\xff\xff\xff',
                                       token_value=tok)),
                        ['http_code', 'code', 'msg', 'meta_keys', 'ctype', 'body_bytes'],
                        note='decode failure vs auth failure ordering'))
    return legs


# ---- part4.py ----
#!/usr/bin/env python3
"""C106 limit-gate channels - part 4: the two signalling sockets.

`limit.signal_message_size_limit` and `limit.agent_signal_message_size_limit` bound two different
sockets: a participant session on `/rtc/v1` and an agent worker control connection on `/agent`. The
Go side enforces each of them twice over, on two different byte counts:

* transport: `conn.SetReadLimit(L)` (rtcservice.go:487-491, agentws.go:92-97). Gorilla accumulates the
  *payload length* of each frame of one message into `readLength` (conn.go:917-926, reset per message at
  conn.go:1006) and refuses once that sum exceeds L, so a frame header costs nothing, the boundary is
  inclusive (payload == L accepted, L+1 refused) and fragmented messages are judged as a whole. With
  permessage-deflate the counted bytes are the compressed ones. The refusal writes close 1009 with an
  empty reason from inside the transport, then the read loop exits;
* application: `io.LimitReader(r, L+1)` in `wsprotocol.go:66-90` bounds the *decompressed* payload, and
  its error reaches `readLoop`, whose teardown writes `CloseWithReason("")` (rtcservice.go:469-471) -
  close 1000, no reason.

So "how the socket dies" tells the two guards apart: 1009 = compressed/frame payload too big,
1000 = inflated message too big, and an accepted message answers with a pong or a `request_response`.
The legs below put a frame header, a plain payload and a deflated payload each on both sides of L so a
side that bounds the wrong quantity cannot pass.

The participant-level metadata/name/attribute limits also have a signalling face: a token claim is
checked at join (`roommanager.go:296-303`) and `SignalRequest.update_metadata` is checked in
`participant.go:693-752`, where the permission check runs *before* the size check and the refusal
travels back as `SignalResponse.request_response{reason, message}` rather than as a close.
"""

import threading


SIG_ROWS = ['YAML:limit.signal_message_size_limit',
            'generated_flags:--limit.signal_message_size_limit']
AGENT_ROWS = ['YAML:limit.agent_signal_message_size_limit',
              'generated_flags:--limit.agent_signal_message_size_limit']
DEFLATE_ROWS = ['WIRE:RTC.PerMessageDeflate']
# Which gate a claim leg measures. A join refused because the token's *identity* is too long is evidence
# about `max_participant_identity_length` and not about the metadata row, so the single-field legs name
# their own gate; `upd_double` puts two limited fields on one request and names both, and
# `upd_no_permission` names the metadata gate it proves stayed silent behind the permission check.
CLAIM_ROWS = {
    'tok_meta_over': ['YAML:limit.max_metadata_size'],
    'tok_meta_under': ['YAML:limit.max_metadata_size'],
    'tok_attrs_over': ['YAML:limit.max_attributes_size'],
    'tok_identity_over': IDENT_ROWS,
    'tok_name_over': NAME_ROWS,
    'upd_meta_over': ['YAML:limit.max_metadata_size'],
    'upd_meta_under': ['YAML:limit.max_metadata_size'],
    'upd_name_over': NAME_ROWS,
    'upd_attrs_over': ['YAML:limit.max_attributes_size'],
    'upd_no_permission': ['YAML:limit.max_metadata_size'],
    'upd_double': ['YAML:limit.max_metadata_size'] + NAME_ROWS,
}


def msgs_brief(msgs) -> str:
    """A stable one-line digest of the frames a session produced."""
    out = []
    for m in msgs or []:
        op = m.get('op')
        if op == 'close':
            out.append(f"close:{m.get('code')}")
        elif op in ('ping', 'pong'):
            out.append(op)
        else:
            fields = m.get('fields') or []
            out.append(f"{op}({'|'.join(str(f) for f in fields)})")
    return ' '.join(out)


def gate_class(verdict: str, reason: str = '') -> str:
    """Whether a size gate spoke, independent of *how* the transport said it.

    A worker socket can be refused by a close frame or by the daemon simply hanging up; both are the
    same gate outcome and only the second carries a code. Grading the outcome keeps the pair column
    about the limit, while `verdict`, `close_code` and `stream_end` stay in the receipt as the
    mechanism record. An in-band `request_response` counts as a refusal only when it carries the limit
    reason - NOT_FOUND or OK is some other guard answering, which must not be graded as the gate speaking.
    """
    v = str(verdict or '')
    if v.startswith('close:') or v.startswith('send-error') or v in ('hangup', 'hangup-without-frame'):
        return 'refused'
    if v == 'request_response':
        return 'refused' if reason == 'LIMIT_EXCEEDED' else 'other'
    if v in ('no-more-frames', ''):
        return 'silent'
    return 'answered'


def verdict_class(verdict: str) -> str:
    """Coarse class of a stream answer, so a leg can claim "the session was answered" without grading
    the exact field list a daemon happens to push alongside."""
    v = str(verdict or '')
    if v.startswith('close:'):
        return 'close'
    if v in ('pong', 'ping', 'request_response'):
        return v
    if v in ('hangup', 'hangup-without-frame'):
        return 'hangup'
    if v in ('no-more-frames', ''):
        return 'silent'
    return 'answered'


def after_cells(cell_list) -> dict:
    """Project the per-send message list into comparable cells.

    One send is rarely answered by exactly one frame: the join ack and its `refresh_token` ride ahead of
    the answer to our request, a fragmented message answers only after the last fragment, and a read
    window can simply expire. So the verdict is the *most decisive* frame seen (close > request_response
    > pong > any other message > hangup > no frames), not the first and not the last - that keeps an
    unrelated `update` from masking a pong, and a quiet window from masking a close.

    `stream_end` separates the two ways a window can produce no frame: the daemon hung the stream up
    (`peer-eof`, i.e. it ended the session without writing a close frame) or nothing arrived in time
    (`window-expired`). Grading them as one cell would let a silent drop and a quiet accept look equal.
    """
    out = {'msgs': msgs_brief(cell_list), 'n': len(cell_list or [])}
    best, best_rank = 'answered-without-close', 0
    ends = [str(m.get('end') or '') for m in (cell_list or []) if m.get('op') == 'no-more-frames']
    out['stream_end'] = ends[-1] if ends else 'frame-seen'
    hung_up = any(e.startswith(('peer-eof', 'read-error')) for e in ends)
    for m in cell_list or []:
        op = m.get('op')
        if op == 'close':
            return dict(out, verdict=f"close:{m.get('code')}", gate_class=gate_class(f"close:{m.get('code')}"),
                        verdict_class='close', close_code=m.get('code'),
                        close_reason=m.get('reason', ''))
        if op in ('ping', 'pong'):
            if 3 > best_rank:
                best, best_rank = op, 3
                out['pong_wire'] = m.get('wire')
            continue
        if op == 'no-more-frames':
            if 1 > best_rank:
                best, best_rank = 'hangup' if hung_up else 'no-more-frames', 1
            continue
        if op == 'binary':
            fields = m.get('fields') or []
            if 'request_response' in fields:
                rr = m.get('rr') or {}
                return dict(out, verdict='request_response',
                            gate_class=gate_class('request_response', rr.get('reason', '')),
                            verdict_class='request_response',
                            rr_reason=rr.get('reason', ''),
                            rr_message=rr.get('message', ''), rr_request_id=rr.get('request_id', ''),
                            rr_has_update=int(bool(rr.get('has_update_metadata'))))
            if 'pong' in fields or 'pong_resp' in fields:
                if 3 > best_rank:
                    best, best_rank = 'pong', 3
                continue
            if 2 > best_rank:
                best, best_rank = ','.join(str(f) for f in fields), 2
    if best_rank < 2 and hung_up:
        best = 'hangup-without-frame'
    return dict(out, verdict=best, verdict_class=verdict_class(best),
                gate_class=gate_class(best))


def status_rest(value: str) -> str:
    """The part of a status line after the code. A refused handshake has no status at all, and an
    empty cell must be recorded rather than raising in the middle of a launch."""
    parts = (value or '').split()
    return ' '.join(parts[1:])[:40] if len(parts) > 1 else ''


def join_leg_cells(res: dict) -> dict:
    msgs = res.get('join_msgs') or []
    fields = sorted({f for m in msgs for f in (m.get('fields') or [])})
    ends = [str(m.get('end') or '') for m in msgs if m.get('op') == 'no-more-frames']
    out = {'http': res.get('http', ''), 'upgrade': status_rest(res.get('upgrade')),
           'msgs': msgs_brief(msgs), 'ext': res.get('ext', ''),
           'refusal': (res.get('refusal') or '')[:120],
           'has_join': int('join' in fields), 'resp_fields': ','.join(fields),
           'stream_end': (ends[-1] if ends else 'frame-seen')}
    if res.get('closed_after_join'):
        out['closed_after_join'] = 1
    return out


class SessionHolder:
    """Keeps a joined participant resident while the HTTP legs run.

    A real client drains what the daemon pushes and answers its ping with a pong. A harness that reads
    its own sockets instead leaves "is the participant still there" dependent on how long the harness
    spent in its previous read window - the wall clock, not the gate under test. The thread owns the
    socket after the join ack, so nothing else reads it while it runs.
    """

    def __init__(self, sock, buf: bytes = b''):
        self.sock = sock
        self.buf = buf
        self.frames = 0
        self.pongs = 0
        self.close_code = ''
        self.ended = ''
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._loop, daemon=True)

    def start(self):
        self._thread.start()
        return self

    def _need(self, n: int):
        while len(self.buf) < n:
            if self._stop.is_set():
                return False
            try:
                self.sock.settimeout(0.3)
                chunk = self.sock.recv(65536)
            except socket.timeout:
                continue
            except OSError as exc:
                self.ended = f'read-error:{type(exc).__name__}'
                return False
            if not chunk:
                self.ended = 'peer-eof'
                return False
            self.buf += chunk
        return True

    def _loop(self):
        try:
            while not self._stop.is_set():
                if not self._need(2):
                    return
                b0, b1 = self.buf[0], self.buf[1]
                op, ln, off = b0 & 0x0F, b1 & 0x7F, 2
                if ln == 126:
                    if not self._need(4):
                        return
                    ln = struct.unpack('!H', self.buf[2:4])[0]
                    off = 4
                elif ln == 127:
                    if not self._need(10):
                        return
                    ln = struct.unpack('!Q', self.buf[2:10])[0]
                    off = 10
                if not self._need(off + ln):
                    return
                payload = self.buf[off:off + ln]
                self.buf = self.buf[off + ln:]
                self.frames += 1
                if op == 8:
                    self.close_code = struct.unpack('!H', payload[:2])[0] if len(payload) >= 2 else 0
                    self.ended = 'close-frame'
                    return
                if op == 9:
                    try:
                        self.sock.sendall(ws_frame(10, payload, fin=True))
                        self.pongs += 1
                    except OSError:
                        self.ended = 'pong-failed'
                        return
        except Exception as exc:  # noqa: BLE001 - a dead holder must not end the launch
            self.ended = f'holder:{type(exc).__name__}'

    def stop(self) -> dict:
        self._stop.set()
        try:
            self.sock.close()
        except OSError:
            pass
        self._thread.join(3.0)
        return {'frames': self.frames, 'pongs': self.pongs, 'close_code': self.close_code,
                'ended': self.ended or 'still-open'}


def hold_participant(port, *, video=None, identity=IDENT, room=ROOM):
    """Join a real participant and keep the socket open.

    The RoomService update legs need an existing session: without one, `UpdateParticipant` answers
    `participant does not exist` for *every* size, so the accept half of a limit family would be
    vacuous - a 404 that does not mention the limit proves nothing except that the room was searched.
    Returns (holder, cells); the caller stops the holder.
    """
    tok = access_token(video=video if video is not None else grants(), identity=identity)
    path = f'{JOIN_PATH}?join_request={JOIN_PARAM}&protocol=15'
    reader, cells = ws_handshake(port, path, tok)
    if reader is None:
        return None, cells
    first = reader.message(2.5)
    cells['join'] = msgs_brief([first] if first else [{'op': 'no-more-frames', 'end': reader.end}])
    holder = SessionHolder(reader.sock, reader.buf).start()
    reader.sock = None               # the holder owns the socket from here
    cells['held'] = 1
    return holder, cells


def signal_legs(port, S, tok_video=None):
    """Participant-socket size gates and the claim/update gates carried by the same socket."""
    legs = []

    def session(**kw):
        return signal_session(port, **kw)

    base = session(read_limit=2, budget=2.0)
    # A negative *length* limit cannot be satisfied by any identity, so in that profile the join control
    # is expected to be refused at the handshake; a positive one must be able to establish a session or
    # every size leg below would be graded over an empty stream.
    join_expect = ({'has_join': 0, 'http': '400'} if S.get('neg_len') else {'has_join': 1})
    legs.append(leg('sig_join_ok', SIG_ROWS, join_leg_cells(base),
                    ['http', 'upgrade', 'has_join'], join_expect))
    L = S.get('sig')
    if L:
        at = largest_frame_payload(L)
        for label, n, note in (
                ('sig_frame_under', largest_frame_payload(L - 1),
                 'the safe accept: payload and whole frame both below the bound'),
                ('sig_frame_at', at,
                 'the whole masked frame equals the limit while the payload sits under it: a guard '
                 'that counted frame headers would already refuse here'),
                ('sig_frame_over', at + 1,
                 'one byte past the limit measured as a frame, still inside it measured as a payload - '
                 'the header-accounting discriminator'),
                ('sig_payload_at', L,
                 f'payload of exactly the limit, so the frame runs {frame_overhead(L)} bytes past it: '
                 'the inclusive boundary of a payload-counting guard'),
                ('sig_payload_over', L + 1,
                 'one byte past the limit as a payload: the refusal both sides must agree on')):
            payload = pad_to(signal_ping(), n)
            res = session(read_limit=1, budget=2.0, send=[('x', 'binary', payload)])
            cells = after_cells((res.get('after') or {}).get('x', []))
            cells['payload'] = len(payload)
            cells['frame'] = len(payload) + frame_overhead(len(payload))
            legs.append(leg(label, SIG_ROWS, cells,
                            ['verdict', 'close_code', 'close_reason', 'payload', 'frame', 'stream_end'],
                            note=note))
        half = max(1, (L + 1) // 2)
        frag = session(read_limit=1, send=[
            ('frag', 'frag-first', pad_to(signal_ping(), half)),
            ('frag2', 'frag-last', b'z' * (L + 1 - half))])
        seen = []
        for lbl in ('frag', 'frag2'):
            seen.extend((frag.get('after') or {}).get(lbl, []))
        legs.append(leg('sig_fragmented_over', SIG_ROWS + DEFLATE_ROWS, after_cells(seen),
                        ['verdict', 'close_code', 'close_reason', 'stream_end'],
                        note='both fragments stay under the per-frame bound while the reassembled '
                             'payload is over it: gorilla sums frame payloads per message '
                             '(conn.go:917-926), so the transport guard is expected to fire'))
        big = session(read_limit=1, send=[('huge', 'binary',
                                           pad_to(signal_ping(), S['sig_huge']))])
        legs.append(leg('sig_huge', SIG_ROWS, after_cells(big.get('after', {}).get('huge', [])),
                        ['verdict', 'close_code', 'stream_end']))
    if S.get('sig_exact') and not L:
        # a disabled gate still has to swallow a large single frame rather than drop the session
        payload = pad_to(signal_ping(), S['sig_exact'])
        res = session(read_limit=1, budget=2.0, send=[('x', 'binary', payload)])
        cells = after_cells((res.get('after') or {}).get('x', []))
        cells['payload'] = len(payload)
        legs.append(leg('sig_large', SIG_ROWS, cells, ['verdict', 'close_code', 'payload', 'stream_end'],
                        note='no positive limit configured, so the frame above must be accepted'))
    if S.get('sig_deflate'):
        plain = pad_to(signal_ping(), S['sig_over'])
        comp = df_comp(plain)
        dl = session(read_limit=1, ext='permessage-deflate; client_max_window_bits',
                     send=[('deflate', 'binary-deflate', plain)])
        cells = after_cells((dl.get('after') or {}).get('deflate', []))
        cells['plain'] = len(plain)
        cells['wire'] = len(comp)
        cells['ext'] = dl.get('ext', '')
        legs.append(leg('sig_deflate_plain_over', SIG_ROWS + DEFLATE_ROWS, cells,
                        ['verdict', 'close_code', 'close_reason', 'ext'],
                        note='plain payload exceeds the limit while the compressed frame does not: '
                             'the transport guard passes it and the application guard must catch it'))
        under = pad_to(signal_ping(), S['sig_under'] or 64)
        du = session(read_limit=1, ext='permessage-deflate; client_max_window_bits',
                     send=[('deflate_under', 'binary-deflate', under)])
        uc = after_cells((du.get('after') or {}).get('deflate_under', []))
        uc['ext'] = du.get('ext', '')
        uc['plain'] = len(under)
        uc['wire'] = len(df_comp(under))
        legs.append(leg('sig_deflate_under', SIG_ROWS + DEFLATE_ROWS, uc,
                        ['verdict', 'close_code', 'ext']))
    # token-claim gates on the join path. Each probe exists only when the profile has a number small
    # enough to carry in a token, so a side that ignores the claim limit still shows up as a pair diff
    # rather than as a leg built from a missing key.
    C = S.get('claim') or {}
    for label, key, build in (
            ('tok_meta_over', 'meta_over', lambda n: {'metadata': 'M' * n}),
            ('tok_meta_under', 'meta_under', lambda n: {'metadata': 'M' * n}),
            ('tok_attrs_over', 'attrs_over', lambda n: {'attributes': {'a': 'v' * n}}),
            ('tok_name_over', 'name_over', lambda n: {'name': 'n' * n}),
            ('tok_identity_over', 'ident_over', lambda n: {'identity': 'i' * n})):
        if not C.get(key):
            continue
        res = session(raw_token=access_token(**build(C[key])), read_limit=2)
        legs.append(leg(label, CLAIM_ROWS[label], join_leg_cells(res),
                        ['http', 'upgrade', 'refusal', 'closed_after_join',
                         'has_join']))
    # update_metadata: the signal-path face of the same size limits
    own = grants(admin=False, own_metadata=True, room_create=False)
    no_own = grants(admin=False, own_metadata=False, room_create=False, can_update=False)
    upd_specs = [
        ('upd_meta_over', 'meta_over', own, lambda n: {'metadata': 'M' * n}),
        ('upd_meta_under', 'meta_under', own, lambda n: {'metadata': 'M' * n}),
        ('upd_name_over', 'name_over', own, lambda n: {'name': 'n' * n}),
        ('upd_attrs_over', 'attrs_over', own, lambda n: {'attributes': {'k': 'v' * n}}),
        ('upd_no_permission', 'meta_over', no_own, lambda n: {'metadata': 'M' * n}),
    ]
    if C.get('name_over'):
        upd_specs.append(('upd_double', 'meta_over', own,
                          lambda n: {'metadata': 'M' * n, 'name': 'n' * C['name_over']}))
    for label, key, video, build in upd_specs:
        if not C.get(key):
            continue
        payload = signal_update_metadata(request_id=S.get('req_id', 7), **build(C[key]))
        res = session(video=video, read_limit=2, send=[('u', 'binary', payload)])
        legs.append(leg(label, CLAIM_ROWS[label] + ['API_BEHAVIOR:RoomService.UpdateParticipant.owner_local_limit_error_echo'],
                        after_cells((res.get('after') or {}).get('u', [])),
                        ['verdict', 'rr_reason', 'rr_message', 'close_code']))
    return legs


def agent_session(port, *, video=None, register_bytes=None, then=None, read_limit=2, ext=None,
                  raw_token=None, path='/agent?protocol=2'):
    """Open the worker control socket, register, and record what the server does.

    `then` is a list of (label, total): a WorkerMessage{ping} grown to exactly `total` bytes.
    """
    tok = raw_token if raw_token is not None else access_token(video=video if video is not None
                                                               else grants(agent=True))
    reader, cells = ws_handshake(port, path, tok, ext=ext, schema='worker')
    if reader is None:
        return cells
    msgs = []
    send_errors = {}
    try:
        reader.sock.sendall(ws_frame(2, register_bytes or worker_register(), fin=True))
    except OSError as exc:
        # Even here the answer may already be on the wire: gorilla writes the transport refusal from
        # inside its read loop, so a failed send says nothing about whether a close frame arrived.
        send_errors['register'] = type(exc).__name__
        reader.sock.settimeout(1.5)
    for _ in range(read_limit):
        got = reader.message(2.5)
        if got is None:
            msgs.append({'op': 'no-more-frames', 'end': reader.end})
            break
        msgs.append(got)
        if got.get('op') == 'close':
            break
    cells['after_register'] = msgs
    if send_errors:
        cells['send_errors'] = send_errors
    for label, total in (then or []):
        send_error = ''
        try:
            reader.sock.sendall(ws_frame(2, pad_to(worker_message_ping(), total), fin=True))
        except (OSError, ValueError) as exc:
            send_error = type(exc).__name__
            cells.setdefault('send_errors', {})[label] = send_error
        seen = read_window(reader, 2, 1.5, decisive=False)
        cells.setdefault('after', {})[label] = seen
        if send_error and seen[-1].get('op') == 'no-more-frames' \
                and str(seen[-1].get('end', '')).startswith(('peer-eof', 'read-error')):
            break
    try:
        reader.sock.close()
    except OSError:
        pass
    return cells


def worker_message_ping(pad=0) -> bytes:
    """WorkerMessage{ping=5 Ping{}} padded to a known size."""
    inner = pb_len(1, b'c106seq')
    if pad:
        inner += pb_len(PAD_FIELD, b'q' * pad)
    return pb_len(5, inner)


def agent_legs(port, S):
    """`limit.agent_signal_message_size_limit` on the worker control socket."""
    legs = []
    A = S.get('agent')

    def register_at(total):
        return pad_to(worker_register(), total)

    def reg_cells(res, size):
        seen = res.get('after_register') or []
        fields = sorted({f for m in seen for f in (m.get('fields') or [])})
        return dict(after_cells(seen), size=size, http=res.get('http', ''),
                    msgs=msgs_brief(seen), resp_fields=','.join(fields),
                    reg_ok=int('register' in fields))

    def ping_cells(res, label):
        return dict(after_cells((res.get('after') or {}).get(label, [])),
                    http=res.get('http', ''))

    small = agent_session(port)
    legs.append(leg('agent_reg_ok', AGENT_ROWS,
                    dict(reg_cells(small, len(worker_register())),
                         reg_len=len(worker_register())),
                    ['http', 'gate_class', 'reg_ok', 'stream_end'],
                    {'reg_ok': 1},
                    note='a worker that registers inside the limit must get a register answer'))
    if A:
        at = largest_frame_payload(A)
        for label, n, note in (
                ('agent_reg_frame_at', at,
                 'registration whose whole masked frame equals the bound while its payload stays under '
                 'it: accepted by a payload-counting guard'),
                ('agent_reg_frame_over', at + 1,
                 'one byte past the limit as a frame, inside it as a payload: the same header-'
                 'accounting discriminator as on the participant socket'),
                ('agent_reg_payload_at', A,
                 f'payload of exactly the limit, frame {frame_overhead(A)} bytes over it: the '
                 'inclusive boundary and the answer the register handshake must still get'),
                ('agent_reg_payload_over', A + 1,
                 'payload one past the limit: the refusal both sides must agree on, and the reason no '
                 'register response may be written')):
            res = agent_session(port, register_bytes=register_at(n), read_limit=1)
            legs.append(leg(label, AGENT_ROWS, reg_cells(res, n),
                            ['gate_class', 'reg_ok', 'http'], note=note))
        mid = agent_session(port, then=[('mid', S['agent_mid'])])
        legs.append(leg('agent_mid_after_ok', AGENT_ROWS, ping_cells(mid, 'mid'),
                        ['gate_class', 'http'],
                        note='a registration-sized message sent *after* the handshake, whose size sits '
                             'between the two byte bounds: which gate governs a live worker socket is '
                             'decided across profiles by the agent_mid_sits_on_one_bound control, not '
                             'by this answer alone'))
        huge = agent_session(port, then=[('huge', S['agent_huge'])])
        legs.append(leg('agent_huge_after_ok', AGENT_ROWS, ping_cells(huge, 'huge'),
                        ['gate_class', 'http']))
    else:
        big = S.get('agent_exact') or 8192
        res = agent_session(port, register_bytes=register_at(big), read_limit=1)
        legs.append(leg('agent_reg_large', AGENT_ROWS, reg_cells(res, big),
                        ['gate_class', 'reg_ok', 'http'],
                        note='no positive agent bound configured, so a large registration must pass'))
    nogrant = agent_session(port, video=grants(admin=False, agent=False))
    legs.append(leg('agent_no_grant', AGENT_ROWS,
                    {'http': nogrant.get('http', ''), 'refusal': (nogrant.get('refusal') or '')[:120],
                     'upgrade': status_rest(nogrant.get('upgrade'))},
                    ['http', 'refusal']))
    noup = cells_of(split_response(http_raw(port, 'GET', '/agent?protocol=2',
                                            {'Authorization': 'Bearer '
                                             + access_token(video=grants(agent=True))})))
    legs.append(leg('agent_no_upgrade', AGENT_ROWS,
                    {'http_code': noup.get('http_code', ''), 'ctype': noup.get('ctype', ''),
                     'body_bytes': noup.get('body_bytes', ''), 'text': noup.get('text', '')[:60]},
                    ['http_code', 'text']))
    return legs


# ---- part5.py ----
#!/usr/bin/env python3
"""C106 limit-gate channels - part 5: profiles, sizes and the leg inventory.

A profile is one YAML overlay (plus optional argv) applied to *both* sides of a pair, together with the
payload sizes that make the legs discriminating for those numbers. Every `*_exact` size is limit+N and
every `*_over` is limit+N+1, so an off-by-one in either direction shows up as a pair mismatch *and* as a
family-vacuity failure.

Profile intents:
* tight        - eight small distinct numbers, the whole gate map in one launch
* zeros        - 0 = disabled on every channel (the documented contract on both sides)
* signed-neg   - the signed fields set negative: Go rejects every non-empty value, so does Rust
* unsigned-neg - `max_metadata_size: -1`: Go fails to parse the config (uint32); the question is whether
                 Rust refuses to start or silently ignores it
* legacy-alias - `room.max_metadata_size` only, the alias channel that must land in the same guard
* yaml-vs-flag - YAML and argv set different numbers on the same key: which one the daemon obeys
* agent-gt-sig - the agent bound sits *above* the participant bound, the mirror order of `tight`, so the
                 two answers together say which limit governs a worker message after registration
* defaults     - no limit block: the built-in numbers, and whether a real-size client still works
"""


# Every `limit.*` key this lane judges, with the Go type that decides whether a negative even parses.
LIMIT_KEYS = {
    'max_metadata_size': 'uint32',
    'max_attributes_size': 'uint32',
    'max_room_name_length': 'int',
    'max_participant_identity_length': 'int',
    'max_participant_name_length': 'int',
    'signal_message_size_limit': 'int64',
    'agent_signal_message_size_limit': 'int64',
    'max_api_request_body_size': 'int64',
}

# config.go:740-748: the legacy `room.*` fields that overwrite their `limit.*` counterpart when non-zero.
ALIAS_KEYS = ('max_metadata_size', 'max_room_name_length', 'max_participant_identity_length')

TIGHT = {'max_metadata_size': 128, 'max_attributes_size': 64, 'max_room_name_length': 16,
         'max_participant_identity_length': 16, 'max_participant_name_length': 16,
         'signal_message_size_limit': 2048, 'agent_signal_message_size_limit': 1024,
         'max_api_request_body_size': 512}


def yaml_block(mapping: dict) -> str:
    if not mapping:
        return ''
    return 'limit:\n' + ''.join(f'  {k}: {v}\n' for k, v in mapping.items())


def merge_sizes(sz: dict, over: dict) -> dict:
    """Overlay a profile's probe sizes. The claim map merges rather than replaces, so a profile that
    renames one probe still carries the derived positive controls the other claim legs need."""
    out = dict(sz)
    for k, v in over.items():
        out[k] = {**(sz.get(k) or {}), **v} if k == 'claim' else v
    return out


def sizes(meta=128, attrs=64, room=16, ident=16, name=16, sig=2048, agent=1024, body=512,
          big_body=None):
    """Probe sizes derived from the limit numbers actually configured.

    `meta`/`attrs`/`room`/`ident`/`name` bound a field's character count, so the probe size *is*
    that count. `body`/`sig`/`agent` bound a byte stream, so the legs ask for the encoded length
    directly (see pad_to) and only exist when the configured number is positive: a disabled or
    negative gate has no boundary to sit on, and `big_*` probes carry the "nothing is refused"
    half of that question instead.
    """
    def off(n):
        return n if n and n > 0 else None

    def huge(over, times, cap=4 << 20):
        """Comfortably past the bound, but capped in absolute size: with the default 2 MiB signal
        limit an uncapped 8x probe would push a 16 MiB frame through loopback to prove a refusal the
        byte past the bound already proves."""
        if not over or over <= 0:
            return None
        return min(max(over * times, over + 1024), cap)

    def claim_pair(limit):
        """(under, over) probes for a claim carried in the join *token*.

        The claim legs put the string in the JWT inside the join query, so a 512 KiB default boundary
        cannot be probed there - that gate is judged over HTTP instead (part3). A non-positive limit has
        no boundary to sit on, so its pair is the "any size must pass" control.
        """
        if limit is None or limit > 4095:
            return None, None
        if limit <= 0:
            return 32, 4096
        return max(1, limit - 1), limit + 1

    mu, mo = claim_pair(meta)
    au, ao = claim_pair(attrs)
    _, no = claim_pair(name)
    _, io = claim_pair(ident)
    claim = {k: v for k, v in (('meta_under', mu), ('meta_over', mo), ('attrs_under', au),
                               ('attrs_over', ao), ('name_over', no), ('ident_over', io)) if v}

    return {
        'meta_under': max(1, meta - 1) if meta > 1 else None, 'meta_exact': off(meta),
        'meta_over': meta + 1 if meta > 0 else None,
        'meta_over_json': meta + 1 if meta > 0 else None,
        'attrs_under': max(1, attrs - 10) if attrs > 10 else None,
        'attrs_over': attrs + 1 if attrs > 0 else None,
        'room_name_exact': off(room), 'room_name_over': room + 1 if room > 0 else None,
        'ident_exact': off(ident), 'ident_over': ident + 1 if ident > 0 else None,
        'name_exact': max(1, name - 1) if name > 1 else None,
        'name_over': name + 1 if name > 0 else None,
        'sig': off(sig), 'sig_exact': off(sig), 'sig_over': sig + 1 if sig > 0 else None,
        'sig_huge': huge(sig, 8), 'sig_under': max(1, (sig or 2048) // 2),
        'sig_deflate': bool(sig) and sig > 0,
        'agent': off(agent), 'agent_exact': off(agent), 'agent_over': agent + 1 if agent > 0 else None,
        # strictly between the two byte bounds whichever order they are in, so the same leg asks the
        # same question in `tight` (agent below) and in `agent-gt-sig` (agent above)
        'agent_mid': ((min(sig, agent) + max(sig, agent)) // 2
                      if sig and agent and sig > 0 and agent > 0 else 0),
        'agent_huge': huge(agent, 6),
        'body_limit': off(body), 'body_big': big_body, 'body_over_http': '413',
        # A length gate that is negative rejects every value, so the join itself cannot be a control
        # there; the byte gates take the opposite rule (a non-positive limit disables the guard).
        'neg_len': any(isinstance(v, int) and v < 0 for v in (meta, attrs, room, ident, name)),
        'claim': claim,
    }


def cfg_for(port, store, limit_yaml='', room_yaml='', extra=''):
    return (f'port: {port}\n'
            f'bind_addresses: [127.0.0.1]\n'
            f'redis:\n  address: 127.0.0.1:{store}\n'
            f'keys:\n  {API_KEY}: {API_SECRET}\n'
            f'rtc:\n  tcp_port: 0\n'
            f'turn:\n  enabled: false\n'
            f'{limit_yaml}{room_yaml}{extra}')


def profiles(P):
    """(id, argv, yaml, sizes, live-legs?, notes)"""
    out = []

    def add(pid, intent, *, argv=(), limit=TIGHT, room_yaml='', live=True, base=None, over=None):
        # `base` is the limit block actually configured; `over` replaces probe sizes the derived
        # numbers cannot express (a disabled or negative gate has no boundary to sit on).
        sz = None
        if base is not None or over is not None:
            sz = merge_sizes(sizes(**(base or {})), over or {})
        out.append({'id': pid, 'intent': intent, 'argv': list(argv),
                    'yaml': cfg_for(P['HTTP'], P['STORE'], yaml_block(limit), room_yaml),
                    'limit': dict(limit), 'room_yaml': room_yaml.strip(),
                    'aliases': {k: f'YAML:room.{k}' for k in ALIAS_KEYS if f'{k}:' in room_yaml},
                    'sizes': sz, 'live': live})

    add('tight', 'eight distinct small limits: the full gate map',
        base=dict(meta=128, attrs=64, room=16, ident=16, name=16, sig=2048, agent=1024, body=512))

    add('zeros', '0 disables every channel, so large payloads must be accepted',
        limit={'max_metadata_size': 0, 'max_attributes_size': 0, 'max_room_name_length': 0,
               'max_participant_identity_length': 0, 'max_participant_name_length': 0,
               'signal_message_size_limit': 0, 'agent_signal_message_size_limit': 0,
               'max_api_request_body_size': 0},
        base=dict(meta=0, attrs=0, room=0, ident=0, name=0, sig=0, agent=0, body=0, big_body=4096),
        over={'meta_exact': 4096, 'attrs_over': 4096, 'room_name_exact': 64, 'ident_exact': 64,
              'name_exact': 64, 'name_over': 4096, 'sig_exact': 65536, 'sig_huge': 262144,
              'agent_exact': 8192, 'agent_huge': 65536})

    # No `*_over` HTTP length probe here: a negative gate has no accept side to compare against, so the
    # family is recorded as not applicable and the pair column carries the reject-everything finding.
    add('signed-neg', 'the signed length fields set negative: reject-everything, not unlimited',
        limit={'max_room_name_length': -1, 'max_participant_identity_length': -1,
               'max_participant_name_length': -1, 'signal_message_size_limit': -1,
               'agent_signal_message_size_limit': -1, 'max_api_request_body_size': -1,
               'max_metadata_size': 128, 'max_attributes_size': 64},
        base=dict(meta=128, attrs=64, room=-1, ident=-1, name=-1, sig=-1, agent=-1, body=-1,
                  big_body=4096),
        over={'room_name_exact': 1, 'ident_exact': 1, 'name_exact': 4, 'sig_deflate': False,
              'sig_exact': 4096, 'sig_huge': 8192, 'agent_exact': 4096, 'agent_huge': 8192,
              'claim': {'name_over': 4, 'ident_over': 4}})

    add('unsigned-neg', 'an unsigned limit set negative: does the daemon even start?',
        limit={'max_metadata_size': -1, 'max_attributes_size': 64, 'max_room_name_length': 16,
               'max_participant_identity_length': 16, 'max_participant_name_length': 16,
               'signal_message_size_limit': 2048, 'agent_signal_message_size_limit': 1024,
               'max_api_request_body_size': 512},
        live=False)

    # config.go:740-748: three legacy room.* fields overwrite their limit.* counterpart when non-zero.
    # Only max_metadata_size is a new row here; the other two alias channels are already VERIFIED and
    # gain a second consumer, so the profile drives all three rather than one.
    add('legacy-alias', 'room.max_* must copy into the limit guard',
        limit={}, room_yaml='room:\n  max_metadata_size: 128\n  max_room_name_length: 16\n'
                            '  max_participant_identity_length: 16\n',
        base=dict(meta=128, attrs=65536, room=16, ident=16, name=256, sig=2097152,
                  agent=2097152, body=10485760, big_body=1048576))

    # The standard legs are sized against the *flag* numbers: a side that obeys YAML instead accepts the
    # 4 KiB body and the 4 KiB frame that the flag numbers refuse, so its own family gate fails and the
    # `body_fixed_4k`/`sig_fixed_4k` expects name the winner.
    add('yaml-vs-flag', 'argv overrides YAML on the same limit key',
        argv=['--limit.signal_message_size_limit=1024', '--limit.max_api_request_body_size=256',
              '--limit.max_metadata_size=96', '--limit.agent_signal_message_size_limit=768'],
        limit={'signal_message_size_limit': 8192, 'max_api_request_body_size': 8192,
               'max_metadata_size': 4096, 'max_attributes_size': 64, 'max_room_name_length': 16,
               'max_participant_identity_length': 16, 'max_participant_name_length': 16,
               'agent_signal_message_size_limit': 4096},
        base=dict(meta=96, attrs=64, room=16, ident=16, name=16, sig=1024, agent=768, body=256))

    # The agent bound and the participant bound swapped, so a post-registration message sits above one
    # and below the other in the *opposite* order from `tight`. Which gate the ongoing worker read obeys
    # is then decided by the two answers together instead of by either one alone.
    add('agent-gt-sig', 'agent bound above the participant bound: which one governs the live socket',
        limit={'signal_message_size_limit': 1024, 'agent_signal_message_size_limit': 4096,
               'max_metadata_size': 128, 'max_attributes_size': 64, 'max_room_name_length': 16,
               'max_participant_identity_length': 16, 'max_participant_name_length': 16,
               'max_api_request_body_size': 512},
        base=dict(meta=128, attrs=64, room=16, ident=16, name=16, sig=1024, agent=4096, body=512))

    add('defaults', 'no limit block at all: the built-in numbers',
        limit={},
        base=dict(meta=524288, attrs=65536, room=256, ident=256, name=256, sig=2097152,
                  agent=2097152, body=10485760, big_body=1048576))
    return out


# ---- part6.py ----
#!/usr/bin/env python3
"""C106 limit-gate channels - part 6: recording, pairing and the gates that make a pass mean something.

Grading rules, all recomputed from stored cells at report time:

* **pair** - for every leg, the two sides must agree on the cells named by that leg's grade list. This
  is the parity claim; nothing else in the receipt can substitute for it.
* **family** - each gate must be seen *talking*: one leg where the payload is inside the number and one
  where it is outside, with different verdicts on both sides. Without this, "Go and Rust agreed" could
  just mean "neither side enforced anything".
* **profile expectation** - the harness declares, per profile, what a configured number must do to a
  fixed-size probe (4 KiB body, 4 KiB signal frame). That is what catches a limit that parses but is
  never consumed, and it doubles as the YAML-vs-argv precedence discriminator.
* **privacy** - the receipt must contain no allocated port digit, no fixture secret, no working
  directory, no non-loopback address, and no verbatim fixture payload.
"""

import copy
import time


SCHEMA = 'spec0077-c106-limit-gate-channels'
PARTS = ('part1.py', 'part2.py', 'part3.py', 'part4.py', 'part5.py', 'part6.py', 'part7.py')

# Fixed-size probes that every live profile runs, so the same byte count can be compared across
# profiles whose configured numbers differ. This is what turns "both sides agreed" into
# "both sides obeyed the number we set".
SIG_FIXED = 4096
BODY_FIXED = 4096

# Which legs a legacy `room.*` number also governs when the profile sets the alias instead of the
# `limit.*` key. The probes are identical; only the channel that supplied the number differs, so the
# alias row must be named by the same cells.
ALIAS_LEGS = {
    'max_metadata_size': ('update_room_meta_over', 'update_room_meta_under', 'update_room_meta_exact',
                          'update_room_over_missing_room', 'update_room_over_json_ctype',
                          'update_room_over_no_token', 'update_participant_meta_over',
                          'dispatch_meta_over', 'tok_meta_over', 'tok_meta_under',
                          'upd_meta_over', 'upd_meta_under'),
    'max_room_name_length': ('room_name_over', 'room_name_exact'),
    'max_participant_identity_length': ('tok_identity_over',),
}


def add_alias_rows(legs, aliases: dict) -> None:
    """Name the legacy `room.*` row on every leg its number actually decided."""
    for key, row in (aliases or {}).items():
        named = 0
        for one in legs:
            if one['id'] in ALIAS_LEGS.get(key, ()):
                one['covers'].append(row)
                named += 1
        assert named, f'alias {key} configured but no leg carries it'


# Per profile: what must happen to the two fixed probes. A side that ignores its config fails here,
# not merely in the pair column. The values are read off the pinned Go source, not guessed:
# `NewRequestBodyLimiter` passes through when `maxBytes <= 0` (utils.go:139-143), both websocket
# guards are installed only `> 0` (rtcservice.go:487-491, agentws.go:92-97) and the application guard
# likewise falls through to an unbounded read when the limit is not positive (wsprotocol.go:73-90), so
# a negative number disables a byte gate instead of rejecting everything - unlike the length gates,
# whose `len <= limit` predicate a negative value can never satisfy.
EXPECT = {
    'tight': {'sig_fixed_4k': 'refuse', 'body_fixed_4k': 'refuse'},
    'zeros': {'sig_fixed_4k': 'accept', 'body_fixed_4k': 'accept'},
    'signed-neg': {'sig_fixed_4k': 'accept', 'body_fixed_4k': 'accept'},
    'legacy-alias': {'sig_fixed_4k': 'accept', 'body_fixed_4k': 'accept'},
    'yaml-vs-flag': {'sig_fixed_4k': 'refuse', 'body_fixed_4k': 'refuse'},
    'agent-gt-sig': {'sig_fixed_4k': 'refuse', 'body_fixed_4k': 'refuse'},
    'defaults': {'sig_fixed_4k': 'accept', 'body_fixed_4k': 'accept'},
}


def is_refuse_gate(cell: dict) -> bool:
    """The worker-socket families are graded on `gate_class`, so they must be classified by it too.

    Classifying by `verdict` while grading by `gate_class` lets a leg read as a refusal in one column
    and an acceptance in the other, which is how a family kept passing after its graded cell changed.
    """
    return str(cell.get('gate_class', '')) == 'refused'


def is_accept_gate(cell: dict) -> bool:
    """An accept is an answer. A window that produced nothing is not one, or a daemon that never
    replies would pass the accept half of every worker-socket family."""
    return str(cell.get('gate_class', '')) in ('answered', 'other')


def is_refuse_signal(cell: dict) -> bool:
    """Classify a websocket leg cell as a refusal produced by a size gate.

    Two mechanisms count: a close frame carrying a refusal code, and a stream the daemon ended without
    writing one. The second is graded as a refusal of the message because the session died with it; the
    *way* it died is what the pair column carries, so the distinction is never lost.
    """
    if gate_class(cell.get('verdict', ''), cell.get('rr_reason', '')) == 'refused':
        return True
    return cell.get('close_code') in (1009, 1002, 1008, 1011)


def setup_c106(port: int) -> dict:
    """Create the room the update legs need, so an acceptance is a real acceptance."""
    tok = access_token()
    created = cells_of(twirp(port, 'RoomService', 'CreateRoom', create_room(ROOM, 'c106setup'),
                             token_value=tok))
    listed = cells_of(twirp(port, 'RoomService', 'ListRooms', b'', token_value=tok))
    return {'create_http': created.get('http_code', ''), 'create_code': created.get('code', ''),
            'create_msg': (created.get('msg') or created.get('text') or '')[:80],
            'list_http': listed.get('http_code', ''), 'list_bytes': listed.get('body_bytes', '')}


def run_probe(port: int, S: dict) -> dict:
    """One side, one profile: setup, then every leg. Only cells, never payloads."""
    out = {'setup': setup_c106(port)}
    legs = []
    legs += http_body_legs(port, S)
    # A live session for the RoomService update legs, closed again before the signalling sockets open
    # so each of those legs still owns its own join.
    holder, res = hold_participant(port)
    legs.append(leg('resident_joined',
                    ['API_BEHAVIOR:RoomService.UpdateParticipant.owner_local_limit_error_echo'],
                    {'http': res.get('http', ''), 'join': res.get('join', ''),
                     'refusal': (res.get('refusal') or '')[:120],
                     'held': res.get('held', 0)}, ['http', 'held'],
                    note='positive control for the update legs: with nobody joined every '
                         'UpdateParticipant answer is a 404 and no size leg can be an accept'))
    legs += twirp_limit_legs(port, S)
    if holder is not None:
        held = holder.stop()
        for one in legs:
            if one['id'] == 'resident_joined':
                # `closed_during`/`held_end` say when *this harness's* read window gave up on a socket
                # both sides had already answered, so they are window timing rather than a gate answer -
                # recorded and shown in the mechanism histogram, never paired.
                one['cells'].update({'held_frames': held['frames'], 'held_pongs': held['pongs'],
                                     'held_end': held['ended'],
                                     'closed_during': int(held['ended'] != 'still-open')})
    else:
        for one in legs:
            if one['id'] == 'resident_joined':
                one['cells']['closed_during'] = 1
    legs += signal_legs(port, S)
    legs += agent_legs(port, S)
    body = update_room_metadata(ROOM, 'M' * BODY_FIXED)
    # `clen` is deliberately not graded here: an accepted body echoes a RoomInfo whose protobuf
    # encoding differs by one byte between the two stacks, which is an encoder fact and not a
    # limiter decision. It stays visible in the ungraded mechanism histogram instead.
    legs.append(leg('body_fixed_4k', ['YAML:limit.max_api_request_body_size',
                                      'generated_flags:--limit.max_api_request_body_size',
                                      'HTTP_BEHAVIOR:middleware.body_limit'],
                    cells_of(twirp(port, 'RoomService', 'UpdateRoomMetadata', body,
                                   token_value=access_token())),
                    ['http_code', 'ctype', 'text', 'json_keys']))
    res = signal_session(port, read_limit=1,
                         send=[('f', 'binary', pad_to(signal_ping(), SIG_FIXED))])
    legs.append(leg('sig_fixed_4k', ['YAML:limit.signal_message_size_limit',
                                     'generated_flags:--limit.signal_message_size_limit',
                                     'WIRE:RTC.PerMessageDeflate'],
                    after_cells((res.get('after') or {}).get('f', [])),
                    ['verdict', 'close_code', 'close_reason']))
    for one in legs:
        one['cells'] = {k: v for k, v in one['cells'].items() if v is not None}
    out['legs'] = legs
    return out


RUN_RE = re.compile(r'([A-Za-z0-9])\1{39,}')


def collapse_runs(text: str) -> str:
    """Keep the shape of a long probe run, drop the bulk.

    A daemon log line can echo what we sent it - the `room_name_exact` leg creates a room whose name is
    64 copies of one letter, and the Go room-manager's "closing room" line prints it. Committing that
    verbatim would put fixture payload bytes in the receipt, which is what the privacy sweep exists to
    prevent, so runs are recorded as `rx64` instead.
    """
    return RUN_RE.sub(lambda m: f'{m.group(1)}x{len(m.group(0))}', text)


def mask_cells(masker, obj):
    if isinstance(obj, str):
        return collapse_runs(masker.text(obj))
    if obj is None or isinstance(obj, (bool, int, float)):
        return obj
    if isinstance(obj, list):
        return [mask_cells(masker, v) for v in obj]
    if isinstance(obj, dict):
        return {k: mask_cells(masker, v) for k, v in obj.items()}
    return collapse_runs(masker.text(str(obj)))


def record(P, work: Path, binaries: dict, sides: list, only_profiles: list, hold: float,
           harness_sha: dict, heads: dict) -> dict:
    """Launch every profile on every side, flushing the store before each launch."""
    store = P['STORE']
    doc = {'schema': SCHEMA, 'batch': BATCH, 'recorded_at': time.strftime('%Y-%m-%dT%H:%M:%S%z'),
           'identities': {}, 'harness': harness_sha,
           'ports': {role: f'<{role}>' for role in P['roles'].values()},
           'fixtures': {'room': ROOM, 'identity': IDENT, 'key_id': API_KEY},
           'probe_sizes': {'sig_fixed': SIG_FIXED, 'body_fixed': BODY_FIXED},
           'profiles': [], 'runs': []}
    for side in sides:
        path = Path(binaries[side])
        doc['identities'][side] = {
            'path_alias': '<ORACLE>' if side == 'go' else 'target/release/livecloud',
            'sha256': sha256_file(path) if path.is_file() else '',
            'bytes': path.stat().st_size if path.is_file() else 0}
    if 'go' in doc['identities']:
        doc['identities']['go']['source_commit'] = FIXED_GO_COMMIT
        doc['identities']['go']['reference'] = 'references/livekit-stable'
    doc['identities']['heads'] = heads
    for prof in profiles(P):
        if only_profiles and prof['id'] not in only_profiles:
            continue
        masker = Masker(P['roles'], {str(work): '<WORK>', str(Path.home()): '<HOME>'})
        entry = {'id': prof['id'], 'intent': prof['intent'], 'live': prof['live'],
                 'argv': prof['argv'], 'yaml_masked': masker.text(prof['yaml']),
                 'limit': prof['limit'], 'sizes': prof['sizes'],
                 'aliases': prof.get('aliases') or {}, 'room_yaml': prof['room_yaml'],
                 'expect': EXPECT.get(prof['id'], {}), 'sides': {}}
        for side in sides:
            C102.redis_cli(store, 'FLUSHDB')
            found = {}

            def on_alive(proc, cfg_path):
                found.update(run_probe(P['HTTP'], prof['sizes'] or {}))
                add_alias_rows(found.get('legs', []), prof.get('aliases') or {})
                return {'probe': found}

            rec = launch_pair(Path(binaries[side]), prof['argv'], {}, prof['yaml'], work,
                              f"c106-{prof['id']}", side, masker, ready_port=P['HTTP'], hold=hold,
                              refuse_wait=None if prof['live'] else 2.0, on_alive=on_alive)
            probe = rec['observed'].get('probe', {})
            legs = mask_cells(masker, probe.get('legs', []))
            if not prof['live']:
                legs = [startup_leg(rec)]
            entry['sides'][side] = {
                'ready': rec['ready'], 'alive': rec['alive_at_probe'], 'exit': rec['exit'],
                'self_exited': rec['self_exited'], 'exit_class': exit_class(rec),
                'ready_ms': rec['observed'].get('ready_ms'), 'sockets_masked': rec['sockets_masked'],
                'log_lines': rec['log_lines'], 'raw': rec['raw'], 'cfg_masked': rec['cfg_masked'],
                'legs': legs, 'setup': mask_cells(masker, probe.get('setup', {})),
                'log_tail': [collapse_runs(masker.text(ln)) for ln in rec['log_tail_masked']]}
            doc['runs'].append({'profile': prof['id'], 'side': side, 'ready': rec['ready'],
                                'exit': rec['exit'], 'signals': rec['signals'],
                                'log_sha256': rec['raw']['stdout_sha256']})
        entry['store_dbsize_after'] = C102.dbsize(store)
        doc['profiles'].append(entry)
    return doc


def exit_class(rec: dict) -> str:
    """Coarse, comparable label for how the run ended."""
    if rec.get('self_exited') and rec.get('exit') not in (0, None):
        return 'refused-to-start'
    if rec.get('ready'):
        return 'ran-and-was-stopped'
    return 'other'


def startup_leg(rec: dict) -> dict:
    """For profiles whose whole question is whether the daemon comes up at all."""
    log = rec.get('log_tail_masked') or []
    errs = [ln for ln in log if re.search(r'error|invalid|cannot|fail|panic', ln, re.I)]
    mention = [ln for ln in errs if 'metadata' in ln.lower() or 'u32' in ln or 'unsigned' in ln.lower()]
    return leg('startup_refusal', ['YAML:limit.max_metadata_size', 'YAML:limit.max_attributes_size'],
               {'ready': int(bool(rec.get('ready'))), 'alive': int(bool(rec.get('alive_at_probe'))),
                'self_exited': int(bool(rec.get('self_exited'))),
                'exit_class': exit_class(rec),
                'error_lines': len(errs), 'mentions_key': int(bool(mention))},
               ['ready', 'alive', 'self_exited', 'exit_class'],
               note='a negative value in an unsigned YAML field has to be answered the same way by '
                    'both implementations - the error text itself is not the contract')


# ------------------------------------------------------------------------------------ grading
def leg_map(profile) -> dict:
    out = {}
    for side, rec in profile['sides'].items():
        out[side] = {one['id']: one for one in rec.get('legs', [])}
    return out


def grade_leg(per_side: dict, keys: list) -> dict:
    if 'go' not in per_side or 'rust' not in per_side:
        return {'pair': 'ONE_SIDED', 'diff': {}}
    if not keys:
        # a leg with nothing to compare cannot prove agreement
        return {'pair': 'UNGRADED', 'diff': {}}
    diff = {}
    for k in keys:
        a = json.dumps((per_side['go'].get('cells') or {}).get(k), sort_keys=True, default=str)
        b = json.dumps((per_side['rust'].get('cells') or {}).get(k), sort_keys=True, default=str)
        if a != b:
            diff[k] = [(per_side['go'].get('cells') or {}).get(k),
                       (per_side['rust'].get('cells') or {}).get(k)]
    return {'pair': 'MATCH' if not diff else 'DIFF', 'diff': diff}


FAMILIES = [
    # (id, row, leg that must exist for the family to apply, accepts, refuses, is_refuse, is_accept,
    #  limit keys that must be configured positive, legs whose accept needs a joined session)
    #
    # The last field names what the accept legs of that family need to exist at all: a room ('room'), a
    # joined participant ('session'), or nothing ('none'). Those preconditions are read back from the
    # recorded setup/join cells, not assumed, because two profiles deliberately remove them - a gate set
    # to 0 or to a negative byte size has no boundary to sit on, and a negative *length* gate refuses
    # every join and every CreateRoom, so no accept leg could ever accept. Grading that as a failure
    # would report the oracle as wrong; it is recorded as not applicable, and the pair column still
    # carries what each side did.
    ('body_gate_talks', ['YAML:limit.max_api_request_body_size',
                      'generated_flags:--limit.max_api_request_body_size'], 'body_over',
     ['body_under', 'body_exact', 'body_large_ok'], ['body_over'],
     lambda c: str(c.get('http_code')) == '413', lambda c: str(c.get('http_code')) != '413',
     ['max_api_request_body_size'], 'none'),
    ('metadata_gate_talks', ['YAML:limit.max_metadata_size',
                         'generated_flags:--limit.max_metadata_size'], 'update_room_meta_over',
     ['update_room_meta_under'], ['update_room_meta_over'],
     lambda c: 'metadata size exceeds limits' in str(c.get('msg', '')),
     lambda c: str(c.get('http_code')) == '200', ['max_metadata_size'], 'room'),
    ('attributes_gate_talks', 'YAML:limit.max_attributes_size', 'update_participant_attrs_over',
     ['update_participant_attrs_under'], ['update_participant_attrs_over'],
     lambda c: 'attribute size exceeds limits' in str(c.get('msg', '')),
     lambda c: str(c.get('http_code')) == '200', ['max_attributes_size'], 'session'),
    ('room_name_gate_talks', 'YAML:limit.max_room_name_length', 'room_name_over',
     ['room_name_exact'], ['room_name_over'],
     lambda c: 'room name length exceeds limits' in str(c.get('msg', '')),
     lambda c: str(c.get('http_code')) == '200', ['max_room_name_length'], 'none'),
    ('participant_name_gate_talks', 'YAML:limit.max_participant_name_length',
     'update_participant_name_over',
     ['update_participant_name_exact'], ['update_participant_name_over'],
     lambda c: 'name length exceeds limits' in str(c.get('msg', '')),
     lambda c: str(c.get('http_code')) == '200', ['max_participant_name_length'], 'session'),
    # sig_frame_over is an *accept* here: gorilla counts payload bytes, so a frame one byte past the
    # bound whose payload is inside it must pass (conn.go:917-926). A side that refuses it bounds the
    # frame, not the message, and the pair column records that as a diff.
    ('signal_gate_talks', ['YAML:limit.signal_message_size_limit',
                       'generated_flags:--limit.signal_message_size_limit'], 'sig_payload_over',
     ['sig_frame_under', 'sig_frame_at', 'sig_frame_over', 'sig_payload_at', 'sig_large'],
     ['sig_payload_over', 'sig_huge', 'sig_fragmented_over'],
     lambda c: is_refuse_signal(c), lambda c: not is_refuse_signal(c),
     ['signal_message_size_limit'], 'session'),
    # Only the *registration* read is claimed to sit on the agent bound here: the mid-size leg is
    # graded separately, because which limit the live worker socket obeys afterwards is its own
    # question (see the agent_mid_sits_on_one_bound control).
    ('agent_gate_talks', ['YAML:limit.agent_signal_message_size_limit',
                      'generated_flags:--limit.agent_signal_message_size_limit'],
     'agent_reg_payload_over',
     ['agent_reg_ok', 'agent_reg_frame_at', 'agent_reg_frame_over', 'agent_reg_payload_at',
      'agent_reg_large'],
     ['agent_reg_payload_over', 'agent_huge_after_ok'],
     is_refuse_gate, is_accept_gate,
     ['agent_signal_message_size_limit'], 'none'),
    ('claim_gate_talks', 'join-time claim limits (roommanager.go:296-303)', 'tok_meta_over',
     ['tok_meta_under'], ['tok_meta_over'],
     lambda c: c.get('http') != '101' or c.get('closed_after_join') == 1,
     lambda c: c.get('http') == '101' and not c.get('closed_after_join'),
     ['max_metadata_size'], 'session'),
    # The identity gate has no update-path leg at all: what carries it is the join claim. The accept is a
    # join that really upgrades, so a side that refuses every join fails the accept half and a side that
    # never gates identity fails the refuse half. `zeros`/`signed-neg` take the key out of the positive
    # list below, which is where those profiles are recorded as having no boundary instead of failing.
    ('identity_gate_talks', 'YAML:limit.max_participant_identity_length', 'tok_identity_over',
     ['sig_join_ok', 'tok_meta_under'], ['tok_identity_over'],
     lambda c: c.get('http') != '101' or c.get('closed_after_join') == 1,
     lambda c: c.get('http') == '101' and not c.get('closed_after_join'),
     ['max_participant_identity_length'], 'none'),
    ('update_metadata_reports', 'signal-path participant limits (participant.go:740-752)',
     'upd_meta_over',
     ['upd_meta_under'], ['upd_meta_over'],
     lambda c: c.get('rr_reason') == 'LIMIT_EXCEEDED',
     lambda c: c.get('rr_reason') != 'LIMIT_EXCEEDED', ['max_metadata_size'], 'session'),
    ('ordering_map_distinct', 'which guard answers first', 'update_room_over_bad_token',
     [], ['update_room_over_no_token', 'update_room_over_bad_token'], None, None,
     ['max_metadata_size'], 'none'),
]


def classify(cell: dict, leg_id: str) -> str:
    if leg_id.startswith('sig_'):
        return 'refuse' if is_refuse_signal(cell) else 'accept'
    return 'refuse' if str(cell.get('http_code')) == '413' else 'accept'


def check_expect(cells: dict, expect: dict) -> list:
    """Grade a leg against the claim the harness recorded when it built it.

    Every `body_*` leg carries e.g. `{'http_code': '413'}` from the profile's own configured number.
    Before this the claim was written into the receipt and then never read, so a leg could record a
    refusal that matched nothing and still count as a pair match - the two sides agreeing to be wrong.
    """
    fails = []
    for key, want in (expect or {}).items():
        neg = key.startswith('not_')
        name = key[4:] if neg else key
        got = cells.get(name)
        hit = str(got) == str(want)
        if neg == hit:
            fails.append([key, want, got])
    return fails


# What each implementation's own wire says, judged one side at a time. These are the claims a receipt
# cannot satisfy by copying the oracle: Go's HTTP stack canonicalises header names and hyper lowercases
# them, so a rust column wearing go's cells fails the rust claim even though the pair column goes green.
SIDE_CLAIMS = [
    ('http_header_casing', 'hdr_case', {'go': {'canonical'}, 'rust': {'lower'}}),
]


def grade(doc: dict) -> dict:
    """Recompute every verdict from cells; stored verdicts are never read."""
    out = {'profiles': [], 'rows': {}, 'rows_unexercised': [], 'gates': [], 'controls': [],
           'expectations': []}
    for prof in doc['profiles']:
        pmap = leg_map(prof)
        sides = [s for s in ('go', 'rust') if s in pmap]
        entry = {'id': prof['id'], 'live': prof['live'], 'legs': [], 'families': {}}
        # Which sides actually hold a joined participant: several accept legs are only meaningful if a
        # session exists to answer them, and a profile that poisons the join is not a gate failure.
        session_ok = {s: str(((pmap.get(s) or {}).get('resident_joined') or {})
                             .get('cells', {}).get('http', '')) == '101' for s in sides}
        # Did the room the update legs write to actually exist? Read from the setup call's own answer.
        room_ok = all(str(((prof['sides'].get(s) or {}).get('setup') or {}).get('create_http')) == '200'
                      for s in sides)
        by_id = {}
        for side in sides:
            for one in pmap[side].values():
                by_id.setdefault(one['id'], {})[side] = one
        for leg_id, per_side in sorted(by_id.items()):
            keys, covers = [], []
            for side in sides:
                keys = per_side[side].get('grade') or keys
                covers = per_side[side].get('covers') or covers
            res = grade_leg(per_side, keys)
            other = {}
            for k in sorted(set((per_side['go'].get('cells') or {})) &
                            set((per_side['rust'].get('cells') or {})) - set(keys)):
                a_v, b_v = per_side['go']['cells'][k], per_side['rust']['cells'][k]
                if json.dumps(a_v, sort_keys=True, default=str) != json.dumps(b_v, sort_keys=True,
                                                                              default=str):
                    other[k] = [a_v, b_v]
            entry['legs'].append({'id': leg_id, 'pair': res['pair'], 'diff': res['diff'],
                                  'grade_keys': keys, 'covers': covers, 'other': other,
                                  'cells': {s: per_side[s].get('cells') for s in sides}})
            for side in sides:
                exp = per_side[side].get('expect') or {}
                if not exp:
                    continue
                fails = check_expect(per_side[side].get('cells') or {}, exp)
                out['expectations'].append({'profile': prof['id'], 'leg': leg_id, 'side': side,
                                           'want': exp, 'got': 'as claimed' if not fails else fails,
                                           'pass': not fails, 'kind': 'leg'})
        for (gid, rows, need, accept_ids, refuse_ids, is_ref, is_acc, positive_keys,
             precondition) in FAMILIES:
            if not prof['live']:
                continue
            lim = prof.get('limit') or {}
            flat = [k for k in positive_keys if isinstance(lim.get(k), int) and lim[k] <= 0]
            if flat:
                entry['families'][gid] = {
                    'rows': rows, 'applicable': False,
                    'reason': 'no boundary to sit on: ' + ','.join(f'{k}={lim[k]}' for k in flat)}
                continue
            sides_with_need = [s for s in sides if need in pmap.get(s, {})]
            if not sides_with_need:
                # The gate has no boundary to sit on in this profile (limit 0 or negative disables it),
                # so "the gate talked" is not a question this launch can answer. Recorded as not
                # applicable rather than a pass, and never counted toward a row's gate.
                entry['families'][gid] = {'rows': rows, 'applicable': False,
                                          'reason': f'no {need} leg: gate not positive here'}
                continue
            if precondition == 'session' and not all(session_ok.get(s) for s in sides_with_need):
                dead = ','.join(s for s in sides_with_need if not session_ok.get(s))
                entry['families'][gid] = {'rows': rows, 'applicable': False,
                                          'reason': f'no joined session on {dead}: an accept leg '
                                                    f'needs a live participant to answer'}
                continue
            if precondition == 'room' and not room_ok:
                entry['families'][gid] = {'rows': rows, 'applicable': False,
                                          'reason': 'the setup CreateRoom was refused, so no update '
                                                    'leg can be an acceptance'}
                continue
            info = {}
            for side in sides:
                legs = pmap.get(side, {})
                if gid == 'ordering_map_distinct':
                    a = (legs.get('update_room_over_no_token') or {}).get('cells', {})
                    b = (legs.get('update_room_over_bad_token') or {}).get('cells', {})
                    info[side] = {'pass': bool(a) and bool(b)
                                  and str(a.get('http_code')) != str(b.get('http_code')),
                                  'detail': f"missing-token={a.get('http_code')} "
                                            f"bad-token={b.get('http_code')}"}
                    continue
                ref_cells = [legs[i]['cells'] for i in refuse_ids if i in legs]
                acc_cells = [legs[i]['cells'] for i in accept_ids if i in legs]
                if not ref_cells or not acc_cells:
                    info[side] = {'pass': False, 'detail': 'legs absent'}
                    continue
                # every present accept must accept and every present refuse must refuse: a side that
                # yields at the wrong byte count fails the family even though it "spoke"
                present_ref = [(i, legs[i]['cells']) for i in refuse_ids if i in legs]
                present_acc = [(i, legs[i]['cells']) for i in accept_ids if i in legs]
                if not present_ref or not present_acc:
                    info[side] = {'pass': False, 'detail': 'legs absent'}
                    continue
                bad_ref = [i for i, c in present_ref if not is_ref(c)]
                bad_acc = [i for i, c in present_acc if not is_acc(c)]
                info[side] = {'pass': not bad_ref and not bad_acc,
                              'detail': f'refused={len(present_ref) - len(bad_ref)}'
                                        f'/{len(present_ref)} '
                                        f'accepted={len(present_acc) - len(bad_acc)}'
                                        f'/{len(present_acc)} '
                                        f'wrong={",".join(sorted(bad_ref + bad_acc)) or "-"}'}
            both = bool(info) and all(v['pass'] for v in info.values())
            entry['families'][gid] = {'rows': rows, 'applicable': True, 'pass': both, 'sides': info}
            out['gates'].append({'profile': prof['id'], 'gate': gid, 'rows': rows,
                                 'pass': both, 'sides': info})
        for leg_id, want in (prof.get('expect') or {}).items():
            for side in sides:
                cell = (pmap.get(side, {}).get(leg_id) or {}).get('cells', {})
                if not cell:
                    out['expectations'].append({'profile': prof['id'], 'leg': leg_id, 'side': side,
                                                'want': want, 'got': 'absent', 'pass': False,
                                                'kind': 'probe'})
                    continue
                got = classify(cell, leg_id)
                out['expectations'].append({'profile': prof['id'], 'leg': leg_id, 'side': side,
                                           'want': want, 'got': got, 'pass': got == want,
                                           'kind': 'probe',
                                           'cell': {k: cell.get(k) for k in
                                                    ('http_code', 'verdict', 'close_code')}})
        out['profiles'].append(entry)
    for cid, key, want in SIDE_CLAIMS:
        seen = {s: set() for s in ('go', 'rust')}
        for prof in doc['profiles']:
            if not prof['live']:
                continue
            for side, rec in prof['sides'].items():
                for one in rec.get('legs', []):
                    val = (one.get('cells') or {}).get(key)
                    if val:
                        seen[side].add(str(val))
        for side, want_set in want.items():
            out['expectations'].append({
                'profile': 'all', 'leg': cid, 'side': side, 'want': sorted(want_set),
                'got': sorted(seen[side]) or 'never observed',
                'pass': bool(seen[side]) and seen[side] == want_set, 'kind': 'side'})
    # cross-profile: the fixed probe must obey the configured number and the two sides must agree
    for leg_id in ('sig_fixed_4k', 'body_fixed_4k'):
        per = {}
        for entry in out['profiles']:
            one = {l['id']: l for l in entry['legs']}.get(leg_id)
            if one:
                per[entry['id']] = {'pair': one['pair'],
                                    'class': {s: classify(c, leg_id)
                                              for s, c in (one['cells'] or {}).items()}}
        agrees = all(v['pair'] == 'MATCH' for v in per.values())
        flips = ('tight' in per and 'zeros' in per
                 and per['tight']['class'] != per['zeros']['class'])
        out['controls'].append({'id': f'{leg_id}_follows_the_configured_number',
                                'profiles': per, 'sides_agree': agrees, 'flips_across_profiles': flips,
                                'pass': agrees and flips})
    # Which bound the live worker socket obeys after registration. The profiles deliberately put the
    # mid-size message above one number and below the other in opposite orders, so exactly one of the
    # two hypotheses can explain every observation; if both fit, the data does not discriminate, and
    # if neither does, the bound is something else again. Both readings are recorded, not averaged.
    mid = {}
    for entry in out['profiles']:
        one = {l['id']: l for l in entry['legs']}.get('agent_mid_after_ok')
        prof = next((x for x in doc['profiles'] if x['id'] == entry['id']), None)
        sz = (prof or {}).get('sizes') or {}
        if not one or not sz.get('agent') or not sz.get('sig') or not sz.get('agent_mid'):
            continue
        mid[entry['id']] = {'agent': sz['agent'], 'sig': sz['sig'], 'mid': sz['agent_mid'],
                            'above_agent': sz['agent_mid'] > sz['agent'],
                            'above_sig': sz['agent_mid'] > sz['sig'],
                            'class': {s: (one['cells'] or {}).get(s, {}).get('gate_class')
                                      for s in ('go', 'rust')}}

    def predicts(key):
        for v in mid.values():
            want = 'refused' if v['mid'] > v[key] else 'answered'
            if any(c != want for c in v['class'].values()):
                return False
        return True

    orders = {(v['above_agent'], v['above_sig']) for v in mid.values()}
    # A profile whose two bounds are equal cannot tell the hypotheses apart, so the requirement is that
    # both discriminating orders appear, not that every recorded profile discriminates.
    discriminating = {(True, False), (False, True)}.issubset(orders)
    bound = ('signal_message_size_limit' if predicts('sig') and not predicts('agent')
             else 'agent_signal_message_size_limit' if predicts('agent') and not predicts('sig')
             else 'undetermined')
    out['controls'].append({
        'id': 'agent_mid_sits_on_one_bound', 'profiles': mid,
        'both_orders_observed': discriminating,
        'obeys': bound,
        'pass': len(mid) >= 2 and discriminating and bound != 'undetermined'})
    argv_by_prof = {x.get('id'): (x.get('argv') or []) for x in doc['profiles']}

    def channel_present(row, pid):
        """Was this number actually delivered through that channel in that launch?

        A `generated_flags:` row may only be named by a profile that put the flag on argv. Otherwise the
        YAML value would be credited to the flag channel, and a flag the daemon never parsed would read
        as proven because a config file said the same thing.
        """
        if not row.startswith('generated_flags:'):
            return True
        flag = row.split(':', 1)[1]
        return any(a == flag or a.startswith(flag + '=') for a in argv_by_prof.get(pid, []))

    for entry in out['profiles']:
        for leg in entry['legs']:
            for row in leg['covers']:
                if not channel_present(row, entry['id']):
                    pair = (row, entry['id'], leg['id'])
                    if pair not in out['rows_unexercised']:
                        out['rows_unexercised'].append(pair)
                    continue
                agg = out['rows'].setdefault(row, {'legs': set(), 'profiles': set(), 'diffs': []})
                agg['legs'].add(leg['id'])
                agg['profiles'].add(entry['id'])
                if leg['pair'] != 'MATCH':
                    agg['diffs'].append({'profile': entry['id'], 'leg': leg['id'],
                                         'pair': leg['pair'], 'diff': leg['diff']})
    gates_by_row = {}
    for g in out['gates']:
        for row in ([g['rows']] if isinstance(g['rows'], str) else g['rows']):
            if channel_present(row, g['profile']):
                gates_by_row.setdefault(row, []).append(g)
    for row, agg in out['rows'].items():
        agg['legs'] = sorted(agg['legs'])
        agg['profiles'] = sorted(agg['profiles'])
        mine = [g for g in gates_by_row.get(row, []) if g['pass']]
        agg['gates'] = [{'profile': g['profile'], 'gate': g['gate'], 'pass': g['pass']}
                        for g in gates_by_row.get(row, [])]
        agg['pair_pass'] = not agg['diffs']
        agg['gate_pass'] = bool(mine) if row.startswith(('YAML:limit', 'generated_flags')) else None
        agg['pass'] = agg['pair_pass'] and bool(agg['legs']) and (
            agg['gate_pass'] is not False)
    return out


def privacy_sweep(doc: dict, P: dict, work: Path) -> list:
    """Return violation strings; an empty list means the receipt is safe to commit."""
    text = json.dumps(doc, ensure_ascii=False, sort_keys=True)
    found = []
    for port, role in P['roles'].items():
        if re.search(rf'(?<![0-9<>]){port}(?![0-9])', text):
            found.append(f'raw allocated port {port} (role {role}) leaked into the receipt')
    for pat in (str(work), API_SECRET, STORE_PASS, str(Path.home())):
        if pat and pat in text:
            found.append(f'literal path/secret {pat[:8]}... present')
    for match in set(re.findall(r'\b(?:\d{1,3}\.){3}\d{1,3}\b', text)):
        if match not in ('127.0.0.1', '0.0.0.0'):
            found.append(f'non-loopback address {match}')
    if re.search(r'\bfe80::', text):
        found.append('link-local IPv6 address present')
    for blob in set(m.group(0) for m in re.finditer(r'([A-Za-z])\1{60,}', text)):
        found.append(f'verbatim fixture payload run of {len(blob)} chars')
    return found



# ---- part7.py ----
#!/usr/bin/env python3
"""C106 limit-gate channels - part 7: CLI, report and the falsification harness.

    --record            launch every profile on both sides, write receipt.json
    --report FILE       recompute all verdicts from the stored cells and print them
    --selftest-gate F   mutate named cells in a copy and require each named target to turn red
    --only a,b          grade a subset (never an acceptance)
"""


LANE = 'tests/interop/limit-gate-channels'


def harness_hashes() -> dict:
    out = {}
    for name in PARTS:
        path = HERE / name
        out[name] = sha256_file(path) if path.is_file() else 'MISSING'
    for label, rel in (('c102', 'base-flag-channels/run-paired.py'),
                       ('c103', 'generated-flag-channels/run-paired.py'),
                       ('c104', 'turn-material-channels/run-paired.py'),
                       ('c105', 'turn-credential-join/run-paired.py')):
        path = INTEROP / rel
        out[label] = sha256_file(path) if path.is_file() else 'MISSING'
    return out


def check_identities(binaries: dict, sides: list) -> list:
    problems = []
    want = {'go': ORACLE_SHA256, 'rust': ACCEPTANCE_SHA256}
    for side in sides:
        path = Path(binaries[side])
        if not path.is_file():
            problems.append(f'{side} binary missing: {path} (rebuild it before recording)')
            continue
        got = sha256_file(path)
        if got != want[side]:
            problems.append(f'{side} sha256 {got[:12]} != pinned {want[side][:12]}')
    if REFERENCE.is_dir():
        head = git_output(REFERENCE, 'rev-parse', 'HEAD')
        if head and head != FIXED_GO_COMMIT:
            problems.append(f'reference checkout {head[:12]} != pinned {FIXED_GO_COMMIT[:12]}')
    else:
        problems.append('references/livekit-stable missing')
    return problems


def git_output(cwd: Path, *args) -> str:
    try:
        return subprocess.run(['git', '-C', str(cwd), *args], capture_output=True, text=True,
                              timeout=20).stdout.strip()
    except Exception:  # noqa: BLE001
        return ''


def third_party_modules() -> list:
    """The lane must run on the stdlib alone: a stray import is a provenance hole."""
    bad = []
    for name in PARTS:
        path = HERE / name
        if not path.is_file():
            continue
        for line in path.read_text(encoding='utf-8').splitlines():
            m = re.match(r'\s*(?:import|from)\s+([A-Za-z_][\w.]*)', line)
            if not m:
                continue
            top = m.group(1).split('.')[0]
            if top in {'part1', 'part2', 'part3', 'part4', 'part5', 'part6', '__future__'}:
                continue
            if top not in sys.stdlib_module_names:
                bad.append(f'{name}: {top}')
    return bad


# ---------------------------------------------------------------------------------- printing
def w(text=''):
    print(text)


def report(doc: dict, args, P=None, work=None) -> int:
    graded = grade(doc)
    subset = bool(args.only or getattr(args, 'profiles', ''))
    w(f'# {BATCH} receipt {doc.get("schema")} recorded {doc.get("recorded_at")}')
    for side in ('go', 'rust'):
        ident = doc['identities'].get(side, {})
        w(f'- {side}: {ident.get("path_alias")} sha256={str(ident.get("sha256"))[:16]}... '
          f'bytes={ident.get("bytes")}')
    w(f'- oracle commit {doc["identities"].get("go", {}).get("source_commit", "")[:12]}  '
      f'heads {doc["identities"].get("heads", {})}')
    w(f'- harness {json.dumps(doc.get("harness", {}), sort_keys=True)[:220]}')
    total_legs = sum(len(p['legs']) for p in graded['profiles'])
    w(f'- profiles {[p["id"] for p in graded["profiles"]]}  graded legs {total_legs}')
    w('')
    diffs = []
    for entry in graded['profiles']:
        prof = next(p for p in doc['profiles'] if p['id'] == entry['id'])
        n_legs = len(entry['legs'])
        w(f'## profile {entry["id"]} (live={entry["live"]}, legs/side={n_legs}, '
          f'limits={json.dumps(prof["limit"], sort_keys=True)})')
        for one in entry['legs']:
            if one['pair'] != 'MATCH':
                diffs.append((entry['id'], one))
                cells = one['cells']
                keys = ', '.join(f"{k}: go={json.dumps((cells.get('go') or {}).get(k))} "
                                 f"rust={json.dumps((cells.get('rust') or {}).get(k))}"
                                 for k in one['grade_keys'] if k in one['diff'])
                w(f'  DIFF {one["id"]} keys={one["grade_keys"]} {keys}')
        if not n_legs:
            w('  NO LEGS RECORDED')
        for gid, fam in sorted(entry['families'].items()):
            if not fam.get('applicable', True):
                w(f'  n/a  family {gid} ({fam["rows"]}) {fam.get("reason", "")}')
                continue
            mark = 'ok  ' if fam['pass'] else 'FAIL'
            det = ' '.join(f"{s}:{v['detail']}" for s, v in sorted(fam['sides'].items()))
            w(f'  {mark} family {gid} ({fam["rows"]}) {det}')
        for one in graded['expectations']:
            if one['profile'] != entry['id']:
                continue
            mark = 'ok  ' if one['pass'] else 'FAIL'
            w(f'  {mark} expect[{one.get("kind", "probe")}] {one["leg"]} {one["side"]} '
              f'want={one["want"]} got={one["got"]} '
              f'{json.dumps(one.get("cell", {}), sort_keys=True)}')
        w('')
    all_side = [e for e in graded['expectations'] if e.get('kind') == 'side']
    if all_side:
        w('## per-side claims (not pairable)')
        for one in all_side:
            w(f'  {"ok  " if one["pass"] else "FAIL"} {one["leg"]} {one["side"]}: '
              f'want={one["want"]} got={one["got"]}')
        w('')
    ungraded = {}
    for entry in graded['profiles']:
        for one in entry['legs']:
            for k, pair in (one.get('other') or {}).items():
                ungraded.setdefault(k, {'legs': 0, 'values': []})
                ungraded[k]['legs'] += 1
                gone = json.dumps(pair[0], ensure_ascii=False)[:60]
                rgot = json.dumps(pair[1], ensure_ascii=False)[:60]
                if [gone, rgot] not in ungraded[k]['values']:
                    ungraded[k]['values'].append([gone, rgot])
    w('## ungraded keys that still differ between the sides')
    w('  (mechanism, not gate outcome: these cells stay out of the pair column on purpose, and the '
      'counts below are the record that they were measured)')
    for k in sorted(ungraded):
        one = ungraded[k]
        pairs = '; '.join(f'go={a} rust={b}' for a, b in one['values'][:6])
        w(f'  {k}: {one["legs"]} legs -> {pairs}')
    if not ungraded:
        w('  none')
    w('')
    w('## controls')
    for c in graded['controls']:
        extra = {k: v for k, v in c.items() if k not in ('id', 'profiles', 'pass')}
        w(f'  {"ok  " if c["pass"] else "FAIL"} {c["id"]}: {json.dumps(extra, sort_keys=True)[:200]}')
        w(f'       {json.dumps(c["profiles"], sort_keys=True)[:400]}')
    w('')
    if graded.get('rows_unexercised'):
        w('## flag channels excluded from profiles that did not carry the flag')
        w('  (a `generated_flags:` row is named only by a profile whose argv put that flag on the '
          'command line; the same legs still prove the YAML channel, so the exclusion is a scoping '
          'fact about the row, not a gap in the pair column)')
        by_row = {}
        for row, pid, leg_id in graded['rows_unexercised']:
            by_row.setdefault(row, []).append((pid, leg_id))
        for row in sorted(by_row):
            held = (graded['rows'].get(row) or {}).get('profiles') or []
            skipped = sorted({pid for pid, _ in by_row[row]})
            first = f'{by_row[row][0][0]}/{by_row[row][0][1]}'
            if held:
                w(f'  {row}: argv carried it in {len(held)} profile(s) {held}; '
                  f'{len(by_row[row])} legs in the other {len(skipped)} profile(s) excluded '
                  f'(first excluded {first})')
            else:
                w(f'  {row}: no profile put that flag on argv (first unseen leg {first})')
        w('')
    w('## rows')
    row_fail = []
    for row, agg in sorted(graded['rows'].items()):
        mark = 'PASS' if agg['pass'] else 'NOT-PASS'
        if not agg['pass']:
            row_fail.append(row)
        w(f'  {mark} {row} profiles={agg["profiles"]}')
        if agg['diffs']:
            for d in agg['diffs']:
                w(f'      diff in {d["profile"]}/{d["leg"]}: '
                  f'{json.dumps(d["diff"], sort_keys=True)[:400]}')
        if agg.get('gates'):
            w(f'      gates: {json.dumps(agg["gates"], sort_keys=True)}')
    w('')
    privacy = []
    if P and work:
        privacy = privacy_sweep(doc, P, work)
    fam_all = [f for e in graded['profiles'] for f in e['families'].values() if f.get('applicable', True)]
    w(f'- graded legs {total_legs}, pair diffs {len(diffs)}, '
      f'families {sum(1 for f in fam_all if f["pass"])}/{len(fam_all)} pass, '
      f'expectations {sum(1 for e in graded["expectations"] if e["pass"])}/'
      f'{len(graded["expectations"])} pass, rows {len(graded["rows"])} '
      f'({len(row_fail)} not passing), privacy violations {len(privacy)}')
    if privacy:
        for v in privacy:
            w(f'  PRIVACY {v}')
    exits = [(r['profile'], r['side'], r['ready'], r['exit']) for r in doc.get('runs', [])]
    w(f'- launches {exits}')
    live = [p for p in graded['profiles'] if p['live']]
    fam_all_ok = all(f['pass'] for e in graded['profiles']
                     for f in e['families'].values() if f.get('applicable', True))
    accepted = (not diffs and not row_fail and not privacy and fam_all_ok
                and all(c['pass'] for c in graded['controls'])
                and all(e['pass'] for e in graded['expectations'])
                and len(live) == 7 and total_legs >= 20 * len(live))
    if subset:
        w('VERDICT: PARTIAL_SUBSET_NOT_ACCEPTANCE')
        return 0
    w(f'VERDICT: {"ACCEPTED" if accepted else "NOT_ACCEPTED"}')
    return 0 if accepted else 1


# --------------------------------------------------------------------------- falsification
def mutate(doc: dict, name: str) -> str:
    """Apply one named corruption to the receipt copy; return what it touched."""
    prof = {p['id']: p for p in doc['profiles']}
    if name == 'body_over_code':
        for side in ('go', 'rust'):
            for one in prof['tight']['sides'][side]['legs']:
                if one['id'] == 'body_over':
                    one['cells']['http_code'] = '200'
        return 'tight/body_over http_code := 200'
    if name == 'metadata_msg_neutral':
        for side in ('go', 'rust'):
            for one in prof['tight']['sides'][side]['legs']:
                if one['id'] == 'update_room_meta_over':
                    one['cells']['msg'] = 'requested room does not exist'
        return 'tight/update_room_meta_over msg := a text the gate never prints'
    if name == 'signal_close_code':
        for one in prof['tight']['sides']['rust']['legs']:
            if one['id'] == 'sig_payload_over':
                one['cells']['close_code'] = 1000
        return 'tight/rust sig_payload_over close_code := 1000'
    if name == 'signal_frame_counting':
        for one in prof['tight']['sides']['rust']['legs']:
            if one['id'] in ('sig_frame_over', 'sig_payload_at'):
                one['cells'] = {'verdict': 'close:1009', 'close_code': 1009, 'close_reason': '',
                                'payload': one['cells'].get('payload'),
                                'frame': one['cells'].get('frame'), 'msgs': 'close:1009'}
        return 'tight/rust: header-counting guard (sig_frame_over and sig_payload_at now refused)'
    if name == 'zeros_body_refuses':
        for one in prof['zeros']['sides']['go']['legs']:
            if one['id'] == 'body_fixed_4k':
                one['cells']['http_code'] = '413'
                one['cells']['text'] = 'request body too large'
        return 'zeros/go body_fixed_4k http_code := 413'
    if name == 'drop_agent_legs':
        for side in ('go', 'rust'):
            prof['tight']['sides'][side]['legs'] = [
                one for one in prof['tight']['sides'][side]['legs']
                if not one['id'].startswith('agent_')]
        return 'tight: every agent_* leg removed'
    if name == 'leak_port':
        prof['tight']['sides']['rust']['legs'][0]['cells']['note_port'] = str(P_LEAK_PORT)
        return 'tight/rust: an unmasked allocated port written into a cell'
    if name == 'startup_agrees_with_rust':
        for one in prof['unsigned-neg']['sides']['rust']['legs']:
            one['cells'] = dict(one['cells'], ready=1, alive=1, self_exited=0,
                                exit_class='ran-and-was-stopped')
        return 'unsigned-neg/rust startup cells := as if the config loaded'
    if name == 'wipe_legs':
        for p in doc['profiles']:
            for side in ('go', 'rust'):
                p['sides'][side]['legs'] = []
        return 'every profile: legs := []'
    if name == 'flips_every_accept':
        for prof in doc['profiles']:
            for side in ('go', 'rust'):
                for one in prof['sides'][side]['legs']:
                    cells = one['cells']
                    if 'http_code' in cells:
                        cells['http_code'] = '413'
                    if 'verdict' in cells:
                        cells['verdict'] = 'close:1009'
                        cells['close_code'] = 1009
                        cells['rr_reason'] = 'LIMIT_EXCEEDED'
                    if cells.get('gate_class') in ('answered', 'other'):
                        # the graded cell for the worker socket is the gate outcome, so the corruption
                        # has to reach it and not only the mechanism text the pair column no longer
                        # compares
                        cells['gate_class'] = 'refused'
                        cells['stream_end'] = 'frame-seen'
                    if 'http' in cells:
                        cells['http'] = '403'
                        cells['closed_after_join'] = 1
                    if cells.get('reg_ok') is not None:
                        cells['reg_ok'] = 0
        return 'every side: each answer rewritten as a refusal'
    if name == 'headers_copied_from_the_other_stack':
        for prof in doc['profiles']:
            for one in prof['sides']['rust']['legs']:
                if 'hdr_case' in one['cells']:
                    one['cells']['hdr_case'] = 'canonical'
        return 'rust: header casing rewritten to the one only Go emits'
    if name == 'mid_bound_never_refuses':
        for x in doc['profiles']:
            for side in ('go', 'rust'):
                for one in x['sides'][side]['legs']:
                    if one['id'] in ('agent_mid_after_ok', 'agent_huge_after_ok'):
                        one['cells']['gate_class'] = 'answered'
        return 'every side: the post-registration worker messages claimed to pass'
    if name == 'agent_class_diverges':
        for one in prof['tight']['sides']['rust']['legs']:
            if one['id'] in ('agent_reg_payload_over', 'agent_mid_after_ok', 'agent_huge_after_ok'):
                one['cells']['gate_class'] = 'answered'
        return 'tight/rust: worker-socket size gates claimed to have answered instead of refusing'
    if name == 'identity_join_never_refuses':
        for x in doc['profiles']:
            for side in ('go', 'rust'):
                for one in x['sides'][side]['legs']:
                    if one['id'] == 'tok_identity_over':
                        one['cells']['http'] = '101'
                        one['cells']['closed_after_join'] = 0
                        one['cells']['has_join'] = 1
        return 'every side: the over-long identity claim asserted to have joined'
    if name == 'identical_cells':
        for p in doc['profiles']:
            base = {one['id']: one['cells'] for one in p['sides']['go']['legs']}
            for one in p['sides']['rust']['legs']:
                if one['id'] in base:
                    one['cells'] = dict(base[one['id']])
        return 'rust cells := go cells for every profile'
    raise KeyError(name)


MUTATIONS = [
    ('body_over_code', 'a body limit that stops refusing fails its family and its claim',
     lambda d: (_leg_pair(d, 'tight', 'body_over') == 'DIFF'
                or _family(d, 'tight', 'body_gate_talks') is False
                or _expect_fail(d, 'tight', 'body_over', 'go'))),
    ('metadata_msg_neutral', 'tight metadata_gate_talks fails (the gate no longer speaks)',
     lambda d: _family(d, 'tight', 'metadata_gate_talks') is False),
    ('signal_close_code', 'tight/rust signal close code no longer matches, gate fails',
     lambda d: (_leg_pair(d, 'tight', 'sig_payload_over') == 'DIFF'
                or _family(d, 'tight', 'signal_gate_talks') is False)),
    ('signal_frame_counting', 'a header-counting side breaks the accept half of the signal family',
     lambda d: _family(d, 'tight', 'signal_gate_talks') is False),
    ('zeros_body_refuses', 'zeros body expectation fails and the control stops flipping',
     lambda d: _expect_fail(d, 'zeros', 'body_fixed_4k', 'go')
     or not all(c['pass'] for c in d['controls'] if 'body_fixed' in c['id'])),
    ('drop_agent_legs', 'deleting the worker-socket legs empties the evidence that claims them',
     lambda d: _family(d, 'tight', 'agent_gate_talks') is False),
    ('leak_port', 'the privacy sweep catches an unmasked port',
     lambda d: bool(d['privacy'])),
    ('startup_agrees_with_rust', 'the startup refusal pair goes DIFF',
     lambda d: _leg_pair(d, 'unsigned-neg', 'startup_refusal') == 'DIFF'),
    ('wipe_legs', 'no legs means no rows, no gates and no acceptance',
     lambda d: (not d['rows']) or all(not r['pass'] for r in d['rows'].values())),
    ('identical_cells', 'copying the oracle cells goes against the per-side claims the lane pins',
     lambda d: not all(e['pass'] for e in d['expectations'] if e['kind'] == 'side')),
    ('flips_every_accept', 'a receipt where nothing is ever accepted fails its own families',
     lambda d: (not all(f['pass'] for e in d['profiles'] for f in e['families'].values()
                        if f.get('applicable', True)))
     and not all(c['pass'] for c in d['controls'])),
    ('mid_bound_never_refuses', 'the worker-socket bound control needs a refusal to discriminate',
     lambda d: not [c for c in d['controls'] if c['id'] == 'agent_mid_sits_on_one_bound'][0]['pass']),
    ('agent_class_diverges', 'the worker-socket gate class is what the agent row is graded on',
     lambda d: _leg_pair(d, 'tight', 'agent_reg_payload_over') == 'DIFF'
     and _family(d, 'tight', 'agent_gate_talks') is False),
    ('identity_join_never_refuses',
     'the identity row\'s gate rests on a join that is actually refused',
     lambda d: _family(d, 'tight', 'identity_gate_talks') is False
     or not (d['rows'].get('YAML:limit.max_participant_identity_length') or {}).get('gate_pass')),
    ('headers_copied_from_the_other_stack',
     'rust cannot be claimed to write go\'s header casing',
     lambda d: not all(e['pass'] for e in d['expectations'] if e['kind'] == 'side'
                       and e['side'] == 'rust')),
]

P_LEAK_PORT = 0


def _leg_pair(graded, pid, leg_id):
    for entry in graded['profiles']:
        if entry['id'] == pid:
            for one in entry['legs']:
                if one['id'] == leg_id:
                    return one['pair']
    return 'ABSENT'


def _family(graded, pid, gid):
    for entry in graded['profiles']:
        if entry['id'] == pid:
            fam = entry['families'].get(gid)
            if fam is None:
                return None
            # not applicable is not a pass: a mutation that removes the legs must read as red
            return fam.get('pass') if fam.get('applicable', True) else False
    return None


def _expect_fail(graded, pid, leg_id, side):
    return any(not e['pass'] for e in graded['expectations']
               if e['profile'] == pid and e['leg'] == leg_id and e['side'] == side)


def selftest(doc: dict, P: dict, work: Path) -> int:
    global P_LEAK_PORT
    P_LEAK_PORT = P['HTTP']
    base = grade(doc)
    base['privacy'] = privacy_sweep(doc, P, work)
    w('# C106 gate falsification')
    w(f'- baseline: profiles {len(base["profiles"])}, legs '
      f'{sum(len(p["legs"]) for p in base["profiles"])}, rows {len(base["rows"])}, '
      f'gates {sum(1 for g in base["gates"] if g["pass"])}/{len(base["gates"])}, '
      f'privacy {len(base["privacy"])}')
    if not base['profiles'] or not base['gates']:
        w('SELFTEST: BASELINE_EMPTY_NOT_A_TEST')
        return 1
    failures = []
    for name, why, probe in MUTATIONS:
        copy_doc = copy.deepcopy(doc)
        touched = mutate(copy_doc, name)
        graded = grade(copy_doc)
        graded['privacy'] = privacy_sweep(copy_doc, P, work)
        try:
            reddened = bool(probe(graded))
        except Exception as exc:  # noqa: BLE001
            reddened = False
            why += f' (probe error {type(exc).__name__})'
        w(f'- {"RED" if reddened else "STILL-GREEN"} {name}: {touched} -> {why}')
        if not reddened:
            failures.append(name)
    w(f'SELFTEST: {"OK" if not failures else "FAILURES " + ",".join(failures)} '
      f'({len(MUTATIONS)} mutations)')
    return 0 if not failures else 1


# --------------------------------------------------------------------------------------- main
def main() -> int:
    ap = argparse.ArgumentParser(description='C106 limit-gate channels paired differential')
    ap.add_argument('--record', action='store_true')
    ap.add_argument('--report', metavar='RECEIPT')
    ap.add_argument('--selftest-gate', metavar='RECEIPT')
    ap.add_argument('--out', default='/tmp/c106-run')
    ap.add_argument('--go', default=str(ORACLE))
    ap.add_argument('--rust', default=str(CANDIDATE))
    ap.add_argument('--profiles', default='')
    ap.add_argument('--sides', default='go,rust')
    ap.add_argument('--only', default='', help='comma list of leg ids (subset: never an acceptance)')
    ap.add_argument('--hold', type=float, default=0.25)
    ap.add_argument('--force', action='store_true')
    args = ap.parse_args()

    work = Path(args.out)
    work.mkdir(parents=True, exist_ok=True)
    used = set()
    P = {'HTTP': alloc_port(used), 'STORE': alloc_port(used)}
    P['roles'] = {P['HTTP']: 'HTTP', P['STORE']: 'STORE'}
    sides = [s for s in args.sides.split(',') if s]
    binaries = {'go': args.go, 'rust': args.rust}
    only_profiles = [p for p in args.profiles.split(',') if p]

    bad = third_party_modules()
    if bad:
        w(f'non-stdlib imports: {bad}')
        return 2
    if args.record:
        problems = check_identities(binaries, sides)
        if problems and not args.force:
            w('identity check failed: ' + '; '.join(problems))
            w('the lane only certifies against the pinned pair; rebuild or pass --force to explore')
            return 2
        store = start_store(P['STORE'])
        try:
            heads = {'rs': git_output(REPO, 'rev-parse', 'HEAD'),
                     'docs': git_output(REPO.parent / 'livekit-docs', 'rev-parse', 'HEAD')}
            doc = record(P, work, binaries, sides, only_profiles, args.hold, harness_hashes(), heads)
            path = work / 'receipt.json'
            path.write_text(json.dumps(doc, indent=1, sort_keys=True, ensure_ascii=False) + '\n',
                            encoding='utf-8')
            w(f'receipt written {path} ({path.stat().st_size} bytes)')
        finally:
            w(f'store stopped: {json.dumps(stop_store(store, P["STORE"]))}')
        args.profiles = ','.join(only_profiles)
        args.only = args.only or ''
        return report(json.loads((work / 'receipt.json').read_text()), args, P, work)
    if args.report:
        doc = json.loads(Path(args.report).read_text())
        return report(doc, args, P, work)
    if args.selftest_gate:
        doc = json.loads(Path(args.selftest_gate).read_text())
        return selftest(doc, P, work)
    ap.print_help()
    return 2


if __name__ == '__main__':
    sys.exit(main())
