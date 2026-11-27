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
