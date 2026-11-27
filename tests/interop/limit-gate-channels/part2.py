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
from __future__ import annotations

from part1 import *  # noqa: F401,F403

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
