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
from __future__ import annotations

import threading

from part1 import *  # noqa: F401,F403
from part2 import (JOIN_PATH, read_window, request_response, signal_session,
                    ws_frame, ws_handshake, df_comp)
from part3 import IDENT_ROWS, NAME_ROWS, cells_of, leg

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
