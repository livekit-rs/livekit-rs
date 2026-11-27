#!/usr/bin/env python3
"""C106 limit-gate channels - part 3: the HTTP body limiter and the Twirp limit gates.

Two families live on the API listener. `limit.max_api_request_body_size` is a middleware guard that
runs before routing, before authentication and before protobuf decoding; the per-field limits
(`max_metadata_size`, `max_attributes_size`, `max_room_name_length`, `max_participant_*_length`) are
handler guards that run at a documented position relative to the permission check and the room
lookup. The legs below deliberately collide those guards so the *first* refusal is the finding.
"""
from __future__ import annotations

from part1 import *  # noqa: F401,F403
from part2 import JOIN_PARAM, signal_session  # noqa: F401

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
