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
from __future__ import annotations

import copy
import time

from part1 import *  # noqa: F401,F403
from part2 import JOIN_PARAM  # noqa: F401
from part3 import cells_of, http_body_legs, leg, twirp_limit_legs
from part4 import (after_cells, agent_legs, gate_class, hold_participant, join_leg_cells, signal_legs)
from part5 import ALIAS_KEYS, cfg_for, profiles, yaml_block

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

