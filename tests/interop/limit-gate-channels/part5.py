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
from __future__ import annotations

from part1 import *  # noqa: F401,F403
from part3 import http_body_legs, twirp_limit_legs, leg
from part4 import signal_legs, agent_legs, agent_session, msgs_brief, after_cells, join_leg_cells

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
