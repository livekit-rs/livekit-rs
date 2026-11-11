#!/usr/bin/env python3
"""Compare official (a) vs Rust (b) responses. Only server-generated
identifiers and clocks are normalized, each by an explicit rule:
- strings shaped like LiveKit GUIDs (`RM_`, `PA_`, `TR_`, `EG_`, `IN_`, `ST_`,
  `SDR_`, `AD_`, `SCL_`... followed by 12 base62 chars) are renamed in order of
  first appearance per side;
- integer-valued fields named creation_time, creation_time_ms, joined_at,
  joined_at_ms, unix_micro, ticks, started_at, ended_at, updated_at are
  replaced by a presence marker (0 stays 0); also created_at/deleted_at;
- `turn_password` (utils.RandomSecret) becomes `<random-secret>` when it is
  42..44 base62 chars, so only its presence/shape is compared.
Everything else (status, kept headers, body bytes for non-JSON, JSON values,
field presence, error code/msg/meta) must be equal."""
import json, re, sys

GUID = re.compile(r'^(RM|PA|TR|EG|IN|ST|SDR|AD|SCL|SCP|SIP|NE|ND|MR|JR)_[0-9A-Za-z]{12}$')
CLOCKS = {'creation_time', 'creation_time_ms', 'joined_at', 'joined_at_ms', 'unix_micro', 'ticks',
          'started_at', 'ended_at', 'updated_at', 'createdAt', 'creationTime', 'created_at', 'deleted_at',
          'creationTimeMs', 'joinedAt', 'joinedAtMs', 'startedAt', 'endedAt', 'updatedAt'}
# utils.RandomSecret(): base62 of 32 random bytes (42..44 chars)
SECRET = re.compile(r'^[0-9A-Za-z]{40,46}$')
SECRETS = {'turn_password', 'turnPassword'}


def norm(v, ids, key=None):
    if isinstance(v, dict):
        return {k: norm(x, ids, k) for k, x in v.items()}
    if isinstance(v, list):
        return [norm(x, ids) for x in v]
    if key in CLOCKS and v not in (0, '0', None, ''):
        return '<clock>'
    if key in SECRETS and isinstance(v, str) and v:
        return '<random-secret>' if SECRET.match(v) else v
    if isinstance(v, str) and GUID.match(v):
        return ids.setdefault(v, f'<{v.split("_")[0]}#{len(ids)}>')
    return v


UNORDERED = {
    # LocalStore ListRooms / ListAgentDispatches iterate Go maps: upstream
    # order is unspecified, so these lists are compared as multisets.
    'rooms': lambda x: (x.get('name', ''), json.dumps(x, sort_keys=True)),
    'agent_dispatches': lambda x: (x.get('agent_name', ''), x.get('metadata', '')),
}


def sort_rooms(v):
    if isinstance(v, dict):
        for k, key in UNORDERED.items():
            if isinstance(v.get(k), list):
                v = dict(v, **{k: sorted(v[k], key=key)})
    return v


def view(r, ids):
    if r is None:
        return None
    if r.get('error'):
        return {'transport_error': r['error'].split(': ')[-1]}
    out = {'status': r['status'], 'headers': r.get('headers', {})}
    if r.get('body_json') is not None:
        out['json'] = norm(sort_rooms(r['body_json']), ids)
    else:
        out['body'] = r.get('body', '')
    if r.get('decode_error'):
        out['decode_error'] = r['decode_error']
    return out


def main():
    raw = json.load(open(sys.argv[1]))['results']
    ida, idb = {}, {}
    rows, counts = [], {}
    for r in raw:
        a, b = view(r['a'], ida), view(r['b'], idb)
        if a == b:
            st = 'match'
        elif a.get('status') != b.get('status'):
            st = 'status'
        else:
            st = 'body'
        counts[st] = counts.get(st, 0) + 1
        rows.append({'id': r['id'], 'status': st, 'a': a, 'b': b})
    groups = {}
    for x in rows:
        if x['status'] != 'match':
            groups.setdefault(x['id'].split('.')[0] + '.' + x['id'].split('.')[1], []).append(x['id'])
    hooks = None
    if len(sys.argv) > 3:
        w = json.load(open(sys.argv[3]))
        def hook_view(events, ids):
            out = []
            for e in events:
                try:
                    body = norm(json.loads(e['body']), ids)
                except ValueError:
                    body = e['body']
                if isinstance(body, dict):
                    body.pop('id', None)  # per-delivery random event id
                hdr = {k.lower(): v for k, v in e['headers'].items()}
                out.append({'path': e['path'], 'content_type': hdr.get('content-type'),
                            'user_agent': hdr.get('user-agent'), 'accept_encoding': hdr.get('accept-encoding'),
                            'auth': e.get('auth_verdict'), 'body': body})
            return out
        ga, gb = hook_view(w['official'], {}), hook_view(w['rust'], {})
        hooks = {'match': ga == gb, 'official_events': [x['body'].get('event') if isinstance(x['body'], dict) else None for x in ga],
                 'rust_events': [x['body'].get('event') if isinstance(x['body'], dict) else None for x in gb],
                 'official': ga, 'rust': gb}
        counts['webhooks'] = 'match' if hooks['match'] else 'mismatch'
    json.dump({'counts': counts, 'mismatch_by_method': groups, 'webhooks': hooks, 'rows': rows},
              open(sys.argv[2], 'w'), indent=1)
    print(json.dumps(counts))


if __name__ == '__main__':
    main()
