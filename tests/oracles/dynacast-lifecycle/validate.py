#!/usr/bin/env python3
"""Strict C58-schema validator with independent literal lifecycle vectors."""
import argparse
import copy
import json
from pathlib import Path

NAMES = ['video/VP8', 'video/VP9']
TIMES = [0, 100, 200, 300, 400, 1099, 1100, 1101, 9999, 10000, 10099, 10100, 10199, 10200, 10300]
# First-codec layer values at every exact checkpoint and event index -> full
# committed first-codec quality. Codec two is always HIGH. These literals do
# not import Go expectations, logs, timers, transition code or observer output.
MATRIX = [
    ('restart-settled', 1, [0, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 0, 0, 0, 0], {11: 0}),
    ('restart-settled', 2, [0, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 0, 0, 0, 0], {11: 0}),
    ('restart-same', 1, [0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0], {2: 0}),
    ('restart-same', 2, [0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0], {2: 0}),
    ('restart-pending', 1, [2, 2, 2, 2, 2, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0], {6: 0}),
    ('restart-pending', 2, [2, 2, 2, 2, 2, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0], {6: 0}),
    ('restart-repeat', 1, [0, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 0, 0], {13: 0}),
    ('restart-repeat', 2, [0, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 0, 0], {13: 0}),
    ('resend-high', 1, [2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2], {}),
    ('resend-high', 2, [2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2], {}),
    ('resend-low', 1, [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0], {1: 0, 2: 0}),
    ('resend-low', 2, [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0], {1: 0, 2: 0}),
    ('resend-pending', 1, [1, 1, 1, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0], {2: 1, 6: 0}),
    ('resend-pending', 2, [1, 1, 1, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0], {2: 1, 6: 0}),
    ('resend-restart', 1, [0, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 0, 0, 0, 0], {11: 0, 13: 0}),
    ('resend-restart', 2, [0, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 0, 0, 0, 0], {11: 0, 13: 0}),
    ('resend-muted', 1, [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0], {3: 0, 4: 0}),
    ('resend-muted', 2, [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0], {3: 0, 4: 0}),
]
FLAGS = {0: [True, False, False], 1: [True, True, False], 2: [True, True, True]}


def require(ok, message):
    if not ok:
        raise ValueError(message)


def expected():
    rows = []
    for name, n, layers, event_states in MATRIX:
        observations = []
        for i, at in enumerate(TIMES):
            events = []
            if i in event_states:
                state = [event_states[i], 2][:n]
                events = [{'codecs': [{'codec': NAMES[j], 'qualities': [
                    {'quality': q, 'enabled': enabled} for q, enabled in enumerate(FLAGS[value])
                ]} for j, value in enumerate(state)]}]
            observations.append({'at_ms': at, 'events': events,
                                 'expected_layers': [{'codec': NAMES[j], 'spatial': value}
                                                     for j, value in enumerate([layers[i], 2][:n])]})
        rows.append({'scenario': name, 'codec_count': n, 'pause_ms': 1000, 'observations': observations})
    return rows


def same(actual, wanted, path='rows'):
    require(type(actual) is type(wanted), path + ': wrong type')
    if isinstance(wanted, dict):
        require(actual.keys() == wanted.keys(), path + ': fields differ')
        for key in wanted:
            same(actual[key], wanted[key], path + '.' + key)
    elif isinstance(wanted, list):
        require(len(actual) == len(wanted), path + ': count differs')
        for i, value in enumerate(wanted):
            same(actual[i], value, path + '[' + str(i) + ']')
    else:
        require(actual == wanted, path + ': value differs')


def unique(pairs):
    result = {}
    for key, value in pairs:
        require(key not in result, 'duplicate JSON key: ' + key)
        result[key] = value
    return result


def load(raw):
    rows = [json.loads(line, object_pairs_hook=unique) for line in raw.splitlines()]
    same(rows, expected())
    return rows


def counts(rows):
    observations = [o for r in rows for o in r['observations']]
    return {
        'rows': len(rows), 'checkpoints': len(observations),
        'events': sum(len(o['events']) for o in observations),
        'empty_event_checkpoints': sum(not o['events'] for o in observations),
        'expected_layer_samples': sum(len(o['expected_layers']) for o in observations),
        'nonvacuous_initialized_codecs': sum(r['codec_count'] for r in rows),
        'nonvacuous_layer_transitions': sum(
            a['spatial'] != b['spatial'] for r in rows
            for before, after in zip(r['observations'], r['observations'][1:])
            for a, b in zip(before['expected_layers'], after['expected_layers'])),
    }


def self_test():
    baseline = expected()
    encode = lambda rows: ('\n'.join(json.dumps(r) for r in rows) + '\n').encode()
    original = encode(baseline)
    load(original)
    def obs(r): return r[1]['observations'][11]
    def event(r): return obs(r)['events'][0]
    def codec(r): return event(r)['codecs'][0]
    def quality(r): return codec(r)['qualities'][0]
    def layer(r): return obs(r)['expected_layers'][0]
    mutations = [
        ('missing row', lambda r: r.pop()),
        ('duplicate row', lambda r: r.append(r[0])),
        ('row order', lambda r: r.reverse()),
        ('wrong scenario', lambda r: r[0].update(scenario='restart-same')),
        ('bool codec_count', lambda r: r[0].update(codec_count=True)),
        ('float pause_ms', lambda r: r[0].update(pause_ms=1000.0)),
        ('wrong pause', lambda r: r[0].update(pause_ms=0)),
        ('missing checkpoint', lambda r: r[0]['observations'].pop(0)),
        ('duplicate checkpoint', lambda r: r[0]['observations'].append(r[0]['observations'][0])),
        ('checkpoint order', lambda r: r[0]['observations'].reverse()),
        ('bool at_ms', lambda r: r[0]['observations'][0].update(at_ms=False)),
        ('wrong time', lambda r: obs(r).update(at_ms=10101)),
        ('null events', lambda r: obs(r).update(events=None)),
        ('duplicate event', lambda r: obs(r)['events'].append(event(r))),
        ('missing event', lambda r: obs(r).update(events=[])),
        ('event codec order', lambda r: event(r)['codecs'].reverse()),
        ('missing event codec', lambda r: event(r)['codecs'].pop()),
        ('duplicate event codec', lambda r: event(r)['codecs'].append(codec(r))),
        ('projected event MIME', lambda r: codec(r).update(codec='vp8')),
        ('missing quality', lambda r: codec(r)['qualities'].pop()),
        ('duplicate quality', lambda r: codec(r)['qualities'].append(quality(r))),
        ('quality order', lambda r: codec(r)['qualities'].reverse()),
        ('bool quality', lambda r: quality(r).update(quality=False)),
        ('int enabled', lambda r: quality(r).update(enabled=1)),
        ('wrong enabled', lambda r: quality(r).update(enabled=False)),
        ('null expected_layers', lambda r: obs(r).update(expected_layers=None)),
        ('object expected_layers', lambda r: obs(r).update(expected_layers={})),
        ('empty expected_layers', lambda r: obs(r).update(expected_layers=[])),
        ('missing layer codec', lambda r: obs(r)['expected_layers'].pop()),
        ('duplicate layer codec', lambda r: obs(r)['expected_layers'].append(layer(r))),
        ('layer codec order', lambda r: obs(r)['expected_layers'].reverse()),
        ('projected layer MIME', lambda r: layer(r).update(codec='vp8')),
        ('wrong layer codec', lambda r: layer(r).update(codec='video/VP9')),
        ('nonstring layer codec', lambda r: layer(r).update(codec=8)),
        ('bool spatial', lambda r: layer(r).update(spatial=False)),
        ('float spatial', lambda r: layer(r).update(spatial=0.0)),
        ('null spatial', lambda r: layer(r).update(spatial=None)),
        ('wrong spatial', lambda r: layer(r).update(spatial=2)),
        ('vacuous medium warmup', lambda r: r[12]['observations'][0]['expected_layers'][0].update(spatial=0)),
        ('restart receiver not HIGH', lambda r: r[0]['observations'][1]['expected_layers'][0].update(spatial=0)),
        ('restart wrongly commits', lambda r: r[0]['observations'][1].update(events=obs(r)['events'])),
        ('same quality initialization omitted', lambda r: r[2]['observations'][2].update(events=[])),
        ('pending deadline canceled by restart', lambda r: r[4]['observations'][6].update(events=[])),
        ('pending restart deadline shifted', lambda r: r[4]['observations'][6].update(at_ms=1200)),
        ('repeat restart timer not reset', lambda r: r[6]['observations'][11].update(events=r[6]['observations'][13]['events'])),
        ('HIGH resend incorrectly emits', lambda r: r[8]['observations'][1].update(events=obs(r)['events'])),
        ('second LOW resend omitted', lambda r: r[10]['observations'][2].update(events=[])),
        ('pending resend commits LOW', lambda r: r[12]['observations'][2]['events'][0]['codecs'][0]['qualities'][1].update(enabled=False)),
        ('resend cancels pending downgrade', lambda r: r[12]['observations'][6].update(events=[])),
        ('resend empty map leaks snapshot', lambda r: r[14]['observations'][2].update(events=obs(r)['events'])),
        ('post restart resend omitted', lambda r: r[14]['observations'][13].update(events=[])),
        ('muted resend leaks publisher', lambda r: r[16]['observations'][2].update(events=r[16]['observations'][3]['events'])),
        ('unmute force omitted', lambda r: r[16]['observations'][3].update(events=[])),
        ('unmuted resend omitted', lambda r: r[16]['observations'][4].update(events=[])),
        ('second codec not HIGH', lambda r: obs(r)['expected_layers'][1].update(spatial=0)),
    ]
    objects = [('row', lambda r: r[0]), ('observation', obs), ('event', event),
               ('codec', codec), ('quality', quality), ('layer', layer)]
    for name, get in objects:
        mutations.append((name + ' unknown field', lambda r, get=get: get(r).update(extra=0)))
        for field in get(baseline):
            mutations.append((name + ' missing ' + field, lambda r, get=get, field=field: get(r).pop(field)))
    rejected = []
    for name, mutate in mutations:
        rows = copy.deepcopy(baseline)
        mutate(rows)
        altered = encode(rows)
        require(altered != original, 'vacuous mutation: ' + name)
        try:
            load(altered)
        except ValueError:
            rejected.append(name)
        else:
            raise AssertionError('accepted mutation: ' + name)
    duplicate_fields = {
        'scenario': '"restart-settled"', 'codec_count': '1', 'pause_ms': '1000',
        'observations': '[', 'at_ms': '0', 'events': '[', 'expected_layers': '[',
        'codecs': '[', 'codec': '"video/VP8"', 'qualities': '[',
        'quality': '0', 'enabled': 'true', 'spatial': '0',
    }
    for field, value in duplicate_fields.items():
        old = ('"' + field + '": ' + value).encode()
        new = ('"' + field + '": null, ').encode() + old
        altered = original.replace(old, new, 1)
        require(altered != original, 'vacuous duplicate-key mutation: ' + field)
        try:
            load(altered)
        except ValueError as error:
            require('duplicate JSON key' in str(error), 'not rejected as duplicate key')
            rejected.append('duplicate key ' + field)
        else:
            raise AssertionError('accepted duplicate key: ' + field)
    print(json.dumps({'negative_controls_rejected': len(rejected), 'nonvacuous_mutations': len(rejected), 'controls': rejected}, sort_keys=True))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--input', type=Path)
    parser.add_argument('--self-test', action='store_true')
    args = parser.parse_args()
    if args.self_test:
        self_test()
    if args.input:
        print(json.dumps({'result': 'PASS', **counts(load(args.input.read_bytes()))}, sort_keys=True))
    if not args.self_test and not args.input:
        parser.error('provide --input or --self-test')
