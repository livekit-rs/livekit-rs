#!/usr/bin/env python3
"""C58 strict raw validator: independently authored literal checkpoint vectors."""
import argparse
import copy
import json
from pathlib import Path

NAMES = ['video/VP8', 'video/VP9']
TIMES = [0, 100, 200, 300, 1199, 1200, 1499, 1500, 1600, 1700, 1800, 1900, 2000]
# Layer vectors and publisher-event positions are literal requirements, not a
# simulation of Go's timers, mute gate, debounce or listener. No Go/log imports.
MATRIX = [
    ('muted-downgrade', 1, 1000, [[2], [2], [2], [2], [2], [0], [0], [0], [0], [0], [0], [0], [0]], [7, 8, 11], [0]),
    ('muted-downgrade', 2, 1000, [[2, 2], [2, 2], [2, 2], [2, 2], [2, 2], [0, 2], [0, 2], [0, 2], [0, 2], [0, 2], [0, 2], [0, 2], [0, 2]], [7, 8, 11], [0, 2]),
    ('muted-upgrade', 2, 1000, [[2, 1], [2, 1], [2, 1], [0, 2], [0, 2], [0, 2], [0, 2], [0, 2], [0, 2], [0, 2], [0, 2], [0, 2], [0, 2]], [7, 8, 11], [0, 2]),
    ('zero-muted', 1, 0, [[2], [2], [0], [0], [0], [0], [0], [0], [0], [0], [0], [0], [0]], [7, 8, 11], [0]),
    ('zero-muted', 2, 0, [[2, 2], [2, 2], [0, 2], [0, 2], [0, 2], [0, 2], [0, 2], [0, 2], [0, 2], [0, 2], [0, 2], [0, 2], [0, 2]], [7, 8, 11], [0, 2]),
    ('off-muted', 1, 1000, [[2], [2], [2], [2], [2], [-1], [-1], [-1], [-1], [-1], [-1], [-1], [-1]], [7, 8, 11], [3]),
    ('off-muted', 2, 1000, [[2, 2], [2, 2], [2, 2], [2, 2], [2, 2], [-1, 2], [-1, 2], [-1, 2], [-1, 2], [-1, 2], [-1, 2], [-1, 2], [-1, 2]], [7, 8, 11], [3, 2]),
    ('unmuted-control', 1, 1000, [[2], [2], [2], [2], [2], [0], [0], [0], [0], [0], [0], [0], [0]], [5, 7, 8, 11], [0]),
    ('unmuted-control', 2, 1000, [[2, 2], [2, 2], [2, 2], [2, 2], [2, 2], [0, 2], [0, 2], [0, 2], [0, 2], [0, 2], [0, 2], [0, 2], [0, 2]], [5, 7, 8, 11], [0, 2]),
]
FLAGS = {0: [True, False, False], 1: [True, True, False], 2: [True, True, True], 3: [False, False, False]}


def require(ok, message):
    if not ok:
        raise ValueError(message)


def expected():
    rows = []
    for name, n, pause, layers, event_indices, state in MATRIX:
        observations = []
        for i, at in enumerate(TIMES):
            events = []
            if i in event_indices:
                events = [{'codecs': [{'codec': NAMES[j], 'qualities': [
                    {'quality': q, 'enabled': enabled} for q, enabled in enumerate(FLAGS[value])
                ]} for j, value in enumerate(state)]}]
            observations.append({'at_ms': at, 'events': events,
                                 'expected_layers': [{'codec': NAMES[j], 'spatial': value} for j, value in enumerate(layers[i])]})
        rows.append({'scenario': name, 'codec_count': n, 'pause_ms': pause, 'observations': observations})
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
    transitions = muted_transitions = 0
    for r in rows:
        for previous, current in zip(r['observations'], r['observations'][1:]):
            changed = sum(a['spatial'] != b['spatial'] for a, b in zip(previous['expected_layers'], current['expected_layers']))
            transitions += changed
            if r['scenario'] != 'unmuted-control' and 100 <= current['at_ms'] < 1500:
                muted_transitions += changed
    return {
        'rows': len(rows), 'checkpoints': len(observations),
        'events': sum(len(o['events']) for o in observations),
        'empty_event_checkpoints': sum(not o['events'] for o in observations),
        'expected_layer_samples': sum(len(o['expected_layers']) for o in observations),
        'nonvacuous_initialized_codecs': sum(len(r['observations'][0]['expected_layers']) for r in rows),
        'nonvacuous_layer_transitions': transitions,
        'nonvacuous_muted_layer_transitions': muted_transitions,
        'nonvacuous_repeated_false_events': sum(len(r['observations'][8]['events']) for r in rows),
        'nonvacuous_off_layer_samples': sum(l['spatial'] == -1 for o in observations for l in o['expected_layers']),
    }


def self_test():
    baseline = expected()
    encode = lambda rows: ('\n'.join(json.dumps(r) for r in rows) + '\n').encode()
    original = encode(baseline)
    load(original)
    def obs(r): return r[1]['observations'][7]
    def event(r): return obs(r)['events'][0]
    def codec(r): return event(r)['codecs'][0]
    def quality(r): return codec(r)['qualities'][0]
    def layer(r): return obs(r)['expected_layers'][0]
    mutations = [
        ('missing row', lambda r: r.pop()),
        ('duplicate row', lambda r: r.append(r[0])),
        ('row order', lambda r: r.reverse()),
        ('bool codec_count', lambda r: r[0].update(codec_count=True)),
        ('float pause_ms', lambda r: r[0].update(pause_ms=1000.0)),
        ('wrong pause', lambda r: r[0].update(pause_ms=0)),
        ('missing checkpoint', lambda r: r[0]['observations'].pop(0)),
        ('duplicate checkpoint', lambda r: r[0]['observations'].append(r[0]['observations'][0])),
        ('checkpoint order', lambda r: r[0]['observations'].reverse()),
        ('bool at_ms', lambda r: r[0]['observations'][0].update(at_ms=False)),
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
        ('vacuous zero warmup', lambda r: r[0]['observations'][0]['expected_layers'][0].update(spatial=0)),
        ('repeat false omitted', lambda r: r[0]['observations'][8].update(events=[])),
        ('repeat true incorrectly notifies', lambda r: r[0]['observations'][10].update(events=r[0]['observations'][8]['events'])),
        ('1900 unmute omitted', lambda r: r[0]['observations'][11].update(events=[])),
        ('muted deadline leaks publisher', lambda r: r[0]['observations'][5].update(events=r[0]['observations'][7]['events'])),
        ('muted deadline layer not updated', lambda r: r[0]['observations'][5]['expected_layers'][0].update(spatial=2)),
        ('muted upgrade vp8 not flushed', lambda r: r[2]['observations'][3]['expected_layers'][0].update(spatial=2)),
        ('muted upgrade vp9 not updated', lambda r: r[2]['observations'][3]['expected_layers'][1].update(spatial=1)),
        ('zero muted layer not updated', lambda r: r[3]['observations'][2]['expected_layers'][0].update(spatial=2)),
        ('off spatial wrong enum', lambda r: r[5]['observations'][5]['expected_layers'][0].update(spatial=3)),
        ('off layer not updated', lambda r: r[5]['observations'][5]['expected_layers'][0].update(spatial=2)),
        ('off quality incorrectly low', lambda r: r[5]['observations'][7]['events'][0]['codecs'][0]['qualities'][0].update(enabled=True)),
        ('control deadline missing', lambda r: r[7]['observations'][5].update(events=[])),
        ('early downgrade', lambda r: r[0]['observations'][4]['expected_layers'][0].update(spatial=0)),
    ]
    # Every object level and every field gets missing-field rejection. This
    # includes new observation.expected_layers, layer.codec and layer.spatial.
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
        'scenario': '"muted-downgrade"', 'codec_count': '1', 'pause_ms': '1000',
        'observations': '[', 'at_ms': '0', 'events': '[', 'expected_layers': '[',
        'codecs': '[', 'codec': '"video/VP8"', 'qualities': '[',
        'quality': '0', 'enabled': 'true', 'spatial': '2',
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
