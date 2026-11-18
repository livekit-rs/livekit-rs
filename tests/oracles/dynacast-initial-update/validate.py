#!/usr/bin/env python3
"""Strict complete C57 raw schema/state checks, independent literal expectations."""
import argparse
import copy
import json
from pathlib import Path

NAMES = ['video/VP8', 'video/VP9']
TIMES = [0, 8000, 9000, 9500, 9999, 10000, 10001, 11499, 11500, 12000]
# Independent, literal agreed matrix: never import expectations from Go or logs.
MATRIX = [
    ('settled-high', 1, 2000, TIMES, {}),
    ('settled-high', 2, 2000, TIMES, {}),
    ('settled-low', 1, 2000, TIMES, {}),
    ('settled-low', 2, 2000, TIMES, {}),
    ('pending-late', 1, 2000, TIMES, {11500: [0]}),
    ('pending-late', 2, 2000, TIMES, {11500: [0, 2]}),
    ('pending-codue', 1, 2000, TIMES, {10000: [0]}),
    ('pending-codue', 2, 2000, TIMES, {10000: [0, 2]}),
    ('equal-codue', 1, 2000, TIMES, {10000: [2]}),
    ('equal-codue', 2, 2000, TIMES, {10000: [2, 2]}),
    ('zero-delay', 1, 0, TIMES, {9500: [0]}),
    ('zero-delay', 2, 0, TIMES, {9500: [0, 2]}),
    ('no-demand', 1, 2000, TIMES, {10000: [3]}),
    ('new-codec-initial', 2, 2000, TIMES, {10000: [0, 3]}),
]


def require(ok, message):
    if not ok:
        raise ValueError(message)


def expected():
    rows = []
    for name, n, pause, times, states in MATRIX:
        observations = []
        for at in times:
            events = []
            if at in states:
                codecs = [{'codec': NAMES[i], 'qualities': [
                    {'quality': q, 'enabled': maximum != 3 and q <= maximum} for q in range(3)
                ]} for i, maximum in enumerate(states[at])]
                events = [{'codecs': sorted(codecs, key=lambda c: c['codec'])}]
            observations.append({'at_ms': at, 'events': events})
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


def validate(rows):
    same(rows, expected())
    return rows


def unique(pairs):
    result = {}
    for key, value in pairs:
        require(key not in result, 'duplicate JSON key: ' + key)
        result[key] = value
    return result


def load(raw):
    return validate([json.loads(line, object_pairs_hook=unique) for line in raw.splitlines()])


def self_test():
    baseline = expected()
    validate(baseline)
    def obs(r):
        return r[5]['observations'][8]
    def event(r):
        return obs(r)['events'][0]
    def codec(r):
        return event(r)['codecs'][0]
    def quality(r):
        return codec(r)['qualities'][0]
    mutations = [
        ('missing row', lambda r: r.pop()),
        ('duplicate row', lambda r: r.append(r[0])),
        ('row order', lambda r: r.reverse()),
        ('unknown row field', lambda r: r[0].update(extra=0)),
        ('missing row field', lambda r: r[0].pop('scenario')),
        ('bool codec_count', lambda r: r[0].update(codec_count=True)),
        ('float pause_ms', lambda r: r[0].update(pause_ms=2000.0)),
        ('wrong pause', lambda r: r[0].update(pause_ms=0)),
        ('missing checkpoint', lambda r: r[0]['observations'].pop(0)),
        ('checkpoint order', lambda r: r[0]['observations'].reverse()),
        ('bool at_ms', lambda r: r[0]['observations'][0].update(at_ms=False)),
        ('null events', lambda r: obs(r).update(events=None)),
        ('unknown observation field', lambda r: obs(r).update(extra=0)),
        ('missing observation field', lambda r: obs(r).pop('at_ms')),
        ('duplicate event', lambda r: obs(r)['events'].append(event(r))),
        ('unknown event field', lambda r: event(r).update(extra=0)),
        ('missing event field', lambda r: event(r).pop('codecs')),
        ('codec order', lambda r: event(r)['codecs'].reverse()),
        ('duplicate codec', lambda r: event(r)['codecs'].append(codec(r))),
        ('projected raw MIME', lambda r: codec(r).update(codec='vp8')),
        ('unknown codec field', lambda r: codec(r).update(extra=0)),
        ('missing codec field', lambda r: codec(r).pop('codec')),
        ('missing quality', lambda r: codec(r)['qualities'].pop()),
        ('quality order', lambda r: codec(r)['qualities'].reverse()),
        ('bool quality', lambda r: quality(r).update(quality=False)),
        ('int enabled', lambda r: quality(r).update(enabled=1)),
        ('unknown quality field', lambda r: quality(r).update(extra=0)),
        ('missing quality field', lambda r: quality(r).pop('enabled')),
        ('wrong enabled', lambda r: quality(r).update(enabled=False)),
        ('initial forced settled resend', lambda r: r[0]['observations'][5].update(events=r[8]['observations'][5]['events'])),
        ('pending flushed at initial', lambda r: r[4]['observations'][5].update(events=r[4]['observations'][8]['events'])),
        ('lost late deadline', lambda r: r[4]['observations'][8].update(events=[])),
        ('lost codue deadline', lambda r: r[6]['observations'][5].update(events=[])),
        ('lost equal resend', lambda r: r[8]['observations'][5].update(events=[])),
        ('duplicate equal initial', lambda r: r[8]['observations'][5]['events'].append(r[8]['observations'][5]['events'][0])),
        ('lost zero-delay', lambda r: r[10]['observations'][3].update(events=[])),
        ('no-demand HIGH', lambda r: r[12]['observations'][5].update(events=r[8]['observations'][5]['events'])),
        ('new-codec old deadline uncancelled', lambda r: r[13]['observations'][8].update(events=r[13]['observations'][5]['events'])),
        ('missing new-codec flush', lambda r: r[13]['observations'][5].update(events=[])),
    ]
    encode = lambda rows: ('\n'.join(json.dumps(r) for r in rows) + '\n').encode()
    original = encode(baseline)
    load(original)
    for name, mutate in mutations:
        rows = copy.deepcopy(baseline)
        mutate(rows)
        # JSON encoding distinguishes bool/int and int/float (Python == does not).
        altered = encode(rows)
        require(altered != original, 'ineffective mutation: ' + name)
        try:
            load(altered)
        except ValueError:
            continue
        raise AssertionError('accepted mutation: ' + name)
    duplicates = [
        (b'"scenario": "settled-high"', b'"scenario": "settled-high", "scenario": "settled-high"'),
        (b'"at_ms": 0', b'"at_ms": 0, "at_ms": 0'),
        (b'"codecs": [', b'"codecs": [], "codecs": ['),
        (b'"codec": "video/VP8"', b'"codec": "video/VP8", "codec": "video/VP8"'),
        (b'"quality": 0', b'"quality": 0, "quality": 0'),
        (b'"enabled": true', b'"enabled": true, "enabled": true'),
    ]
    for old, new in duplicates:
        altered = original.replace(old, new, 1)
        require(altered != original, 'ineffective duplicate-key mutation')
        try:
            load(altered)
        except ValueError as error:
            require('duplicate JSON key' in str(error), 'duplicate key not detected')
        else:
            raise AssertionError('accepted duplicate key')
    print(f'PASS: valid matrix and {len(mutations) + len(duplicates)} rejection controls')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--input', type=Path)
    parser.add_argument('--self-test', action='store_true')
    args = parser.parse_args()
    if args.self_test:
        self_test()
    if args.input:
        rows = load(args.input.read_bytes())
        print('PASS: 14 rows, 140 checkpoints, 10 full-state events')
    if not args.self_test and not args.input:
        parser.error('provide --input or --self-test')
