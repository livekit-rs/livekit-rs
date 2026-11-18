#!/usr/bin/env python3
"""Strict complete C56 raw schema/state checks, independent of Go execution."""
import argparse
import copy
import json
from pathlib import Path

NAMES = ['video/VP8', 'video/VP9', 'video/H264']
MATRIX = [
    ('shared-expiry', 2, 1000, [0, 200, 400, 999, 1000, 1200], {1000: [0, 3]}),
    ('shared-expiry', 3, 1000, [0, 200, 400, 999, 1000, 1200], {1000: [0, 3, 0]}),
    ('upgrade-flush', 2, 1000, [0, 200, 400, 999, 1000, 1399, 1400], {200: [0, 2], 1400: [3, 2]}),
    ('upgrade-flush', 3, 1000, [0, 200, 400, 999, 1000, 1399, 1400], {200: [0, 2, 1], 1400: [0, 2, 3]}),
    ('return-keeps-deadline', 1, 1000, [0, 200, 400, 999, 1000, 1400], {1000: [3]}),
    ('return-keeps-deadline', 2, 1000, [0, 200, 400, 999, 1000, 1400], {1000: [3, 1]}),
    ('equal-expiry', 1, 1000, [0, 200, 999, 1000], {1000: [2]}),
    ('equal-expiry', 2, 1000, [0, 200, 999, 1000], {1000: [2, 1]}),
    ('zero-delay', 2, 0, [0, 200, 400, 1000], {0: [0, 1], 200: [0, 3], 400: [2, 3]}),
    ('zero-delay', 3, 0, [0, 200, 400, 1000], {0: [0, 1, 1], 200: [0, 3, 1], 400: [0, 3, 2]}),
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
    mutations = [
        lambda r: r.pop(),
        lambda r: r.reverse(),
        lambda r: r[0].update(extra=0),
        lambda r: r[0].update(codec_count=True),
        lambda r: r[0].update(pause_ms=0),
        lambda r: r[0]['observations'].pop(0),
        lambda r: r[0]['observations'].reverse(),
        lambda r: r[0]['observations'][0].update(at_ms=False),
        lambda r: r[0]['observations'][0].update(events=None),
        lambda r: r[0]['observations'][4]['events'].append(r[0]['observations'][4]['events'][0]),
        lambda r: r[2]['observations'][4].update(events=r[2]['observations'][6]['events']),
        lambda r: r[2]['observations'][6].update(events=[]),
        lambda r: r[6]['observations'][3].update(events=[]),
        lambda r: r[1]['observations'][4]['events'][0]['codecs'].reverse(),
        lambda r: r[0]['observations'][4]['events'][0]['codecs'][0].update(codec='vp8'),
        lambda r: r[0]['observations'][4]['events'][0]['codecs'][0]['qualities'].pop(),
        lambda r: r[0]['observations'][4]['events'][0]['codecs'][0]['qualities'][0].update(quality=False),
        lambda r: r[0]['observations'][4]['events'][0]['codecs'][0]['qualities'][0].update(enabled=1),
        lambda r: r[0]['observations'][4]['events'][0]['codecs'][0]['qualities'][1].update(enabled=True),
    ]
    for mutate in mutations:
        rows = copy.deepcopy(baseline)
        mutate(rows)
        try:
            validate(rows)
        except ValueError:
            continue
        raise AssertionError('accepted mutation')
    try:
        load(b'{"scenario":"x","scenario":"x"}\n')
    except ValueError as error:
        require('duplicate JSON key' in str(error), 'duplicate key not detected')
    else:
        raise AssertionError('accepted duplicate key')
    print('PASS: valid matrix and 20 rejection controls')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--input', type=Path)
    parser.add_argument('--self-test', action='store_true')
    args = parser.parse_args()
    if args.self_test:
        self_test()
    if args.input:
        rows = load(args.input.read_bytes())
        print('PASS: 10 rows, 54 checkpoints, 16 full-state events')
    if not args.self_test and not args.input:
        parser.error('provide --input or --self-test')
