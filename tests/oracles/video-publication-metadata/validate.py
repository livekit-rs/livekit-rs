#!/usr/bin/env python3
"""Strict literal expectations and raw-callback evidence checks; never rewrites input."""
import argparse
import copy
import json
from pathlib import Path

from fixtures import fixtures

HERE = Path(__file__).resolve().parent


def pairs(items):
    result = {}
    for key, value in items:
        if key in result:
            raise ValueError('duplicate key: ' + key)
        result[key] = value
    return result


def decode(raw):
    return json.loads(raw, object_pairs_hook=pairs)


def equal(actual, expected, path='$'):
    if type(actual) is not type(expected):
        raise ValueError(path + ': type mismatch')
    if isinstance(expected, dict):
        if actual.keys() != expected.keys():
            raise ValueError(path + ': field mismatch: ' + repr(actual.keys() ^ expected.keys()))
        for key, value in expected.items():
            equal(actual[key], value, path + '.' + key)
    elif isinstance(expected, list):
        if len(actual) != len(expected):
            raise ValueError(path + ': length mismatch')
        for index, value in enumerate(expected):
            equal(actual[index], value, path + '[' + str(index) + ']')
    elif actual != expected:
        raise ValueError(path + ': expected ' + repr(expected) + ', got ' + repr(actual))


def validate(path):
    cases, expected = fixtures()
    equal(decode((HERE / 'cases.json').read_bytes()), cases)
    equal(decode((HERE / 'expected.json').read_bytes()), expected)
    rows = [decode(line) for line in path.read_bytes().splitlines()]
    equal(rows, expected)
    events = [decode(line) for line in Path(str(path) + '.events').read_bytes().splitlines()]
    for row in rows:
        selected = [e for e in events if e['case'] == row['case']]
        def values(name):
            return [e['value'] for e in selected if e['event'] == name]
        for name in ('participant_closed', 'peer_closed', 'participant_socket_closed', 'peer_socket_closed',
                     'participant_mdns_disabled', 'peer_mdns_disabled', 'request_immutable'):
            equal(values(name), [True], row['case'] + '.' + name)
        equal(values('connected_before_rtp'), ['connected'])
        for event, boundary in [('on_track_published_track', 'callback'), ('settled_track', 'settled')]:
            tracks = values(event)
            if len(tracks) != 1 or not tracks[0]['sid'] or not tracks[0]['version']:
                raise ValueError('missing original full TrackInfo at ' + event)
            projected = {k: v for k, v in tracks[0].items() if k not in ('sid', 'version')}
            equal(projected, row['output'][boundary], event)
        packets = values('rtp_packet_hex')
        if len(packets) != row['input']['rtp']['count']:
            raise ValueError('missing RTP stimulus')
        stats = values('received_rtp_stats')
        if len(stats) != 1 or stats[0]['packets'] < len(packets):
            raise ValueError('missing actual RTP receipt')
        names = [e['event'] for e in selected]
        before = names.index('on_track_published_track') < names.index('rtp_packet_hex')
        equal(before, row['input']['fire_on_track_by_sdp'])
    return len(rows)


def controls():
    _, expected = fixtures()
    tested = 0
    def walk(value, path):
        yield path, value
        if isinstance(value, dict):
            for k, v in value.items():
                yield from walk(v, path + [k])
        elif isinstance(value, list):
            for k, v in enumerate(value):
                yield from walk(v, path + [k])
    for path, value in walk(expected, []):
        variants = [None]
        if isinstance(value, dict):
            variants += [dict(value, unexpected=True)]
            if value:
                v = dict(value)
                del v[next(iter(v))]
                variants.append(v)
        elif isinstance(value, list):
            variants.append(value + [None])
        elif isinstance(value, bool):
            variants += [not value, int(value)]
        elif isinstance(value, int):
            variants += [value + 1, str(value)]
        elif isinstance(value, str):
            variants.append(value + '-corrupt')
        for replacement in variants:
            mutated = copy.deepcopy(expected)
            if not path:
                mutated = replacement
            else:
                parent = mutated
                for k in path[:-1]:
                    parent = parent[k]
                parent[path[-1]] = replacement
            try:
                equal(mutated, expected)
            except ValueError:
                tested += 1
            else:
                raise ValueError('accepted corruption at ' + repr(path))
    return tested


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--input', type=Path)
    parser.add_argument('--self-test', action='store_true')
    args = parser.parse_args()
    result = {}
    if args.self_test:
        result['rejected_corruptions'] = controls()
    if args.input:
        result['validated_rows'] = validate(args.input)
    if not result:
        parser.error('--input or --self-test required')
    print(json.dumps(result, sort_keys=True))
