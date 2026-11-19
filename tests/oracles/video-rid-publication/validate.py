#!/usr/bin/env python3
"""Strict literal expectations and raw-callback evidence checks; never rewrites input."""
import argparse
import copy
import json
import struct
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
                     'participant_mdns_disabled', 'peer_mdns_disabled', 'request_immutable', 'final_request_immutable'):
            equal(values(name), [True], row['case'] + '.' + name)
        equal(values('connected_before_rtp'), ['connected'])
        for event, boundary in [('on_track_published_track', 'callback'), ('settled_track', 'settled')]:
            tracks = values(event)
            if len(tracks) != 1 or not tracks[0]['sid'] or not tracks[0]['version']:
                raise ValueError('missing original full TrackInfo at ' + event)
            projected = {k: v for k, v in tracks[0].items() if k not in ('sid', 'version')}
            equal(projected, row['output'][boundary], event)
        equal(values('input'), [row['input']])
        equal(values('on_track_published_rtp_writes'), [1])
        equal(values('rtp_write_result'), [dict(success=True, error='<nil>')])
        equal(values('publication_channel_overflow'), [])
        quiet = values('quiet_counts')
        if len(quiet) != 1 or quiet[0]['elapsed_ns'] < 100_000_000:
            raise ValueError('missing 100ms quiet window')
        equal({k: v for k, v in quiet[0].items() if k != 'elapsed_ns'},
              dict(publications=0, published_tracks=0, rtp_writes=0))
        equal(values('settled_counts'), [dict(publications=1, published_tracks=1, rtp_writes=1, idle_rtp_writes=0)])
        equal(values('final_counts'), [dict(publications=1, published_tracks=1, rtp_writes=1,
                                          rtp_write_successes=1, idle_rtp_writes=0)])
        extensions = values('negotiated_extensions')
        if len(extensions) != 1:
            raise ValueError('missing negotiated extension IDs')
        mid_uri = 'urn:ietf:params:rtp-hdrext:sdes:mid'
        rid_uri = 'urn:ietf:params:rtp-hdrext:sdes:rtp-stream-id'
        ids = extensions[0]
        equal(set(ids), {mid_uri, rid_uri})
        if any(type(v) is not int or not 1 <= v <= 14 for v in ids.values()) or len(set(ids.values())) != 2:
            raise ValueError('invalid extension IDs')
        for event, direction in [('offer', 'send'), ('answer', 'recv')]:
            descriptions = values(event)
            if len(descriptions) != 1:
                raise ValueError('missing raw ' + event)
            lines = descriptions[0]['sdp'].splitlines()
            equal([x for x in lines if x.startswith('a=rid:')],
                  ['a=rid:q ' + direction, 'a=rid:f ' + direction])
            equal([x for x in lines if x.startswith('a=simulcast:')], ['a=simulcast:' + direction + ' q;f'])
            equal([x for x in lines if x.startswith('a=mid:')], ['a=mid:0'])
            equal([x for x in lines if x.startswith('a=rtpmap:')], ['a=rtpmap:96 VP8/90000'])
            for uri, id_ in ids.items():
                ext = [x.split()[0].split(':')[1].split('/')[0] for x in lines
                       if x.startswith('a=extmap:') and x.split()[1] == uri]
                equal(ext, [str(id_)])
        packet = struct.pack('!BBHII', 0x90, 96 | (0x80 if row['input']['rtp']['marker'] else 0),
                             1000, 90000, 424242)
        extension = bytes([ids[mid_uri] << 4, ord('0'), ids[rid_uri] << 4, ord('q')])
        packet += struct.pack('!HH', 0xbede, 1) + extension + bytes.fromhex('1001000000')
        equal(values('rtp_packet_hex'), [packet.hex()])
        for event in ('received_rtp_stats', 'settled_rtp_stats'):
            stats = values(event)
            if len(stats) != 1 or stats[0]['packets'] != 1:
                raise ValueError('missing exact single-packet receipt at ' + event)
        names = [e['event'] for e in selected]
        ordered = ['connected_before_rtp', 'quiet_start', 'quiet_counts', 'rtp_packet_hex',
                   'on_track_published_track', 'received_rtp_stats', 'settled_track',
                   'final_counts', 'participant_closed', 'peer_closed']
        positions = [names.index(n) for n in ordered]
        equal(positions, sorted(positions))
    return len(rows)


def controls():
    cases, expected = fixtures()
    equal(decode((HERE / 'cases.json').read_bytes()), cases)
    equal(decode((HERE / 'expected.json').read_bytes()), expected)
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
