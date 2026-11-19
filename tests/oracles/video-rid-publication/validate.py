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
    return validate_events(rows, events)


def validate_events(rows, events):
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
        padding_only = row['case'].startswith('c66-')
        packet = struct.pack('!BBHII', 0xb0 if padding_only else 0x90,
                             96 | (0x80 if row['input']['rtp']['marker'] else 0), 1000, 90000, 424242)
        extension = bytes([ids[mid_uri] << 4, ord('0'), ids[rid_uri] << 4, ord('q')])
        packet += struct.pack('!HH', 0xbede, 1) + extension
        packet += b'\x01' if padding_only else bytes.fromhex('1001000000')
        equal(values('rtp_packet_hex'), [packet.hex()])
        for event in ('received_rtp_stats', 'settled_rtp_stats'):
            stats = values(event)
            if padding_only:
                equal(stats, [None], event)
            elif len(stats) != 1 or stats[0]['packets'] != 1:
                raise ValueError('missing exact single-packet receipt at ' + event)
        equal(values('unexpected_padding_rtp_stats'), [])
        receipt = values('padding_stats_observation')
        if padding_only:
            if len(receipt) != 1:
                raise ValueError('missing padding stats observation')
            sample = receipt[0]
            equal(set(sample), {'samples', 'elapsed_ns', 'stats'})
            if type(sample['samples']) is not int or sample['samples'] <= 0:
                raise ValueError('padding stats were not sampled')
            if type(sample['elapsed_ns']) is not int or sample['elapsed_ns'] < row['input']['settled_ms'] * 1_000_000:
                raise ValueError('padding stats settled window too short')
            equal(sample['stats'], None)
        else:
            equal(receipt, [])
        names = [e['event'] for e in selected]
        ordered = ['connected_before_rtp', 'quiet_start', 'quiet_counts', 'rtp_packet_hex',
                   'on_track_published_track', 'received_rtp_stats']
        if padding_only:
            ordered.append('padding_stats_observation')
        ordered += ['settled_track', 'final_counts', 'participant_closed', 'peer_closed']
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


def evidence_controls():
    """Synthetic validator unit inputs ONLY; never Go receipts or parity evidence."""
    _, expected = fixtures()
    tested = 0
    for row in expected:
        padding_only = row['case'].startswith('c66-')
        events = []
        def add(name, value):
            events.append(dict(case=row['case'], event=name, value=value))
        for name in ('participant_socket_closed', 'peer_socket_closed', 'participant_mdns_disabled',
                     'peer_mdns_disabled', 'request_immutable', 'final_request_immutable'):
            add(name, True)
        add('input', row['input'])
        add('connected_before_rtp', 'connected')
        add('quiet_start', True)
        add('quiet_counts', dict(publications=0, published_tracks=0, rtp_writes=0, elapsed_ns=100_000_000))
        ids = {'urn:ietf:params:rtp-hdrext:sdes:mid': 1,
               'urn:ietf:params:rtp-hdrext:sdes:rtp-stream-id': 2}
        add('negotiated_extensions', ids)
        for name, direction in [('offer', 'send'), ('answer', 'recv')]:
            add(name, dict(sdp='\r\n'.join([
                'a=rid:q ' + direction, 'a=rid:f ' + direction,
                'a=simulcast:' + direction + ' q;f', 'a=mid:0', 'a=rtpmap:96 VP8/90000',
                *['a=extmap:' + str(id_) + ' ' + uri for uri, id_ in ids.items()]])))
        # Independent literal packet for extension IDs 1/2. No packet is sent.
        packet = ('b0' if padding_only else '90') + ('e0' if row['input']['rtp']['marker'] else '60')
        packet += '03e800015f9000067932bede000110302071'
        packet += '01' if padding_only else '1001000000'
        add('rtp_packet_hex', packet)
        add('rtp_write_result', dict(success=True, error='<nil>'))
        add('on_track_published_track', dict(row['output']['callback'], sid='synthetic-only', version='1'))
        add('on_track_published_rtp_writes', 1)
        add('received_rtp_stats', None if padding_only else dict(packets=1))
        if padding_only:
            add('padding_stats_observation', dict(samples=11, elapsed_ns=100_000_000, stats=None))
        add('settled_track', dict(row['output']['settled'], sid='synthetic-only', version='1'))
        add('settled_counts', dict(publications=1, published_tracks=1, rtp_writes=1, idle_rtp_writes=0))
        add('settled_rtp_stats', None if padding_only else dict(packets=1))
        add('final_counts', dict(publications=1, published_tracks=1, rtp_writes=1,
                                 rtp_write_successes=1, idle_rtp_writes=0))
        add('participant_closed', True)
        add('peer_closed', True)
        equal(validate_events([row], events), 1)

        def reject(mutated, label):
            nonlocal tested
            try:
                validate_events([row], mutated)
            except ValueError:
                tested += 1
            else:
                raise ValueError('accepted raw evidence corruption: ' + row['case'] + '.' + label)

        def replace(name, value):
            mutated = copy.deepcopy(events)
            next(e for e in mutated if e['event'] == name)['value'] = value
            reject(mutated, name)

        for name in ('quiet_counts', 'settled_counts', 'final_counts'):
            original = next(e['value'] for e in events if e['event'] == name)
            for key, value in original.items():
                replacement = 99_999_999 if key == 'elapsed_ns' else value + 1
                replace(name, dict(original, **{key: replacement}))
        for writes in (0, 2, True):
            replace('on_track_published_rtp_writes', writes)
        replace('rtp_write_result', dict(success=False, error='write failed'))
        replace('rtp_write_result', dict(success=True, error='write failed'))
        for name in ('received_rtp_stats', 'settled_rtp_stats'):
            variants = [dict(packets=1), dict(packets=1, padding_packets=1), {}] if padding_only else [dict(packets=0), dict(packets=2)]
            for value in variants:
                replace(name, value)
        for offset, mask in ((0, 0x20), (1, 0x80), (16, 0x10), (17, 1), (18, 0x10), (19, 1)):
            raw = bytearray.fromhex(packet)
            raw[offset] ^= mask
            replace('rtp_packet_hex', raw.hex())
        if padding_only:
            for suffix in ('', '00', '02', '0002', '00000004', '100100000001'):
                replace('rtp_packet_hex', packet[:-2] + suffix)
            receipt = next(e['value'] for e in events if e['event'] == 'padding_stats_observation')
            for samples in (0, -1, True, '11'):
                replace('padding_stats_observation', dict(receipt, samples=samples))
            for elapsed in (99_999_999, True, '100000000'):
                replace('padding_stats_observation', dict(receipt, elapsed_ns=elapsed))
            for stats in ({}, dict(packets=0), dict(packets=1, padding_packets=1)):
                replace('padding_stats_observation', dict(receipt, stats=stats))
            # The sample count is diagnostic, not a deterministic output literal.
            diagnostic = copy.deepcopy(events)
            next(e for e in diagnostic if e['event'] == 'padding_stats_observation')['value'] = dict(
                receipt, samples=17, elapsed_ns=110_000_000)
            equal(validate_events([row], diagnostic), 1)
            for name in ('padding_stats_observation', 'received_rtp_stats', 'settled_rtp_stats'):
                reject([e for e in events if e['event'] != name], 'missing-' + name)
                reject(events + [next(e for e in events if e['event'] == name)], 'duplicate-' + name)
        prepublication = copy.deepcopy(events)
        callback = next(e for e in prepublication if e['event'] == 'on_track_published_track')
        prepublication.remove(callback)
        prepublication.insert(0, callback)
        reject(prepublication, 'prepublication-order')
    return tested


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--input', type=Path)
    parser.add_argument('--self-test', action='store_true')
    args = parser.parse_args()
    result = {}
    if args.self_test:
        result['rejected_corruptions'] = controls()
        result['rejected_raw_corruptions'] = evidence_controls()
    if args.input:
        result['validated_rows'] = validate(args.input)
    if not result:
        parser.error('--input or --self-test required')
    print(json.dumps(result, sort_keys=True))
