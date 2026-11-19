#!/usr/bin/env python3
"""Compare actual native Room callbacks; independent transports, not MAIN wire parity."""
import argparse
import copy
import json
from pathlib import Path

from validate import decode, equal, validate


def check_rows(rows, go_rows, padding=False):
    equal(len(rows), 8)
    expected_cases = {(duplicate, marker, late) for duplicate in (False, True)
                      for marker in (False, True) for late in (False, True)}
    seen = set()
    for row in rows:
        equal(set(row), {'duplicate', 'marker', 'late_bind', 'mid', 'sent', 'offer_sdp',
                        'answer_sdp', 'receiver', 'rejected_bad_auth', 'rtp', 'authenticated',
                        'registrations', 'media_started_events', 'media_started', 'visible', 'ack', 'published', 'settled'})
        for key in ('duplicate', 'marker', 'late_bind'):
            equal(type(row[key]).__name__, 'bool')
        case = tuple(row[k] for k in ('duplicate', 'marker', 'late_bind'))
        if case in seen:
            raise ValueError('duplicate native observation')
        seen.add(case)
        name = ('c66-' if padding else 'c65-') + ('low-low-high' if row['duplicate'] else 'low-high')
        if padding:
            name += '-marked-padding' if row['marker'] else '-unmarked-padding'
        else:
            name += '-complete' if row['marker'] else '-incomplete'
        source = next(go for go in go_rows if go['case'] == name)
        for key in ('sent', 'rejected_bad_auth'):
            equal(row[key], True, name + '.' + key)
        for key in ('authenticated', 'registrations', 'visible'):
            equal(row[key], 1, name + '.' + key)
        equal(row['receiver'], [424242, 0, 0 if padding else 1])
        started = row['marker'] and not row['late_bind'] and not padding
        equal(row['media_started_events'], int(started))
        equal(row['media_started'], started)
        equal(row['rtp'], dict(pt=96, ssrc=424242, sequence_number=1000, timestamp=90000,
                              payload_hex='' if padding else '1001000000', marker=row['marker'], rid='q'))
        mid = row['mid']
        if not isinstance(mid, str) or not mid or any(c.isspace() for c in mid):
            raise ValueError('invalid actual native MID')
        for key, direction in (('offer_sdp', 'send'), ('answer_sdp', 'recv')):
            lines = row[key].splitlines()
            equal([line for line in lines if line.startswith('a=mid:')], ['a=mid:' + mid])
            equal([line for line in lines if line.startswith('a=rid:')],
                  ['a=rid:q ' + direction, 'a=rid:f ' + direction])
            equal([line for line in lines if line.startswith('a=simulcast:')],
                  ['a=simulcast:' + direction + ' q;f'])
            equal([line for line in lines if line.startswith('a=rtpmap:96 ')], ['a=rtpmap:96 VP8/90000'])
        sids = set()
        for native, original in (('ack', 'ack'), ('published', 'callback'), ('settled', 'settled')):
            equal(len(row[native]), 1)
            actual = copy.deepcopy(row[native][0])
            sid, version = actual.pop('sid'), actual.pop('version')
            if not isinstance(sid, str) or not sid.startswith('TR_'):
                raise ValueError('missing native track identity')
            if type(version['unix_micro']) is not int or version['unix_micro'] <= 0 or type(version['ticks']) is not int:
                raise ValueError('missing native version')
            equal(set(version), {'unix_micro', 'ticks'})
            sids.add(sid)
            expected = copy.deepcopy(source['output'][original])
            if native != 'ack':
                equal(expected['mid'], source['input']['sdp_mid'])
                equal(expected['codecs'][0]['mid'], source['input']['sdp_mid'])
                expected['mid'] = mid
                expected['codecs'][0]['mid'] = mid
            equal(actual, expected, name + '.' + native)
        equal(len(sids), 1)
    equal(seen, expected_cases)
    return len(rows)


def check_rebind(rows, initial):
    equal(len(rows), 8)
    seen = set()
    for row in rows:
        equal(set(row), {'duplicate', 'marker', 'late_bind', 'before', 'after'})
        case = tuple(row[k] for k in ('duplicate', 'marker', 'late_bind'))
        if case in seen:
            raise ValueError('duplicate rebind observation')
        seen.add(case)
        source = next(r for r in initial if tuple(r[k] for k in ('duplicate', 'marker', 'late_bind')) == case)
        equal(row['after'], row['before'], 'rebind snapshot including version')
        expected = copy.deepcopy(source['settled'])
        expected[0]['layers'][-1]['ssrc'] = 434343
        expected[0]['codecs'][0]['layers'][-1]['ssrc'] = 434343
        version = row['before'][0]['version']
        previous = expected[0]['version']
        if (version['unix_micro'], version['ticks']) <= (previous['unix_micro'], previous['ticks']):
            raise ValueError('new RID binding must advance version')
        expected[0]['version'] = version
        equal(row['before'], expected, 'second authenticated RID')
    equal(seen, {tuple(r[k] for k in ('duplicate', 'marker', 'late_bind')) for r in initial})
    return len(rows)


def check_padding_phases(wire, recovery, initial):
    import struct
    expected = {tuple(r[k] for k in ('duplicate', 'marker', 'late_bind')) for r in initial}
    for rows, kind in ((wire, 'wire'), (recovery, 'recovery')):
        equal(len(rows), 8)
        seen = set()
        for row in rows:
            for key in ('duplicate', 'marker', 'late_bind'):
                equal(type(row[key]).__name__, 'bool')
            case = tuple(row[k] for k in ('duplicate', 'marker', 'late_bind'))
            if case in seen:
                raise ValueError('duplicate padding ' + kind)
            seen.add(case)
            if kind == 'wire':
                equal(set(row), {'duplicate', 'marker', 'late_bind', 'packets'})
                equal(len(row['packets']), 1)
                packet = row['packets'][0]
                equal(set(packet), {'header_hex', 'encrypted_len'})
                equal(packet['header_hex'], struct.pack('!BBHII', 0xb0, 96 | (128 if row['marker'] else 0), 1000, 90000, 424242).hex())
                if type(packet['encrypted_len']) is not int or packet['encrypted_len'] <= 12:
                    raise ValueError('missing encrypted padding datagram')
            else:
                equal(set(row), {'duplicate', 'marker', 'late_bind', 'media_started', 'media_started_events'})
                equal(row['media_started'], True)
                equal(row['media_started_events'], 1)
        equal(seen, expected)


def controls(rows, go_rows, padding=False):
    mutations = [
        ('duplicate-binding-fanout', lambda r: r[next(i for i, v in enumerate(r) if v['duplicate'])]['published'][0]['layers'][1].update(ssrc=424242)),
        ('dropped-callback', lambda r: r[0].update(published=[])),
        ('wrong-native-mid', lambda r: r[0]['published'][0].update(mid='wrong')),
        ('complete-frame-event-for-fragment', lambda r: r[next(i for i, v in enumerate(r) if not v['marker'])].update(media_started_events=1)),
        ('unauthenticated-publication', lambda r: r[0].update(rejected_bad_auth=False)),
        ('multiple-native-packets', lambda r: r[0].update(receiver=[424242, 0, 2])),
        ('settled-repair-drift', lambda r: r[0]['settled'][0]['codecs'][0]['layers'][0].update(repair_ssrc=424243)),
        ('lost-late-binding', lambda r: r[next(i for i, v in enumerate(r) if v['late_bind'])].update(registrations=0)),
    ]
    if padding:
        mutations.extend([
            ('padding-counted-as-media', lambda r: r[0].update(receiver=[424242, 0, 1])),
            ('marker-started-padding-media', lambda r: r[next(i for i, v in enumerate(r) if v['marker'] and not v['late_bind'])].update(media_started=True, media_started_events=1)),
            ('padding-carried-media-payload', lambda r: r[0]['rtp'].update(payload_hex='1001000000')),
        ])
    rejected = []
    for name, mutate in mutations:
        changed = copy.deepcopy(rows)
        mutate(changed)
        try:
            check_rows(changed, go_rows, padding)
        except ValueError as error:
            rejected.append({'name': name, 'rejection': str(error)})
        else:
            raise ValueError('accepted semantic corruption: ' + name)
    return rejected


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--go', type=Path, required=True)
    parser.add_argument('--rust-stderr', type=Path, required=True)
    args = parser.parse_args()
    go_count = validate(args.go)
    go_rows = [decode(line) for line in args.go.read_text().splitlines()]
    lines = args.rust_stderr.read_text().splitlines()

    def records(prefix):
        return [decode(line[len(prefix):]) for line in lines if line.startswith(prefix)]

    batches = {}
    for batch, padding in (('C65', False), ('C66', True)):
        rows = records(batch + '_RID_NATIVE ')
        count = check_rows(rows, go_rows, padding)
        rebind = records(batch + '_RID_REBIND ')
        rebind_count = check_rebind(rebind, rows)
        rejected = controls(rows, go_rows, padding)
        broken = copy.deepcopy(rebind)
        broken[0]['after'][0]['layers'][-1]['ssrc'] = 0
        try:
            check_rebind(broken, rows)
        except ValueError as error:
            rejected.append({'name': 'stale-rebind-snapshot', 'rejection': str(error)})
        else:
            raise ValueError('accepted stale rebind metadata')
        if padding:
            wire, recovery = records('C66_RID_WIRE '), records('C66_RID_RECOVERY ')
            check_padding_phases(wire, recovery, rows)
            for name in ('missing-padding-bit', 'duplicate-padding-datagram', 'missing-media-recovery', 'duplicate-wire-row'):
                bad_wire, bad_recovery = copy.deepcopy(wire), copy.deepcopy(recovery)
                if name == 'missing-padding-bit':
                    bad_wire[0]['packets'][0]['header_hex'] = '90' + bad_wire[0]['packets'][0]['header_hex'][2:]
                elif name == 'duplicate-padding-datagram':
                    bad_wire[0]['packets'] *= 2
                elif name == 'missing-media-recovery':
                    bad_recovery[0]['media_started'] = False
                else:
                    bad_wire[1] = copy.deepcopy(bad_wire[0])
                try:
                    check_padding_phases(bad_wire, bad_recovery, rows)
                except ValueError as error:
                    rejected.append({'name': name, 'rejection': str(error)})
                else:
                    raise ValueError('accepted padding corruption: ' + name)
        batches[batch] = dict(native_rows=count, native_rebind_rows=rebind_count, negative_controls=rejected)
    print(json.dumps({'go_rows': go_count, 'batches': batches,
                      'scope': 'actual native component callback semantics; negotiated MID bijection, not identical SDP/RTP wire or launched MAIN'}, indent=2))
