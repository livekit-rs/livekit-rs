#!/usr/bin/env python3
import argparse
import hashlib
import json
from pathlib import Path

PARTICIPANT = 'references/livekit-stable/pkg/rtc/participant.go'
PARTICIPANT_SHA256 = 'f715f1a4b2e354cfa17b23e9f688f17f3a79644a5ebe3696578b2796ec885fd8'


def project(rows):
    for row in rows:
        for event in row['events']:
            for codec in event['codecs']:
                name = codec['codec']
                if name not in ('video/VP8', 'video/VP9', 'video/H264'):
                    raise ValueError('unexpected raw manager codec: ' + name)
                codec['codec'] = name.removeprefix('video/').lower()
            event['codecs'].sort(key=lambda codec: codec['codec'])
    return rows


def main():
    parser = argparse.ArgumentParser(description='Explicit pinned participant.go:2935-2938 name projection; raw manager snapshots remain unchanged.')
    parser.add_argument('--fixed-source-root', type=Path, required=True)
    parser.add_argument('--input', type=Path, required=True)
    parser.add_argument('--out', type=Path, required=True)
    args = parser.parse_args()
    source = args.fixed_source_root / PARTICIPANT
    if hashlib.sha256(source.read_bytes()).hexdigest() != PARTICIPANT_SHA256:
        raise ValueError('participant codec projection source drift')
    raw = args.input.read_bytes()
    rows = project([json.loads(line) for line in raw.splitlines()])
    content = ('\n'.join(json.dumps(row) for row in rows) + '\n').encode()
    with args.out.open('xb') as output:
        output.write(content)
    receipt = {
        'scope': 'Name-only boundary adapter from raw Go manager callbacks to participant signal names; no execution of ParticipantImpl or MAIN claimed',
        'source': PARTICIPANT, 'source_lines': [2935, 2938], 'source_sha256': PARTICIPANT_SHA256,
        'raw_sha256': hashlib.sha256(raw).hexdigest(),
        'projected_sha256': hashlib.sha256(content).hexdigest(),
        'mapping': {'video/VP8': 'vp8', 'video/VP9': 'vp9', 'video/H264': 'h264'},
        'unchanged': ['case order', 'event order and multiplicity', 'quality order', 'quality numbers', 'enabled booleans'],
    }
    with args.out.with_suffix('.projection.json').open('x') as output:
        json.dump(receipt, output, indent=2)
        output.write('\n')
    assert args.input.read_bytes() == raw
    print(json.dumps(receipt))


if __name__ == '__main__':
    main()
