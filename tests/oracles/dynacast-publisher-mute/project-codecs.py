#!/usr/bin/env python3
import argparse
import copy
import hashlib
import json
from pathlib import Path
import sys

sys.dont_write_bytecode = True
from validate import load, require

PARTICIPANT = 'references/livekit-stable/pkg/rtc/participant.go'
PARTICIPANT_SHA256 = 'f715f1a4b2e354cfa17b23e9f688f17f3a79644a5ebe3696578b2796ec885fd8'


def project(rows):
    rows = copy.deepcopy(rows)
    for row in rows:
        for observation in row['observations']:
            for event in observation['events']:
                for codec in event['codecs']:
                    name = codec['codec']
                    if name not in ('video/VP8', 'video/VP9'):
                        raise ValueError('unexpected raw manager codec: ' + name)
                    codec['codec'] = name.removeprefix('video/').lower()
            for layer in observation['expected_layers']:
                name = layer['codec']
                require(name in ('video/VP8', 'video/VP9'), 'unexpected raw receiver codec')
                layer['codec'] = name.removeprefix('video/').lower()
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
    require(args.out.is_absolute() and args.out.parent.is_dir(), 'absolute output with existing parent required')
    destination = args.out.resolve()
    require(destination.is_relative_to(Path('/tmp').resolve()) and
            any(p.startswith('c58-go-') for p in destination.parts), 'output must be task-owned /tmp/c58-go-*')
    require(not args.out.exists() and not args.out.is_symlink() and
            not args.out.with_suffix('.projection.json').exists(), 'projection outputs must be fresh')
    raw = args.input.read_bytes()
    baseline = load(raw)
    rows = project(baseline)
    restored = copy.deepcopy(rows)
    changed_names = 0
    for row in restored:
        for observation in row['observations']:
            fields = observation['expected_layers'] + [codec for event in observation['events'] for codec in event['codecs']]
            for field in fields:
                field['codec'] = {'vp8': 'video/VP8', 'vp9': 'video/VP9'}[field['codec']]
                changed_names += 1
    require(json.dumps(restored, sort_keys=True) == json.dumps(baseline, sort_keys=True), 'projection changed more than codec names')
    content = ('\n'.join(json.dumps(row) for row in rows) + '\n').encode()
    with args.out.open('xb') as output:
        output.write(content)
    receipt = {
        'scope': 'Name-only adapter on publisher events and receiver expected_layers, using pinned participant signal naming; no ParticipantImpl or MAIN execution claimed',
        'source': PARTICIPANT, 'source_lines': [2935, 2938], 'source_sha256': PARTICIPANT_SHA256,
        'raw_sha256': hashlib.sha256(raw).hexdigest(),
        'projected_sha256': hashlib.sha256(content).hexdigest(),
        'mapping': {'video/VP8': 'vp8', 'video/VP9': 'vp9'},
        'name_fields_projected': changed_names,
        'name_only_roundtrip_verified': True,
        'unchanged': ['scenario order', 'codec_count', 'pause_ms', 'at_ms chronology', 'empty observations', 'event order and multiplicity', 'quality order', 'quality numbers', 'enabled booleans', 'expected_layers order and count', 'spatial values'],
    }
    with args.out.with_suffix('.projection.json').open('x') as output:
        json.dump(receipt, output, indent=2)
        output.write('\n')
    assert args.input.read_bytes() == raw
    print(json.dumps(receipt))


if __name__ == '__main__':
    main()
