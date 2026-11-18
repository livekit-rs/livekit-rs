#!/usr/bin/env python3
import argparse
import copy
import hashlib
import json
import os
from pathlib import Path
import subprocess

parser = argparse.ArgumentParser()
parser.add_argument('--binary', type=Path, required=True)
parser.add_argument('--go-results', type=Path, required=True)
parser.add_argument('--out', type=Path, required=True)
args = parser.parse_args()
assert not args.out.exists() and args.out.parent.is_dir()
args.out.mkdir()
binary = args.binary.resolve(strict=True)
original = args.go_results.read_bytes()
rows = [json.loads(line) for line in original.splitlines()]
encode = lambda data: ('\n'.join(json.dumps(row) for row in data) + '\n').encode()
variants = [('valid', original, 0), ('empty', b'', 101),
            ('missing-row', encode(rows[:-1]), 101), ('duplicate-row', encode(rows + [rows[0]]), 101),
            ('reordered-rows', encode(rows[::-1]), 101)]


def mutate(name, operation):
    changed = copy.deepcopy(rows)
    operation(changed)
    variants.append((name, encode(changed), 101))


for key in ['codec_count', 'batch', 'events']:
    mutate('missing-' + key, lambda data, key=key: data[0].pop(key))
mutate('wrong-count', lambda data: data[0].update(codec_count=2))
mutate('wrong-batch', lambda data: data[0].update(batch=True))
mutate('unknown-field', lambda data: data[0].update(extra=True))
mutate('wrong-type', lambda data: data[0].update(batch=0))
mutate('no-events', lambda data: data[3].update(events=[]))
mutate('missing-event', lambda data: data[3]['events'].pop(0))
mutate('duplicate-event', lambda data: data[3]['events'].append(copy.deepcopy(data[3]['events'][0])))
mutate('reverse-events', lambda data: data[3]['events'].reverse())
mutate('missing-codecs', lambda data: data[3]['events'][0].pop('codecs'))
mutate('missing-codec', lambda data: data[3]['events'][0]['codecs'].pop(1))
mutate('duplicate-codec', lambda data: data[3]['events'][0]['codecs'].append(copy.deepcopy(data[3]['events'][0]['codecs'][0])))
for key in ['codec', 'qualities']:
    mutate('missing-codec-' + key, lambda data, key=key: data[3]['events'][0]['codecs'][0].pop(key))
mutate('wrong-codec', lambda data: data[3]['events'][0]['codecs'][0].update(codec='av1'))
mutate('missing-quality', lambda data: data[3]['events'][0]['codecs'][0]['qualities'].pop())
for key in ['quality', 'enabled']:
    mutate('missing-quality-' + key, lambda data, key=key: data[3]['events'][0]['codecs'][0]['qualities'][0].pop(key))
mutate('wrong-quality', lambda data: data[3]['events'][0]['codecs'][0]['qualities'][0].update(quality=77))
for index in range(3):
    mutate('changed-enabled-' + str(index), lambda data, index=index: data[3]['events'][0]['codecs'][0]['qualities'][index].update(enabled=False))
variants.append(('duplicate-json-key', encode(rows).replace(b'"batch": false', b'"batch": false, "batch": false', 1), 101))
results = []
valid_output = None
for name, contents, expected in variants + [('existing-output', original, 101)]:
    source = args.out / (name + '.input.jsonl')
    output = args.out / (name + '.rust.jsonl')
    source.write_bytes(contents)
    if name == 'existing-output':
        output.write_bytes(b'preserve-existing-output\n')
    command = [str(binary), '--exact', 'regression::dynacast_notify::fixed_go_notification_snapshot_differential', '--ignored', '--nocapture']
    env = dict(os.environ, C55_GO_JSONL=str(source.resolve()), C55_RUST_JSONL=str(output.resolve()))
    run = subprocess.run(command, env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=30)
    (args.out / (name + '.log')).write_bytes(run.stdout)
    if name == 'valid':
        valid_output = output.read_bytes()
    output_independent = output.read_bytes() == (b'preserve-existing-output\n' if name == 'existing-output' else valid_output)
    results.append({'case': name, 'exit_code': run.returncode, 'expected_exit': expected,
                    'output_independent_of_reference': output_independent,
                    'passed': run.returncode == expected and b'running 1 test' in run.stdout and source.read_bytes() == contents and output_independent})
receipt = {'binary_sha256': hashlib.sha256(binary.read_bytes()).hexdigest(),
           'go_sha256': hashlib.sha256(original).hexdigest(), 'controls': results,
           'passed': all(row['passed'] for row in results) and args.go_results.read_bytes() == original}
(args.out / 'results.json').write_text(json.dumps(receipt, indent=2) + '\n')
print(json.dumps(receipt, indent=2))
raise SystemExit(0 if receipt['passed'] else 1)
