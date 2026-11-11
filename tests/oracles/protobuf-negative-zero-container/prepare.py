#!/usr/bin/env python3
"""Stage pinned module inputs; never execute Go or modify references.

The generated .pb.go is produced later by the pinned protoc-gen-go, not here.
"""
import argparse
import hashlib
import json
import re
import shutil
from pathlib import Path

HERE = Path(__file__).resolve().parent


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--repository', required=True)
    parser.add_argument('--out', required=True)
    args = parser.parse_args()
    repository = Path(args.repository).resolve()
    target = Path(args.out).resolve()
    if target.exists():
        raise SystemExit('fresh stage only: ' + str(target))
    target.mkdir(parents=True)

    manifest = json.loads((HERE / 'SOURCE.json').read_text())
    for rel, want in manifest['source_inputs_sha256'].items():
        if sha(repository / rel) != want:
            raise SystemExit('Source drift: ' + rel)

    cases = json.loads((HERE / 'cases.json').read_text())
    if len(cases) != 4:
        raise SystemExit('the Rust consumer asserts exactly 4 rows, corpus has %d' % len(cases))
    if len({c['name'] for c in cases}) != 4:
        raise SystemExit('case names must be unique')

    mod = (repository / 'references/livekit-stable/go.mod').read_text()
    mod = re.sub(r'^module [^\n]+', 'module local.test/protobuf-negative-zero-container',
                 mod, count=1)
    (target / 'go.mod').write_text(mod)
    lines = set()
    for rel in ('references/livekit-stable/go.sum', 'references/protocol-stable/go.sum'):
        lines.update((repository / rel).read_text().splitlines())
    (target / 'go.sum').write_text('\n'.join(sorted(lines)) + '\n')

    for name in ('main.go', 'cases.json', 'float_containers.proto'):
        shutil.copyfile(HERE / name, target / name)

    inputs = ['go.mod', 'go.sum', 'main.go', 'cases.json', 'float_containers.proto']
    (target / 'STAGED.json').write_text(json.dumps({
        'status': 'STAGED_NOT_RUN',
        'source_manifest': sha(HERE / 'SOURCE.json'),
        'cases': len(cases),
        'inputs': {n: sha(target / n) for n in inputs},
    }, indent=2) + '\n')
    print(str(target))


if __name__ == '__main__':
    main()