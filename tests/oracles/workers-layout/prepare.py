#!/usr/bin/env python3
"""Stage pinned module inputs; never execute Go or modify references."""
import argparse
import hashlib
import json
import shutil
from pathlib import Path

HERE = Path(__file__).resolve().parent


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--repository', required=True)
    parser.add_argument('--out', required=True)
    args = parser.parse_args()
    repository = Path(args.repository).resolve()
    target = Path(args.out).resolve()
    target.mkdir(exist_ok=False)

    expected = json.loads((HERE / 'SOURCE.json').read_text())['source_sha256']
    for rel, want in expected.items():
        actual = hashlib.sha256((repository / rel).read_bytes()).hexdigest()
        if actual != want:
            raise SystemExit('Source drift: ' + rel)

    cases = json.loads((HERE / 'cases.json').read_text())
    if len(cases) != 7:
        raise SystemExit('the Rust consumer asserts exactly 7 rows, corpus has %d' % len(cases))
    if len({c['name'] for c in cases}) != 7:
        raise SystemExit('case names must be unique')

    mod = (repository / 'references/livekit-stable/go.mod').read_text()
    needle = 'module github.com/livekit/livekit-server\n'
    if mod.count(needle) != 1:
        raise SystemExit('unexpected module line in the pinned server go.mod')
    mod = mod.replace(needle, 'module local.test/livekit-workers-layout\n', 1)
    mod += '\nrequire github.com/livekit/livekit-server v0.0.0\n'
    mod += 'replace github.com/livekit/livekit-server => %s\n' % (repository / 'references/livekit-stable')
    mod += 'replace github.com/livekit/protocol => %s\n' % (repository / 'references/protocol-stable')
    (target / 'go.mod').write_text(mod)
    shutil.copyfile(repository / 'references/livekit-stable/go.sum', target / 'go.sum')
    shutil.copyfile(HERE / 'main.go', target / 'main.go')
    shutil.copyfile(HERE / 'cases.json', target / 'cases.json')

    inputs = ['go.mod', 'go.sum', 'main.go', 'cases.json']
    (target / 'STAGED.json').write_text(json.dumps({
        'status': 'STAGED_NOT_RUN',
        'source_manifest': hashlib.sha256((HERE / 'SOURCE.json').read_bytes()).hexdigest(),
        'cases': len(cases),
        'inputs': {n: hashlib.sha256((target / n).read_bytes()).hexdigest() for n in inputs},
    }, indent=2) + '\n')
    print(str(target))


if __name__ == '__main__':
    main()