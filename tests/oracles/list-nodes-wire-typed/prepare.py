#!/usr/bin/env python3
"""Stage pinned module inputs; never execute Go or modify references.

Reuses the committed tests/oracles/list-nodes-wire/cases.jsonl verbatim so the
typed observer and the original wire observer are driven by one corpus.
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

    mod = (repository / 'references/livekit-stable/go.mod').read_text()
    mod = re.sub(r'^module [^\n]+', 'module livekit-rs-tests/list-nodes-wire-typed',
                 mod, count=1)
    mod += '\nreplace github.com/livekit/protocol => %s\n' % (
        repository / 'references/protocol-stable')
    (target / 'go.mod').write_text(mod)

    # Exact original MVS graph; union sums only, no version rewriting.
    lines = set()
    for rel in ('references/livekit-stable/go.sum', 'references/protocol-stable/go.sum'):
        lines.update((repository / rel).read_text().splitlines())
    (target / 'go.sum').write_text('\n'.join(sorted(lines)) + '\n')

    shutil.copyfile(HERE / 'main.go', target / 'main.go')
    cases = repository / 'tests/oracles/list-nodes-wire/cases.jsonl'
    shutil.copyfile(cases, target / 'cases.jsonl')

    inputs = ['go.mod', 'go.sum', 'main.go', 'cases.jsonl']
    (target / 'STAGED.json').write_text(json.dumps({
        'status': 'STAGED_NOT_RUN',
        'source_manifest': sha(HERE / 'SOURCE.json'),
        'cases': len(cases.read_text().splitlines()),
        'inputs': {n: sha(target / n) for n in inputs},
    }, indent=2) + '\n')
    print(str(target))


if __name__ == '__main__':
    main()