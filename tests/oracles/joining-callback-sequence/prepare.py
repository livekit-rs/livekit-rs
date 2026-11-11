#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Stage the pinned Go server tree plus the joining-callback observer.

Verifies every fixed-source.json sha256 BEFORE staging, requires a fresh
output directory, never executes Go, never modifies references. Appends the
three local replaces and writes the sorted+deduped union go.sum of all four
modules, then records STAGED.json in the output directory.
"""
from pathlib import Path
import argparse
import hashlib
import json
import shutil

p = argparse.ArgumentParser()
p.add_argument('--root', type=Path, required=True)
p.add_argument('--out', type=Path, required=True)
a = p.parse_args()
here = Path(__file__).resolve().parent

manifest = json.loads((here / 'fixed-source.json').read_text())
for n, v in manifest.items():
    got = hashlib.sha256((a.root / n).read_bytes()).hexdigest()
    assert got == v, f'sha256 mismatch for {n}: {got} != {v}'

assert not a.out.exists(), f'output directory must be fresh: {a.out}'
shutil.copytree(a.root / 'references/livekit-stable', a.out,
                ignore=shutil.ignore_patterns('.git'))
for n in ['joining_callback_observer_test.go', 'source_functional_observer_init_test.go']:
    shutil.copy2(here / n, a.out / 'pkg/rtc' / n)
shutil.copy2(here / 'stages.json', a.out / 'joining-callback-stages.json')

replaces = [
    ('github.com/livekit/protocol', 'references/protocol-stable'),
    ('github.com/livekit/mediatransportutil', 'references/go-modules/mediatransportutil-f234b534b095'),
    ('github.com/livekit/psrpc', 'references/psrpc'),
]
f = a.out / 'go.mod'
mod = f.read_text()
resolved = {}
for name, rel in replaces:
    target = str((a.root / rel).resolve())
    resolved[name] = target
    mod += '\nreplace ' + name + ' => ' + json.dumps(target) + '\n'
f.write_text(mod)

sums = set()
for d in [a.out, a.root / 'references/protocol-stable', a.root / 'references/psrpc',
          a.root / 'references/go-modules/mediatransportutil-f234b534b095']:
    sums.update((d / 'go.sum').read_text().splitlines())
(a.out / 'go.sum').write_text('\n'.join(sorted(sums)) + '\n')

staged = {
    'root': str(a.root.resolve()),
    'source_manifest_sha256': hashlib.sha256((here / 'fixed-source.json').read_bytes()).hexdigest(),
    'observer_sha256': hashlib.sha256((here / 'joining_callback_observer_test.go').read_bytes()).hexdigest(),
    'init_shim_sha256': hashlib.sha256((here / 'source_functional_observer_init_test.go').read_bytes()).hexdigest(),
    'stages_sha256': hashlib.sha256((here / 'stages.json').read_bytes()).hexdigest(),
    'replaces': resolved,
    'go_sum_union_lines': len(sums),
    'staged_go_mod_sha256': hashlib.sha256(f.read_bytes()).hexdigest(),
    'staged_go_sum_sha256': hashlib.sha256((a.out / 'go.sum').read_bytes()).hexdigest(),
}
(a.out / 'STAGED.json').write_text(json.dumps(staged, indent=2) + '\n')
print(json.dumps(staged, indent=2))