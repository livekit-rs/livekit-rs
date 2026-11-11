#!/usr/bin/env python3
"""Stage the ordinary-data-writer Go oracle into a fresh directory.

Verifies every pinned manifest sha256 (upstream sources, oracle inputs, and the
COMMITTED/IMMUTABLE corpus) before staging. Copies the two upstream source files
with exactly two textual transforms: delete the protocol/utils/mono import and
replace mono.Now()/time.Now() with sourceNow(). Never executes Go and never
modifies the pinned references or the committed corpus. Writes STAGED.json.
"""
import argparse, hashlib, json, shutil
from pathlib import Path

p = argparse.ArgumentParser()
p.add_argument('--root', type=Path, required=True)
p.add_argument('--out', type=Path, required=True)
a = p.parse_args()
here = Path(__file__).resolve().parent
s = json.loads((here / 'SOURCE.json').read_text())
sha = lambda f: hashlib.sha256(f.read_bytes()).hexdigest()

for name, h in s['source_sha256'].items():
    assert sha(a.root / name) == h, 'SOURCE_DRIFT ' + name
for name, h in s['oracle_inputs_sha256'].items():
    assert sha(here / name) == h, 'INPUT_DRIFT ' + name
for name, h in s['corpus_sha256'].items():
    assert sha(a.root / name) == h, 'CORPUS_DRIFT ' + name

assert not a.out.exists(), 'output directory must be fresh: ' + str(a.out)
a.out.mkdir(mode=0o700)
(a.out / 'source').mkdir()

for name in ['datachannel_writer.go', 'bitrate.go']:
    text = (a.root / 'references/livekit-stable/pkg/sfu/datachannel' / name).read_text()
    assert '"github.com/livekit/protocol/utils/mono"' in text, 'MONO_IMPORT_MISSING ' + name
    text = text.replace('"github.com/livekit/protocol/utils/mono"', '')
    text = text.replace('mono.Now()', 'sourceNow()').replace('time.Now()', 'sourceNow()')
    assert 'mono.Now()' not in text and 'time.Now()' not in text and 'utils/mono' not in text, 'TRANSFORM_INCOMPLETE ' + name
    (a.out / 'source' / name).write_text(text)

shutil.copyfile(here / 'oracle/driver.go', a.out / 'source/driver.go')
shutil.copyfile(here / 'oracle/main.go', a.out / 'main.go')
for name in s['corpus_sha256']:
    shutil.copyfile(a.root / name, a.out / Path(name).name)

mod = (a.root / 'references/livekit-stable/go.mod').read_text().replace(
    'module github.com/livekit/livekit-server', 'module local.test/livekit-ordinary-writer-oracle', 1)
mod += '\nreplace github.com/livekit/protocol => ' + json.dumps(str((a.root / 'references/protocol-stable').resolve())) + '\n'
(a.out / 'go.mod').write_text(mod)
shutil.copyfile(a.root / 'references/livekit-stable/go.sum', a.out / 'go.sum')
shutil.copyfile(here / 'LICENSE-UPSTREAM', a.out / 'LICENSE-UPSTREAM')

(a.out / 'STAGED.json').write_text(json.dumps({
    'status': 'STAGED_NOT_EXECUTED',
    'source_manifest': sha(here / 'SOURCE.json'),
    'changes': 'clock calls only replaced with explicit supplied Instant-equivalent clock; package source logic unchanged',
    'files': {str(f.relative_to(a.out)): sha(f) for f in sorted(a.out.rglob('*')) if f.is_file()},
}, indent=2) + '\n')
print(a.out)