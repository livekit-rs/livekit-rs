#!/usr/bin/env python3
"""Stage the pinned original Go server plus the DD padding physical observer.

Verifies every SOURCE.json hash, copies the whole reference tree (so all pinned
*_test.go helpers stay available), drops the observer into pkg/sfu with a
source_dd_padding_ prefix, and appends the two local replace directives.
Never executes Go and never modifies the references.
"""
import argparse, hashlib, json, shutil
from pathlib import Path

p = argparse.ArgumentParser()
p.add_argument('--root', required=True, type=Path)
p.add_argument('--out', required=True, type=Path)
a = p.parse_args()
root = a.root.resolve()
out = a.out.resolve()
here = Path(__file__).resolve().parent
ref = root / 'references/livekit-stable'
manifest = json.loads((here / 'SOURCE.json').read_text())
sha = lambda f: hashlib.sha256(f.read_bytes()).hexdigest()

for name, digest in manifest['original_inputs'].items():
    assert sha(ref / name) == digest, 'original input drift: ' + name
for name, digest in manifest['helpers'].items():
    assert sha(here / name) == digest, 'observer drift: ' + name
assert not out.exists(), 'fresh isolated stage required'
out.mkdir(parents=True)
shutil.copytree(ref, out / 'source', ignore=shutil.ignore_patterns('.git'))

for name in manifest['helpers']:
    shutil.copyfile(here / name, out / 'source/pkg/sfu' / ('source_dd_padding_' + name))

mod = out / 'source/go.mod'
mod.write_text(mod.read_text()
    + '\nreplace github.com/livekit/protocol => ' + json.dumps(str(root / 'references/protocol-stable'))
    + '\nreplace github.com/livekit/mediatransportutil => ' + json.dumps(str(root / 'references/go-modules/mediatransportutil-f234b534b095'))
    + '\n')
(out / 'STAGED.json').write_text(json.dumps({
    'status': 'STAGED_NOT_RUN',
    'source': manifest,
    'algorithm_substitutions': [],
    'go_mod_sha256': sha(mod),
}, indent=2) + '\n')
print(out)