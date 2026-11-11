#!/usr/bin/env python3
"""Stage an oracle with exact reference module locks; never edit reference trees."""
import argparse
import hashlib
import json
import shutil
import subprocess
from pathlib import Path

p = argparse.ArgumentParser()
p.add_argument('--repository', required=True)
p.add_argument('--oracle', required=True)
p.add_argument('--out', required=True)
a = p.parse_args()
r, source, out = (Path(v).resolve() for v in (a.repository, a.oracle, a.out))
locked = json.loads((r/'references/0001-references-lock.json').read_text())
target = {row['path'].removeprefix('references/'): row['checkout_commit']
          for row in locked['repositories']}
target = {'server': 'v1.13.10', 'commit': target['livekit-stable'], 'protocol': target['protocol-stable']}
for name, expected in [('livekit-stable', target['commit']), ('protocol-stable', target['protocol'])]:
    ref = r/'references'/name
    actual = subprocess.run(['git', '-C', str(ref), 'rev-parse', 'HEAD'], capture_output=True, text=True, check=True).stdout.strip()
    if actual != expected:
        raise SystemExit('Reference commit drift: '+name)
    subprocess.run(['git', '-C', str(ref), 'diff', '--quiet', 'HEAD', '--'], check=True)
manifest = json.loads((source/'SOURCE.json').read_text())
sha = lambda path: hashlib.sha256(path.read_bytes()).hexdigest()
for name, expected in manifest['inputs'].items():
    if sha(source/name) != expected:
        raise SystemExit('Oracle input drift: '+name)
if out.is_relative_to(r/'references') or out.is_relative_to(source):
    raise SystemExit('Output must not modify oracle/reference inputs')
out.mkdir(mode=0o700, parents=True, exist_ok=False)
mod = (r/'references/livekit-stable/go.mod').read_text()
header = 'module github.com/livekit/livekit-server\n'
assert mod.count(header) == 1
mod = mod.replace(header, 'module local.test/livekit-fixed-source-oracle\n', 1)
mod += '\nrequire github.com/livekit/livekit-server v0.0.0\n'
mod += 'replace github.com/livekit/livekit-server => '+json.dumps(str(r/'references/livekit-stable'), ensure_ascii=False)+'\n'
mod += 'replace github.com/livekit/protocol => '+json.dumps(str(r/'references/protocol-stable'), ensure_ascii=False)+'\n'
(out/'go.mod').write_text(mod)
shutil.copyfile(r/'references/livekit-stable/go.sum', out/'go.sum')
for name in ['main.go', 'cases.jsonl']:
    if (source/name).is_file():
        shutil.copyfile(source/name, out/name)
(out/'STAGED.json').write_text(json.dumps({'status':'STAGED_NOT_EXECUTED','target':target,'oracle_manifest_sha256':sha(source/'SOURCE.json'),'inputs':{v.name:sha(v) for v in out.iterdir() if v.is_file()}}, indent=2)+'\n')
print(out)
