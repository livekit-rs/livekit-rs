#!/usr/bin/env python3
"""Stage pinned module inputs; never execute Go or modify references."""
import argparse, hashlib, json, shutil
from pathlib import Path
p=argparse.ArgumentParser(); p.add_argument('--repository',required=True); p.add_argument('--out',required=True); a=p.parse_args()
r=Path(a.repository).resolve(); target=Path(a.out).resolve(); target.mkdir(exist_ok=False)
here=Path(__file__).resolve().parent
expected=json.loads((here/'SOURCE.json').read_text())['source_sha256']
for rel, want in expected.items():
    actual=hashlib.sha256((r/rel).read_bytes()).hexdigest()
    if actual != want: raise SystemExit('Source drift: '+rel)
mod=(r/'references/livekit-stable/go.mod').read_text()
needle='module github.com/livekit/livekit-server\n'
assert mod.count(needle)==1
mod=mod.replace(needle,'module local.test/livekit-owner-metadata-errors\n',1)
mod+='\nrequire github.com/livekit/livekit-server v0.0.0\n'
mod+='replace github.com/livekit/livekit-server => '+str(r/'references/livekit-stable')+'\n'
mod+='replace github.com/livekit/protocol => '+str(r/'references/protocol-stable')+'\n'
(target/'go.mod').write_text(mod)
shutil.copyfile(r/'references/livekit-stable/go.sum',target/'go.sum')
shutil.copyfile(here/'main.go',target/'main.go')
(target/'STAGED.json').write_text(json.dumps({'status':'STAGED_NOT_RUN','source_manifest':hashlib.sha256((here/'SOURCE.json').read_bytes()).hexdigest(),'inputs':{n:hashlib.sha256((target/n).read_bytes()).hexdigest() for n in ['go.mod','go.sum','main.go']}},indent=2)+'\n')
print(str(target))
