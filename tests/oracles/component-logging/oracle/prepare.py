#!/usr/bin/env python3
"""Stage actual source observer. Does not invoke a compiler or mutate references."""
import argparse, pathlib, shutil
p=argparse.ArgumentParser();p.add_argument('--repo',required=True);p.add_argument('--out',required=True);a=p.parse_args()
r=pathlib.Path(a.repo).resolve();out=pathlib.Path(a.out).resolve()
if out.exists(): raise SystemExit('output must be fresh')
out.mkdir(parents=True)
for name in ['go.mod','go.sum']: shutil.copyfile(r/'references/livekit-stable'/name,out/name)
# Keep exact fixed-server MVS and source package. No copied arithmetic or logger.
with (out/'go.mod').open('a') as f:
 f.write('\nreplace github.com/livekit/livekit-server => '+str(r/'references/livekit-stable')+'\n')
 f.write('replace github.com/livekit/protocol => '+str(r/'references/protocol-stable')+'\n')
# The staging module needs a distinct name to import real server/config rather
# than treating this empty observer directory as the whole server module.
s=(out/'go.mod').read_text().replace('module github.com/livekit/livekit-server','module logging-source-observer',1)
s+='\nrequire github.com/livekit/livekit-server v0.0.0\n';(out/'go.mod').write_text(s)
shutil.copyfile(pathlib.Path(__file__).with_name('main.go'),out/'main.go')
shutil.copyfile(pathlib.Path(__file__).with_name('cases.json'),out/'cases.json')
