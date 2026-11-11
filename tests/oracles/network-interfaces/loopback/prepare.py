#!/usr/bin/env python3
"""Verify immutable Go/source inputs and restore one new owned stage; no execution."""
import argparse,hashlib,json,pathlib,shutil
p=argparse.ArgumentParser();p.add_argument('--root',required=True,type=pathlib.Path);p.add_argument('--out',required=True,type=pathlib.Path);a=p.parse_args()
here=pathlib.Path(__file__).resolve().parent;m=json.loads((here/'SOURCE.json').read_text());h=lambda p:hashlib.sha256(p.read_bytes()).hexdigest()
assert not a.out.exists(),'new stage required'
for name,value in m['oracle_inputs'].items():assert h(here/name)==value,'INPUT_DRIFT '+name
for name,value in m['reference_inputs'].items():assert h(a.root/name)==value,'REFERENCE_DRIFT '+name
shutil.copytree(here/'oracle',a.out)
p=a.out/'go.mod';s=p.read_text();token='./__LOCKED_SERVER_REFERENCE__';assert s.count(token)==1
p.write_text(s.replace(token,json.dumps(str((a.root/'references/livekit-stable').resolve()))))
(a.out/'RESTORED_SOURCE.json').write_text(json.dumps({'status':'STAGED_NOT_RUN','source_sha256':h(here/'SOURCE.json'),'staged_go_mod_sha256':h(p)},indent=2)+'\n')
print('STAGED_NOT_RUN')
