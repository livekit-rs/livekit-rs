#!/usr/bin/env python3
"""Restore a new owned Go oracle stage; no compiler/process execution."""
import argparse,hashlib,json,pathlib,shutil
p=argparse.ArgumentParser();p.add_argument('--root',required=True,type=pathlib.Path);p.add_argument('--out',required=True,type=pathlib.Path);a=p.parse_args();here=pathlib.Path(__file__).resolve().parent
lock=json.loads((here/'oracle-stage-lock.json').read_text());h=lambda p:hashlib.sha256(p.read_bytes()).hexdigest()
assert not a.out.exists(),'new stage required'
for name,value in lock['oracle_inputs'].items():assert h(here/name)==value,'ORACLE_DRIFT '+name
for name,value in lock['reference_inputs'].items():assert h(a.root/name)==value,'REFERENCE_DRIFT '+name
shutil.copytree(here/'oracle',a.out)
f=a.out/'go.mod';mod=f.read_text();token='./__LOCKED_SERVER_REFERENCE__';assert mod.count(token)==1
mod=mod.replace(token,json.dumps(str((a.root/'references/livekit-stable').resolve())));f.write_text(mod)
(a.out/'RESTORED_SOURCE.json').write_text(json.dumps({'status':'STAGED_NOT_EXECUTED','inputs':lock,'staged_go_mod_sha256':h(f)},indent=2)+'\n')
print('STAGED_NOT_EXECUTED')
