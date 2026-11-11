#!/usr/bin/env python3
"""Run unchanged-SDK decoder cases against externally owned exact Go/Rust inputs.
Starts no LiveKit service. Browser/static-server lifetime is owned by each node run.
"""
import argparse,json,os,subprocess,hashlib
from pathlib import Path
p=argparse.ArgumentParser();p.add_argument('--targets',type=Path,required=True);p.add_argument('--out',type=Path,required=True);p.add_argument('--dependencies',type=Path,required=True);p.add_argument('--codecs',nargs='+',default=['vp9','av1']);p.add_argument('--modes',nargs='+',default=['L1T3']);a=p.parse_args()
assert a.out.is_absolute() and not a.out.exists(),'fresh absolute --out required';a.out.mkdir()
targets=json.loads(a.targets.read_text());assert {t['c21'] for t in targets}>={'original','enabled','disabled'},'must include actual original Go and Rust C21 enabled/disabled servers'
results=[];failed=False;blocked=False
for target in targets:
 for codec in a.codecs:
  for mode in a.modes:
   for single in [True,False]:
    name=f"{target['label']}-{codec}-{mode}-{'single' if single else 'dual'}";out=a.out/name
    env=os.environ|{'INTEROP_DEPENDENCY_ROOT':str(a.dependencies.resolve()),'INTEROP_OUTPUT_DIR':str(out),'LIVEKIT_PEER_URLS':json.dumps(target.get('entries',[target['url'],target['url']])),'VIDEO_CODEC':codec,'SCALABILITY_MODE':mode,'SINGLE_PC':str(single).lower(),'C21_STATE':target['c21'],'SERVER_LABEL':target['label'],'SERVER_CONFIG_FILE':str(Path(target['config']).resolve()),'SERVER_BINARY':str(Path(target['binary']).resolve())}
    if target.get('references'):
     reference=target['references'].get(f"{codec}-{mode}-{'single' if single else 'dual'}")
     if reference:env['ORIGINAL_GO_RECEIPT']=str(Path(reference).resolve())
    for k,v in target.get('env',{}).items():env[k]=str(v)
    with (a.out/f'{name}.log').open('w') as log:
     r=subprocess.run(['node',str(Path(__file__).with_name('run.mjs'))],env=env,stdout=log,stderr=subprocess.STDOUT)
    receipt=json.loads((out/'evidence.json').read_text()) if (out/'evidence.json').exists() else {'status':'MISSING_RECEIPT'}
    results.append({'case':name,'exit_code':r.returncode,'status':receipt['status'],'receipt':str(out/'evidence.json')})
    failed|=r.returncode not in [0,2] or receipt['status']=='FAIL';blocked|=r.returncode==2
    (a.out/'summary.json').write_text(json.dumps({'status':'RUNNING','cases':results},indent=2)+'\n')
status='FAIL' if failed else 'BLOCKED_SCOPED' if blocked else 'PASS';(a.out/'summary.json').write_text(json.dumps({'status':status,'targets_sha256':hashlib.sha256(a.targets.read_bytes()).hexdigest(),'cases':results},indent=2)+'\n');print(status);raise SystemExit(1 if failed else 2 if blocked else 0)
