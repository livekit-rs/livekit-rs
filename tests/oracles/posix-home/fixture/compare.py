#!/usr/bin/env python3
"""Actual public CLI consumer pairs, no service/compiler; fresh process each row."""
import argparse,os,json,platform,tempfile,hashlib,sys
from pathlib import Path
from byte_support import execute,encoded,jwt_semantics,canonical_identity,create_owned_file,KEY,SECRET

def main():
 p=argparse.ArgumentParser();p.add_argument('--official',required=True);p.add_argument('--rust',required=True);p.add_argument('--profile',choices=['darwin','linux'],required=True);p.add_argument('--go-oracle',required=True);p.add_argument('--output',required=True);a=p.parse_args()
 assert os.name=='posix'and {'Darwin':'darwin','Linux':'linux'}.get(platform.system())==a.profile
 oracle=json.loads(Path(a.go_oracle).read_text());assert oracle['status']=='CAPTURED_ACTUAL_OBSERVATIONS'and oracle['profile']==a.profile
 observed=next(r['value']for r in oracle['rows']if r['name']=='home-unset-with-decoy');assert not observed['error'],'actual Go must first resolve platform HOME; do not fabricate positive path'
 actual_home=bytes.fromhex(observed['path_hex']);assert actual_home and actual_home.startswith(b'/')
 binaries={k:Path(v).resolve()for k,v in [('official',a.official),('rust',a.rust)]}
 report={'status':'RUNNING','scope':'Home-unset platform fallback and raw ENV name real keyfile consumers; no family100 claim','profile':a.profile,'oracle_sha256':hashlib.sha256(Path(a.go_oracle).read_bytes()).hexdigest(),'binaries':{k:hashlib.sha256(v.read_bytes()).hexdigest()for k,v in binaries.items()},'cases':[]}
 with tempfile.TemporaryDirectory(prefix='f69-cli-owned-')as owned:
  cwd=os.fsencode(owned);key=cwd+b'/keys.yaml';probe=create_owned_file(key,KEY+b': '+SECRET+b'\n');assert probe['status']=='SUPPORTED'
  decoy=cwd+b'/decoy';os.mkdir(decoy);raw_dir=cwd+b'/raw-target';os.mkdir(raw_dir);assert create_owned_file(raw_dir+b'/keys.yaml',KEY+b': '+SECRET+b'\n')['status']=='SUPPORTED';only=cwd+b'/only-sh';os.mkdir(only);os.symlink(b'/bin/sh',only+b'/sh')
  normal={b'PATH':b'/usr/bin:/bin',b'LC_ALL':b'C',b'TZ':b'UTC'}
  # Actual Go result selects a relative route to an owned file. Never create/read
  # arbitrary files in actual HOME. Only unique-owned filenames/stat paths occur.
  fallback=b'~/'+os.path.relpath(key,actual_home)
  token=[b'--config-body',b'keys: {}',b'create-join-token',b'--room=f52-room',b'--identity=f52-identity']
  cases=[
   ('home-unset-userprofile-decoy-positive',[b'--node-ip=127.0.0.1',b'--key-file',fallback,*token],normal|{b'USERPROFILE':decoy},'jwt'),
   ('home-empty-userprofile-decoy-positive',[b'--node-ip=127.0.0.1',b'--key-file',fallback,*token],normal|{b'HOME':b'',b'USERPROFILE':decoy},'jwt'),
   ('home-unset-no-userprofile-positive',[b'--node-ip=127.0.0.1',b'--key-file',fallback,*token],normal,'jwt'),
   ('home-unset-empty-path-actualerror',[b'--node-ip=127.0.0.1',b'--key-file',b'~/f69-never-created-'+os.fsencode(Path(owned).name),*token],normal|{b'PATH':b'',b'USERPROFILE':decoy},'exact'),
   ('home-unset-only-sh-actualsource',[b'--node-ip=127.0.0.1',b'--key-file',b'~/f69-never-created-'+os.fsencode(Path(owned).name),*token],normal|{b'PATH':only,b'USERPROFILE':decoy},'exact'),
   ('raw-env-name-positive',[b'--node-ip=127.0.0.1',b'--key-file',cwd+b'/${F69_PATH\xff}/keys.yaml',*token],normal|{b'HOME':cwd,b'F69_PATH\xff':b'raw-target'},'jwt'),
   ('raw-known-looking-name-positive',[b'--node-ip=127.0.0.1',b'--key-file',cwd+b'/${LIVEKIT_PORT\xff}/keys.yaml',*token],normal|{b'HOME':cwd,b'LIVEKIT_PORT\xff':b'raw-target'},'jwt'),
   ('raw-known-name-does-not-declare-flag',[b'--node-ip=127.0.0.1',b'ports'],normal|{b'HOME':cwd,b'LIVEKIT_PORT\xff':b'not-a-port'},'exact'),
   ('raw-env-name-missing',[b'--node-ip=127.0.0.1',b'--key-file',cwd+b'/${F69_PATH\xff}/missing-key.yaml',*token],normal|{b'HOME':cwd,b'F69_PATH\xff':b'raw-target'},'exact'),
  ]
  for name,args,env,mode in cases:
   row={'name':name,'argv':[encoded(v)for v in args],'env':[{ 'key':encoded(k),'value':encoded(v)}for k,v in env.items()],'mode':mode,'raw':{},'comparison_performed':True};outputs={}
   for side,binary in binaries.items():
    result,stdout,stderr=execute(binary,args,env,cwd,8);row['raw'][side]=result;outputs[side]=(result,stdout,stderr)
   if mode=='jwt':
    semantics={}
    try:
     for side,(result,stdout,stderr)in outputs.items():
      assert result['exit']==0and not result['timed_out']and not result['overflow']and not stderr;semantics[side]=jwt_semantics(stdout)
     row['semantic']=semantics;equal=semantics['official']['claims_without_generated_times']==semantics['rust']['claims_without_generated_times']
    except Exception as e:equal=False;row['error']=str(e)
   else:
    x,y=outputs['official'],outputs['rust'];equal=not(x[0]['timed_out']or y[0]['timed_out']or x[0]['overflow']or y[0]['overflow'])and x[0]['exit']==y[0]['exit']and canonical_identity(x[1])==canonical_identity(y[1])and canonical_identity(x[2])==canonical_identity(y[2])
   row['status']='PASS'if equal else'FAIL';report['cases'].append(row)
 report['ownedCleanup']=not Path(owned).exists();report['executed']=len(report['cases']);report['passed']=sum(r['status']=='PASS'for r in report['cases']);report['failed']=report['executed']-report['passed'];report['status']='PASS_SCOPED'if report['failed']==0and report['executed']==9and report['ownedCleanup']else'FAIL'
 Path(a.output).write_text(json.dumps(report,indent=2)+'\n');return 0if report['status']=='PASS_SCOPED'else 1
if __name__=='__main__':raise SystemExit(main())
