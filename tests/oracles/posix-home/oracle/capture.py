#!/usr/bin/env python3
"""Coordinator-run fresh actual Go process per source branch. Never runs go build."""
import argparse,json,os,platform,tempfile,hashlib,sys
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'fixture'))
from byte_support import execute,encoded

def main():
 p=argparse.ArgumentParser();p.add_argument('--go',required=True);p.add_argument('--profile',choices=['darwin','linux'],required=True);p.add_argument('--output',required=True);a=p.parse_args()
 assert os.name=='posix' and {'Darwin':'darwin','Linux':'linux'}.get(platform.system())==a.profile
 binary=Path(a.go).resolve();report={'status':'RUNNING','profile':a.profile,'scope':'actual pinned Go library process observations, not Rust compatibility PASS','go_binary_sha256':hashlib.sha256(binary.read_bytes()).hexdigest(),'rows':[]}
 with tempfile.TemporaryDirectory(prefix='f69-home-oracle-')as owned:
  cwd=os.fsencode(owned);only=cwd+b'/only-sh';os.mkdir(only);os.symlink(b'/bin/sh',only+b'/sh')
  normal={b'PATH':b'/usr/bin:/bin',b'LC_ALL':b'C',b'TZ':b'UTC'}
  cases=[
   ('home-present',b'~',normal|{b'HOME':b'/f69-present',b'USERPROFILE':b'/f69-decoy'}),
   ('home-unset-with-decoy',b'~',normal|{b'USERPROFILE':b'/f69-decoy'}),
   ('home-empty-with-decoy',b'~',normal|{b'HOME':b'',b'USERPROFILE':b'/f69-decoy'}),
   ('home-unset-without-decoy',b'~',normal),
   ('home-unset-path-empty',b'~',normal|{b'PATH':b'',b'USERPROFILE':b'/f69-decoy'}),
   ('home-unset-only-real-sh',b'~',normal|{b'PATH':only,b'USERPROFILE':b'/f69-decoy'}),
   ('home-empty-only-real-sh',b'~',normal|{b'HOME':b'',b'PATH':only,b'USERPROFILE':b'/f69-decoy'}),
   ('raw-name',b'${F69_PATH\xff}/keys.yaml',normal|{b'F69_PATH\xff':b'/f69-target'}),
   ('raw-name-parent-not-home',b'${F69_PATH\xff}/../keys.yaml',normal|{b'F69_PATH\xff':b'/f69-target/subdir'}),
   ('raw-known-looking-name',b'${LIVEKIT_PORT\xff}/keys.yaml',normal|{b'LIVEKIT_PORT\xff':b'/f69-target'}),
  ]
  for label,path,env in cases:
   result,stdout,stderr=execute(binary,[path.hex().encode()],env,cwd,8)
   row={'name':label,'path':encoded(path),'env':[{ 'key':encoded(k),'value':encoded(v)}for k,v in env.items()],'raw':result}
   if result['exit']==0 and not result['timed_out']and not result['overflow']:
    try:row['value']=json.loads(stdout);assert set(row['value'])=={'path_hex','error','error_type'};row['status']='CAPTURED_ACTUAL'
    except Exception as e:row.update(status='FAIL_ORACLE_OUTPUT',error=str(e))
   else:row['status']='FAIL_ORACLE_PROCESS'
   report['rows'].append(row)
 report['ownedCleanup']=not Path(owned).exists();report['status']='CAPTURED_ACTUAL_OBSERVATIONS'if all(r['status']=='CAPTURED_ACTUAL'for r in report['rows'])and report['ownedCleanup']else'FAIL'
 Path(a.output).write_text(json.dumps(report,indent=2)+'\n');return 0if report['status']=='CAPTURED_ACTUAL_OBSERVATIONS'else 1
if __name__=='__main__':raise SystemExit(main())
