#!/usr/bin/env python3
"""Copy exact reference inputs plus one test profile; never invoke a compiler."""
from pathlib import Path
import argparse,hashlib,json,re,shutil
p=argparse.ArgumentParser();p.add_argument('--profile',required=True,choices=['stages','adapter','numeric','extra','rfc8888','sent-size']);p.add_argument('--source',required=True,type=Path);p.add_argument('--server',required=True,type=Path);p.add_argument('--out',required=True,type=Path);a=p.parse_args();here=Path(__file__).resolve().parent
h=lambda p:hashlib.sha256(p.read_bytes()).hexdigest();lock=json.loads((here/'interceptor-source-lock.json').read_text());manifest=json.loads((here/'SOURCE.json').read_text())
assert not a.out.exists(),'new owned stage required'
for name,value in manifest['files_sha256'].items():assert h(here/name)==value,'ORACLE_DRIFT '+name
for name,value in lock['files_sha256'].items():assert h(a.source/name)==value,'SOURCE_DRIFT '+name
for name,value in manifest['fixed_server_inputs'].items():assert h(a.server/name)==value,'SERVER_DRIFT '+name
shutil.copytree(a.source,a.out)
mod=(a.server/'go.mod').read_text();assert mod.startswith('module github.com/livekit/livekit-server\n');mod=mod.replace('module github.com/livekit/livekit-server\n','module github.com/pion/interceptor\n',1);mod,n=re.subn(r'(?m)^\s*github.com/pion/interceptor v0\.1\.49\s*\n','\n',mod);assert n==1
(a.out/'go.mod').write_text(mod);sums=set((a.server/'go.sum').read_text().splitlines())|set((a.source/'go.sum').read_text().splitlines());(a.out/'go.sum').write_text('\n'.join(sorted(sums))+'\n')
changes=[]
if a.profile in ['stages','extra']:
 for name in ['adaptive_threshold.go','overuse_detector.go','rate_controller.go','loss_based_bwe.go']:
  f=a.out/'pkg/gcc'/name;before=h(f);s=f.read_text();f.write_text(s.replace('time.Now','oracleNow').replace('time.Since','oracleSince'));changes.append({'path':str(f.relative_to(a.out)),'before':before,'after':h(f),'transform':'clock references only'})
if a.profile=='extra':
 f=a.out/'pkg/gcc/send_side_bwe.go';before=h(f);s=f.read_text();assert s.count('time.Now')==3;f.write_text(s.replace('time.Now','oracleNow'));changes.append({'path':'pkg/gcc/send_side_bwe.go','before':before,'after':h(f),'transform':'three clock references only'});shutil.copyfile(here/'stages/oracle_test.go',a.out/'pkg/gcc/source_clock_driver_test.go')
if a.profile=='rfc8888':
 f=a.out/'pkg/gcc/send_side_bwe.go';before=h(f);s=f.read_text();assert s.count('now := time.Now()')==1;f.write_text(s.replace('now := time.Now()','now := rfcSourceNow()'));changes.append({'path':'pkg/gcc/send_side_bwe.go','before':before,'after':h(f),'transform':'one WriteRTCP clock call only'})
package='internal/cc' if a.profile in ['adapter','sent-size'] else 'pkg/gcc'
shutil.copyfile(here/a.profile/'oracle_test.go',a.out/package/'locked_source_profile_test.go')
(a.out/'LOCKED_SOURCE_STAGE.json').write_text(json.dumps({'profile':a.profile,'production_changes':changes,'module_source':manifest['module_source'],'stage_go_mod_sha256':h(a.out/'go.mod'),'stage_go_sum_sha256':h(a.out/'go.sum'),'status':'NOT_RUN'},indent=2)+'\n')
print('STAGED_NOT_EXECUTED')
