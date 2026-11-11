#!/usr/bin/env python3
"""Owned staging only; no compiler/process execution. Source hash checked first."""
from pathlib import Path
import argparse,json,hashlib,shutil
p=argparse.ArgumentParser();p.add_argument('--source',type=Path,required=True);p.add_argument('--out',type=Path,required=True);a=p.parse_args();here=Path(__file__).resolve().parent;catalog=json.loads((here.parents[2]/'references/0018-go-module-archives-lock.json').read_text());row=next(x for x in catalog['archives'] if x['module']=='github.com/pion/interceptor' and x['version']=='v0.1.49');lock={'files_sha256':{x['path']:x['sha256'] for x in row['selected_files']}};assert len(lock['files_sha256'])==178;h=lambda b:hashlib.sha256(b).hexdigest()
assert not a.out.exists(),'output must be a new owned directory'
for name,expected in lock['files_sha256'].items():assert h((a.source/name).read_bytes())==expected,'SOURCE_DRIFT '+name
shutil.copytree(a.source,a.out)
changed=[]
for name in ['adaptive_threshold.go','overuse_detector.go','rate_controller.go','loss_based_bwe.go']:
 f=a.out/'pkg/gcc'/name;s=f.read_text();s=s.replace('time.Now','oracleNow').replace('time.Since','oracleSince');f.write_text(s);changed.append({'path':'pkg/gcc/'+name,'before':lock['files_sha256']['pkg/gcc/'+name],'after':h(f.read_bytes()),'transform':'wall clock function reference replacement only'})
shutil.copyfile(here/'oracle_test.go',a.out/'pkg/gcc/source_oracle_test.go')
(a.out/'SOURCE_ORACLE_STAGE.json').write_text(json.dumps({'source_module':'github.com/pion/interceptor v0.1.49','clock_only_changes':changed,'driver_sha256':h((here/'oracle_test.go').read_bytes()),'status':'NOT_RUN'},indent=2)+'\n')
