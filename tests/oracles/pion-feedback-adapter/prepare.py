#!/usr/bin/env python3
"""Verify and stage exact original module; zero production source changes."""
from pathlib import Path
import argparse,json,hashlib,shutil
p=argparse.ArgumentParser();p.add_argument('--source',type=Path,required=True);p.add_argument('--out',type=Path,required=True);a=p.parse_args();here=Path(__file__).resolve().parent;catalog=json.loads((here.parents[2]/'references/0018-go-module-archives-lock.json').read_text());row=next(x for x in catalog['archives']if x['module']=='github.com/pion/interceptor'and x['version']=='v0.1.49');lock={'files_sha256':{x['path']:x['sha256']for x in row['selected_files']}};assert len(lock['files_sha256'])==178;h=lambda p:hashlib.sha256(p.read_bytes()).hexdigest();assert not a.out.exists()
for name,expected in lock['files_sha256'].items():assert h(a.source/name)==expected,'SOURCE_DRIFT '+name
shutil.copytree(a.source,a.out);shutil.copyfile(here/'oracle_test.go',a.out/'internal/cc/source_adapter_oracle_test.go')
(a.out/'ADAPTER_ORACLE_STAGE.json').write_text(json.dumps({'production_source_transformations':[],'driver_sha256':h(here/'oracle_test.go'),'status':'NOT_RUN'},indent=2)+'\n')
