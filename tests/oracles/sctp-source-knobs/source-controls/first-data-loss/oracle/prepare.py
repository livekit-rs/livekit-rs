#!/usr/bin/env python3
import argparse,pathlib,json,hashlib,shutil
p=argparse.ArgumentParser();p.add_argument('--staged-source',type=pathlib.Path,required=True);p.add_argument('--source-reference',type=pathlib.Path,required=True);a=p.parse_args();here=pathlib.Path(__file__).resolve().parent;source=json.loads((here/'pion-source-inputs.json').read_text());h=lambda p:hashlib.sha256(p.read_bytes()).hexdigest()
assert(a.staged_source/'source/association.go').is_file()and(a.staged_source/'go.mod').is_file()
for name,sha in source['file_sha256'].items():
 if name.endswith('.go')and not name.endswith('_test.go'):
  assert h(a.source_reference/name)==sha,name;target=a.staged_source/'source'/name
  if not target.exists():target.parent.mkdir(parents=True,exist_ok=True);shutil.copyfile(a.source_reference/name,target)
  assert h(target)==sha,name
dst=a.staged_source/'source/source_first_data_loss_test.go'
assert not dst.exists(), 'use a new coordinator-owned stage'
shutil.copyfile(here/'first_data_loss_test.go',dst)
