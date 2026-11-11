#!/usr/bin/env python3
import argparse,pathlib,json,hashlib,shutil
p=argparse.ArgumentParser();p.add_argument('--staged-source',type=pathlib.Path,required=True);p.add_argument('--source-reference',type=pathlib.Path,required=True);a=p.parse_args();here=pathlib.Path(__file__).resolve().parent;source=json.loads((here/'pion-source-inputs.json').read_text());h=lambda p:hashlib.sha256(p.read_bytes()).hexdigest()
assert(a.staged_source/'source/association.go').is_file()and(a.staged_source/'go.mod').is_file()
for name,sha in source['file_sha256'].items():
 if name.endswith('.go')and not name.endswith('_test.go'):
  assert h(a.source_reference/name)==sha,name;target=a.staged_source/'source'/name
  if not target.exists():target.parent.mkdir(parents=True,exist_ok=True);shutil.copyfile(a.source_reference/name,target)
  assert h(target)==sha,name
for name,dst in [('stream_policy_test.go',a.staged_source/'source/source_current_stream_policy_test.go'),('cases.jsonl',a.staged_source/'current-stream-policy-cases.jsonl')]:
 assert not dst.exists(),'use a new coordinator-owned stage';shutil.copyfile(here/name,dst)
