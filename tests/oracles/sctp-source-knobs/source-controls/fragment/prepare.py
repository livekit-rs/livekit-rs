#!/usr/bin/env python3
"""Stage source observer beside original F136 tests, without altering them."""
from pathlib import Path
import argparse, hashlib, json, shutil
p=Path(__file__).resolve().parent
a=argparse.ArgumentParser();a.add_argument('--staged-source',type=Path,required=True);a.add_argument('--source-reference',type=Path,required=True);args=a.parse_args()
s=json.loads((p/'SOURCE.json').read_text());h=lambda q:hashlib.sha256(q.read_bytes()).hexdigest()
for name,sha in s['assets_sha256'].items():assert h(p/name)==sha,name
stage=args.staged_source
assert (stage/'source/association.go').is_file() and (stage/'go.mod').is_file()
original=json.loads((p/'pion-source-inputs.json').read_text())
for name,sha in original['file_sha256'].items():
 if name.endswith('.go') and not name.endswith('_test.go'):
  assert h(args.source_reference/name)==sha,name
  target=stage/'source'/name
  if not target.exists():target.parent.mkdir(parents=True,exist_ok=True);shutil.copyfile(args.source_reference/name,target)
  assert h(target)==sha,name
for src,dst in [(p/'oracle/fragment_pr_test.go',stage/'source/source_fragment_pr_test.go'),(p/'oracle/cases.jsonl',stage/'fragment-pr-cases.jsonl')]:
 assert not dst.exists(),'USE_NEW_STAGE_OR_REMOVE_ONLY_PRIOR_OBSERVER_UNDER_COORDINATOR_GUARD';shutil.copyfile(src,dst)
print(json.dumps({'status':'STAGED_NOT_RUN','cases':12}))
