from pathlib import Path
import argparse,json,hashlib,shutil,re
p=argparse.ArgumentParser();p.add_argument('--source-manifest',type=Path,required=True);p.add_argument('--repo',type=Path,required=True);p.add_argument('--stage',type=Path,required=True);a=p.parse_args();here=Path(__file__).resolve().parent;source=json.loads(a.source_manifest.read_text());h=lambda p:hashlib.sha256(p.read_bytes()).hexdigest()
for name,sha in source['source_inputs_sha256'].items():assert h(a.repo/name)==sha,name
assert not a.stage.exists(),'fresh stage only';a.stage.mkdir(parents=True)
s=(a.repo/'references/livekit-stable/go.mod').read_text();s=re.sub(r'^module [^\n]+','module livekit-rs-tests/list-nodes-wire',s,count=1);s+='\nreplace github.com/livekit/protocol => '+str((a.repo/'references/protocol-stable').resolve())+'\n';(a.stage/'go.mod').write_text(s)
# Exact original MVS graph; union sums only, no version rewriting or algorithm edits.
lines=set()
for p in [a.repo/'references/livekit-stable/go.sum',a.repo/'references/protocol-stable/go.sum']:lines.update(p.read_text().splitlines())
(a.stage/'go.sum').write_text('\n'.join(sorted(lines))+'\n');shutil.copyfile(here/'main.go',a.stage/'main.go');shutil.copyfile(here/'cases.jsonl',a.stage/'cases.jsonl')
print('source-only observer stage prepared; no execution')
