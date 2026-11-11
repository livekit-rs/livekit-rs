from pathlib import Path
import argparse,json,hashlib
p=argparse.ArgumentParser();p.add_argument('--root',type=Path,required=True);p.add_argument('--phase',choices=['all','tests-only','production'],default='all');p.add_argument('--fixed-source-root',type=Path);p.add_argument('--apply',action='store_true');a=p.parse_args();d=Path(__file__).resolve().parent;s=json.loads((d/'SOURCE.json').read_text());h=lambda b:hashlib.sha256(b).hexdigest()
for n,v in s['assets_sha256'].items():assert h((d/n).read_bytes())==v,'asset drift '+n
for n,v in s['context_sha256'].items():assert h((a.root/n).read_bytes())==v,'context drift '+n
fixed=a.fixed_source_root or Path(s['fixed_source_root'])
for n,v in s['fixed_source_sha256'].items():assert h((fixed/n).read_bytes())==v,'fixed-source drift '+n
built=[]
for t in s['targets']:
 if a.phase!='all' and t['phase']!=a.phase:continue
 n=t['path'];q=a.root/n;current=q.read_bytes() if q.exists() else None
 assert (h(current) if current is not None else None)==t['base_sha256'],'target drift '+n
 if current is None:result=(d/'payload'/n).read_bytes()
 else:
  lines=current.decode().splitlines(True);parts=[];at=0
  for block in s['line_hunks'][n]:
   lo,hi=block['old_start'],block['old_end'];assert lines[lo:hi]==block['old_lines'],n
   parts+=lines[at:lo];parts+=block['new_lines'];at=hi
  result=''.join(parts+lines[at:]).encode()
 assert h(result)==t['payload_sha256'],'result drift '+n
 built.append((q,result))
if a.apply:
 for q,result in built:q.parent.mkdir(parents=True,exist_ok=True);q.write_bytes(result)
print(json.dumps({'phase':a.phase,'targets':len(built),'applied':a.apply}))
