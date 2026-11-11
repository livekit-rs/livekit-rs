from pathlib import Path
import argparse,json,hashlib
p=argparse.ArgumentParser();p.add_argument('--actual',type=Path,required=True);p.add_argument('--cases',type=Path,required=True);p.add_argument('--out',type=Path,required=True);a=p.parse_args()
cases=json.loads(a.cases.read_text());actual=[json.loads(line)for line in a.actual.read_text().splitlines()];assert len(actual)==sum(len(c['actions'])for c in cases);offset=0;lines=[]
for case in cases:
 for i,action in enumerate(case['actions']):
  row=actual[offset];offset+=1;assert (row['id'],row['index'],row['at_ns'],row['op'])==(case['id'],i,*action)
  callbacks=row['callbacks'];assert all(c['track']=='TR_missing'and isinstance(c['fatal'],bool)and c['error']=='track cannot be found' for c in callbacks)
  lines.append('\t'.join(map(str,[row['id'],row['index'],row['at_ns'],row['op'],str(row['desired']).lower(),row['attempts'],len(callbacks),str(any(c['fatal']for c in callbacks)).lower(),row['reconcile_ns'],row['missing_ns']])))
a.out.write_text('\n'.join(lines)+'\n');print(json.dumps({'actual_sha256':hashlib.sha256(a.actual.read_bytes()).hexdigest(),'cases_sha256':hashlib.sha256(a.cases.read_bytes()).hexdigest(),'tsv_sha256':hashlib.sha256(a.out.read_bytes()).hexdigest(),'rows':offset}))
