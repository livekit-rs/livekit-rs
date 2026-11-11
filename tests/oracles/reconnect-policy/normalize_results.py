#!/usr/bin/env python3
import argparse,json
from pathlib import Path
p=argparse.ArgumentParser();p.add_argument('--kind',choices=['monitor','receiver'],required=True);p.add_argument('--input',required=True);p.add_argument('--output',required=True);a=p.parse_args()
rows=[]
for line in Path(a.input).read_text().splitlines():
 if a.kind=='receiver':
  if not line.startswith('F58ROW '):continue
  d=json.loads(line[7:]);assert set(d)=={'name','sdp_receiver_eligible','incoming_tracks'}
  assert type(d['sdp_receiver_eligible']) is bool and type(d['incoming_tracks']) is int
  row=[d['name'],str(d['sdp_receiver_eligible']).lower(),str(d['incoming_tracks'])]
 else:
  d=json.loads(line);assert set(d)=={'case','index','timeout','idle','has_published'}
  assert all(type(d[k]) is bool for k in ['timeout','idle','has_published']) and type(d['index']) is int
  row=[d['case'],str(d['index']),str(d['timeout']).lower(),str(d['idle']).lower(),str(d['has_published']).lower()]
 assert all('\t' not in x and '\n' not in x for x in row);rows.append('\t'.join(row))
assert len(rows)==(51 if a.kind=='monitor' else 12)
Path(a.output).write_text('\n'.join(rows)+'\n')
