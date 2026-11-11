#!/usr/bin/env python3
import argparse,json
from pathlib import Path
p=argparse.ArgumentParser();p.add_argument('--input',required=True);p.add_argument('--output',required=True);a=p.parse_args()
rows=[]
for line in Path(a.input).read_text().splitlines():
 d=json.loads(line);assert set(d)=={'id','bytes','room','identity','name'}
 assert type(d['bytes'])is int and all(type(d[k])is bool for k in ['room','identity','name'])
 row=[d['id'],str(d['bytes'])]+[str(d[k]).lower()for k in ['room','identity','name']]
 assert all('\t'not in v and '\n'not in v for v in row);rows.append('\t'.join(row))
assert len(rows)==64
Path(a.output).write_text('\n'.join(rows)+'\n')
