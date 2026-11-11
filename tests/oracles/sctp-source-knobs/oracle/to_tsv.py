#!/usr/bin/env python3
from pathlib import Path
import argparse,json
p=argparse.ArgumentParser();p.add_argument('--cases',type=Path,required=True);p.add_argument('--actual',type=Path,required=True);p.add_argument('--out',type=Path,required=True);a=p.parse_args();cases=[json.loads(s) for s in a.cases.read_text().splitlines() if s];rows=[json.loads(s) for s in a.actual.read_text().splitlines() if s];assert len(cases)==len(rows)==10
fields=['min','fast','step','mtu','initial','floor','ca_before','ca_after','fast_selected'];lines=[]
for case,row in zip(cases,rows):
 assert row['id']==case['id'];assert set(row)==set(fields+['id','packet_sizes'])
 assert '\t'not in row['id'] and '\n'not in row['id'];assert all(type(row[k])==int for k in fields);assert all(type(s)==int for s in row['packet_sizes'])
 lines.append('\t'.join([case['id']]+[str(case[k]) for k in ['min','fast','step']]+[str(row[k]) for k in fields]+[','.join(str(s) for s in row['packet_sizes'])]))
a.out.write_text('\n'.join(lines)+'\n')
