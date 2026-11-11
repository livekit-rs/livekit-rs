#!/usr/bin/env python3
"""Lossless source fragment stage rows -> Rust typed assertions."""
from pathlib import Path
import argparse,json
p=argparse.ArgumentParser();p.add_argument('actual',type=Path);p.add_argument('cases',type=Path);p.add_argument('out',type=Path);a=p.parse_args();cases=[json.loads(x)for x in a.cases.read_text().splitlines()if x];rows=[json.loads(x)for x in a.actual.read_text().splitlines()if x];assert len(cases)==12 and len(rows)==60
out=[]
def ints(v):assert all(type(x)is int and x>=0 for x in v);return ','.join(map(str,v))
def bits(v):assert len(v)==3 and all(type(x)is bool for x in v);return ''.join('1'if x else'0'for x in v)
for c,group in zip(cases,[rows[i:i+5]for i in range(0,len(rows),5)]):
 assert [r['stage']for r in group]==['original-first','original-middle','original-last','fast','sack']
 for r in group:
  assert r['id']==c['id']
  out.append('\t'.join([r['id'],r['stage'],c['kind'],str(c['value']),str(c['fast']),ints(r['sent']),bits(r['abandoned']),bits(r['raw_head_abandoned']),bits(r['all_inflight']),str(r['buffered']),str(r['flight_bytes']),str(r['pending']),ints(r['packet_sizes']),ints(r['transmitted_tsn'])]))
a.out.write_text('\n'.join(out)+'\n')
