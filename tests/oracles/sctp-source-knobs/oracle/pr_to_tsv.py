#!/usr/bin/env python3
"""Lossless actual Go PRv2 rows -> strict typed Rust adapter. No expected data."""
from pathlib import Path
import argparse,json
p=argparse.ArgumentParser();p.add_argument('actual',type=Path);p.add_argument('cases',type=Path);p.add_argument('out',type=Path);a=p.parse_args()
inputs=[json.loads(x)for x in a.cases.read_text().splitlines()if x];rows=[json.loads(x)for x in a.actual.read_text().splitlines()if x];assert len(inputs)==len(rows)==12
out=[]
def numbers(v):assert all(type(x)is int and x>=0 for x in v);return ','.join(map(str,v))
def bits(v):assert all(type(x)is bool for x in v);return ''.join('1'if x else'0'for x in v)
for c,r in zip(inputs,rows):
 assert (r['id'],r['kind'],r['value'],r['fast'])==(c['id'],c['kind'],c['value'],c['fast'])
 assert r['all_inflight']==[True]*5,'actual source send prerequisite missing'
 assert r['raw_abandoned']==r['abandoned'],'all-inflight raw/effective state must agree'
 assert len(r['sent'])==len(r['initial_abandoned'])==len(r['abandoned'])==5
 out.append('\t'.join([r['id'],r['kind'],str(r['value']),str(r['fast']),bits(r['initial_abandoned']),numbers(r['sent']),bits(r['abandoned']),numbers(r['packet_sizes']),numbers(r['transmitted_tsn'])]))
a.out.write_text('\n'.join(out)+'\n')
