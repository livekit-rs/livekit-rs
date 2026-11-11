#!/usr/bin/env python3
import argparse,json,pathlib
p=argparse.ArgumentParser();p.add_argument('actual',type=pathlib.Path);p.add_argument('out',type=pathlib.Path);a=p.parse_args()
cases={
 'fragmented-max0-unlost':(51200,False,False),
 'fragmented-max0-until-real-t3':(51200,False,True),
 'single-max0-until-real-t3':(1000,False,True),
 'fragmented-reliable-until-real-t3':(51200,True,True),
}
rows=[json.loads(s) for s in a.actual.read_text().splitlines() if s.strip()]
assert len(rows)==4 and {r['id'] for r in rows}==set(cases)
lines=[]
for r in rows:
 size,reliable,loss=cases[r['id']]
 assert r['fast_rtx_wnd']==0 and r['mtu']==1228 and r['interleaving'] is False
 assert r['requested_bytes']==size and isinstance(r['delivered'],bool)
 assert r['received_bytes']==(size if r['delivered'] else 0)
 if r['delivered']:assert r['received_sha256']==r['expected_payload_sha256']
 if loss:assert r['actual_t3_timeouts']>0 and r['dropped_data_datagrams']>0
 lines.append('\t'.join([r['id'],str(size),str(reliable).lower(),str(loss).lower(),str(r['delivered']).lower()]))
assert not a.out.exists(),'do not overwrite historical actual table'
a.out.write_text('\n'.join(lines)+'\n')
