#!/usr/bin/env python3
import argparse,pathlib,json
p=argparse.ArgumentParser();p.add_argument('--cases',type=pathlib.Path,required=True);p.add_argument('--actual',type=pathlib.Path,required=True);p.add_argument('--out',type=pathlib.Path,required=True);a=p.parse_args();assert not a.out.exists(),'preserve prior actual receipt';cases=[json.loads(s)for s in a.cases.read_text().splitlines()if s];rows=[json.loads(s)for s in a.actual.read_text().splitlines()if s];assert len(cases)==len(rows)==8;out=[]
for c,r in zip(cases,rows):
 for key in ['id','selector','operation']:assert c[key]==r[key],key
 ints=lambda values:','.join(str(v)for v in values);bit=lambda value:'1'if value else'0'
 out.append('\t'.join([c['id'],c['selector'],c['operation'],c['initial_kind'],str(c['initial_value']),c['replacement_kind'],str(c['replacement_value']),str(r['sent']),bit(r['abandoned']),bit(r['raw']),ints(r['packet_sizes']),ints(r['transmitted_tsn']),bit(r['stream_present'])]))
a.out.write_text('\n'.join(out)+'\n')
