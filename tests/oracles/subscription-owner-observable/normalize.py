"""Lossless schema validation, not a source-vs-Rust full-row PASS."""
from pathlib import Path
import argparse,json,hashlib
HERE=Path(__file__).resolve().parent
SHA={'permission':'96dd60e361be3322a6222d00e1fa59737241f6a7dbe2716b9fe88da52c50c227','settings':'47c5817430b77beafc81d4530151259f25ba899b5d36684085827a7ee2d5aec3'}
def duplicate(pairs):
 result={}
 for k,v in pairs:
  if k in result:raise ValueError('duplicate member '+k)
  result[k]=v
 return result
def loads(b):
 def bad(v):raise ValueError('unexpected noninteger numeric '+v)
 return json.loads(b,object_pairs_hook=duplicate,parse_float=bad,parse_constant=bad)
def exact(v,t):assert type(v)is t,(t,type(v));return v
def integer(v):exact(v,int);assert -(1<<63)<=v<(1<<63);return v
def validate(kind,data):
 cases=loads((HERE/'inputs'/kind/'cases.json').read_bytes());expected=[(c,i,at,op) for c in cases for i,(at,op) in enumerate(c['actions'])]
 raw=data.splitlines();assert len(raw)==len(expected) and all(raw)
 result=[]
 for line,(case,index,at,op) in zip(raw,expected):
  x=loads(line);exact(x,dict)
  for k in ['id','op']:exact(x[k],str)
  assert (x['id'],integer(x['index']),integer(x['at_ns']),x['op'])==(case['id'],index,at,op)
  if kind=='permission':
   assert set(x)=={'id','index','at_ns','op','desired','bound','needs_bind','publisher_has_permission','errors','started_ns','subscribe_ns'}
   for k in ['desired','bound','needs_bind','publisher_has_permission']:exact(x[k],bool)
   for k in ['started_ns','subscribe_ns']:integer(x[k])
   exact(x['errors'],list)
   for e in x['errors']:
    exact(e,dict);assert set(e)=={'track','fatal','error'};exact(e['track'],str);exact(e['fatal'],bool);exact(e['error'],str)
   cells=[x['id'],index,at,op,x['desired'],x['bound'],x['needs_bind'],x['publisher_has_permission'],len(x['errors']),x['started_ns'],x['subscribe_ns'],json.dumps(x['errors'],ensure_ascii=True,separators=(',',':')).encode().hex(),line.hex()]
  else:
   base={'id','index','at_ns','op','entry','one_shot','physical_transport','controlled_close_receipts','actual_downtrack_setting_calls'}
   for k in ['entry','one_shot','physical_transport']:exact(x[k],bool)
   assert x['one_shot']==case['one_shot'] and x['physical_transport']is False
   assert integer(x['controlled_close_receipts'])>=0
   extra={'desired','has_downtrack','kept','keep_until_ns','settings_present'} if x['entry'] else set()
   if x['entry']:
    for k in ['desired','has_downtrack','kept','settings_present']:exact(x[k],bool)
    integer(x['keep_until_ns'])
    if x['settings_present']:extra.add('disabled');exact(x['disabled'],bool)
   assert set(x)==base|extra
   calls=exact(x['actual_downtrack_setting_calls'],list)
   for c in calls:
    exact(c,dict);assert set(c)=={'disabled','downtrack','immediate'};exact(c['disabled'],bool);exact(c['immediate'],bool);assert integer(c['downtrack'])>=0
   cells=[x['id'],index,at,op,x['one_shot'],x['entry'],*[x.get(k,'-')for k in ['desired','has_downtrack','kept','keep_until_ns','settings_present','disabled']],x['controlled_close_receipts'],json.dumps(calls,ensure_ascii=True,separators=(',',':')).encode().hex(),line.hex()]
  result.append('\t'.join(str(c).lower()if type(c)is bool else str(c)for c in cells))
 return result
def main():
 p=argparse.ArgumentParser();p.add_argument('--kind',choices=SHA,required=True);p.add_argument('--jsonl',type=Path,required=True);p.add_argument('--out',type=Path,required=True);a=p.parse_args();data=a.jsonl.read_bytes();digest=hashlib.sha256(data).hexdigest();assert digest==SHA[a.kind],'require original coordinator actual source bytes; new actual execution needs explicit reviewed revision'
 rows=validate(a.kind,data)
 with a.out.open('x')as out:out.write('#GO_OWNER_OBSERVABLE_V1\t'+a.kind+'\t'+str(len(rows))+'\t'+digest+'\n'+'\n'.join(rows)+'\n')
 print(json.dumps({'status':'VALIDATED_FULL_SOURCE_SCHEMA_NOT_PARITY','kind':a.kind,'rows':len(rows),'source_sha256':digest,'tsv_sha256':hashlib.sha256(a.out.read_bytes()).hexdigest(),'raw_each_row_hex_retained':True}))
if __name__=='__main__':main()
