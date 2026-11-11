from pathlib import Path
import argparse,json,hashlib
p=argparse.ArgumentParser();p.add_argument('--go',type=Path,required=True);p.add_argument('--rust',type=Path,required=True);a=p.parse_args()
ids=['pending-AUDIO-0','pending-AUDIO-1','pending-VIDEO-0','pending-VIDEO-1','published-AUDIO-0','published-AUDIO-1','published-VIDEO-0','published-VIDEO-1','published-VIDEO-2','missing']
gkeys={'id','kind','width','height','callbacks','telemetry','track_pointer_replaced','participant_version_delta','track_version_unchanged','error'}
rkeys={'id','kind','width','height','callbacks','participant_version_delta','fields_changed','track_version_unchanged','missing_not_found','missing_message_empty'}
def obj(pairs):
 d={}
 for k,v in pairs:
  assert k not in d,('duplicate JSON member',k)
  d[k]=v
 return d
def read(path,keys):
 rows=[json.loads(x,object_pairs_hook=obj) for x in path.read_text().splitlines()];assert len(rows)==10
 assert [x['id'] for x in rows]==ids;assert len({x['id'] for x in rows})==10
 for x in rows:
  assert set(x)==keys,(x['id'],'exact field set')
  for k in ['callbacks','participant_version_delta']:assert type(x[k])is int and x[k]>=0
  for k in ['kind','width','height']:assert x[k] is None or type(x[k])is int
  assert x['track_version_unchanged'] is None or type(x['track_version_unchanged'])is bool
 return rows
go=read(a.go,gkeys);rust=read(a.rust,rkeys)
for g,r in zip(go,rust):
 for k in ['kind','width','height','callbacks','participant_version_delta','track_version_unchanged']:assert g[k]==r[k],(g['id'],k,g[k],r[k])
 assert type(g['telemetry'])is int and g['telemetry']>=0;assert type(g['track_pointer_replaced'])is bool;assert type(g['error'])is str
 assert r['fields_changed'] is None or type(r['fields_changed'])is bool
 assert type(r['missing_not_found'])is bool and type(r['missing_message_empty'])is bool
 if g['id']=='missing':
  assert g['error']=='could not find track';assert r['missing_not_found'] and r['missing_message_empty'];assert r['fields_changed'] is None
  assert g['callbacks']==0 and g['telemetry']==0 and not g['track_pointer_replaced']
 elif g['id'].startswith('pending-'):
  assert g['error']=='' and r['fields_changed'] is None;assert g['callbacks']==0 and g['telemetry']==0 and not g['track_pointer_replaced']
  assert not r['missing_not_found'] and not r['missing_message_empty']
 else:
  assert g['error']=='';assert g['track_pointer_replaced']==r['fields_changed'];assert g['telemetry']==int(r['fields_changed'])
  assert not r['missing_not_found'] and not r['missing_message_empty']
print(json.dumps({'status':'PASS','rows':10,'go_sha256':hashlib.sha256(a.go.read_bytes()).hexdigest(),'rust_sha256':hashlib.sha256(a.rust.read_bytes()).hexdigest(),'scope':'actual source owner metadata/callbacks; source receiver pointer/telemetry mapped to Rust metadata field changes, not invented Rust telemetry or native RTP'}))
