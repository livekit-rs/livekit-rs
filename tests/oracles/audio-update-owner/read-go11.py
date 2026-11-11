"""Strict actual source-vs-owner results; no Go result fixture is embedded."""
from pathlib import Path
import argparse,hashlib,json,re
p=argparse.ArgumentParser();p.add_argument('--go',required=True,type=Path);p.add_argument('--rust',required=True,type=Path);p.add_argument('--go-receipt',required=True,type=Path);p.add_argument('--rust-receipt',required=True,type=Path);p.add_argument('--out',required=True,type=Path);p.add_argument('--rust-source',type=Path);p.add_argument('--profile',choices=['go11','compact9'],default='go11');a=p.parse_args();here=Path(__file__).resolve().parent
H=lambda b:hashlib.sha256(b).hexdigest()
def observer_source(name):
    # Staged layouts keep a copy under oracle/; the committed observers sit beside this reader.
    for candidate in (here/'oracle'/name, here/name):
        if candidate.is_file():return candidate
    raise AssertionError('missing original observer source '+name)
# Each original output file must be bound by a coordinator execution receipt,
# including a real successful process result. Rows alone never certify execution.
def receipt(path,actual,kind):
 x=json.loads(path.read_text());assert x['kind']==kind and type(x['exit_code'])is int and x['exit_code']==0,(kind,'setup/test process failed')
 assert x['actual_sha256']==H(actual.read_bytes()),(kind,'actual output digest')
 for field in ['binary_sha256','source_sha256']:
  assert re.fullmatch('[0-9a-f]{64}',x[field]),(kind,field)
 if kind.startswith('original_go'):assert x['source_sha256']==H(observer_source('update_audio_owner_observer_test.go' if a.profile=='go11' else 'update_audio_compact_tail_observer_test.go').read_bytes()),'wrong original observer source'
 else:assert x['source_sha256']==H((a.rust_source or here/'payload/livekit-rs-server/crates/rtc-core/src/room/tests.rs').read_bytes()),'wrong actual reviewed Rust owner-test source'
 return x
rg=receipt(a.go_receipt,a.go,'original_go11'if a.profile=='go11'else'original_go_compact9');rr=receipt(a.rust_receipt,a.rust,'rust_owner11'if a.profile=='go11'else'rust_owner_compact9')
ids=[f'pending-{prefix}{kind}'for kind in ['AUDIO','VIDEO']for prefix in ['', 'empty-']]+[f'published-{kind}-{i}'for kind in ['AUDIO','VIDEO']for i in range(3)]+['missing']
if a.profile=='compact9':
 ids=[f'compact-pending-{kind}-{label}'for kind in ['AUDIO','VIDEO']for label in ['duplicate-no-zero','unique-no-zero','empty-clear']]+['compact-published-AUDIO-'+label for label in ['duplicate-no-zero','unique-no-zero','empty-clear']]
expected_count=len(ids)
def load(path):
 text=path.read_text();lines=text.splitlines();assert len(lines)==expected_count and all(lines),'complete profile rows required'
 def unique_pairs(pairs):
  d={}
  for k,v in pairs:assert k not in d,'duplicate JSON key '+k;d[k]=v
  return d
 rows=[json.loads(s,object_pairs_hook=unique_pairs)for s in lines];assert all(type(x)is dict for x in rows);assert [x.get('id')for x in rows]==ids,'exact unique ordered profile IDs required';assert len({x['id']for x in rows})==expected_count
 return rows
gos,rusts=load(a.go),load(a.rust)
go_base={'id','features','callbacks','telemetry','track_pointer_replaced','participant_version_delta','error'}
rs_keys={'id','kind','features','stereo','disable_dtx','callbacks','participant_version_delta','fields_changed','track_version_unchanged','missing_not_found','missing_message_empty'}
checked=[]
for g,r in zip(gos,rusts):
 missing=g['id']=='missing';pending='pending-'in g['id'];published='published-'in g['id'];kind=0 if 'AUDIO'in g['id']else 1
 assert set(g)==go_base if missing else set(g)==go_base|{'kind','stereo','disable_dtx','track_version'},(g['id'],'source field schema')
 assert set(r)==rs_keys,(g['id'],'Rust field schema')
 for x in [g,r]:
  assert type(x['features'])is list and all(type(n)is int and -(2**31)<=n<2**31 for n in x['features']),(x['id'],'raw signed feature numbers')
  for k in ['callbacks','participant_version_delta']:assert type(x[k])is int and x[k]>=0,(x['id'],k)
 assert type(g['telemetry'])is int and g['telemetry']>=0 and type(g['track_pointer_replaced'])is bool and type(g['error'])is str
 for k in ['missing_not_found','missing_message_empty']:assert type(r[k])is bool
 assert g['features']==r['features'] and g['callbacks']==r['callbacks'] and g['participant_version_delta']==r['participant_version_delta'],(g['id'],'actual normalized metadata/outer callback/version differs')
 if missing:
  assert g['error']=='could not find track' and g['features']==[] and g['callbacks']==0 and g['participant_version_delta']==0
  assert g['telemetry']==0 and g['track_pointer_replaced']is False
  assert r['kind']is None and r['stereo']is None and r['disable_dtx']is None and r['fields_changed']is None and r['track_version_unchanged']is None
  assert r['missing_not_found']and r['missing_message_empty'], 'actual Rust missing wire-model diagnosis differs'
 else:
  assert type(g['kind'])is int and type(r['kind'])is int
  assert g['error']=='' and g['kind']==r['kind']==kind
  for k in ['stereo','disable_dtx']:assert type(g[k])is bool and type(r[k])is bool and g[k]==r[k],(g['id'],k)
  assert not r['missing_not_found']and not r['missing_message_empty']
  if a.profile=='go11'and g['id']in ['pending-AUDIO','pending-VIDEO']:
   assert 0 not in g['features'] and g['stereo']is True and g['disable_dtx']is False,'actual original Go11 compact-tail premise must remain'
  if a.profile=='compact9':
   duplicate=g['id'].endswith('duplicate-no-zero');empty=g['id'].endswith('empty-clear')
   assert 0 not in g['features'] and 1 not in g['features'],'no explicit legacy flags in these inputs'
   assert g['stereo']==duplicate and g['disable_dtx']is False,'original compact side effect only from duplicate tail zero'
   assert (g['features']==[])==empty,'empty clear control remains distinct'
  if pending:
   assert g['callbacks']==0 and g['participant_version_delta']==0 and g['telemetry']==0 and g['track_pointer_replaced']is False
   assert g['track_version']is None,'original pending AddTrack has not yet assigned native publication version'
   assert r['fields_changed']is None and r['track_version_unchanged']is None,'promoted Rust pending version is intentionally not cross-clock compared'
  else:
   assert type(r['fields_changed'])is bool and type(r['track_version_unchanged'])is bool and r['track_version_unchanged']is True
   assert type(g['track_version'])is dict and set(g['track_version'])<={'unix_micro','ticks'}
   assert all(type(n)is int for n in g['track_version'].values())
   assert g['track_version'].get('unix_micro',0)==0 and g['track_version'].get('ticks',0)==0,'fixture starts source trackversion at0; source update must retain it'
   assert g['callbacks']==1 and g['participant_version_delta']==1
   changed=r['fields_changed'];assert g['track_pointer_replaced']==changed and g['telemetry']==int(changed),(g['id'],'original receiver pointer/telemetry must correspond to actual Rust changed metadata')
   if kind==1:assert changed is False,'published VIDEO must ignore audio updates'
 checked.append({'id':g['id'],'consumed_source_columns':sorted(g),'rust_actual_columns':sorted(r)})
result={'status':'PASS_ACTUAL_SOURCE11_OWNER11_SCOPED'if a.profile=='go11'else'PASS_ACTUAL_SOURCE_COMPACT9_OWNER9_SCOPED','go_actual_sha256':H(a.go.read_bytes()),'rust_actual_sha256':H(a.rust.read_bytes()),'go_receipt_sha256':H(a.go_receipt.read_bytes()),'rust_receipt_sha256':H(a.rust_receipt.read_bytes()),'rows':expected_count,'profile':a.profile,'checked':checked,'scope':'actual owner metadata/flags, callback/version and missing core signal; Go receiver pointer/telemetry compared to actual Rust field change, not a nonexistent native Rust receiver/telemetry observation','open':['real wire full cloned missing echo','native receiver propagation/DSP/codec/telemetry','actual SDK/RTP consumer']}
with a.out.open('x')as out:json.dump(result,out,indent=2);out.write('\n')
print(json.dumps({'status':result['status'],'rows':expected_count}))
