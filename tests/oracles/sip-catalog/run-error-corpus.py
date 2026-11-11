#!/usr/bin/env python3
"""Real Go RedisStore parse-error corpus, actual Rust catalog comparison. No services started."""
import argparse,base64,hashlib,json,pathlib,subprocess,uuid
p=argparse.ArgumentParser()
for name in ['go-bin','rust-bin','redis','out']:p.add_argument('--'+name,required=True)
a=p.parse_args();run=uuid.uuid4().hex
vectors=json.loads(pathlib.Path(__file__).with_name('store-error-vectors.json').read_text())['vectors']
def request(binary,cmds):
    commands=[dict(c,run_id=run) for c in cmds]
    process=subprocess.run([binary,'--redis',a.redis,'--run-id',run],input=''.join(json.dumps(c)+'\n' for c in commands).encode(),stdout=subprocess.PIPE,stderr=subprocess.PIPE,timeout=75)
    if process.returncode:raise RuntimeError('oracle failed exit='+str(process.returncode)+' stderrsha='+hashlib.sha256(process.stderr).hexdigest())
    rows=[json.loads(x) for x in process.stdout.splitlines()]
    assert len(rows)==len(commands)
    for c,r in zip(commands,rows):assert r['run_id']==run and r['op']==c['op']
    return rows
ids=['ST_error_'+run+'_'+str(i) for i in range(len(vectors))]
seed=request(a.go_bin,[{'op':'seed_raw_inbound','id':ident,'protobuf':base64.b64encode(bytes.fromhex(v['hex'])).decode()} for ident,v in zip(ids,vectors)])
assert all(not r.get('error') for r in seed)
queries=[{'op':'load_inbound','id':ident,'protobuf':''} for ident in ids]
expected=request(a.go_bin,queries);actual=request(a.rust_bin,queries)
rows=[{'name':v['name'],'go_actual_error':g.get('error'),'rust_actual_error':r.get('error'),'status':'PASS' if g.get('error')==r.get('error') and g.get('error') else 'FAIL'} for v,g,r in zip(vectors,expected,actual)]
request(a.go_bin,[{'op':'delete_trunk','id':ident,'protobuf':''} for ident in ids])
report={'status':'PASS' if all(r['status']=='PASS' for r in rows) else 'FAIL','scope':'actual fixed Go stored-protobuf parse failures vs actual Rust; no API success claim','vectors':rows,'source_sha256':hashlib.sha256(pathlib.Path(__file__).with_name('store-error-vectors.json').read_bytes()).hexdigest()}
pathlib.Path(a.out).write_text(json.dumps(report,indent=2)+'\n')
print(json.dumps({'status':report['status'],'cases':len(rows)}))
raise SystemExit(0 if report['status']=='PASS' else 1)
