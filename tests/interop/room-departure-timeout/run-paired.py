#!/usr/bin/env python3
"""Fresh paired Go/Rust servers per room.departure_timeout profile; retain failures,
never count partials. Both profiles pin room.empty_timeout=600 so the participant-
departure branch (not empty-from-creation) is what can close the room:
  dt-short (departure_timeout: 2)   -> after the sole peer departs, room AUTO-CLOSES
  dt-long  (departure_timeout: 600) -> after the sole peer departs, room PERSISTS
Each profile requires Rust's observations to equal the pinned Go oracle's.
Cross-profile control: the oracle closes dt-short and persists dt-long, so a config
the server ignores could never masquerade as agreement."""
import argparse, hashlib, json, os, pathlib, signal, socket, subprocess, time, urllib.request
import yaml

p=argparse.ArgumentParser()
p.add_argument('--go',required=True,type=pathlib.Path)
p.add_argument('--rust',required=True,type=pathlib.Path)
p.add_argument('--node',default='node')
p.add_argument('--output',required=True,type=pathlib.Path)
a=p.parse_args()
here=pathlib.Path(__file__).resolve().parent
a.output.mkdir(parents=True,exist_ok=False)
GO_PORT,RUST_PORT=18600,18610
# profile -> (overlay, window_ms, expected departure_timeout, expected_closed)
PROFILES={
 'dt-short':({'room':{'empty_timeout':600,'departure_timeout':2}},12000,2,True),
 'dt-long':({'room':{'empty_timeout':600,'departure_timeout':600}},6000,600,False),
}
def cfg(port,mt,mu,overlay):
 c={'port':port,'bind_addresses':['127.0.0.1'],
    'rtc':{'tcp_port':mt,'udp_port':mu,'node_ip':'127.0.0.1','use_external_ip':False,'enable_loopback_candidate':True,'ips':{'includes':['127.0.0.1/32']}},
    'keys':{'devkey':'secret'},'turn':{'enabled':False},'logging':{'level':'error'}}
 c.update(overlay); return c
manifest={'status':'INCOMPLETE','scope':'configured room.departure_timeout last-participant-departure auto-close vs pinned Go oracle; not media/webhook claim','binaries':{},'profiles':{}}
for side in ['go','rust']:
 b=getattr(a,side).resolve();manifest['binaries'][side]={'path':str(b),'sha256':hashlib.sha256(b.read_bytes()).hexdigest()}
def save():(a.output/'execution.json').write_text(json.dumps(manifest,indent=2)+'\n')
save()
def free(port):
 with socket.socket() as s:return s.connect_ex(('127.0.0.1',port))!=0
def wait_ready(url,proc,deadline=40):
 end=time.monotonic()+deadline
 while True:
  assert proc.poll() is None,('server exited',proc.returncode)
  try:
   with urllib.request.urlopen(url+'/',timeout=1) as r:
    if r.status==200:return
  except (OSError,TimeoutError):
   if time.monotonic()>=end:raise
  time.sleep(.1)
try:
 for name,(overlay,win,exp_dt,exp_closed) in PROFILES.items():
  assert free(GO_PORT) and free(RUST_PORT),('occupied base ports',name)
  rec={'profile':name,'overlay':overlay,'window_ms':win,'expected_closed':exp_closed};manifest['profiles'][name]=rec
  servers={}
  for side,(port,mt,mu) in [('go',(GO_PORT,18601,18602)),('rust',(RUST_PORT,18611,18612))]:
   path=a.output/(f'{name}-{side}.yaml');path.write_text(yaml.safe_dump(cfg(port,mt,mu,overlay)))
   rec[side+'_config_sha256']=hashlib.sha256(path.read_bytes()).hexdigest()
   log=(a.output/(f'{name}-{side}.server.log')).open('wb')
   proc=subprocess.Popen([str(getattr(a,side).resolve()),'--dev','--config',str(path)],stdout=log,stderr=subprocess.STDOUT)
   servers[side]=(proc,log);rec[side+'_pid']=proc.pid
  save()
  try:
   for side,port in [('go',GO_PORT),('rust',RUST_PORT)]:wait_ready(f'http://127.0.0.1:{port}',servers[side][0])
   env=dict(os.environ,REFERENCE_WS_URL=f'ws://127.0.0.1:{GO_PORT}',LIVEKIT_WS_URL=f'ws://127.0.0.1:{RUST_PORT}',
    DT_PROFILE=name,DT_WINDOW=str(win),DT_EXPECT_DEPARTURE_TIMEOUT=str(exp_dt),DT_OUTPUT=str((a.output/f'{name}.pair.json').resolve()))
   with (a.output/(f'{name}.runner.log')).open('wb') as out:
    r=subprocess.run([a.node,str(here/'run.mjs')],env=env,stdout=out,stderr=subprocess.STDOUT,timeout=180)
   rec['runner_exit']=r.returncode;assert r.returncode==0,(name,'runner failed; see',f'{name}.runner.log')
  finally:
   for side,(proc,log) in servers.items():
    if proc.poll() is None:proc.send_signal(signal.SIGINT)
    try:rec[side+'_exit']=proc.wait(timeout=30)
    except subprocess.TimeoutExpired:proc.kill();rec[side+'_exit']=proc.wait();rec[side+'_forced_kill']=True
    log.close()
  save()
  for side in ['go','rust']:assert rec.get(side+'_exit')==0,rec
  pair=json.loads((a.output/f'{name}.pair.json').read_text());rec['pair_status']=pair['status']
  assert pair['status']=='PASS_SCOPED',(name,pair.get('error'))
  closed={s:pair['observations'][s]['closed_after_departure'] for s in ['reference','rust']}
  rec['closed_after_departure']=closed
  assert closed['reference']==closed['rust'],(name,'Rust closed != Go',closed)
  assert closed['reference']==exp_closed,(name,'Go closed != expected',closed['reference'],exp_closed)
 sc=manifest['profiles']['dt-short']['closed_after_departure']['reference']
 lc=manifest['profiles']['dt-long']['closed_after_departure']['reference']
 assert sc is True and lc is False,('config ignored: oracle did not differentiate short vs long departure_timeout',sc,lc)
 manifest['cross_profile_control']={'dt-short_closed':sc,'dt-long_closed':lc,'differs':True}
 manifest['status']='PASS_CONFIGURED_DEPARTURE_TIMEOUT_EFFECT'
finally:
 save()
print(json.dumps({'status':manifest['status'],'profiles':list(PROFILES),'closed_after_departure':{n:manifest['profiles'][n]['closed_after_departure'] for n in PROFILES}}))
