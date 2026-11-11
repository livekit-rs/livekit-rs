#!/usr/bin/env python3
"""Fresh paired Go/Rust servers per room.empty_timeout profile; retain failures,
never count partials. Profiles:
  et-short (room.empty_timeout: 2)   -> empty room must AUTO-CLOSE within the 8s window
  et-long  (room.empty_timeout: 600) -> empty room must PERSIST within the 4s window
Each profile requires Rust's observations to equal the pinned Go oracle's.
Cross-profile control: the oracle closes et-short and persists et-long, so a config
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
GO_PORT,RUST_PORT=18400,18410
# profile -> (overlay, window_ms, expected_timeout_reported, expected_closed)
PROFILES={
 'et-short':({'room':{'empty_timeout':2}},8000,2,True),
 'et-long':({'room':{'empty_timeout':600}},4000,600,False),
}
def cfg(port,mt,mu,overlay):
 c={'port':port,'bind_addresses':['127.0.0.1'],
    'rtc':{'tcp_port':mt,'udp_port':mu,'node_ip':'127.0.0.1','use_external_ip':False,'enable_loopback_candidate':True,'ips':{'includes':['127.0.0.1/32']}},
    'keys':{'devkey':'secret'},'turn':{'enabled':False},'logging':{'level':'error'}}
 c.update(overlay); return c
manifest={'status':'INCOMPLETE','scope':'configured room.empty_timeout empty-room auto-close vs pinned Go oracle; not departure timing/media/webhook claim','binaries':{},'profiles':{}}
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
 for name,(overlay,win,exp_to,exp_closed) in PROFILES.items():
  assert free(GO_PORT) and free(RUST_PORT),('occupied base ports',name)
  rec={'profile':name,'overlay':overlay,'window_ms':win,'expected_closed':exp_closed};manifest['profiles'][name]=rec
  servers={}
  for side,(port,mt,mu) in [('go',(GO_PORT,18401,18402)),('rust',(RUST_PORT,18411,18412))]:
   path=a.output/(f'{name}-{side}.yaml');path.write_text(yaml.safe_dump(cfg(port,mt,mu,overlay)))
   rec[side+'_config_sha256']=hashlib.sha256(path.read_bytes()).hexdigest()
   log=(a.output/(f'{name}-{side}.server.log')).open('wb')
   proc=subprocess.Popen([str(getattr(a,side).resolve()),'--dev','--config',str(path)],stdout=log,stderr=subprocess.STDOUT)
   servers[side]=(proc,log);rec[side+'_pid']=proc.pid
  save()
  try:
   for side,port in [('go',GO_PORT),('rust',RUST_PORT)]:wait_ready(f'http://127.0.0.1:{port}',servers[side][0])
   env=dict(os.environ,REFERENCE_URL=f'http://127.0.0.1:{GO_PORT}',LIVEKIT_URL=f'http://127.0.0.1:{RUST_PORT}',
    ET_PROFILE=name,ET_WINDOW=str(win),ET_EXPECT_TIMEOUT=str(exp_to),ET_OUTPUT=str((a.output/f'{name}.pair.json').resolve()))
   with (a.output/(f'{name}.runner.log')).open('wb') as out:
    r=subprocess.run([a.node,str(here/'run.mjs')],env=env,stdout=out,stderr=subprocess.STDOUT,timeout=120)
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
  closed={s:pair['observations'][s]['closed_in_window'] for s in ['reference','rust']}
  rec['closed_in_window']=closed
  # Per-profile: Rust must equal Go, and Go must match the profile's expected behavior.
  assert closed['reference']==closed['rust'],(name,'Rust closed_in_window != Go',closed)
  assert closed['reference']==exp_closed,(name,'Go closed_in_window != expected',closed['reference'],exp_closed)
 # Cross-profile control: oracle closes the short-timeout room and persists the long one.
 sc=manifest['profiles']['et-short']['closed_in_window']['reference']
 lc=manifest['profiles']['et-long']['closed_in_window']['reference']
 assert sc is True and lc is False,('config ignored: oracle did not differentiate short vs long empty_timeout',sc,lc)
 manifest['cross_profile_control']={'et-short_closed':sc,'et-long_closed':lc,'differs':True}
 manifest['status']='PASS_CONFIGURED_EMPTY_TIMEOUT_EFFECT'
finally:
 save()
print(json.dumps({'status':manifest['status'],'profiles':list(PROFILES),'closed_in_window':{n:manifest['profiles'][n]['closed_in_window'] for n in PROFILES}}))
