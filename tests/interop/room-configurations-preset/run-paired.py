#!/usr/bin/env python3
"""Fresh paired Go/Rust servers per room.room_configurations preset profile; retain
failures, never count partials. Go roomallocator.go applyNamedRoomConfiguration
applies a named preset to a CreateRoom only for fields the request left unset, and
400s an unknown preset; Rust manager.rs (preset.get -> inherit) mirrors it. Two
profiles share the SAME request sequence:
  preset : room_configurations.presetA present   -> presetA CreateRoom 200 w/ preset values
  absent : room_configurations = {}               -> presetA CreateRoom 400 unknown
Cross-profile control: the SAME presetA request yields 200 (preset) vs 400 (absent)
on the pinned oracle, so a server that ignored the preset map could not reproduce
the difference; Rust must deep-equal the oracle observation per profile."""
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
GO_PORT,RUST_PORT=19000,19010
PROFILES={
 'preset':({'room':{'empty_timeout':300,'departure_timeout':20,
   'room_configurations':{'presetA':{'empty_timeout':41,'departure_timeout':7,'max_participants':3,'metadata':'from-preset'}}}}),
 'absent':({'room':{'empty_timeout':300,'departure_timeout':20,'room_configurations':{}}}),
}
def cfg(port,mt,mu,overlay):
 c={'port':port,'bind_addresses':['127.0.0.1'],
    'rtc':{'tcp_port':mt,'udp_port':mu,'node_ip':'127.0.0.1','use_external_ip':False,'enable_loopback_candidate':True,'ips':{'includes':['127.0.0.1/32']}},
    'keys':{'devkey':'secret'},'turn':{'enabled':False},'logging':{'level':'error'}}
 c.update(overlay); return c
manifest={'status':'INCOMPLETE','scope':'configured room.room_configurations.{key} named-preset application to CreateRoom effective fields vs pinned Go oracle; not sync_streams/agents (internal-only), CLI per-key flag, ENV, WS join, media or webhook claim','binaries':{},'profiles':{}}
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
def preset_applied_status(pair):
 return next(o['status'] for o in pair['observations']['reference'] if o['case']=='preset-applied')
try:
 for name,overlay in PROFILES.items():
  assert free(GO_PORT) and free(RUST_PORT),('occupied base ports',name)
  rec={'profile':name,'overlay':overlay};manifest['profiles'][name]=rec
  servers={}
  for side,(port,mt,mu) in [('go',(GO_PORT,19001,19002)),('rust',(RUST_PORT,19011,19012))]:
   path=a.output/(f'{name}-{side}.yaml');path.write_text(yaml.safe_dump(cfg(port,mt,mu,overlay)))
   rec[side+'_config_sha256']=hashlib.sha256(path.read_bytes()).hexdigest()
   log=(a.output/(f'{name}-{side}.server.log')).open('wb')
   proc=subprocess.Popen([str(getattr(a,side).resolve()),'--dev','--config',str(path)],stdout=log,stderr=subprocess.STDOUT)
   servers[side]=(proc,log);rec[side+'_pid']=proc.pid
  save()
  try:
   for side,port in [('go',GO_PORT),('rust',RUST_PORT)]:wait_ready(f'http://127.0.0.1:{port}',servers[side][0])
   env=dict(os.environ,REFERENCE_URL=f'http://127.0.0.1:{GO_PORT}',LIVEKIT_URL=f'http://127.0.0.1:{RUST_PORT}',
    RC_PROFILE=name,RC_OUTPUT=str((a.output/f'{name}.pair.json').resolve()))
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
  rec['observations']=pair['observations']
  rec['oracle_preset_applied_status']=preset_applied_status(pair)
 ps=manifest['profiles']['preset']['oracle_preset_applied_status']
 as_=manifest['profiles']['absent']['oracle_preset_applied_status']
 assert ps==200 and as_==400 and ps!=as_,('config ignored: oracle did not differentiate present vs absent preset map',ps,as_)
 manifest['cross_profile_control']={'preset_applied_status_present':ps,'preset_applied_status_absent':as_,'differs':True}
 manifest['status']='PASS_CONFIGURED_ROOM_PRESET_EFFECT'
finally:
 save()
print(json.dumps({'status':manifest['status'],'profiles':list(PROFILES),'preset_applied_status':{n:manifest['profiles'][n].get('oracle_preset_applied_status') for n in PROFILES}}))
