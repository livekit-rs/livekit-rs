#!/usr/bin/env python3
"""Fresh paired Go/Rust servers per room.* LEGACY ALIAS profile; retain failures,
never count partials. Each profile sets exactly ONE room.* alias key to 3 and the
runner exercises the affected endpoint at the length boundary. Go config.go:743-747
copies a non-zero room.max_room_name_length / room.max_participant_identity_length
into Limit.*; Rust load.rs:650-654 mirrors it. The alias feeding the SAME limitConf
that enforces CreateRoom (roomservice.go CheckRoomNameLength) and /rtc/validate is
what is being accepted as effective.
  name-alias     (room.max_room_name_length:3)             -> CreateRoom boundary
  identity-alias (room.max_participant_identity_length:3)  -> /rtc/validate boundary
The pinned Go oracle accepts every length<=3 and rejects every length>3 with the
alias limit in the body, so a server ignoring the alias (default limit) could never
reproduce the boundary; Rust must deep-equal the oracle observation byte-for-byte."""
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
GO_PORT,RUST_PORT=18800,18810
# profile -> (overlay, expected alias limit)
PROFILES={
 'name-alias':({'room':{'max_room_name_length':3}},3),
 'identity-alias':({'room':{'max_participant_identity_length':3}},3),
}
def cfg(port,mt,mu,overlay):
 c={'port':port,'bind_addresses':['127.0.0.1'],
    'rtc':{'tcp_port':mt,'udp_port':mu,'node_ip':'127.0.0.1','use_external_ip':False,'enable_loopback_candidate':True,'ips':{'includes':['127.0.0.1/32']}},
    'keys':{'devkey':'secret'},'turn':{'enabled':False},'logging':{'level':'error'}}
 c.update(overlay); return c
manifest={'status':'INCOMPLETE','scope':'configured room.max_room_name_length / room.max_participant_identity_length LEGACY ALIAS effect on CreateRoom and /rtc/validate vs pinned Go oracle; not the limit.* key, WS signaling join, media or webhook claim','binaries':{},'profiles':{}}
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
 for name,(overlay,exp_limit) in PROFILES.items():
  assert free(GO_PORT) and free(RUST_PORT),('occupied base ports',name)
  rec={'profile':name,'overlay':overlay,'expected_limit':exp_limit};manifest['profiles'][name]=rec
  servers={}
  for side,(port,mt,mu) in [('go',(GO_PORT,18801,18802)),('rust',(RUST_PORT,18811,18812))]:
   path=a.output/(f'{name}-{side}.yaml');path.write_text(yaml.safe_dump(cfg(port,mt,mu,overlay)))
   rec[side+'_config_sha256']=hashlib.sha256(path.read_bytes()).hexdigest()
   log=(a.output/(f'{name}-{side}.server.log')).open('wb')
   proc=subprocess.Popen([str(getattr(a,side).resolve()),'--dev','--config',str(path)],stdout=log,stderr=subprocess.STDOUT)
   servers[side]=(proc,log);rec[side+'_pid']=proc.pid
  save()
  try:
   for side,port in [('go',GO_PORT),('rust',RUST_PORT)]:wait_ready(f'http://127.0.0.1:{port}',servers[side][0])
   env=dict(os.environ,REFERENCE_URL=f'http://127.0.0.1:{GO_PORT}',LIVEKIT_URL=f'http://127.0.0.1:{RUST_PORT}',
    NA_PROFILE=name,NA_EXPECT_LIMIT=str(exp_limit),NA_OUTPUT=str((a.output/f'{name}.pair.json').resolve()))
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
  # Harness-level config-effect control: the pinned oracle must show BOTH a 200
  # (length<=alias limit accepted) and a 400 (length>alias limit rejected). A server
  # that ignored the alias and used the default limit could not reject length 4/5.
  go_statuses=[o['status'] for o in pair['observations']['reference']]
  assert 200 in go_statuses and 400 in go_statuses,(name,'oracle boundary absent: alias may be ignored',go_statuses)
  rec['oracle_statuses']=go_statuses
 manifest['status']='PASS_CONFIGURED_NAME_IDENTITY_ALIAS_EFFECT'
finally:
 save()
print(json.dumps({'status':manifest['status'],'profiles':list(PROFILES),'oracle_statuses':{n:manifest['profiles'][n].get('oracle_statuses') for n in PROFILES}}))
