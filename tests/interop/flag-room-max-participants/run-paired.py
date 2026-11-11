#!/usr/bin/env python3
"""Fresh paired Go/Rust servers for the GENERATED integer CLI FLAG
--room.max_participants; both sides launched with the same base YAML plus the same extra
argv. Retain failures, never count partials. This verifies the integer FLAG INPUT PATH
(flag parsed via parse_value Int -> config Room.MaxParticipants -> the WS signaling join
gate) applied on the real authenticated join. Profiles:
  flag-mp-1   (--room.max_participants=1)  -> over-limit peer B must be REJECTED
  flag-mp-off (--room.max_participants=0)  -> peer B must be ACCEPTED (unlimited)
Each profile requires Rust's join/membership observations to equal the pinned Go oracle's.
Cross-profile control: the two profiles' Go peer-B outcomes must DIFFER (rejected vs
joined) so a flag the server ignores could never masquerade as agreement. This is the flag
path; the YAML value path is a separate verified fixture (room-max-participants)."""
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
GO_PORT,RUST_PORT=19800,19810
# profile -> (extra argv, expected reference peer_b.joined)
FLAGS={'flag-mp-1':(['--room.max_participants=1'],False),'flag-mp-off':(['--room.max_participants=0'],True)}
def cfg(port,mt,mu):
 return {'port':port,'bind_addresses':['127.0.0.1'],
    'rtc':{'tcp_port':mt,'udp_port':mu,'node_ip':'127.0.0.1','use_external_ip':False,'enable_loopback_candidate':True,'ips':{'includes':['127.0.0.1/32']}},
    'keys':{'devkey':'secret'},'turn':{'enabled':False},'logging':{'level':'error'}}
manifest={'status':'INCOMPLETE','scope':'generated integer CLI flag --room.max_participants applied on the real WS signaling join gate vs pinned Go oracle; not the YAML value path, /rtc/validate, media, RTP or webhook claim','binaries':{},'profiles':{}}
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
 for name,(extra,exp_b) in FLAGS.items():
  assert free(GO_PORT) and free(RUST_PORT),('occupied base ports',name)
  rec={'profile':name,'flag_args':extra,'expected_reference_peer_b_joined':exp_b};manifest['profiles'][name]=rec
  servers={}
  for side,(port,mt,mu) in [('go',(GO_PORT,19801,19802)),('rust',(RUST_PORT,19811,19812))]:
   path=a.output/(f'{name}-{side}.yaml');path.write_text(yaml.safe_dump(cfg(port,mt,mu)))
   rec[side+'_config_sha256']=hashlib.sha256(path.read_bytes()).hexdigest()
   log=(a.output/(f'{name}-{side}.server.log')).open('wb')
   exe=[str(getattr(a,side).resolve()),'--dev','--config',str(path),'--port',str(port)]+extra
   proc=subprocess.Popen(exe,stdout=log,stderr=subprocess.STDOUT)
   servers[side]=(proc,log);rec[side+'_pid']=proc.pid
  save()
  try:
   for side,port in [('go',GO_PORT),('rust',RUST_PORT)]:wait_ready(f'http://127.0.0.1:{port}',servers[side][0])
   env=dict(os.environ,REFERENCE_WS_URL=f'ws://127.0.0.1:{GO_PORT}',LIVEKIT_WS_URL=f'ws://127.0.0.1:{RUST_PORT}',
    LIVEKIT_API_KEY='devkey',LIVEKIT_API_SECRET='secret',FMP_PROFILE=name,FMP_OUTPUT=str((a.output/f'{name}.pair.json').resolve()))
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
  rec['peer_b_joined']={s:pair['observations'][s]['peer_b']['joined'] for s in ['reference','rust']}
  rec['membership']={s:pair['observations'][s]['membership'] for s in ['reference','rust']}
  assert rec['peer_b_joined']['reference']==exp_b,(name,'oracle peer-B != expected',rec['peer_b_joined']['reference'],exp_b)
  assert rec['peer_b_joined']['rust']==rec['peer_b_joined']['reference'],(name,'Rust peer-B != Go',rec['peer_b_joined'])
 # Cross-profile control: Go peer-B outcome must DIFFER between flag-mp-1 (rejected) and flag-mp-off (joined).
 bj=manifest['profiles']['flag-mp-1']['peer_b_joined']['reference']
 yo=manifest['profiles']['flag-mp-off']['peer_b_joined']['reference']
 assert bj is False,('flag ignored: --room.max_participants=1 Go accepted the over-limit peer B',bj)
 assert yo is True,('flag-mp-off: Go rejected peer B with max_participants=0; baseline broken',yo)
 assert bj!=yo,('flag ignored: identical peer-B outcome across flag profiles',bj,yo)
 manifest['cross_profile_control']={'flag_mp1_peer_b_joined':bj,'flag_mpoff_peer_b_joined':yo,'differs':True}
 manifest['status']='PASS_GENERATED_CLI_FLAG_ROOM_MAX_PARTICIPANTS_EFFECT'
finally:
 save()
print(json.dumps({'status':manifest['status'],'profiles':list(FLAGS),'peer_b_joined':{n:manifest['profiles'][n].get('peer_b_joined') for n in FLAGS}}))
