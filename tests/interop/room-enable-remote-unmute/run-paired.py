#!/usr/bin/env python3
"""Fresh paired Go/Rust servers for the YAML value leaf room.enable_remote_unmute;
both sides launched with the SAME base YAML (setting room.enable_remote_unmute to the
profile value) and NO extra argv. Retain failures, never count partials. This verifies
the YAML INPUT PATH (yaml room.enable_remote_unmute -> RoomPolicy.enable_remote_unmute
-> the admin RoomService/MutePublishedTrack unmute gate) and its real side effect:
  eru-false : room.enable_remote_unmute=false -> an admin UNMUTE (muted=false) against a
              real WS-joined present peer is DENIED (HTTP 412, code failed_precondition,
              message "remote unmute not enabled").
  eru-true  : room.enable_remote_unmute=true  -> the SAME admin unmute is ALLOWED (200).
Cross-profile control: the oracle gives 412 (false) vs 200 (true) for the identical
unmute request against a present participant, so the YAML boolean is genuinely parsed
and applied at the gate, not ignored. Rust must equal the oracle per profile, byte-for-
byte on the unmute response (status + error body). This is the YAML value leaf; the
sibling --room.enable_remote_unmute CLI flag and LIVEKIT_ROOM_ENABLE_REMOTE_UNMUTE ENV
are separate input paths."""
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
GO_PORT,RUST_PORT=20300,20310
# profile -> (room.enable_remote_unmute YAML value, oracle expected unmute HTTP status)
PROFILES={
 'eru-false':(False,412),
 'eru-true':(True,200),
}
def cfg(port,mt,mu,eru):
 c={'port':port,'bind_addresses':['127.0.0.1'],
    'rtc':{'tcp_port':mt,'udp_port':mu,'node_ip':'127.0.0.1','use_external_ip':False,'enable_loopback_candidate':True,'ips':{'includes':['127.0.0.1/32']}},
    'keys':{'devkey':'secret'},'turn':{'enabled':False},'logging':{'level':'error'},
    'room':{'empty_timeout':60,'enable_remote_unmute':eru}}
 return c
manifest={'status':'INCOMPLETE','scope':'YAML value leaf room.enable_remote_unmute parsed and applied on the admin MutePublishedTrack unmute gate vs pinned Go oracle; not the CLI flag/ENV input path, not a real published-track mute effect, not media, RTP or webhook claim','binaries':{},'profiles':{}}
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
 for name,(eru,exp_status) in PROFILES.items():
  assert free(GO_PORT) and free(RUST_PORT),('occupied base ports',name)
  rec={'profile':name,'yaml_enable_remote_unmute':eru,'flag_args':[],'expected_unmute_status':exp_status};manifest['profiles'][name]=rec
  servers={}
  for side,(port,mt,mu) in [('go',(GO_PORT,20301,20302)),('rust',(RUST_PORT,20311,20312))]:
   path=a.output/(f'{name}-{side}.yaml');path.write_text(yaml.safe_dump(cfg(port,mt,mu,eru)))
   rec[side+'_config_sha256']=hashlib.sha256(path.read_bytes()).hexdigest()
   log=(a.output/(f'{name}-{side}.server.log')).open('wb')
   # NO extra argv: only YAML drives the leaf
   argv=[str(getattr(a,side).resolve()),'--dev','--config',str(path),'--port',str(port)]
   proc=subprocess.Popen(argv,stdout=log,stderr=subprocess.STDOUT)
   servers[side]=(proc,log);rec[side+'_pid']=proc.pid
  save()
  try:
   for side,port in [('go',GO_PORT),('rust',RUST_PORT)]:wait_ready(f'http://127.0.0.1:{port}',servers[side][0])
   env=dict(os.environ,REFERENCE_WS_URL=f'ws://127.0.0.1:{GO_PORT}',LIVEKIT_WS_URL=f'ws://127.0.0.1:{RUST_PORT}',
    ERU_PROFILE=name,ERU_EXPECT_UNMUTE=('true' if exp_status==200 else 'false'),ERU_OUTPUT=str((a.output/f'{name}.pair.json').resolve()))
   with (a.output/(f'{name}.runner.log')).open('wb') as out:
    r=subprocess.run([a.node,str(here/'run.mjs')],env=env,stdout=out,stderr=subprocess.STDOUT,timeout=90)
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
  refu=pair['observations']['reference']['unmute'];rustu=pair['observations']['rust']['unmute']
  rec['oracle_unmute']=refu;rec['rust_unmute']=rustu
  # oracle must produce the expected profile outcome; Rust byte-equal Go
  assert refu['status']==exp_status,(name,'oracle unmute status != expected',refu['status'],exp_status)
  assert rustu==refu,(name,'Rust != Go',rustu,refu)
  if exp_status==412:
   assert refu.get('error',{}).get('code')=='failed_precondition',(name,'oracle denial code',refu)
   assert refu.get('error',{}).get('msg')=='remote unmute not enabled',(name,'oracle denial msg',refu)
 save()
 f=manifest['profiles']['eru-false']['oracle_unmute']['status'];t=manifest['profiles']['eru-true']['oracle_unmute']['status']
 assert f==412 and t==200 and f!=t,('YAML leaf ignored: same unmute request not differentiated',f,t)
 manifest['cross_profile_control']={'eru_false_unmute_status':f,'eru_true_unmute_status':t,'yaml_leaf_parsed_and_applied':True,'rust_deep_equal_go_per_profile':True}
 manifest['status']='PASS_YAML_ROOM_ENABLE_REMOTE_UNMUTE_EFFECT'
finally:
 save()
print(json.dumps({'status':manifest['status'],'profiles':list(PROFILES),'oracle_unmute_status':{n:manifest['profiles'][n].get('oracle_unmute',{}).get('status') for n in PROFILES}}))
