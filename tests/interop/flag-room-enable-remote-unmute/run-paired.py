#!/usr/bin/env python3
"""Fresh paired Go/Rust servers for the GENERATED CLI FLAG --room.enable_remote_unmute;
both sides launched with the SAME base YAML (which leaves room.enable_remote_unmute at
the upstream default false) plus the SAME extra argv carrying the flag under test (only
HTTP/media ports differ). Retain failures, never count partials. This verifies the
FLAG INPUT PATH (flag parsed -> config.room.enable_remote_unmute -> RoomPolicy
-> admin RoomService/MutePublishedTrack unmute gate) and flag-over-YAML PRECEDENCE:
  flag-false            : base YAML (enable_remote_unmute unset=false), flag --room.enable_remote_unmute=false -> 412
  flag-true             : base YAML (enable_remote_unmute unset=false), flag --room.enable_remote_unmute=true  -> 200
  yaml-true-flag-false  : YAML room.enable_remote_unmute=true,           flag --room.enable_remote_unmute=false -> 412 (flag wins)
Cross-profile control: the oracle gives 412 (flag-false) vs 200 (flag-true) for the
identical admin unmute request against a real WS-joined present peer, and 412 when the
flag overrides YAML true -- so the flag is genuinely parsed and applied, not ignored.
Rust must equal the oracle per profile, byte-for-byte on the unmute response. This is
the flag INPUT PATH; the YAML value leaf room.enable_remote_unmute is a separate input
path (sibling fixture room-enable-remote-unmute)."""
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
GO_PORT,RUST_PORT=20400,20410
# profile -> (room YAML overlay, extra argv, oracle expected unmute HTTP status)
PROFILES={
 'flag-false':({},['--room.enable_remote_unmute=false'],412),
 'flag-true':({},['--room.enable_remote_unmute=true'],200),
 'yaml-true-flag-false':({'enable_remote_unmute':True},['--room.enable_remote_unmute=false'],412),
}
def cfg(port,mt,mu,room_overlay):
 c={'port':port,'bind_addresses':['127.0.0.1'],
    'rtc':{'tcp_port':mt,'udp_port':mu,'node_ip':'127.0.0.1','use_external_ip':False,'enable_loopback_candidate':True,'ips':{'includes':['127.0.0.1/32']}},
    'keys':{'devkey':'secret'},'turn':{'enabled':False},'logging':{'level':'error'},
    'room':{'empty_timeout':60}}
 if room_overlay: c['room'].update(room_overlay)
 return c
manifest={'status':'INCOMPLETE','scope':'generated CLI flag --room.enable_remote_unmute parse+apply+precedence vs pinned Go oracle on the admin MutePublishedTrack unmute gate; not the YAML value path, ENV, media, RTP or webhook claim','binaries':{},'profiles':{}}
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
 for name,(room_overlay,extra,exp_status) in PROFILES.items():
  assert free(GO_PORT) and free(RUST_PORT),('occupied base ports',name)
  rec={'profile':name,'yaml_room':room_overlay,'flag_args':extra,'expected_unmute_status':exp_status};manifest['profiles'][name]=rec
  servers={}
  for side,(port,mt,mu) in [('go',(GO_PORT,20401,20402)),('rust',(RUST_PORT,20411,20412))]:
   path=a.output/(f'{name}-{side}.yaml');path.write_text(yaml.safe_dump(cfg(port,mt,mu,room_overlay)))
   rec[side+'_config_sha256']=hashlib.sha256(path.read_bytes()).hexdigest()
   log=(a.output/(f'{name}-{side}.server.log')).open('wb')
   argv=[str(getattr(a,side).resolve()),'--dev','--config',str(path),'--port',str(port)]+extra
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
  assert refu['status']==exp_status,(name,'oracle unmute status != expected',refu['status'],exp_status)
  assert rustu==refu,(name,'Rust != Go',rustu,refu)
  if exp_status==412:
   assert refu.get('error',{}).get('code')=='failed_precondition',(name,'oracle denial code',refu)
   assert refu.get('error',{}).get('msg')=='remote unmute not enabled',(name,'oracle denial msg',refu)
 save()
 ff=manifest['profiles']['flag-false']['oracle_unmute']['status'];ft=manifest['profiles']['flag-true']['oracle_unmute']['status'];yo=manifest['profiles']['yaml-true-flag-false']['oracle_unmute']['status']
 assert ff==412 and ft==200 and ff!=ft,('flag ignored: oracle did not differentiate --room.enable_remote_unmute false vs true',ff,ft)
 assert yo==412,('flag did not override YAML room.enable_remote_unmute=true',yo)
 manifest['cross_profile_control']={'flag_false_unmute_status':ff,'flag_true_unmute_status':ft,'yaml_true_flag_false_unmute_status':yo,'flag_parsed_and_applied':True,'flag_overrides_yaml':True,'rust_deep_equal_go_per_profile':True}
 manifest['status']='PASS_GENERATED_CLI_FLAG_ROOM_ENABLE_REMOTE_UNMUTE_EFFECT'
finally:
 save()
print(json.dumps({'status':manifest['status'],'profiles':list(PROFILES),'oracle_unmute_status':{n:manifest['profiles'][n].get('oracle_unmute',{}).get('status') for n in PROFILES}}))
