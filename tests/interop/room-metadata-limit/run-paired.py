#!/usr/bin/env python3
"""Fresh paired Go/Rust servers per metadata-limit profile; retain failures, never count partials.
Each profile writes the SAME effective byte limit through a different YAML surface:
  - 'room-alias'  sets room.max_metadata_size (Go config.go copies it into Limit.MaxMetadataSize)
  - 'limit-key'   sets limit.max_metadata_size directly
Both must produce byte-identical CreateRoom/UpdateRoomMetadata enforcement in Rust vs the
pinned Go oracle, proving the config is consumed and enforced, not merely stored."""
import argparse, hashlib, json, os, pathlib, signal, socket, subprocess, time, urllib.request

p=argparse.ArgumentParser()
p.add_argument('--go',required=True,type=pathlib.Path)
p.add_argument('--rust',required=True,type=pathlib.Path)
p.add_argument('--node',default='node')
p.add_argument('--output',required=True,type=pathlib.Path)
a=p.parse_args()
here=pathlib.Path(__file__).resolve().parent
a.output.mkdir(parents=True,exist_ok=False)
GO_PORT,RUST_PORT,LIMIT=17880,17980,8
OVERLAYS={
 'room-alias':{'room':{'max_metadata_size':LIMIT}},
 'limit-key':{'limit':{'max_metadata_size':LIMIT}},
}
BASE={'bind_addresses':['127.0.0.1'],
 'rtc':{'tcp_port':None,'udp_port':None,'node_ip':'127.0.0.1','use_external_ip':False,'enable_loopback_candidate':True,'ips':{'includes':['127.0.0.1/32']}},
 'keys':{'devkey':'secret'},'turn':{'enabled':False},'logging':{'level':'error'}}
def write_config(port,media_tcp,media_udp,overlay):
 cfg=dict(BASE); cfg['port']=port
 cfg['rtc']={**BASE['rtc'],'tcp_port':media_tcp,'udp_port':media_udp}
 cfg.update(overlay)
 import yaml
 return cfg
manifest={'status':'INCOMPLETE','scope':'configured HTTP metadata-size enforcement vs pinned Go oracle; no SDK/media/webhook claim','binaries':{},'profiles':{}}
for side in ['go','rust']:
 b=getattr(a,side).resolve();manifest['binaries'][side]={'path':str(b),'sha256':hashlib.sha256(b.read_bytes()).hexdigest()}
def save():(a.output/'execution.json').write_text(json.dumps(manifest,indent=2)+'\n')
save()
def free(port):
 with socket.socket() as s: return s.connect_ex(('127.0.0.1',port))!=0
def wait_ready(url,proc,deadline=30):
 end=time.monotonic()+deadline
 while True:
  assert proc.poll() is None,('server exited',proc.returncode)
  try:
   with urllib.request.urlopen(url+'/',timeout=1) as r:
    if r.status==200: return
  except (OSError,TimeoutError):
   if time.monotonic()>=end: raise
  time.sleep(.1)
try:
 for name,overlay in OVERLAYS.items():
  assert free(GO_PORT) and free(RUST_PORT),('occupied base ports',name)
  rec={'profile':name,'overlay':overlay,'byte_limit':LIMIT}; manifest['profiles'][name]=rec
  servers={}
  for side,(port,mt,mu) in [('go',(GO_PORT,7881,7882)),('rust',(RUST_PORT,7981,7982))]:
   import yaml
   path=a.output/(f'{name}-{side}.yaml'); path.write_text(yaml.safe_dump(write_config(port,mt,mu,overlay)))
   rec[side+'_config_sha256']=hashlib.sha256(path.read_bytes()).hexdigest()
   log=(a.output/(f'{name}-{side}.server.log')).open('wb')
   proc=subprocess.Popen([str(getattr(a,side).resolve()),'--dev','--config',str(path)],stdout=log,stderr=subprocess.STDOUT)
   servers[side]=(proc,log); rec[side+'_pid']=proc.pid
  save()
  try:
   for side,port in [('go',GO_PORT),('rust',RUST_PORT)]:
    wait_ready(f'http://127.0.0.1:{port}',servers[side][0])
   env=dict(os.environ,REFERENCE_URL=f'http://127.0.0.1:{GO_PORT}',LIVEKIT_URL=f'http://127.0.0.1:{RUST_PORT}',
    LIVEKIT_API_KEY='devkey',LIVEKIT_API_SECRET='secret',EXPECT_BYTE_LIMIT=str(LIMIT),META_PROFILE=name,
    META_OUTPUT=str((a.output/f'{name}.pair.json').resolve()))
   with (a.output/(f'{name}.runner.log')).open('wb') as out:
    r=subprocess.run([a.node,str(here/'run.mjs')],env=env,stdout=out,stderr=subprocess.STDOUT,timeout=180)
   rec['runner_exit']=r.returncode
   assert r.returncode==0,(name,'runner failed; see',f'{name}.runner.log')
  finally:
   for side,(proc,log) in servers.items():
    if proc.poll() is None: proc.send_signal(signal.SIGINT)
    try: rec[side+'_exit']=proc.wait(timeout=30)
    except subprocess.TimeoutExpired: proc.kill(); rec[side+'_exit']=proc.wait(); rec[side+'_forced_kill']=True
    log.close()
  save()
  for side in ['go','rust']: assert rec.get(side+'_exit')==0,rec
  pair=json.loads((a.output/f'{name}.pair.json').read_text()); rec['pair_status']=pair['status']
  assert pair['status']=='PASS_SCOPED',(name,pair.get('error'))
 manifest['status']='PASS_CONFIGURED_METADATA_ENFORCEMENT_BOTH_PROFILES'
finally:
 save()
print(json.dumps({'status':manifest['status'],'profiles':list(OVERLAYS),'limit':LIMIT}))
