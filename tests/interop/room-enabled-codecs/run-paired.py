#!/usr/bin/env python3
"""Fresh paired Go/Rust servers per room.enabled_codecs profile; retain failures,
never count partials. Go roomallocator.go applyDefaultRoomConfig appends the
configured conf.EnabledCodecs into the created room verbatim; Rust normalize.rs:575
projects them in order. Two profiles share the SAME CreateRoom request:
  custom  : room.enabled_codecs = [opus, VP8(x=7), H264(profile=42e01f)] -> 3 codecs
  default : room.enabled_codecs unset                                     -> 10 defaults
Cross-profile control: the pinned oracle reports 3 vs 10 codecs for the same request,
so a server ignoring the config could not reproduce the difference; Rust must
deep-equal the oracle ordered [{mime,fmtp_line}] per profile."""
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
GO_PORT,RUST_PORT=19100,19110
PROFILES={
 'custom':({'room':{'enabled_codecs':[{'mime':'audio/opus'},{'mime':'video/VP8','fmtp_line':'x=7'},{'mime':'video/H264','fmtp_line':'profile=42e01f'}]}},3),
 'default':({'room':{}},10),
}
def cfg(port,mt,mu,overlay):
 c={'port':port,'bind_addresses':['127.0.0.1'],
    'rtc':{'tcp_port':mt,'udp_port':mu,'node_ip':'127.0.0.1','use_external_ip':False,'enable_loopback_candidate':True,'ips':{'includes':['127.0.0.1/32']}},
    'keys':{'devkey':'secret'},'turn':{'enabled':False},'logging':{'level':'error'}}
 c.update(overlay); return c
manifest={'status':'INCOMPLETE','scope':'configured room.enabled_codecs list projected into the created room public-Room enabled_codecs vs pinned Go oracle; not codec negotiation, media, CLI per-key flag, ENV or webhook claim','binaries':{},'profiles':{}}
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
 for name,(overlay,exp_count) in PROFILES.items():
  assert free(GO_PORT) and free(RUST_PORT),('occupied base ports',name)
  rec={'profile':name,'overlay':overlay,'expected_codec_count':exp_count};manifest['profiles'][name]=rec
  servers={}
  for side,(port,mt,mu) in [('go',(GO_PORT,19101,19102)),('rust',(RUST_PORT,19111,19112))]:
   path=a.output/(f'{name}-{side}.yaml');path.write_text(yaml.safe_dump(cfg(port,mt,mu,overlay)))
   rec[side+'_config_sha256']=hashlib.sha256(path.read_bytes()).hexdigest()
   log=(a.output/(f'{name}-{side}.server.log')).open('wb')
   proc=subprocess.Popen([str(getattr(a,side).resolve()),'--dev','--config',str(path)],stdout=log,stderr=subprocess.STDOUT)
   servers[side]=(proc,log);rec[side+'_pid']=proc.pid
  save()
  try:
   for side,port in [('go',GO_PORT),('rust',RUST_PORT)]:wait_ready(f'http://127.0.0.1:{port}',servers[side][0])
   env=dict(os.environ,REFERENCE_URL=f'http://127.0.0.1:{GO_PORT}',LIVEKIT_URL=f'http://127.0.0.1:{RUST_PORT}',
    EC_PROFILE=name,EC_OUTPUT=str((a.output/f'{name}.pair.json').resolve()))
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
  rec['oracle_codecs']=pair['observations']['reference']['codecs']
  rec['oracle_codec_count']=len(pair['observations']['reference']['codecs'])
  assert rec['oracle_codec_count']==exp_count,(name,'oracle codec count != expected',rec['oracle_codec_count'],exp_count)
 cc=manifest['profiles']['custom']['oracle_codec_count']
 dc=manifest['profiles']['default']['oracle_codec_count']
 assert cc==3 and dc==10 and cc!=dc,('config ignored: oracle did not differentiate custom vs default enabled_codecs',cc,dc)
 manifest['cross_profile_control']={'custom_codec_count':cc,'default_codec_count':dc,'differs':True}
 manifest['status']='PASS_CONFIGURED_ENABLED_CODECS_EFFECT'
finally:
 save()
print(json.dumps({'status':manifest['status'],'profiles':list(PROFILES),'codec_count':{n:manifest['profiles'][n].get('oracle_codec_count') for n in PROFILES}}))
