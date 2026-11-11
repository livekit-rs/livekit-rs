#!/usr/bin/env python3
"""Fresh paired Go/Rust servers for the GENERATED integer CLI FLAG
--limit.max_metadata_size; both sides launched with the same base YAML plus the same extra
argv. Retain failures, never count partials. This verifies the integer FLAG INPUT PATH
(flag parsed via parse_value Int -> config limits.max_metadata_size -> the CreateRoom
top-level metadata size gate) and its real side effect:
  metadata-10 : flag --limit.max_metadata_size=10 -> a 40-byte metadata is rejected
                (400, msg embeds the flag value "10"), a 5-byte metadata is accepted.
  default     : no flag (upstream default 512*1024) -> the SAME 40-byte metadata accepted.
Cross-profile control: the oracle gives 400 (metadata-10) vs 200 (default) for the same
over-budget request, and 200 for the under-budget request within metadata-10 -- so the
flag is genuinely parsed and applied as a threshold, not ignored. Rust must equal the
oracle per profile, byte-for-byte on the rejection error body."""
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
GO_PORT,RUST_PORT=19900,19910
# profile -> (extra argv, {case: expected oracle status})
PROFILES={
 'metadata-10':(['--limit.max_metadata_size=10'],{'over':400,'under':200}),
 'default':([],{'over':200,'under':200}),
}
def cfg(port,mt,mu):
 return {'port':port,'bind_addresses':['127.0.0.1'],
    'rtc':{'tcp_port':mt,'udp_port':mu,'node_ip':'127.0.0.1','use_external_ip':False,'enable_loopback_candidate':True,'ips':{'includes':['127.0.0.1/32']}},
    'keys':{'devkey':'secret'},'turn':{'enabled':False},'logging':{'level':'error'}}
manifest={'status':'INCOMPLETE','scope':'generated integer CLI flag --limit.max_metadata_size parse+apply on the CreateRoom top-level metadata size gate vs pinned Go oracle; not the UpdateParticipant/attributes path, WS join, media, ENV or webhook claim','binaries':{},'profiles':{}}
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
 for name,(extra,exp) in PROFILES.items():
  assert free(GO_PORT) and free(RUST_PORT),('occupied base ports',name)
  rec={'profile':name,'flag_args':extra,'expected_status':exp};manifest['profiles'][name]=rec
  servers={}
  for side,(port,mt,mu) in [('go',(GO_PORT,19901,19902)),('rust',(RUST_PORT,19911,19912))]:
   path=a.output/(f'{name}-{side}.yaml');path.write_text(yaml.safe_dump(cfg(port,mt,mu)))
   rec[side+'_config_sha256']=hashlib.sha256(path.read_bytes()).hexdigest()
   log=(a.output/(f'{name}-{side}.server.log')).open('wb')
   argv=[str(getattr(a,side).resolve()),'--dev','--config',str(path),'--port',str(port)]+extra
   proc=subprocess.Popen(argv,stdout=log,stderr=subprocess.STDOUT)
   servers[side]=(proc,log);rec[side+'_pid']=proc.pid
  save()
  try:
   for side,port in [('go',GO_PORT),('rust',RUST_PORT)]:wait_ready(f'http://127.0.0.1:{port}',servers[side][0])
   env=dict(os.environ,REFERENCE_URL=f'http://127.0.0.1:{GO_PORT}',LIVEKIT_URL=f'http://127.0.0.1:{RUST_PORT}',
    AMD_PROFILE=name,AMD_OUTPUT=str((a.output/f'{name}.pair.json').resolve()))
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
  refout={o['case']:o for o in pair['observations']['reference']['out']}
  rustout={o['case']:o for o in pair['observations']['rust']['out']}
  rec['oracle_status']={c:refout[c]['status'] for c in exp}
  rec['rust_status']={c:rustout[c]['status'] for c in exp}
  for c,e in exp.items():
   assert refout[c]['status']==e,(name,c,'oracle status != expected',refout[c]['status'],e)
   assert rustout[c]==refout[c],(name,c,'Rust != Go',rustout[c],refout[c])
  if exp['over']==400:
   assert refout['over'].get('error',{}).get('msg','').endswith(' 10'),(name,'flag value 10 not embedded in oracle rejection msg',refout['over'].get('error'))
 m10=manifest['profiles']['metadata-10'];dflt=manifest['profiles']['default']
 over10=m10['oracle_status']['over'];overdef=dflt['oracle_status']['over'];under10=m10['oracle_status']['under']
 assert over10==400 and overdef==200 and over10!=overdef,('flag ignored: same over-budget request not differentiated',over10,overdef)
 assert under10==200,('flag not a threshold: under-budget wrongly rejected under metadata-10',under10)
 manifest['cross_profile_control']={'metadata10_over':over10,'default_over':overdef,'metadata10_under':under10,'flag_parsed_and_applied':True,'flag_value_in_error_msg':True}
 manifest['status']='PASS_GENERATED_CLI_FLAG_LIMIT_MAX_METADATA_SIZE_EFFECT'
finally:
 save()
print(json.dumps({'status':manifest['status'],'profiles':list(PROFILES),'oracle_status':{n:manifest['profiles'][n].get('oracle_status') for n in PROFILES}}))
