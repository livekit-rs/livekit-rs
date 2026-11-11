#!/usr/bin/env python3
"""Fresh paired Go/Rust servers for the YAML value leaf limit.max_attributes_size;
both sides launched with the SAME base YAML overlay (setting the leaf) and NO extra
argv. Retain failures, never count partials. This verifies the YAML INPUT PATH
(yaml -> config limits.max_attributes_size -> the CreateRoom agent-attributes size
gate) and its real side effect:
  yaml-10 : limit.max_attributes_size=10 -> a 41-byte agent attribute set is rejected
            (400, msg embeds the YAML value "10"), a 6-byte set is accepted.
  default : no YAML limit (upstream default 65536) -> the SAME 41-byte set is
            accepted (200).
Cross-profile control: the oracle gives 400 (yaml-10) vs 200 (default) for the same
over-limit request, and 200 for the under-limit request within yaml-10 -- so the YAML
value is genuinely parsed and applied as a threshold, not ignored. Rust must equal the
oracle per profile, byte-for-byte on the rejection error body. Sibling of
flag-limit-attributes-size (CLI flag input path, same enforcement method)."""
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
GO_PORT,RUST_PORT=20000,20010
# profile -> (limit YAML overlay, {case: expected oracle status})
PROFILES={
 'yaml-10':({'max_attributes_size':10},{'over':400,'under':200}),
 'default':({},{'over':200,'under':200}),
}
def cfg(port,mt,mu,limit_overlay):
 c={'port':port,'bind_addresses':['127.0.0.1'],
    'rtc':{'tcp_port':mt,'udp_port':mu,'node_ip':'127.0.0.1','use_external_ip':False,'enable_loopback_candidate':True,'ips':{'includes':['127.0.0.1/32']}},
    'keys':{'devkey':'secret'},'turn':{'enabled':False},'logging':{'level':'error'}}
 if limit_overlay: c['limit']=dict(limit_overlay)
 return c
manifest={'status':'INCOMPLETE','scope':'YAML value leaf limit.max_attributes_size parsed and applied on the CreateRoom agent-attributes size gate vs pinned Go oracle; not the CLI flag input path (sibling fixture), not the metadata/agents dispatch side effects, WS join, media, ENV or webhook claim','binaries':{},'profiles':{}}
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
 for name,(limit_overlay,exp) in PROFILES.items():
  assert free(GO_PORT) and free(RUST_PORT),('occupied base ports',name)
  rec={'profile':name,'yaml_limit':limit_overlay,'flag_args':[],'expected_status':exp};manifest['profiles'][name]=rec
  servers={}
  for side,(port,mt,mu) in [('go',(GO_PORT,20001,20002)),('rust',(RUST_PORT,20011,20012))]:
   path=a.output/(f'{name}-{side}.yaml');path.write_text(yaml.safe_dump(cfg(port,mt,mu,limit_overlay)))
   rec[side+'_config_sha256']=hashlib.sha256(path.read_bytes()).hexdigest()
   log=(a.output/(f'{name}-{side}.server.log')).open('wb')
   # NO extra argv: only YAML drives the limit
   argv=[str(getattr(a,side).resolve()),'--dev','--config',str(path),'--port',str(port)]
   proc=subprocess.Popen(argv,stdout=log,stderr=subprocess.STDOUT)
   servers[side]=(proc,log);rec[side+'_pid']=proc.pid
  save()
  try:
   for side,port in [('go',GO_PORT),('rust',RUST_PORT)]:wait_ready(f'http://127.0.0.1:{port}',servers[side][0])
   env=dict(os.environ,REFERENCE_URL=f'http://127.0.0.1:{GO_PORT}',LIVEKIT_URL=f'http://127.0.0.1:{RUST_PORT}',
    YAML_AF_PROFILE=name,YAML_AF_OUTPUT=str((a.output/f'{name}.pair.json').resolve()))
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
  # the YAML value must reach the enforcement message (int parse proven observable)
  if exp['over']==400:
   assert refout['over'].get('error',{}).get('msg','').endswith(' 10'),(name,'YAML value 10 not embedded in oracle rejection msg',refout['over'].get('error'))
 save()
 y10=manifest['profiles']['yaml-10'];dflt=manifest['profiles']['default']
 over10=y10['oracle_status']['over'];overdef=dflt['oracle_status']['over'];under10=y10['oracle_status']['under']
 assert over10==400 and overdef==200 and over10!=overdef,('YAML ignored: same over-limit request not differentiated',over10,overdef)
 assert under10==200,('YAML not a threshold: under-limit wrongly rejected under yaml-10',under10)
 manifest['cross_profile_control']={'yaml10_over':over10,'default_over':overdef,'yaml10_under':under10,'yaml_value_parsed_and_applied':True,'yaml_value_in_error_msg':True}
 manifest['status']='PASS_YAML_LIMIT_MAX_ATTRIBUTES_SIZE_EFFECT'
finally:
 save()
print(json.dumps({'status':manifest['status'],'profiles':list(PROFILES),'oracle_status':{n:manifest['profiles'][n].get('oracle_status') for n in PROFILES}}))
