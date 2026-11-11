#!/usr/bin/env python3
"""Actual raw argv/ENV -> production loader/logger processes. Coordinator only."""
import argparse,hashlib,json,os,pathlib,subprocess,tempfile
p=argparse.ArgumentParser();p.add_argument('--go',required=True);p.add_argument('--rust',required=True);p.add_argument('--out',required=True);a=p.parse_args()
if os.name!='posix':raise SystemExit('POSIX raw argument fixture requires POSIX')
raw=b'debug-\xff'
base={'development':True,'rtc':{'node_ip':'127.0.0.1'},'keys':{'controlled':'raw-logging-owned-secret-at-least-32-bytes'},'logging':{'json':True,'level':'debug','pion_level':'debug','component_levels':{'transport.pion.ice':'error'}}}
cases=[]
for field,env in [('log_level','LIVEKIT_LOG_LEVEL'),('logging.level','LIVEKIT_LOGGING_LEVEL'),('logging.pion_level','LIVEKIT_LOGGING_PION_LEVEL')]:
 cases.append((field+'-cli',[('--'+field).encode(),raw],{},{}))
 cases.append((field+'-env',[],{env.encode():raw},{}))
cases += [
 ('unicode-cli-over-raw-env',[b'--logging.level',b'error'],{b'LIVEKIT_LOGGING_LEVEL':raw},{}),
 ('raw-cli-over-unicode-env',[b'--logging.level',raw],{b'LIVEKIT_LOGGING_LEVEL':b'fatal'},{}),
 ('deprecated-unicode-over-raw-root',[b'--logging.level',raw],{}, {'log_level':'warn'}),
 ('deprecated-raw-over-unicode-root',[b'--log_level',raw],{}, {'logging':{'json':True,'level':'error','pion_level':'error'}}),
 ('repeated-last-unicode',[b'--logging.level',raw,b'--logging.level',b'fatal'],{},{}),
 ('repeated-last-raw',[b'--logging.level',b'fatal',b'--logging.level',raw],{},{}),
 ('pion-raw-child-debug',[b'--logging.pion_level',raw],{}, {'logging':{'json':True,'level':'error','pion_level':'error','component_levels':{'transport.pion.ice':'debug'}}}),
]
env={k:v for k,v in os.environb.items() if not k.startswith((b'LIVEKIT_',b'LIVECLOUD_'))}
h=lambda b:hashlib.sha256(b).hexdigest()
def run(binary,config,args,extra):
 e=env.copy();e.update(extra)
 try:r=subprocess.run([os.fsencode(binary),b'--config',os.fsencode(config)]+args,env=e,stdout=subprocess.PIPE,stderr=subprocess.PIPE,timeout=10)
 except subprocess.TimeoutExpired:return {'exit':'TIMEOUT','markers':[]}
 markers=[]
 for rawline in r.stderr.splitlines():
  if b'F137:' not in rawline:continue
  value=json.loads(rawline.decode('utf-8',errors='strict'));message=value.get('msg',value.get('fields',{}).get('message'))
  if not isinstance(message,str) or not message.startswith('F137:'):raise RuntimeError('invalid structured log marker')
  markers.append(message)
 return {'exit':r.returncode,'markers':markers}
rows=[]
with tempfile.TemporaryDirectory(prefix='raw-logging-owned-') as d:
 for name,args,extra,changes in cases:
  cfg=json.loads(json.dumps(base));cfg.update(changes);f=pathlib.Path(d)/(name+'.yaml');f.write_text(json.dumps(cfg))
  g=run(a.go,f,args,extra);rust=run(a.rust,f,args,extra)
  rows.append({'id':name,'go':g,'rust':rust,'equal':g==rust and g['exit']==0,'raw_value_hex':raw.hex()})
report={'status':'PASS_SCOPED' if all(r['equal'] for r in rows) else 'FAIL','scope':'13 actual raw-generated logging level/Pion loader+logger process cases; not server ICE/media or remaining26 rawString consumers','go_binary_sha256':h(pathlib.Path(a.go).read_bytes()),'rust_binary_sha256':h(pathlib.Path(a.rust).read_bytes()),'rows':rows}
pathlib.Path(a.out).write_text(json.dumps(report,indent=2)+'\n');raise SystemExit(0 if report['status']=='PASS_SCOPED' else 1)
