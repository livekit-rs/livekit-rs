#!/usr/bin/env python3
"""Paired actual loader+logger subprocesses; no server/browser launch.
Supply compiled Go source observer and Rust production-logger example.
"""
import argparse,json,os,pathlib,subprocess,tempfile
p=argparse.ArgumentParser();p.add_argument('--go',required=True);p.add_argument('--rust',required=True);p.add_argument('--out',required=True);a=p.parse_args()
profiles=[
 ('pion-muted','debug','error',{}),
 ('pion-enabled','error','debug',{}),
 ('child-fatal','error','debug',{'transport.pion.ice':'fatal'}),
 ('child-debug','error','error',{'transport.pion.ice':'debug'}),
 ('literal-key','error','error',{'bad target,[x]=debug':'debug'}),
 ('warning-alias','warning','error',{}),
 ('fatal','fatal','error',{}),
 ('dtls-child','error','error',{'transport.pion.dtls':'debug'}),
 ('pion-overwrites-parent','error','error',{'transport.pion':'debug'}),
]
env={k:v for k,v in os.environ.items() if not k.startswith(('LIVEKIT_','LIVECLOUD_'))}
def run(cmd):
 r=subprocess.run(cmd,env=env,stdout=subprocess.PIPE,stderr=subprocess.PIPE,timeout=10)
 markers=[]
 for line in r.stderr.decode('utf-8',errors='strict').splitlines():
  if 'F137:' not in line:continue
  item=json.loads(line);message=item.get('msg',item.get('fields',{}).get('message'))
  if not isinstance(message,str) or not message.startswith('F137:'):raise RuntimeError('invalid marker record')
  markers.append(message)
 return {'exit':r.returncode,'markers':markers}
rows=[]
with tempfile.TemporaryDirectory(prefix='logging-f137-owned-') as d:
 for id,level,pion,components in profiles:
  # JSON is valid YAML; source loader, not a manually constructed normalized tree.
  cfg={'development':True,'rtc':{'node_ip':'127.0.0.1'},'keys':{'devkey':'owned-logging-fixture-secret'},'logging':{'json':True,'level':level,'pion_level':pion,'component_levels':components}}
  file=pathlib.Path(d)/(id+'.yaml');file.write_text(json.dumps(cfg))
  g=run([a.go,'process',str(file)]);r=run([a.rust,'--config',str(file)])
  rows.append({'id':id,'go':g,'rust':r,'equal':g==r and g['exit']==0})
pathlib.Path(a.out).write_text(json.dumps({'status':'PASS_SCOPED' if all(x['equal'] for x in rows) else 'FAIL','rows':rows},indent=2)+'\n')
raise SystemExit(0 if all(x['equal'] for x in rows) else 1)
