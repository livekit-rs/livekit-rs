#!/usr/bin/env python3
"""Owned real server + unchanged official-SDK media + native logging consumers.
Raw logs/configs/SDK reports stay in --work; persist only sanitized report.
"""
import argparse, hashlib, json, os, pathlib, signal, socket, subprocess, time
p=argparse.ArgumentParser()
p.add_argument('--server', required=True, type=pathlib.Path)
p.add_argument('--work', required=True, type=pathlib.Path)
p.add_argument('--out', required=True, type=pathlib.Path)
a=p.parse_args()
server=a.server.resolve();root=pathlib.Path(__file__).resolve().parents[3]
a.work.mkdir(parents=True, exist_ok=False)
identity=hashlib.sha256(server.read_bytes()).hexdigest()
profiles=[('pion-muted','debug','error',{},False),
          ('pion-enabled','error','debug',{},True),
          ('child-fatal','error','debug',{'transport.pion.ice':'fatal'},False),
          ('child-debug','error','error',{'transport.pion.ice':'debug'},True),
          ('literal-key','error','error',{'bad target,[x]=debug':'debug'},False)]
report={'status':'RUNNING_NOT_ACCEPTED','binary_sha256':identity,'runs':[],
        'scope':'Actual native ICE producer filtering with unchanged Single/dual official SDK media; not sampling or complete component attribution'}
try:
 for index,(name,level,pion,components,enabled) in enumerate(profiles):
  port=19120+index;cfg={'development':True,'port':port,'bind_addresses':['127.0.0.1'],
   'rtc':{'tcp_port':19220+index,'udp_port':19320+index,'node_ip':'127.0.0.1','use_external_ip':False,
          'enable_loopback_candidate':True,'congestion_control':{'enabled':True,'use_send_side_bwe':False,'use_send_side_bwe_interceptor':False}},
   'keys':{'devkey':'secret'},'logging':{'json':True,'level':level,'pion_level':pion,'component_levels':components}}
  conf=a.work/(name+'.yaml');conf.write_text(json.dumps(cfg));raw=a.work/(name+'-sdk.json');logfile=a.work/(name+'-server.log');row={'profile':name,'config_sha256':hashlib.sha256(conf.read_bytes()).hexdigest()};proc=None
  report['runs'].append(row)
  try:
   with logfile.open('w') as log:
    env={k:v for k,v in os.environ.items() if not k.startswith(('LIVEKIT_','LIVECLOUD_'))}
    proc=subprocess.Popen([str(server),'--config',str(conf)],cwd=root,env=env,stdout=log,stderr=subprocess.STDOUT)
    deadline=time.monotonic()+20
    while True:
     assert proc.poll() is None, 'server exited before readiness'
     try:
      with socket.create_connection(('127.0.0.1',port),timeout=.2):break
     except OSError:
      assert time.monotonic()<deadline,'server readiness timeout';time.sleep(.1)
    env.update(LIVEKIT_URL='ws://127.0.0.1:'+str(port),FEEDBACK_MODE='remote',FEEDBACK_OUTPUT=str(raw))
    with (a.work/(name+'-runner.log')).open('w') as f:
     runner=subprocess.Popen(['node',str(root/'tests/interop/functional-feedback-runtime.mjs')],cwd=root.parent,env=env,stdout=f,stderr=subprocess.STDOUT,start_new_session=True)
     try:code=runner.wait(timeout=180)
     except subprocess.TimeoutExpired:
      os.killpg(runner.pid,signal.SIGTERM)
      try:runner.wait(timeout=5)
      except subprocess.TimeoutExpired:os.killpg(runner.pid,signal.SIGKILL);runner.wait()
      raise
    row['runner_exit']=code;assert code==0,'unchanged SDK failed'
    sdk=json.loads(raw.read_text());assert sdk['status']=='PASS' and len(sdk['cases'])==2
    row['sdk']={'status':sdk['status'],'cases':[{'single':x['single'],'status':x['status']} for x in sdk['cases']],'source_inputs':sdk['sourceInputs']}
  finally:
   if proc is not None:
    if proc.poll() is None:proc.send_signal(signal.SIGINT)
    try:row['server_exit']=proc.wait(timeout=20)
    except subprocess.TimeoutExpired:
     proc.kill();row['server_exit']=proc.wait();row['forced_cleanup']=True
  assert row['server_exit']==0,'server did not stop cleanly'
  records=[]
  for line in logfile.read_text().splitlines():
   try:item=json.loads(line)
   except json.JSONDecodeError:continue
   if isinstance(item,dict):records.append(item)
  ice=[x for x in records if x.get('target','').startswith('is::') and x.get('level') in ('DEBUG','TRACE')]
  row['actual_native_ice_debug_or_trace_records']=len(ice)
  row['expected_native_ice_enabled']=enabled
  assert bool(ice)==enabled,'native ICE component threshold not consumed'
  if level=='error':assert not any(x.get('level')=='INFO' and x.get('target','').startswith('livecloud') for x in records),'root policy changed'
  row['status']='PASS'
  a.out.write_text(json.dumps(report,indent=2)+'\n')
 assert hashlib.sha256(server.read_bytes()).hexdigest()==identity,'binary changed midcampaign'
 report['status']='PASS_FIVE_CONFIG_TEN_SDK_PROFILES_AND_REAL_NATIVE_LOGS'
finally:
 a.out.write_text(json.dumps(report,indent=2)+'\n')
