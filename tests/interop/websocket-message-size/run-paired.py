#!/usr/bin/env python3
"""Fresh real server pairs for the complete finite raw WebSocket size corpus."""
import argparse,hashlib,json,os,pathlib,signal,socket,subprocess,time,urllib.request
p=argparse.ArgumentParser();p.add_argument('--go',type=pathlib.Path,required=True);p.add_argument('--rust',type=pathlib.Path,required=True);p.add_argument('--output',type=pathlib.Path,required=True);a=p.parse_args();here=pathlib.Path(__file__).resolve().parent;a.output.mkdir(exist_ok=False,parents=True);sha=lambda f:hashlib.sha256(f.read_bytes()).hexdigest()
m={'status':'INCOMPLETE','binaries':{side:{'path':str(getattr(a,side).resolve()),'sha256':sha(getattr(a,side))}for side in ['go','rust']},'runs':[]};save=lambda:(a.output/'execution.json').write_text(json.dumps(m,indent=2)+'\n')
try:
 for profile,limit in [('positive20MiB',20971520),('positive1024',1024),('zero',0),('negative',-1)]:
  row={'profile':profile,'limit':limit,'servers':[]};m['runs'].append(row);save();processes=[];logs=[]
  try:
   for side,port in [('go',17880),('rust',17980)]:
    with socket.socket()as s:assert s.connect_ex(('127.0.0.1',port))!=0,('occupied',port)
    config=a.output/(side+'-'+profile+'.yaml');config.write_text((here/('base-'+side+'.yaml')).read_text()+'  signal_message_size_limit: '+str(limit)+'\n')
    log=(a.output/(side+'-'+profile+'.log')).open('wb');logs.append(log);proc=subprocess.Popen([str(getattr(a,side).resolve()),'--dev','--config',str(config.resolve())],stdout=log,stderr=subprocess.STDOUT);processes.append(proc);record={'side':side,'pid':proc.pid,'config_sha256':sha(config)};row['servers'].append(record);save();end=time.time()+30
    while True:
     assert proc.poll()is None,('server-exited',side,proc.returncode)
     try:
      with urllib.request.urlopen('http://127.0.0.1:'+str(port)+'/',timeout=1)as r:assert r.status==200
      break
     except OSError:
      assert time.time()<end
      time.sleep(.1)
   env=dict(os.environ,WS_CAP_INTEROP_ROOT=str(here.parent),WS_CAP_LIMIT=str(limit),WS_CAP_OFFICIAL_URL='ws://127.0.0.1:17880',WS_CAP_RUST_URL='ws://127.0.0.1:17980',LIVEKIT_API_KEY='devkey',LIVEKIT_API_SECRET='secret',WS_CAP_OUTPUT=str((a.output/(profile+'.json')).resolve()))
   with(a.output/(profile+'.runner.log')).open('wb')as log:result=subprocess.run(['node',str(here/'run.mjs')],env=env,stdout=log,stderr=subprocess.STDOUT,timeout=240)
   row['runner_exit']=result.returncode;save()
  finally:
   for proc,record in zip(processes,row['servers']):
    if proc.poll()is None:proc.send_signal(signal.SIGINT)
    try:record['exit']=proc.wait(timeout=30)
    except subprocess.TimeoutExpired:proc.kill();record['exit']=proc.wait();record['forced_kill']=True
   for log in logs:log.close()
   save()
 m['status']='PASS_SCOPED_ALL4_PROFILES'if len(m['runs'])==4 and all(r['runner_exit']==0 and len(r['servers'])==2 and all(s['exit']==0 for s in r['servers'])for r in m['runs'])else'FAIL_COMPLETE_CAMPAIGN'
finally:save()
raise SystemExit(0 if m['status']=='PASS_SCOPED_ALL4_PROFILES'else 1)
