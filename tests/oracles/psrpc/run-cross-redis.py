#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Real cross-language synthetic psrpc evidence only; broker is coordinator owned."""
import subprocess, pathlib, json, time, signal, os, hashlib
root=pathlib.Path(__file__).resolve().parents[3]; tmp=pathlib.Path(os.environ.get('PSRPC_RESULT_DIR','/tmp')); tmp.mkdir(parents=True,exist_ok=True)
rust=str(root/'target/debug/examples/psrpc_redis_probe'); go=os.environ.get('PSRPC_GO_BIN','/tmp/livekit-rs-f03-psrpc-go'); url=os.environ.get('PSRPC_REDIS_URL','redis://127.0.0.1:17639/')
rows=[]
run_id=str(os.getpid())+"-"+str(time.time_ns())
for direction in ['go-server','rust-server']:
 for mode in ['plain','claim','queue']:
  label='go' if direction=='go-server' else 'rust'; topic='cross-東京😀-'+run_id+'-'+direction+'-'+mode
  stem='livekit-rs-f03-cross-'+run_id+'-'+direction+'-'+mode
  ready=tmp/(stem+'-ready.json'); stop=tmp/(stem+'-stopped.json'); log=tmp/(stem+'-server.log')
  claim=mode!='plain'; queue=mode=='queue'
  if direction=='go-server':
   command=[go,'-mode','redis-serve','-redis-url',url,'-topic',topic,'-label',label,'-ready',str(ready),'-result',str(stop),'-lifetime','120s','-claim='+str(claim).lower(),'-queue='+str(queue).lower(),'-skip-claim='+str(queue).lower()]
  else:
   command=[rust,'serve','--redis-url',url,'--topic',topic,'--label',label,'--ready',str(ready),'--result',str(stop),'--lifetime-ms','120000','--claim',str(claim).lower(),'--queue',str(queue).lower(),'--skip-claim',str(queue).lower()]
  with log.open('w') as output:
   server=subprocess.Popen(command,stdout=output,stderr=subprocess.STDOUT)
   try:
    until=time.monotonic()+10
    while not ready.exists():
     if server.poll() is not None:raise RuntimeError('server exited before readiness: '+log.read_text()[:1000])
     if time.monotonic()>until:raise RuntimeError('actual subscription readiness timeout')
     time.sleep(.01)
    assert json.loads(ready.read_text())['ready']
    for case in ['hello','large','remote-error','deadline']:
     result=tmp/(stem+'-'+case+'.json'); clientlog=tmp/(stem+'-'+case+'.log')
     value='error' if case=='remote-error' else 'sleep:500' if case=='deadline' else 'hello'
     expected='invalid_argument' if case=='remote-error' else 'deadline_exceeded' if case=='deadline' else None
     if direction=='go-server':
      cmd=[rust,'call','--redis-url',url,'--topic',topic,'--expect-label',label,'--claim',str(claim).lower(),'--queue',str(queue).lower(),'--value',value,'--result',str(result)]
      if case=='large':cmd+=['--payload-bytes','32768']
      if case=='deadline':cmd+=['--timeout-ms','100']
      if expected:cmd+=['--expect-code',expected]
     else:
      cmd=[go,'-mode','redis-call','-redis-url',url,'-topic',topic,'-expect-label',label,'-claim='+str(claim).lower(),'-queue='+str(queue).lower(),'-value',value,'-result',str(result)]
      if case=='large':cmd+=['-payload-bytes','32768']
      if case=='deadline':cmd+=['-timeout','100ms']
      if expected:cmd+=['-expect-code',expected]
     with clientlog.open('w') as sink:r=subprocess.run(cmd,stdout=sink,stderr=subprocess.STDOUT,timeout=15)
     row={'direction':direction,'mode':mode,'case':case,'exit_code':r.returncode,'server_pid':server.pid}
     if result.exists():row['actual']=json.loads(result.read_text())
     rows.append(row)
     if r.returncode!=0:raise RuntimeError(str(row)+' '+clientlog.read_text()[:1000])
   finally:
    server.send_signal(signal.SIGINT) if server.poll() is None else None
    try:code=server.wait(timeout=10)
    except subprocess.TimeoutExpired:server.kill();code=server.wait();raise RuntimeError('server did not cleanly stop')
    for row in rows:
     if row.get('server_pid')==server.pid:
      row['server_exit_code']=code
      if stop.exists():row['server_stop']=json.loads(stop.read_text())
    (tmp/'livekit-rs-f03-redis-cross-results.json').write_text(json.dumps({'rows':rows},indent=2)+'\n')
    assert code==0, 'server stop failed'
print(json.dumps({'status':'PASS','cases':len(rows)}))
