#!/usr/bin/env python3
"""Owned two-node shared-Redis official-SDK media matrix; never attaches to existing servers."""
from pathlib import Path
import argparse,hashlib,json,os,signal,shutil,socket,subprocess,threading,time,urllib.request
p=argparse.ArgumentParser(description=__doc__)
p.add_argument('--go',type=Path,required=True);p.add_argument('--rust',type=Path,required=True)
p.add_argument('--redis-server',type=Path,default=Path(shutil.which('redis-server') or '/missing/redis-server'))
p.add_argument('--output',type=Path,required=True);p.add_argument('--duration-ms',type=int,default=5000)
p.add_argument('--capture-stalls',action='store_true',help='macOS read-only sample of an owned Rust process after its observed Redis heartbeat is older than4s')
p.add_argument('--only',choices=[left+'-'+right+'-'+mode for left,right in [('go','go'),('rust','rust'),('go','rust'),('rust','go')]for mode in ['single','dual']],help='explicit calibration subset; never labeled complete matrix')
a=p.parse_args();here=Path(__file__).resolve().parent;interop=here.parent
assert a.output.is_absolute(),'absolute independent output required';a.output.mkdir(parents=True,exist_ok=False,mode=0o700)
assert 5000<=a.duration_ms<=30000,'bounded meaningful media duration5000..30000ms'
sha=lambda f:hashlib.sha256(f.read_bytes()).hexdigest()
manifest={'runner_sha256':sha(Path(__file__)), 'status':'INCOMPLETE','scope':'Official unmodified livekit-client2.23.0 two actual entries/shared Redis with real tones/data/VP8/reconnect/release; no workers/global certification','binaries':{key:{'path':str(getattr(a,key).resolve()),'sha256':sha(getattr(a,key))}for key in ['go','rust','redis_server']},'sdk_inputs':{str(f.relative_to(interop)):sha(f)for f in [interop/'package.json',interop/'package-lock.json',interop/'run.mjs',interop/'client.html']},'selection':a.only or 'all8','cases':[]}
def save():(a.output/'execution.json').write_text(json.dumps(manifest,indent=2)+'\n')
def redis(port,*args):
 with socket.create_connection(('127.0.0.1',port),timeout=2)as s:
  s.settimeout(2);parts=[str(x).encode()for x in args];s.sendall(b'*'+str(len(parts)).encode()+b'\r\n'+b''.join(b'$'+str(len(x)).encode()+b'\r\n'+x+b'\r\n'for x in parts));f=s.makefile('rb')
  def read():
   line=f.readline();assert line.endswith(b'\r\n'),('redis truncated',line);kind=line[:1];value=line[1:-2]
   if kind==b'+':return value
   if kind==b'-':raise RuntimeError(value.decode())
   if kind==b':':return int(value)
   if kind==b'$':
    n=int(value)
    if n==-1:return None
    data=f.read(n);assert len(data)==n and f.read(2)==b'\r\n';return data
   if kind==b'*':return [read()for _ in range(int(value))]
   raise RuntimeError('unexpected RESP type')
  return read()
def wire_fields(data):
 fields={};offset=0
 def varint():
  nonlocal offset
  value=0
  for shift in range(0,70,7):
   assert offset<len(data),'truncated node protobuf';byte=data[offset];offset+=1;value|=(byte&127)<<shift
   if byte<128:return value
  raise ValueError('oversized node protobuf varint')
 while offset<len(data):
  tag=varint();number,kind=tag>>3,tag&7;assert number>0
  if kind==0:value=varint()
  elif kind in (1,2,5):
   length=varint()if kind==2 else(8 if kind==1 else 4);assert length<=len(data)-offset;value=data[offset:offset+length];offset+=length
  else:raise ValueError('unexpected node protobuf wire type')
  fields[number]=value
 return fields

def node_samples(port):
 rows=redis(port,'HGETALL','nodes');result={}
 for i in range(0,len(rows),2):
  raw=rows[i+1];node=wire_fields(raw);stats=wire_fields(node.get(4,b''))
  result[rows[i].decode()]={'id':node.get(1,b'').decode(),'state':node.get(6,0),'updated_at':stats.get(2,0),'wire_sha256':hashlib.sha256(raw).hexdigest(),'raw_wire_hex':raw.hex()}
 return result

def mapping(port,key):
 rows=redis(port,'HGETALL',key);return{rows[i].decode():rows[i+1].decode(errors='replace')for i in range(0,len(rows),2)}
def free(port,udp=False):
 with socket.socket(socket.AF_INET,socket.SOCK_DGRAM if udp else socket.SOCK_STREAM)as s:
  # A completed owned TCP listener can leave TIME_WAIT; reuse matches the
  # actual servers while a still-listening foreign owner remains rejected.
  if not udp:s.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1)
  s.bind(('127.0.0.1',port))
def wait(predicate,label,timeout=30):
 end=time.monotonic()+timeout
 while time.monotonic()<end:
  if predicate():return
  time.sleep(.05)
 raise RuntimeError('deadline: '+label)
def alive_http(port,proc):
 assert proc.poll()is None,('server exited',proc.returncode)
 try:
  with urllib.request.urlopen('http://127.0.0.1:'+str(port)+'/',timeout=1)as r:return r.status==200
 except OSError:return False
def stop(proc,record):
 if proc.poll()is None:proc.send_signal(signal.SIGINT)
 try:record['exit']=proc.wait(timeout=30)
 except subprocess.TimeoutExpired:record['forced_kill']=True;proc.kill();record['exit']=proc.wait(timeout=10)
def config(http,media_tcp,media_udp,redis_port):
 return {'port':http,'bind_addresses':['127.0.0.1'],'redis':{'address':'127.0.0.1:'+str(redis_port)},'rtc':{'tcp_port':media_tcp,'udp_port':media_udp,'node_ip':'127.0.0.1','use_external_ip':False,'enable_loopback_candidate':True,'ips':{'includes':['127.0.0.1/32']}},'keys':{'devkey':'secret'},'turn':{'enabled':False},'logging':{'level':'debug'},'room':{'empty_timeout':10,'departure_timeout':2}}
save()
for kinds in [('go','go'),('rust','rust'),('go','rust'),('rust','go')]:
 for single in [True,False]:
  name='-'.join(kinds)+('-single'if single else'-dual')
  if a.only and a.only!=name:continue
  out=a.output/name;out.mkdir();row={'name':name,'kinds':kinds,'single':single,'status':'INCOMPLETE','servers':[],'ownership_samples':[],'routing_samples':[]};manifest['cases'].append(row);save()
  owned=[];logs=[];collector_stop=threading.Event();collector=None;captures=[];captured=set();row['stall_captures']=[];redis_port=20799;http_ports=[20780,20790];media_tcp=[20781,20791];media_udp=[20782,20792]
  try:
   for port in [redis_port]+http_ports+media_tcp:free(port)
   for port in media_udp:free(port,True)
   log=(out/'redis.log').open('wb');logs.append(log);rp=subprocess.Popen([str(a.redis_server.resolve()),'--bind','127.0.0.1','--port',str(redis_port),'--save','','--appendonly','no','--dir',str(out)],stdout=log,stderr=subprocess.STDOUT);rr={'kind':'redis','pid':rp.pid};owned.append((rp,rr));row['redis']=rr
   def redis_ready():
    assert rp.poll()is None,('redis exited',rp.returncode)
    try:return redis(redis_port,'PING')==b'PONG'
    except OSError:return False
   wait(redis_ready,'owned redis ready')
   ids=set()
   for index,kind in enumerate(kinds):
    cfg=out/f'node-{index}.yaml';cfg.write_text(json.dumps(config(http_ports[index],media_tcp[index],media_udp[index],redis_port),indent=2)+'\n');log=(out/f'node-{index}.log').open('wb');logs.append(log)
    proc=subprocess.Popen([str(getattr(a,kind).resolve()),'--config',str(cfg),'--dev'],stdout=log,stderr=subprocess.STDOUT);record={'kind':kind,'index':index,'pid':proc.pid,'config_sha256':sha(cfg),'entry':'ws://127.0.0.1:'+str(http_ports[index]),'media_tcp':media_tcp[index],'media_udp':media_udp[index]};owned.append((proc,record));row['servers'].append(record);save();wait(lambda:alive_http(http_ports[index],proc),'node http readiness')
    wait(lambda:len(set(x.decode()for x in redis(redis_port,'HKEYS','nodes'))-ids)==1,'one actual Redis node registration')
    current=set(x.decode()for x in redis(redis_port,'HKEYS','nodes'));record['node_id']=(current-ids).pop();ids=current;save()
   assert len(ids)==2
   def collect():
    while not collector_stop.wait(.05):
     try:
      now=time.monotonic_ns();assignments=mapping(redis_port,'room_node_map');nodes=node_samples(redis_port)
      row['routing_samples'].append({'monotonic_ns':now,'wall_time_ns':time.time_ns(),'room_mapping':assignments,'nodes':nodes})
      if a.capture_stalls and Path('/usr/bin/sample').is_file():
       for server in row['servers']:
        node=nodes.get(server['node_id']);pid=server['pid']
        if server['kind']!='rust' or pid in captured or not node or not node['updated_at'] or time.time()-node['updated_at']<=4:continue
        captured.add(pid);entry={'pid':pid,'node_id':server['node_id'],'observed_updated_at':node['updated_at'],'wall_time_ns':time.time_ns()};row['stall_captures'].append(entry)
        actual=subprocess.run(['/bin/ps','-p',str(pid),'-o','command='],capture_output=True,timeout=2)
        if actual.returncode or not actual.stdout.decode().strip().startswith(str(a.rust.resolve())+' '):entry['status']='OWNERSHIP_NOT_CURRENT_CAPTURE_SKIPPED';continue
        report=out/f'owned-rust-{pid}-stalled.sample.txt';stderr=(out/f'owned-rust-{pid}-sample.stderr').open('wb');process=subprocess.Popen(['/usr/bin/sample',str(pid),'1','1','-file',str(report)],stdout=subprocess.DEVNULL,stderr=stderr);captures.append((process,stderr,entry,report));entry.update(status='CAPTURE_RUNNING',capture_pid=process.pid)
      for room,owner in assignments.items():row['ownership_samples'].append({'room':room,'owner':owner,'monotonic_ns':now})
     except Exception as error:row.setdefault('collector_errors',[]).append(str(error));return
   collector=threading.Thread(target=collect,daemon=True);collector.start()
   env=dict(os.environ,LIVEKIT_URL=row['servers'][0]['entry'],LIVEKIT_PEER_URLS=json.dumps([x['entry']for x in row['servers']]),INTEROP_OUTPUT_DIR=str(out/'sdk-output'),CLUSTER_MEDIA_PROBE='true',SINGLE_PC=str(single).lower(),RUNS='1',DURATION_MS=str(a.duration_ms),VIDEO='true',VIDEO_CODEC='vp8',EXTENDED='false',FORCE_RELAY='false',TURN_TLS='false',NETWORK_PROFILE='',BANDWIDTH_ACCURACY='false',SCALABILITY_MODE='',SVC_SWITCH='false',SVC_TEMPORAL='false',COMPLEX_VIDEO='false',SERVER_LABEL=name,LIVEKIT_API_KEY='devkey',LIVEKIT_API_SECRET='secret')
   log=(out/'sdk.log').open('wb');logs.append(log);sdk=subprocess.Popen(['node',str(interop/'run.mjs')],cwd=interop,env=env,stdout=log,stderr=subprocess.STDOUT,start_new_session=True);row['sdk_pid']=sdk.pid
   try:row['sdk_exit']=sdk.wait(timeout=180)
   except subprocess.TimeoutExpired:
    row['sdk_timeout']=True;os.killpg(sdk.pid,signal.SIGTERM)
    try:sdk.wait(timeout=10)
    except subprocess.TimeoutExpired:os.killpg(sdk.pid,signal.SIGKILL);sdk.wait(timeout=10)
    raise RuntimeError('SDK/browser owned process-group deadline')
   collector_stop.set();collector.join(timeout=5);assert not collector.is_alive()
   reports=list((out/'sdk-output').glob('*.json'));assert len(reports)==1,('missing or ambiguous SDK evidence',reports);row['sdk_report_sha256']=sha(reports[0]);report=json.loads(reports[0].read_text());row['sdk_status']=report['status'];assert row['sdk_exit']==0 and report['status']=='PASS',('actual official SDK failure',row['sdk_exit'],report.get('error'))
   assert len(report['runs'])==1;run=report['runs'][0];owners={sample['owner']for sample in row['ownership_samples']if sample['room']==run['room']};assert len(owners)==1 and owners<=ids,('room missing/stale/moved owner',owners,ids);owner=owners.pop();owner_index=next(i for i,x in enumerate(row['servers'])if x['node_id']==owner);remote=[i for i,x in enumerate(row['servers'])if x['node_id']!=owner];assert remote, 'no actual remote entry';row['owner_proof']={'room':run['room'],'owner_node_id':owner,'owner_index':owner_index,'remote_entry_peer_indices':remote}
   for index,client in enumerate(run['reports']):
    assert client['audibleSamples']>0 and client['videoFrames']>=30 and len(client['received'])>=100
    selected=client['transports'];assert selected and all(int(t['remotePort'])in[media_udp[owner_index],media_tcp[owner_index]]for t in selected),('media did not reach common actual owner',index,selected,row['owner_proof'])
   assert len(run['clusterResults']['reconnect'])==2 and run['clusterResults']['resources']['bothEntriesEmpty']
   assert not row.get('collector_errors'),row.get('collector_errors')
   wait(lambda:redis(redis_port,'HGET','room_node_map',run['room'])is None,'room owner routing release',15)
   row['room_owner_released']=True;row['status']='PASS'
  except Exception as error:row['status']='FAIL';row['error']=str(error)
  finally:
   collector_stop.set()
   if collector:collector.join(timeout=5)
   for process,stderr,entry,report in captures:
    try:entry['capture_exit']=process.wait(timeout=5)
    except subprocess.TimeoutExpired:
     process.kill();entry['capture_exit']=process.wait(timeout=5);entry['forced_capture_cleanup']=True;row.setdefault('cleanup_errors',[]).append('owned diagnostic sampler timeout')
    stderr.close();entry['status']='CAPTURED'if entry['capture_exit']==0 and report.is_file()else'CAPTURE_FAILED'
    if report.is_file():entry.update(report=str(report.relative_to(a.output)),report_sha256=sha(report),report_bytes=report.stat().st_size)
   for proc,record in reversed(owned[1:]):stop(proc,record)
   if owned:
    try:wait(lambda:redis(redis_port,'HLEN','nodes')==0,'owned nodes unregister',10);row['nodes_unregistered']=True
    except Exception as error:row['nodes_unregistered']=False;row.setdefault('cleanup_errors',[]).append(str(error))
    stop(*owned[0])
   for log in logs:log.close()
   if any(record.get('forced_kill')or record.get('exit')!=0 for _,record in owned)or row.get('cleanup_errors'):row['status']='FAIL'
   save()
manifest['final_sdk_inputs']={key:sha(interop/key)for key in manifest['sdk_inputs']}
manifest['final_binary_hashes']={key:sha(Path(value['path']))for key,value in manifest['binaries'].items()}
manifest['input_identity_stable']=(manifest['final_sdk_inputs']==manifest['sdk_inputs'] and all(manifest['final_binary_hashes'][key]==value['sha256']for key,value in manifest['binaries'].items()) and sha(Path(__file__))==manifest['runner_sha256'])
passed=len(manifest['cases'])==(1 if a.only else 8) and all(row['status']=='PASS'for row in manifest['cases']) and manifest['input_identity_stable']
manifest['status']=('PASS_SCOPED_SUBSET'if a.only else'PASS_SCOPED_ALL8')if passed else'FAIL_COMPLETE_MATRIX';save();raise SystemExit(0 if passed else 1)
