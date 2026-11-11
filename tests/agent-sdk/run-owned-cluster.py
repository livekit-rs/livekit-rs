#!/usr/bin/env python3
"""Actual Linux official Agent SDK 3 job types × 4 ordered server pairs.
No worker emulation/build/install. Imports the existing exact owned tree helper.
"""
import argparse,hashlib,importlib.util,json,os,pathlib,platform,re,shutil,signal,socket,subprocess,tempfile,threading,time,urllib.request
p=argparse.ArgumentParser()
for key in ['source-root','go','rust','agent-sdk-root','interop-project','output']:p.add_argument('--'+key,type=pathlib.Path,required=True)
for key in ['go','rust']:p.add_argument('--'+key+'-sha256',required=True)
p.add_argument('--node',default='node');p.add_argument('--redis-server',default='redis-server')
p.add_argument('--probe',type=pathlib.Path,default=pathlib.Path(__file__).resolve().parent/'run-worker.mjs')
p.add_argument('--cancel-delete-response',action='store_true',help='actual signed HTTP caller reset while real SDK shutdown callback held; not a claim server handler still in flight')
p.add_argument('--only',choices=[str(j)+'-'+a+'-'+b for j in range(3)for a,b in [('go','go'),('rust','rust'),('go','rust'),('rust','go')]])
a=p.parse_args();assert platform.system()=='Linux'and os.getuid()!=0,'owned Linux nonroot runtime required'
sha=lambda f:hashlib.sha256(pathlib.Path(f).read_bytes()).hexdigest()
root=a.source_root.resolve();helper=root/'tests/interop/egress-linux/run-owned-stack.py'
spec=importlib.util.spec_from_file_location('actual_owned_linux_tree',helper);helpers=importlib.util.module_from_spec(spec);spec.loader.exec_module(helpers)
node=pathlib.Path(shutil.which(a.node)or a.node).resolve()
# Ubuntu's redis-server can be a multicall symlink to redis-check-rdb. Preserve
# its requested argv[0] basename for exec; resolve only for hashes/proc identity.
redis_server_entry=pathlib.Path(shutil.which(a.redis_server)or a.redis_server).absolute()
redis_server=redis_server_entry.resolve()
bins={s:getattr(a,s).resolve()for s in ['go','rust']}
for side,b in bins.items():assert sha(b)==getattr(a,side+'_sha256'),side+' immutable binary SHA mismatch'
sdk=a.agent_sdk_root.resolve();interop=a.interop_project.resolve();probe=a.probe.resolve();out=a.output.resolve();out.mkdir(parents=True,exist_ok=False,mode=0o700)
clean={k:os.environ[k]for k in ['PATH','HOME','TMPDIR','TMP','TEMP']if k in os.environ};clean.update(TZ='UTC',LANG='C')
inputs=[pathlib.Path(__file__).resolve(),helper,probe,sdk/'package.json',sdk/'package-lock.json',interop/'package.json',interop/'package-lock.json']
inputs_before={str(f):sha(f)for f in inputs}
receipt={'status':'RUNNING','scope':'unchanged official AgentServer1.9.4/rtc-node1.2.1 actual job0/1/2 lifecycle across 2 owned nodes, no AI pipeline claim','selection':a.only or'all12','cancellation_profile':a.cancel_delete_response,'binaries':{s:{'path':str(b),'sha256':sha(b)}for s,b in bins.items()},'tools':{'node':{'path':str(node),'sha256':sha(node)},'redis':{'entry_path':str(redis_server_entry),'resolved_executable':str(redis_server),'sha256':sha(redis_server),'launch_argv0':str(redis_server_entry)}},'inputs':inputs_before,'environment_names':sorted(clean),'inherited_livekit_names_forwarded':0,'cases':[]}
def save():(out/'execution.json').write_text(json.dumps(receipt,indent=2)+'\n')
def command(args,timeout=10):return subprocess.run([str(x)for x in args],env=clean,capture_output=True,timeout=timeout)
versions="""const fs=require('fs'),path=require('path'),rows={};for(const[root,name,want]of [[process.argv[1],'@livekit/agents','1.9.4'],[process.argv[1],'@livekit/rtc-node','1.2.1'],[process.argv[2],'livekit-server-sdk','2.19.2']]){const p=path.join(root,'node_modules',name,'package.json'),v=JSON.parse(fs.readFileSync(p)).version;if(v!==want)throw Error(name+' version mismatch');rows[name]=v}console.log(JSON.stringify(rows))"""
r=command([node,'-e',versions,sdk,interop]);assert r.returncode==0,'actual installed SDK version mismatch';receipt['sdk_versions']=json.loads(r.stdout);save()
def redis(port,*args):
 with socket.create_connection(('127.0.0.1',port),timeout=2)as sock:
  parts=[str(x).encode()for x in args];sock.sendall(b'*'+str(len(parts)).encode()+b'\r\n'+b''.join(b'$'+str(len(x)).encode()+b'\r\n'+x+b'\r\n'for x in parts));f=sock.makefile('rb')
  def read():
   line=f.readline();assert line.endswith(b'\r\n');kind,val=line[:1],line[1:-2]
   if kind==b'+':return val
   if kind==b'-':raise RuntimeError(val.decode())
   if kind==b':':return int(val)
   if kind==b'$':
    n=int(val)
    if n<0:return None
    data=f.read(n);assert len(data)==n and f.read(2)==b'\r\n';return data
   if kind==b'*':return[read()for _ in range(int(val))]
   raise ValueError('invalid actual Redis RESP')
  return read()
def reserve():
 sockets=[];ports={}
 for key in ['redis','a_http','a_tcp','a_udp','b_http','b_tcp','b_udp','a_debug','b_debug']:
  s=socket.socket(socket.AF_INET,socket.SOCK_DGRAM if key.endswith('udp')else socket.SOCK_STREAM);s.bind(('127.0.0.1',0));ports[key]=s.getsockname()[1];sockets.append(s)
 return sockets,ports
def wait(predicate,label,seconds=45):
 end=time.monotonic()+seconds;last=None
 while time.monotonic()<end:
  try:
   value=predicate()
   if value:return value
  except(OSError,ValueError,KeyError,json.JSONDecodeError)as e:last=type(e).__name__
  time.sleep(.05)
 raise TimeoutError(label+' deadline'+(' last='+last if last else''))
def proc_proof(proc,binary):
 info=helpers.process_identity(proc.pid);assert info and info['state']!='Z'and proc.poll()is None
 assert pathlib.Path('/proc',str(proc.pid)).stat().st_uid==os.getuid();assert pathlib.Path('/proc',str(proc.pid),'exe').resolve()==binary;assert sha(pathlib.Path('/proc',str(proc.pid),'exe'))==sha(binary)
 return{**info,'executable':str(binary),'executable_sha256':sha(binary)}
def alive(proc,proof):
 now=helpers.process_identity(proc.pid);return now and now['start']==proof['start']
def stop(proc,proof,record):
 if proc.poll()is None:
  assert alive(proc,proof),'refusing to signal reused owned PID';proc.send_signal(signal.SIGINT)
 try:record['exit']=proc.wait(timeout=30)
 except subprocess.TimeoutExpired:
  assert alive(proc,proof);proc.kill();record['forced_kill']=True;record['exit']=proc.wait(timeout=5)
def server_config(prefix,ports):return{'port':ports[prefix+'_http'],'bind_addresses':['127.0.0.1'],'keys':{'devkey':'secret'},'redis':{'address':'127.0.0.1:'+str(ports['redis'])},'rtc':{'node_ip':'127.0.0.1','use_external_ip':False,'enable_loopback_candidate':True,'ips':{'includes':['127.0.0.1/32']},'tcp_port':ports[prefix+'_tcp'],'udp_port':ports[prefix+'_udp']},'room':{'empty_timeout':300,'departure_timeout':20},'debug_handler':{'port':ports[prefix+'_debug']},'logging':{'level':'info'}}
opener=urllib.request.build_opener(urllib.request.ProxyHandler({}))
def health(proc,port):
 assert proc.poll()is None,'actual owned server exited'
 with opener.open('http://127.0.0.1:'+str(port)+'/',timeout=1)as r:return r.status==200
def debug_snapshot(port):
 with opener.open('http://127.0.0.1:'+str(port)+'/debug/rooms',timeout=3)as response:
  assert response.status==200;raw=response.read(4*1024*1024+1);assert len(raw)<=4*1024*1024
 value=json.loads(raw);assert isinstance(value,list),'original /debug/rooms must be an array'
 return value,raw
def debug_listener_inode(pid,port):
 inodes=set()
 for fd in pathlib.Path('/proc',str(pid),'fd').iterdir():
  try:
   m=re.fullmatch(r'socket:\[(\d+)\]',os.readlink(fd))
   if m:inodes.add(m.group(1))
  except FileNotFoundError:pass
 for filename in ['tcp','tcp6']:
  for line in pathlib.Path('/proc',str(pid),'net',filename).read_text().splitlines()[1:]:
   f=line.split()
   if f[9]in inodes and int(f[1].rsplit(':',1)[1],16)==port and f[3]=='0A':return f[9]
 return None
def prove_actual_room_owner(running,observer,servers,ports,folder):
 # Read each node's independent live actor debug endpoint, not shared Redis
 # projections or public RoomService redirects that could conceal shadow state.
 snapshots=[]
 for prefix,server in zip(['a','b'],servers):
  inode=debug_listener_inode(server.pid,ports[prefix+'_debug'])
  assert inode,'debug listener is not held by the exact owned server PID'
  value,raw=debug_snapshot(ports[prefix+'_debug']);path=folder/(prefix+'-job-running-debug-rooms.json');path.write_bytes(raw)
  snapshots.append((value,{'server_pid':server.pid,'port':ports[prefix+'_debug'],'listener_socket_inode':inode,'snapshot_sha256':hashlib.sha256(raw).hexdigest()}))
 owned=[r for r in snapshots[0][0]if r.get('Name')==observer['room']]
 shadows=[r for r in snapshots[1][0]if r.get('Name')==observer['room']]
 assert len(owned)==1,'node A must have one actual live owner room'
 assert not shadows,'worker entry B must not contain a same-name shadow room'
 room=owned[0];assert room['Sid']==observer['room_sid'],'actual owner room SID differs from original CreateRoom'
 participant=room['Participants'].get(running['agent_identity']);assert isinstance(participant,dict),'agent is absent from actual owner A room'
 assert participant['ID']==running['agent_sid'],'actual owner participant SID differs from SDK/REST agent'
 assert participant.get('State')!='DISCONNECTED','actual owner agent already disconnected before termination'
 if running['human_identity']is not None:assert running['human_identity']in room['Participants'],'real human must also be in owner A room'
 return {'a':snapshots[0][1],'b':snapshots[1][1],'owner_room_sid':room['Sid'],'agent_identity':running['agent_identity'],'agent_sid':participant['ID'],'agent_state':participant.get('State'),'worker_node_has_shadow':False,'owner_participant_count':len(room['Participants'])}
def node_id(port,previous):
 ids={x.decode()for x in redis(port,'HKEYS','nodes')};added=ids-previous
 if len(added)!=1:return None
 node=added.pop();raw=redis(port,'HGET','nodes',node);assert raw and node.encode()in raw
 return node,{'node_id':node,'raw_sha256':hashlib.sha256(raw).hexdigest()}
def tcp_owned_ports(pid,redis_port):
 inodes=set()
 for fd in pathlib.Path('/proc',str(pid),'fd').iterdir():
  try:
   m=re.fullmatch(r'socket:\[(\d+)\]',os.readlink(fd))
   if m:inodes.add(m.group(1))
  except FileNotFoundError:pass
 ports={}
 for filename in ['tcp','tcp6']:
  for line in pathlib.Path('/proc',str(pid),'net',filename).read_text().splitlines()[1:]:
   f=line.split()
   if f[9]in inodes and int(f[2].rsplit(':',1)[1],16)==redis_port and f[3]=='01':ports[int(f[1].rsplit(':',1)[1],16)]=f[9]
 return ports
class Monitor:
 """Only actual Redis SUBSCRIBE metadata is retained, never raw job/token bytes."""
 def __init__(self,port):
  self.rows=[];self.error=None;self.lock=threading.Lock();self.sock=socket.create_connection(('127.0.0.1',port),timeout=2);self.sock.settimeout(None);self.sock.sendall(b'*1\r\n$7\r\nMONITOR\r\n');self.stream=self.sock.makefile('rb');assert self.stream.readline()==b'+OK\r\n';self.thread=threading.Thread(target=self.run,daemon=True);self.thread.start()
 def run(self):
  try:
   while True:
    line=self.stream.readline(2*1024*1024)
    if not line:break
    if b'"subscribe"'not in line.lower():continue
    m=re.match(rb'\+?([0-9.]+) \[\d+ ([^\]]+)\] "([A-Za-z]+)"(.*)\r\n$',line)
    if not m:continue
    args=re.findall(rb'"([^"\\]*)"',m.group(4))
    for channel in args:
     if channel.startswith(b'AgentInternal|JobRequest|')or channel.startswith(b'AgentInternal|JobTerminate|'):
      with self.lock:self.rows.append({'time':m.group(1).decode(),'redis_client_address':m.group(2).decode(),'command':m.group(3).decode(),'channel':channel.decode(),'source_line_sha256':hashlib.sha256(line).hexdigest()})
  except OSError as e:self.error=type(e).__name__
 def snapshot(self):
  with self.lock:return list(self.rows)
 def close(self):
  try:self.sock.shutdown(socket.SHUT_RDWR)
  except OSError:pass
  self.sock.close();self.thread.join(timeout=3);self.stream.close();assert not self.thread.is_alive()
def escaped(value):return''.join(c if c.isascii()and(c.isalnum()or c=='_')else('u+'+format(ord(c),'04x')if ord(c)<65536 else'U+'+format(ord(c),'08x'))for c in value)
def worker_proof(monitor,observer,b_proc,redis_port,bid,method='JobRequest'):
 channel=('AgentInternal|JobRequest|'+escaped(observer['agent_name'])+'|'+['room','publisher','participant'][job]+'|REQ')if method=='JobRequest'else('AgentInternal|JobTerminate|'+escaped(observer['job_id'])+'|REQ')
 ports=tcp_owned_ports(b_proc.pid,redis_port);rows=[r for r in monitor.snapshot()if r['channel']==channel and int(r['redis_client_address'].rsplit(':',1)[1])in ports]
 if not rows:return None
 clients=[dict(x.split('=',1)for x in line.split()if'='in x)for line in redis(redis_port,'CLIENT','LIST').decode().splitlines()]
 for row in rows:
  client=next((c for c in clients if c.get('addr')==row['redis_client_address']),None)
  if client and int(client.get('sub','0'))>0:return{'node_id':bid,'server_pid':b_proc.pid,'actual_subscribe':row,'actual_redis_client_id':client['id'],'owned_redis_local_port':int(row['redis_client_address'].rsplit(':',1)[1]),'socket_fd_inode_proof':True,'owned_socket_inode':ports[int(row['redis_client_address'].rsplit(':',1)[1])]}
 return None
try:
 for job in [0,1,2]:
  for kinds in [('go','go'),('rust','rust'),('go','rust'),('rust','go')]:
   name=str(job)+'-'+'-'.join(kinds)
   if a.only and a.only!=name:continue
   folder=out/name;folder.mkdir(mode=0o700);case={'name':name,'job_type':job,'kinds':list(kinds),'status':'RUNNING','processes':[]};receipt['cases'].append(case);save()
   held,ports=reserve();case['ports']=ports;temp=pathlib.Path(tempfile.mkdtemp(prefix='livekit-agent-owned-'));tree=helpers.OwnedProcessTree();processes=[];logs=[];monitor=None;probe_proc=None
   def launch(label,argv,env=clean):
    log=(folder/(label+'.log')).open('xb');logs.append(log);proc=subprocess.Popen([str(x)for x in argv],env=env,stdout=log,stderr=subprocess.STDOUT,start_new_session=True);tree.add(proc);proof=helpers.process_identity(proc.pid)or{};record={'name':label,'identity':proof};processes.append((proc,proof,record));case['processes'].append(record);binary=pathlib.Path(argv[0]).resolve();proof.update(proc_proof(proc,binary));save();return proc,record
   try:
    for s in held:s.close()
    held=[];redis_proc,_=launch('redis',[redis_server_entry,'--bind','127.0.0.1','--port',str(ports['redis']),'--save','','--appendonly','no','--dir',str(temp)])
    wait(lambda:redis(ports['redis'],'PING')==b'PONG','owned Redis readiness');monitor=Monitor(ports['redis']);ids=set();servers=[];nodeids=[]
    for prefix,kind in zip(['a','b'],kinds):
     config=server_config(prefix,ports);path=temp/(prefix+'.yaml');path.write_text(json.dumps(config));case[prefix+'_config']={'sha256':sha(path),'public_config':config}
     server,_=launch(prefix,[bins[kind],'--dev','--config',path]);servers.append(server);wait(lambda:health(server,ports[prefix+'_http']),prefix+' owned HTTP readiness')
     nid,proof=wait(lambda:node_id(ports['redis'],ids),prefix+' Redis node registration');ids.add(nid);nodeids.append(nid);case[prefix+'_node']=proof;wait(lambda:debug_snapshot(ports[prefix+'_debug']),prefix+' actual debug rooms readiness')
    url_a='http://127.0.0.1:'+str(ports['a_http']);url_b='http://127.0.0.1:'+str(ports['b_http']);phase=folder/'routing-phase.json';release=temp/'admit.json';terminate_release=temp/'terminate-admit.json'
    env=dict(clean,AGENT_SDK_ROOT=str(sdk),INTEROP_PROJECT=str(interop),LIVEKIT_URL=url_a,AGENT_WORKER_LIVEKIT_URL=url_b,AGENT_TARGET_NODE_ID=nodeids[0],LIVEKIT_API_KEY='devkey',LIVEKIT_API_SECRET='secret',AGENT_JOB_TYPE=str(job),AGENT_SDK_EVIDENCE=str(folder/'probe.json'),AGENT_ROUTING_OBSERVER=str(phase),AGENT_ROUTING_RELEASE=str(release),AGENT_TERMINATION_RELEASE=str(terminate_release))
    if a.cancel_delete_response:env['AGENT_CANCEL_TERMINATION']='1'
    probe_proc,probe_record=launch('probe',[node,probe],env)
    observer=wait(lambda:json.loads(phase.read_text())if phase.exists()else None,'actual official worker registered and pinned room created')
    assert observer['target_node_id']==nodeids[0]and observer['rest_url']==url_a and observer['worker_url']==url_b and re.fullmatch('AW_[A-Za-z0-9]+',observer['worker_id'])
    owner=wait(lambda:redis(ports['redis'],'HGET','room_node_map',observer['room']),'actual Redis room owner');assert owner.decode()==nodeids[0],'room not owned by explicitly selected node A'
    actual_worker=wait(lambda:worker_proof(monitor,observer,servers[1],ports['redis'],nodeids[1]),'actual AgentInternal subscription on node B owned Redis socket')
    assert nodeids[0]!=nodeids[1];case['routing_proof']={'room':observer['room'],'room_sid':observer['room_sid'],'owner_node_id':owner.decode(),'owner_server_pid':servers[0].pid,'worker_node':actual_worker,'worker_id':observer['worker_id']};save();release.write_text('{"admit":true}\n')
    def running_phase():
     data=json.loads(phase.read_text());return data if data.get('phase')=='job_running'else None
    running=wait(running_phase,'actual accepted job before termination')
    actual_terminate=wait(lambda:worker_proof(monitor,running,servers[1],ports['redis'],nodeids[1],'JobTerminate'),'actual JobTerminate subscription on node B owned Redis socket')
    case['actual_owner_debug_proof']=prove_actual_room_owner(running,observer,servers,ports,folder)
    case['routing_proof']['terminate_worker_node']=actual_terminate;case['routing_proof']['job_id']=running['job_id'];assert redis(ports['redis'],'HGET','room_node_map',observer['room'])==nodeids[0].encode();save();terminate_release.write_text('{"admit":true}\n')
    probe_record['exit']=probe_proc.wait(timeout=180);assert probe_record['exit']==0,'original official SDK probe failed';result=json.loads((folder/'probe.json').read_text());assert result['status']=='PASS'and result['cleanup_errors']==[]
    observations=result['observations'];assert observations['actual_agent_participant']and observations['actual_child_exited']and observations['terminated']and observations['accepted_jobs']==1
    if job!=0:assert observations['parent_native_rtc_disposed']and observations['actual_human_participant']
    if job==1:assert observations['publisher_audio']['packets_sent']>0 and observations['publisher_audio']['bytes_sent']>0
    
    if a.cancel_delete_response:
     cancel=observations['cancelled_termination'];assert all(cancel[k]is True for k in ['actual_callback_started','actual_child_alive_before_release','dispatch_deleted_before_reset','caller_tcp_reset','caller_socket_closed','response_deliberately_unread']);assert cancel['request_bytes_flushed']>0;case['cancelled_termination']=cancel
    case['probe_sha256']=sha(folder/'probe.json');case['actual_child_pid']=observations['actual_child_pid'];wait(lambda:redis(ports['redis'],'HGET','room_node_map',observer['room'])is None,'actual owner mapping removal after SDK room cleanup',20);case['status']='PASS_SCOPED'
   except BaseException as error:
    case['status']='FAIL';case['failure']={'type':type(error).__name__,'message':str(error)[:1000]}
    if isinstance(error,(KeyboardInterrupt,SystemExit)):raise
   finally:
    for s in held:s.close()
    if monitor:
     case['actual_subscription_observations']=monitor.snapshot();monitor.close()
    for proc,proof,record in reversed(processes):stop(proc,proof,record)
    for log in logs:log.close()
    case['owned_tree']=tree.finish();case['owned_child_births']=[row for pid,row in tree.known.items()if pid not in tree.roots]
    shutil.rmtree(temp);case['temporary_configs_removed']=not temp.exists();case['ports_released']=True
    for key,port in ports.items():
     try:helpers.free(port,key.endswith('udp'))
     except OSError:case['ports_released']=False
    if not case['ports_released']or not case['owned_tree']['released_without_reclamation']or any(r.get('exit')!=0 or r.get('forced_kill')for _,_,r in processes):case['status']='FAIL';case['release_gate_failed']=True
    if 'actual_child_pid'in case and not any(row['pid']==case['actual_child_pid']for row in case['owned_child_births']):case['status']='FAIL';case['actual_child_not_observed_by_owned_tree']=True
    case['debug_snapshot_sha256']={p.name:sha(p)for p in folder.glob('*debug-rooms.json')};case['log_sha256']={p.name:sha(p)for p in folder.glob('*.log')};save()
   assert all(sha(f)==digest for f,digest in inputs_before.items()),'source/SDK lock drift'
   assert all(sha(b)==receipt['binaries'][side]['sha256']for side,b in bins.items()),'binary drift'
 expected=1 if a.only else 12;assert len(receipt['cases'])==expected
 receipt['status']='PASS_SCOPED_ALL_REQUESTED_CASES'if all(c['status']=='PASS_SCOPED'for c in receipt['cases'])else'FAIL'
except BaseException as error:receipt['status']='FAIL';receipt['failure']={'type':type(error).__name__,'message':str(error)[:1000]}
finally:save()
raise SystemExit(0 if receipt['status']=='PASS_SCOPED_ALL_REQUESTED_CASES'else 1)
