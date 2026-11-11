#!/usr/bin/env python3
"""Owned Linux original Go/Rust public-SDK first-response recovery.
No build, install, production pause hook, Redis emulation or invented success.
"""
import argparse,hashlib,importlib.util,json,os,pathlib,re,shutil,signal,socket,subprocess,tempfile,time,urllib.request
from redis_signal_gate import RedisSignalGate,read_resp
p=argparse.ArgumentParser()
for key in ['source-root','go','rust','interop-project','output']:p.add_argument('--'+key,type=pathlib.Path,required=True)
for key in ['go','rust']:p.add_argument('--'+key+'-sha256',required=True)
p.add_argument('--interop-lock-sha256',required=True);p.add_argument('--node',default='node');p.add_argument('--redis-server',default='redis-server')
p.add_argument('--browser-executable',type=pathlib.Path,required=True)
p.add_argument('--pair',choices=['go-go','rust-go','go-rust','rust-rust','all'],default='all')
p.add_argument('--attempts',choices=['-1','0','1','2','3','all'],default='all')
p.add_argument('--mechanism',choices=['yaml','config-body','config-env'],default='yaml')
p.add_argument('--fault',choices=['first-join','none'],default='first-join')
a=p.parse_args();assert os.uname().sysname=='Linux'and os.getuid()!=0,'owned nonroot Linux VM required'
def interrupted(signum,frame):raise KeyboardInterrupt('owned interruption '+str(signum))
signal.signal(signal.SIGTERM,interrupted)
sha=lambda f:hashlib.sha256(pathlib.Path(f).read_bytes()).hexdigest()
here=pathlib.Path(__file__).resolve().parent;source=a.source_root.resolve();helper=source/'tests/interop/egress-linux/run-owned-stack.py'
spec=importlib.util.spec_from_file_location('actual_signal_owned_tree',helper);helpers=importlib.util.module_from_spec(spec);spec.loader.exec_module(helpers)
node=pathlib.Path(shutil.which(a.node)or a.node).resolve();redis_entry=pathlib.Path(shutil.which(a.redis_server)or a.redis_server).absolute();redis_exe=redis_entry.resolve()
bins={k:getattr(a,k).resolve()for k in ['go','rust']}
for k,b in bins.items():assert sha(b)==getattr(a,k+'_sha256')
project=a.interop_project.resolve();lock=project/'package-lock.json';assert sha(lock)==a.interop_lock_sha256
browser=a.browser_executable.resolve();assert browser.is_file()
helper_browser=here.parent/'owned-browser-exit.mjs';assert helper_browser.is_file()
out=a.output.resolve();out.mkdir(parents=True,exist_ok=False)
clean={k:os.environ[k]for k in ['PATH','HOME','TMPDIR','TMP','TEMP']if k in os.environ};clean.update(TZ='UTC',LANG='C')
inputs=[pathlib.Path(__file__).resolve(),here/'redis_signal_gate.py',here/'probe.mjs',here/'zero-observer.mjs',helper,helper_browser,project/'package.json',lock]
before={str(f):sha(f)for f in inputs}
receipt={'status':'RUNNING','scope':'actual original SDK single WebSocket and real Redis first-Join-stream fault; complete server start retry, not all signaling/media','fixed_go':'e5655ac20267fa5005dccaf3f0e3bfc3ea688c07','inputs':before,'binaries':{k:{'path':str(b),'sha256':sha(b)}for k,b in bins.items()},'browser':{'path':str(browser),'sha256':sha(browser)},'redis':{'entry':str(redis_entry),'resolved':str(redis_exe),'sha256':sha(redis_exe)},'environment_names':sorted(clean),'inherited_livekit_names_forwarded':0,'mechanism':a.mechanism,'cases':[]}
def save():(out/'execution.json').write_text(json.dumps(receipt,indent=2)+'\n')
def wait(check,label,seconds=35):
 end=time.monotonic()+seconds;last=None
 while time.monotonic()<end:
  try:
   value=check()
   if value:return value
  except(OSError,ValueError,KeyError)as error:last=str(error)[:100]
  time.sleep(.03)
 raise TimeoutError(label+' deadline '+str(last))
def redis(port,*args):
 with socket.create_connection(('127.0.0.1',port),timeout=2)as sock:
  parts=[x if isinstance(x,bytes)else str(x).encode()for x in args]
  sock.sendall(b'*'+str(len(parts)).encode()+b'\r\n'+b''.join(b'$'+str(len(x)).encode()+b'\r\n'+x+b'\r\n'for x in parts))
  return read_resp(sock.makefile('rb'))[0]
def identity(proc,exe):
 info=helpers.process_identity(proc.pid);assert info and proc.poll()is None and info['state']!='Z'
 assert pathlib.Path('/proc',str(proc.pid)).stat().st_uid==os.getuid()
 assert pathlib.Path('/proc',str(proc.pid),'exe').resolve()==exe
 assert sha(pathlib.Path('/proc',str(proc.pid),'exe'))==sha(exe)
 return{**info,'executable':str(exe),'executable_sha256':sha(exe)}
def same(proc,proof):
 now=helpers.process_identity(proc.pid);return now and now['start']==proof['start']
def stop(proc,proof,row):
 if proc.poll()is None:
  assert same(proc,proof),'owned PID identity reused';proc.send_signal(signal.SIGINT)
 try:row['exit']=proc.wait(timeout=25)
 except subprocess.TimeoutExpired:
  assert same(proc,proof);proc.kill();row['forced_kill']=True;row['exit']=proc.wait(timeout=5)
def reserve():
 held=[];ports={}
 for key in ['redis','proxy','a_http','a_debug','a_tcp','a_udp','b_http','b_debug','b_tcp','b_udp']:
  sock=socket.socket(socket.AF_INET,socket.SOCK_DGRAM if key.endswith('udp')else socket.SOCK_STREAM);sock.bind(('127.0.0.1',0));ports[key]=sock.getsockname()[1];held.append(sock)
 return held,ports
def socket_inode(pid,port,udp=False):
 owned=set()
 for fd in pathlib.Path('/proc',str(pid),'fd').iterdir():
  try:
   m=re.fullmatch(r'socket:\[(\d+)\]',os.readlink(fd))
   if m:owned.add(m.group(1))
  except FileNotFoundError:pass
 result=set()
 for table in (['udp','udp6']if udp else ['tcp','tcp6']):
  for line in pathlib.Path('/proc/net',table).read_text().splitlines()[1:]:
   f=line.split()
   if int(f[1].rsplit(':',1)[1],16)==port and (udp or f[3]=='0A'):result.add(f[9])
 assert result and result.issubset(owned),'actual port not exclusively held by exact owned process'
 return sorted(result)
opener=urllib.request.build_opener(urllib.request.ProxyHandler({}))
def health(proc,port):
 assert proc.poll()is None,'owned server unexpectedly exited'
 with opener.open('http://127.0.0.1:'+str(port)+'/',timeout=1)as r:return r.status==200
def debug(port,path):
 with opener.open('http://127.0.0.1:'+str(port)+'/debug/rooms',timeout=3)as r:raw=r.read(4*1024*1024+1)
 assert len(raw)<=4*1024*1024;value=json.loads(raw);assert isinstance(value,list);path.write_bytes(raw);return value
def config(prefix,ports,attempts):
 return {'port':ports[prefix+'_http'],'bind_addresses':['127.0.0.1'],'keys':{'devkey':'secret'},'redis':{'address':'127.0.0.1:'+str(ports['proxy']if prefix=='a'else ports['redis'])},'rtc':{'node_ip':'127.0.0.1','use_external_ip':False,'enable_loopback_candidate':True,'ips':{'includes':['127.0.0.1/32']},'tcp_port':ports[prefix+'_tcp'],'udp_port':ports[prefix+'_udp']},'room':{'empty_timeout':300,'departure_timeout':20},'debug_handler':{'port':ports[prefix+'_debug']},'logging':{'level':'info'},'signal_relay':{'connect_attempts':attempts},'psrpc':{'compression':{'quality':0}}}
pairs=[('go','go'),('rust','go'),('go','rust'),('rust','rust')]if a.pair=='all'else[tuple(a.pair.split('-'))]
attempts=[1,2,3]if a.attempts=='all'else[int(a.attempts)]
try:
 for attempt in attempts:
  for pair in pairs:
   positive=attempt>0 and(a.fault=='none'or attempt>1)
   folder=out/(str(attempt)+'-'+'-'.join(pair));folder.mkdir();case={'status':'RUNNING','profile_kind':('POSITIVE_BASELINE'if a.fault=='none'else'POSITIVE_RECOVERY')if positive else('NEGATIVE_ONE_ATTEMPT_CONTROL'if attempt==1 else'PUBLIC_ZERO_NEGATIVE_OBSERVER'),'fault':a.fault,'attempts':attempt,'pair':pair,'processes':[],'ports':{},'cleanup_errors':[]};receipt['cases'].append(case);save()
   held,ports=reserve();case['ports']=ports;temp=pathlib.Path(tempfile.mkdtemp(prefix='signal-start-owned-'));tree=helpers.OwnedProcessTree();processes=[];logs=[];gate=None
   try:
    for sock in held:sock.close()
    held=[]
    def launch(label,argv,exe,env=clean):
     log=(folder/(label+'.log')).open('xb');logs.append(log);proc=subprocess.Popen([str(x)for x in argv],env=env,stdout=log,stderr=subprocess.STDOUT,start_new_session=True);tree.add(proc);proof=identity(proc,exe);row={'name':label,'identity':proof};case['processes'].append(row);processes.append((proc,proof,row));return proc
    redis_cfg=temp/'redis.conf';redis_cfg.write_text('bind 127.0.0.1\nport '+str(ports['redis'])+'\nsave ""\nappendonly no\n')
    broker=launch('redis',[redis_entry,redis_cfg],redis_exe);wait(lambda:redis(ports['redis'],'PING')==b'PONG','owned actual Redis ready')
    control=folder/'control.json';gate=RedisSignalGate(ports['proxy'],ports['redis'],control)
    case['proxy_listener_inode']=socket_inode(os.getpid(),ports['proxy'])
    servers=[]
    for prefix,kind in zip(['a','b'],pair):
     cfg=config(prefix,ports,attempt);text=json.dumps(cfg);path=temp/(prefix+'.yaml');path.write_text(text);case[prefix+'_config_sha256']=sha(path);argv=[bins[kind],'--dev'];env=dict(clean)
     if a.mechanism=='yaml':argv+=['--config',path]
     elif a.mechanism=='config-body':argv+=['--config-body',text]
     else:env['LIVEKIT_CONFIG']=text
     server=launch(prefix,argv,bins[kind],env);servers.append(server);wait(lambda:health(server,ports[prefix+'_http']),'owned '+prefix+' ready')
     case[prefix+'_listeners']={suffix:socket_inode(server.pid,ports[prefix+'_'+suffix],suffix=='udp')for suffix in ['http','debug','tcp','udp']}
    nodeids=[]
    for prefix in ['a','b']:
     data=(folder/(prefix+'.log')).read_text(errors='replace');ids=re.findall(r'ND_[A-Za-z0-9_-]+',data);assert ids,'actual startup NodeID missing';nodeids.append(ids[0])
    actual=redis(ports['redis'],'HGETALL','nodes');assert isinstance(actual,list);registered={actual[i].decode()for i in range(0,len(actual),2)};assert set(nodeids).issubset(registered)and nodeids[0]!=nodeids[1]
    phase=folder/'phase.json';release=folder/'release.json';result_path=folder/'probe.json'
    env=dict(clean,INTEROP_PROJECT=str(project),LIVEKIT_URL='http://127.0.0.1:'+str(ports['a_http']),SIGNAL_OWNER_URL='http://127.0.0.1:'+str(ports['b_http']),SIGNAL_OWNER_NODE=nodeids[1],SIGNAL_CONTROL=str(control),SIGNAL_PHASE=str(phase),SIGNAL_RELEASE=str(release),SIGNAL_RESULT=str(result_path),SIGNAL_STDERR=str(folder/'browser.stderr'),SIGNAL_BROWSER_OWNER=str(folder/'browser-owner.json'),SIGNAL_BROWSER_EXECUTABLE=str(browser),SIGNAL_EXPECT_CONNECTED='1'if positive else'0',SIGNAL_FIRST_JOIN_FAULT='1'if a.fault=='first-join'else'0')
    observer_only=attempt<=0
    probe=launch('probe',[node,here/('zero-observer.mjs'if observer_only else'probe.mjs')],node,env)
    observed=wait(lambda:json.loads(phase.read_text()),'actual SDK connection outcome',50)
    if observer_only:
     assert probe.wait(timeout=20)==0,'public zero/negative source observer failed'
     actual=json.loads(result_path.read_text());assert actual['status']=='OBSERVED_NOT_ACCEPTANCE'and not actual.get('cleanup_error')
     case['zero_negative_public_wire']=actual;case['status']='OBSERVED_NOT_ACCEPTANCE'
     continue
    rows=gate.snapshot();assert not rows['errors'],'actual wire parser/transport failed'
    assert bool(rows['blocked_stream'])==(a.fault=='first-join'),'actual first Join stream gating differs from requested fault'
    starts={r['connection_id']for r in rows['rows']if r.get('kind')=='start'and r.get('room')==observed['room']}
    expected_starts=1 if a.fault=='none'or attempt==1 else 2
    assert len(starts)==expected_starts,('actual complete StartSession count',starts)
    joins={r['stream_id']for r in rows['rows']if r.get('kind')=='join'and r.get('room')==observed['room']}
    assert len(joins)==expected_starts,'first response did not recover through a different actual stream'
    assert redis(ports['redis'],'HGET','room_node_map',observed['room'])==nodeids[1].encode()
    arooms=debug(ports['a_debug'],folder/'entry-debug.json');brooms=debug(ports['b_debug'],folder/'owner-debug.json')
    assert not any(r.get('Name')==observed['room']for r in arooms),'entry shadow room'
    matching=[r for r in brooms if r.get('Name')==observed['room']];assert len(matching)==1 and matching[0]['Sid']==observed['room_sid']
    if positive:
     participant=matching[0]['Participants'][observed['identity']];assert participant['ID']==observed['participant_sid']and participant.get('State')!='DISCONNECTED'
    browser_owner=json.loads((folder/'browser-owner.json').read_text());browser_info=helpers.process_identity(browser_owner['pid']);assert browser_info and str(browser_info['start'])==str(browser_owner['start']);assert pathlib.Path('/proc',str(browser_owner['pid']),'exe').resolve()==browser
    case['actual_browser_identity']={**browser_info,'executable_sha256':sha(browser)}
    case['actual_owner']={'node_id':nodeids[1],'server_pid':servers[1].pid,'room_sid':observed['room_sid'],'entry_no_shadow':True};case['actual_start_connection_ids']=sorted(starts)
    release.write_text('{"release":true}\n');assert probe.wait(timeout=35)==0,'actual original SDK probe failed'
    result=json.loads(result_path.read_text());assert result['status']=='PASS'and result['cleanup_errors']==[]and result['browser']['contextsAfter']==0and result['browser']['exit']['code']==0and not result['browser']['failures']
    assert sha(folder/'browser.stderr')==result['browser_stderr']['sha256']
    wait(lambda:redis(ports['redis'],'HGET','room_node_map',observed['room'])is None,'actual room mapping cleanup')
    case['probe_sha256']=sha(result_path);case['status']='PASS_POSITIVE_RECOVERY'if positive else'PASS_NEGATIVE_CONTROL'
   except BaseException as error:
    case['status']='FAIL';case['failure']={'type':type(error).__name__,'message':str(error)[:1000]}
    if isinstance(error,(KeyboardInterrupt,SystemExit)):raise
   finally:
    for sock in held:sock.close()
    for proc,proof,row in reversed(processes):
     try:stop(proc,proof,row)
     except BaseException as error:case['cleanup_errors'].append('owned process cleanup '+row['name']+': '+str(error));case['status']='FAIL'
    if gate:
     case['wire']=gate.snapshot()
     try:gate.close();case['proxy_threads_joined']=True
     except BaseException as error:case['cleanup_errors'].append('owned proxy cleanup: '+str(error));case['status']='FAIL'
    for log in logs:log.close()
    case['owned_tree']=tree.finish();case['owned_child_births']=[row for pid,row in tree.known.items()if pid not in tree.roots];shutil.rmtree(temp);case['temporary_configs_removed']=not temp.exists();case['ports_released']=True
    for key,port in ports.items():
     try:helpers.free(port,key.endswith('udp'))
     except OSError:case['ports_released']=False
    unexpected_exit=any((row.get('exit')!=0 and not(attempt<=0 and row['name']=='a'))or row.get('forced_kill')for _,_,row in processes)
    if case['cleanup_errors']or not case['ports_released']or not case['owned_tree']['released_without_reclamation']or unexpected_exit:case['status']='FAIL';case['release_gate_failed']=True
    if 'actual_browser_identity'in case and not any(row['pid']==case['actual_browser_identity']['pid']and row['start']==case['actual_browser_identity']['start']for row in case['owned_child_births']):case['status']='FAIL';case['browser_not_observed_in_owned_tree']=True
    case['logs_sha256']={f.name:sha(f)for f in folder.glob('*.log')};save()
    assert all(sha(f)==digest for f,digest in before.items()),'source/SDK lock changed'
    assert all(sha(b)==receipt['binaries'][k]['sha256']for k,b in bins.items()),'binary changed'
   assert all(sha(f)==digest for f,digest in before.items()),'source/SDK lock changed'
   assert all(sha(b)==receipt['binaries'][k]['sha256']for k,b in bins.items()),'binary changed'
 receipt['status']='OBSERVED_NOT_ACCEPTANCE'if attempts[0]<=0 and all(c['status']=='OBSERVED_NOT_ACCEPTANCE'for c in receipt['cases'])else('PASS_SCOPED'if all(c['status']in ['PASS_POSITIVE_RECOVERY','PASS_NEGATIVE_CONTROL']for c in receipt['cases'])else'FAIL')
except BaseException as error:receipt['status']='FAIL';receipt['failure']={'type':type(error).__name__,'message':str(error)[:1000]}
finally:save()
raise SystemExit(0 if receipt['status']in ['PASS_SCOPED','OBSERVED_NOT_ACCEPTANCE']else 1)
