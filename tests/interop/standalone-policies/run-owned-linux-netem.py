#!/usr/bin/env python3
"""Four owned Linux-netem standalone campaigns: actual fixed Go/Rust × RPC/SendData.
No builds/installs. All inherited LiveKit/Redis/runtime options are excluded.
"""
import argparse,hashlib,json,os,pathlib,shutil,signal,socket,subprocess,tempfile,time,urllib.request,importlib.util,re,fcntl
p=argparse.ArgumentParser()
for side in ['go','rust']:
 p.add_argument('--'+side,type=pathlib.Path,required=True);p.add_argument('--'+side+'-sha256',required=True)
p.add_argument('--interop-project',type=pathlib.Path,required=True);p.add_argument('--interop-lock-sha256',required=True)
p.add_argument('--node',default='node');p.add_argument('--tc',default='tc');p.add_argument('--output',type=pathlib.Path,required=True)
p.add_argument('--source-root',type=pathlib.Path,required=True);p.add_argument('--exclusive-owned-vm-loopback',action='store_true',required=True);p.add_argument('--browser-executable',type=pathlib.Path)
p.add_argument('--fixtures',type=pathlib.Path,default=pathlib.Path(__file__).resolve().parent.parent)
p.add_argument('--profile',choices=['all','rpc-lifecycle','local-room-senddata','publisher-lifecycle'],default='all')
p.add_argument('--side',choices=['all','go','rust'],default='all');p.add_argument('--browser-channel',default='chrome')
a=p.parse_args();
def interrupted(signum, frame): raise KeyboardInterrupt('owned campaign interrupted: '+str(signum))
signal.signal(signal.SIGTERM,interrupted)
sha=lambda path:hashlib.sha256(pathlib.Path(path).read_bytes()).hexdigest()
assert os.uname().sysname=='Linux'and os.getuid()!=0 and pathlib.Path('/etc/cloud/cloud.cfg').is_file(),'owned nonroot Linux cloud VM required'
assert a.exclusive_owned_vm_loopback,'coordinator must have stopped all other VM campaigns'
node=pathlib.Path(shutil.which(a.node)or a.node).resolve();tc=pathlib.Path(shutil.which(a.tc)or a.tc).resolve();assert node.is_file()and tc.is_file()
helper=a.source_root.resolve()/'tests/interop/egress-linux/run-owned-stack.py'
spec=importlib.util.spec_from_file_location('standalone_owned_linux_tree',helper);helpers=importlib.util.module_from_spec(spec);spec.loader.exec_module(helpers)
binaries={s:getattr(a,s).resolve()for s in ['go','rust']}
for s,b in binaries.items():assert sha(b)==getattr(a,s+'_sha256'),s+' immutable binary SHA mismatch'
project=a.interop_project.resolve();lock=project/'package-lock.json';assert sha(lock)==a.interop_lock_sha256,'original SDK lock mismatch'
fixtures=a.fixtures.resolve();a.output=a.output.resolve();a.output.mkdir(parents=True,exist_ok=False)
clean={k:os.environ[k]for k in ['PATH','HOME','TMPDIR','TMP','TEMP']if k in os.environ};clean.update(TZ='UTC',LANG='C')
report={'status':'RUNNING','scope':'actual standalone original Go/Rust typed RPC and public SendData SDK consumers; not all media/worker parity','go_source_commit':'e5655ac20267fa5005dccaf3f0e3bfc3ea688c07','binaries':{s:{'path':str(b),'sha256':sha(b)}for s,b in binaries.items()},'sdk_lock_sha256':sha(lock),'inherited_livekit_names_forwarded':0,'environment_names':sorted(clean),'node':{'path':str(node),'sha256':sha(node)},'tc':{'path':str(tc),'sha256':sha(tc)},'owned_tree_helper':{'path':str(helper),'sha256':sha(helper)},'network_scope':'exclusive owned VM lo401:netem delay20ms; actual UDP/RTP/SR/RR unchanged SDK','runs':[],'cleanup_errors':[]}
def save(): (a.output/'execution.json').write_text(json.dumps(report,indent=2)+'\n')
def command(args,timeout=10,cwd=None):
 r=subprocess.run([str(x)for x in args],env=clean,cwd=cwd,capture_output=True,timeout=timeout);assert len(r.stdout)<4<<20 and len(r.stderr)<4<<20;return r
def ps(pid,field):
 r=command(['/bin/ps','-ww','-p',pid,'-o',field+'=']);return r.stdout.decode().strip()if r.returncode==0 else None
def identity(pid,executable=None):
 info=helpers.process_identity(pid);assert info and info['state']!='Z','owned PID disappeared before proof'
 assert pathlib.Path('/proc',str(pid)).stat().st_uid==os.getuid()
 result={**info,'birth':ps(pid,'lstart'),'uid':os.getuid(),'command_sha256':hashlib.sha256(pathlib.Path('/proc',str(pid),'cmdline').read_bytes()).hexdigest()}
 if executable:
  actual=pathlib.Path('/proc',str(pid),'exe').resolve();assert actual==executable.resolve(),'owned executable mapping mismatch';assert sha(actual)==sha(executable)
  result['executable']=str(actual);result['executable_sha256']=sha(actual)
 return result
def same(proof):
 now=helpers.process_identity(proof['pid']);return now and now['start']==proof['start']and pathlib.Path('/proc',str(proof['pid'])).stat().st_uid==proof['uid']
def fd_inodes(pid):
 result=set()
 for fd in pathlib.Path('/proc',str(pid),'fd').iterdir():
  try:
   m=re.fullmatch(r'socket:\[(\d+)\]',os.readlink(fd))
   if m:result.add(m.group(1))
  except FileNotFoundError:pass
 return result
def socket_owners(port,kind,pid=None):
 result=set()
 for name in (['tcp','tcp6']if kind=='TCP'else['udp','udp6']):
  for line in pathlib.Path('/proc/net',name).read_text().splitlines()[1:]:
   f=line.split()
   if int(f[1].rsplit(':',1)[1],16)==port and (kind!='TCP'or f[3]=='0A'):result.add(f[9])
 return result if pid is None else result&fd_inodes(pid)
def reserve():
 held=[];ports={}
 for name,kind in [('http',socket.SOCK_STREAM),('rtc_tcp',socket.SOCK_STREAM),('rtc_udp',socket.SOCK_DGRAM)]:
  sock=socket.socket(socket.AF_INET,kind);sock.bind(('127.0.0.1',0));ports[name]=sock.getsockname()[1];held.append(sock)
 return held,ports
def server_proof(child,binary,ports,birth):
 assert child.poll()is None and same(birth),'tracked server identity changed'
 assert sha(binary)==report['binaries'][birth['side']]['sha256']
 listeners=[]
 for name,kind in [('http','TCP'),('rtc_tcp','TCP'),('rtc_udp','UDP')]:
  all_inodes=socket_owners(ports[name],kind);owned=socket_owners(ports[name],kind,child.pid);assert all_inodes and owned==all_inodes,('exact owned server socket inode',name)
  listeners.append({'field':name,'port':ports[name],'protocol':kind,'owned_pid':child.pid,'socket_inodes':sorted(owned)})
 result=identity(child.pid,binary);result['listeners']=listeners;return result
def terminate(child,proof,record,label):
 if child is None:return
 if child.poll()is None:
  assert proof and same(proof),'refusing to signal reused/unproven '+label+' PID'
  child.send_signal(signal.SIGINT)
 try:record[label+'_exit']=child.wait(timeout=30 if label=='server'else 15)
 except subprocess.TimeoutExpired:
  assert same(proof),'refusing SIGKILL on changed identity'
  child.kill();record[label+'_forced_kill']=True;record[label+'_exit']=child.wait(timeout=5)
def cleanup_browser(ownership,record):
 if not ownership.exists():return
 own=json.loads(ownership.read_text());pid=own.get('browserPid');birth=own.get('browserBirth')
 if pid and (current:=helpers.process_identity(pid)) and current['start']==own.get('browserLinuxStart'):
  assert pathlib.Path('/proc',str(pid)).stat().st_uid==own['browserUid']==os.getuid();assert sha(own['browserExecutable'])==own['browserExecutableSha256'];identity(pid,pathlib.Path(own['browserExecutable']));os.kill(pid,signal.SIGTERM);record['browser_emergency_signal']=True
  deadline=time.monotonic()+5
  while ps(pid,'lstart')==birth and time.monotonic()<deadline:time.sleep(.05)
  if ps(pid,'lstart')==birth:os.kill(pid,signal.SIGKILL);record['browser_emergency_forced_kill']=True
save()
version=command([node,'--version']);assert version.returncode==0;report['node']['version']=version.stdout.decode().strip()
identity_js="""const fs=require('fs'),path=require('path'),crypto=require('crypto');const project=process.argv[1],result={};for(const[name,want]of [['livekit-client','2.23.0'],['livekit-server-sdk','2.19.2']]){const p=path.join(project,'node_modules',name,'package.json'),b=fs.readFileSync(p),v=JSON.parse(b).version;if(v!==want)throw Error(name+' version mismatch');result[name]={version:v,package_sha256:crypto.createHash('sha256').update(b).digest('hex')}}console.log(JSON.stringify(result));"""
r=command([node,'-e',identity_js,project]);assert r.returncode==0,'actual original SDK dependency identity failed';report['sdk']=json.loads(r.stdout);save()
netem_installed=False
netem_lock=None
netem_lock_owned=False
netem_lock_inode=None
netem_baseline=None
netem_lock_path=pathlib.Path('/tmp/livekit-owned-vm-lo-netem.lock')
def qdisc():
 result=command([tc,'-j','qdisc','show','dev','lo']);assert result.returncode==0,'tc qdisc query failed'
 rows=json.loads(result.stdout);assert isinstance(rows,list);return rows

def netem_proof():
 rows=qdisc();assert len(rows)==1 and rows[0].get('kind')=='netem'and rows[0].get('handle')=='401:'and rows[0].get('root'),'owned lo401:netem changed'
 text=command([tc,'qdisc','show','dev','lo']);assert text.returncode==0
 value=text.stdout.decode();assert re.search(r'\bdelay\s+20(?:\.0+)?ms\b',value),'actual wire delay is not20ms'
 return {'qdisc':rows,'text':value.strip(),'qdisc_sha256':hashlib.sha256(json.dumps(rows,sort_keys=True).encode()).hexdigest()}
def install_netem():
 global netem_installed,netem_lock,netem_lock_owned,netem_lock_inode,netem_baseline
 netem_lock=os.open(netem_lock_path,os.O_CREAT|os.O_RDWR,0o600);assert os.fstat(netem_lock).st_uid==os.getuid()
 fcntl.flock(netem_lock,fcntl.LOCK_EX|fcntl.LOCK_NB);netem_lock_owned=True;netem_lock_inode=os.fstat(netem_lock).st_ino
 netem_baseline=qdisc();report['netem']={'baseline':netem_baseline,'device':'lo','owned_handle':'401:','delay_ms':20,'lock_inode':netem_lock_inode}
 assert len(netem_baseline)==1 and netem_baseline[0].get('kind')=='noqueue'and netem_baseline[0].get('handle')=='0:'and netem_baseline[0].get('root'),'preexisting non-noqueue VM loopback qdisc; refusing mutation'
 save();result=command(['sudo','-n',tc,'qdisc','add','dev','lo','root','handle','401:','netem','delay','20ms'])
 report['netem']['install_exit']=result.returncode;report['netem']['install_stderr']=result.stderr.decode()[:4096];assert result.returncode==0,'owned netem installation failed'
 netem_installed=True;report['netem']['installed']=netem_proof();save()
def restore_netem():
 global netem_installed,netem_lock
 try:
  if netem_installed:
   report['netem']['before_remove']=netem_proof()
   result=command(['sudo','-n',tc,'qdisc','del','dev','lo','root','handle','401:']);report['netem']['remove_exit']=result.returncode;assert result.returncode==0,'owned netem removal failed'
   netem_installed=False;after=qdisc();report['netem']['restored']=after;assert after==netem_baseline,'loopback qdisc baseline did not restore exactly'
   assert sha(tc)==report['tc']['sha256'],'tc executable changed';report['netem']['restored_exactly']=True
 finally:
  if netem_lock is not None:
   if netem_lock_owned:
    assert netem_lock_path.stat().st_ino==netem_lock_inode and netem_lock_path.stat().st_uid==os.getuid(),'owned lock identity changed'
    netem_lock_path.unlink();fcntl.flock(netem_lock,fcntl.LOCK_UN)
   os.close(netem_lock);netem_lock=None

profiles=['rpc-lifecycle','local-room-senddata']if a.profile=='all'else[a.profile];sides=['go','rust']if a.side=='all'else[a.side]
opener=urllib.request.build_opener(urllib.request.ProxyHandler({}))
try:
 install_netem()
 for profile in profiles:
  for side in sides:
   name=side+'-'+profile;folder=a.output/name;folder.mkdir();script=fixtures/profile/'run.mjs';assert script.is_file()
   record={'profile':profile,'side':side,'status':'RUNNING','script_sha256':sha(script),'browser_helper_sha256':sha(fixtures/'owned-browser-exit.mjs'),'started_unix':time.time()};report['runs'].append(record);save()
   child=probe=None;birth=probe_birth=None;result=None;owned=folder/'ownership.json';temp=pathlib.Path(tempfile.mkdtemp(prefix='livekit-standalone-owned-'));record['temporary_config_root']=str(temp);tree=helpers.OwnedProcessTree();record['cleanup_errors']=[]
   held,ports=reserve();record['ports']=ports;config=temp/'config.yaml'
   config.write_text('port: '+str(ports['http'])+'\nbind_addresses: [127.0.0.1]\nkeys:\n  devkey: secret\nrtc:\n  node_ip: 127.0.0.1\n  use_external_ip: false\n  enable_loopback_candidate: true\n  ips:\n    includes: [127.0.0.1/32]\n  tcp_port: '+str(ports['rtc_tcp'])+'\n  udp_port: '+str(ports['rtc_udp'])+'\nroom:\n  empty_timeout: 300\n  departure_timeout: 20\n')
   record['config_sha256']=sha(config);record['config_yaml']=config.read_text();assert 'redis'not in record['config_yaml'].lower()
   slog=(folder/'server.log').open('xb');plog=None
   try:
    record['netem_before']=netem_proof()
    for s in held:s.close()
    held=[];binary=binaries[side];argv=[str(binary),'--dev','--config',str(config)];child=subprocess.Popen(argv,env=clean,stdout=slog,stderr=subprocess.STDOUT,start_new_session=True)
    tree.add(child);birth=identity(child.pid,binary);birth['side']=side;record['server_identity']=birth;save();url='http://127.0.0.1:'+str(ports['http']);deadline=time.monotonic()+30
    while True:
     assert child.poll()is None,'owned server exited during readiness'
     try:
      with opener.open(url+'/',timeout=1)as response:assert response.status==200
      break
     except(OSError,TimeoutError):
      if time.monotonic()>=deadline:raise
      time.sleep(.1)
    record['before_probe']=server_proof(child,binary,ports,birth);save()
    probe_env=dict(clean,INTEROP_PROJECT=str(project),LIVEKIT_URL=url.replace('http:','ws:'),LIVEKIT_API_KEY='devkey',LIVEKIT_API_SECRET='secret',BROWSER_CHANNEL=a.browser_channel,POLICY_BROWSER_STDERR=str(folder/'browser.stderr'),POLICY_OWNERSHIP=str(owned))
    if a.browser_executable:probe_env['BROWSER_EXECUTABLE']=str(a.browser_executable.resolve())
    prefix={'rpc-lifecycle':'RPC','local-room-senddata':'SENDDATA','publisher-lifecycle':'PUBLISHER'}[profile];probe_env[prefix+'_DEPLOYMENT']='standalone';probe_env[prefix+'_OUTPUT']=str(folder/'probe.json')
    plog=(folder/'probe.log').open('xb');probe=subprocess.Popen([str(node),str(script)],env=probe_env,stdout=plog,stderr=subprocess.STDOUT,start_new_session=True);tree.add(probe);probe_birth=identity(probe.pid,node);record['probe_identity']=probe_birth;save()
    record['probe_exit']=probe.wait(timeout=180);plog.close();plog=None
    record['after_probe']=server_proof(child,binary,ports,birth);assert record['probe_exit']==0,'actual Node probe failed'
    result=json.loads((folder/'probe.json').read_text());assert result['status']=='PASS'and result['cleanup_errors']==[]
    assert result['browserExit']['code']==0 and result['browserExit']['signal']is None,'actual Chromium child exit required'
    assert result['browserCleanup']['contextsAfter']==0 and not result['browserCleanup']['failures'];assert result['ownership']==json.loads(owned.read_text())
    assert result['ownership']['nodePid']==probe.pid and result['ownership']['nodeBirth']==probe_birth['birth']and result['ownership']['nodeLinuxStart']==probe_birth['start'];assert result['ownership']['browserPid']>0 and result['ownership']['browserBirth'];assert result['ownership']['browserUid']==os.getuid();assert sha(result['ownership']['browserExecutable'])==result['ownership']['browserExecutableSha256']
    assert not((current:=helpers.process_identity(result['ownership']['browserPid']))and current['start']==result['ownership']['browserLinuxStart']),'owned Chromium remains'
    assert result['browserStderr']['sha256']==sha(folder/'browser.stderr'),'actual owned child stderr mismatch'
    record['probe_sha256']=sha(folder/'probe.json');record['browser_stderr_sha256']=sha(folder/'browser.stderr');record['browser_exit']=result['browserExit'];record['status']='PASS_SCOPED'
   except BaseException as error:
    record['status']='FAIL';record['failure']={'type':type(error).__name__,'message':str(error)[:1000]}
    if isinstance(error,(KeyboardInterrupt,SystemExit)):raise
   finally:
    for s in held:s.close()
    try:
     for label,action in [('probe',lambda:terminate(probe,probe_birth,record,'probe')),('browser',lambda:cleanup_browser(owned,record)),('server',lambda:terminate(child,birth,record,'server'))]:
      try:action()
      except BaseException as error:record['cleanup_errors'].append({'resource':label,'type':type(error).__name__,'message':str(error)[:1000]})
    finally:
     if plog:plog.close()
     record['owned_tree']=tree.finish();record['owned_descendants']=[row for pid,row in tree.known.items()if pid not in tree.roots]
     slog.close();shutil.rmtree(temp);record['temporary_config_removed']=not temp.exists();record['finished_unix']=time.time();record['server_log_sha256']=sha(folder/'server.log')
     try:record['netem_after']=netem_proof()
     except BaseException as error:record['cleanup_errors'].append({'resource':'netem-after','type':type(error).__name__,'message':str(error)[:1000]})
     deadline=time.monotonic()+5
     while any(socket_owners(ports[n],k)for n,k in [('http','TCP'),('rtc_tcp','TCP'),('rtc_udp','UDP')])and time.monotonic()<deadline:time.sleep(.05)
     record['ports_released']=all(not socket_owners(ports[n],k)for n,k in [('http','TCP'),('rtc_tcp','TCP'),('rtc_udp','UDP')]);save()
    try:
     assert record['cleanup_errors']==[],'owned resource cleanup error'
     assert record.get('server_exit')==0 and not record.get('server_forced_kill'), 'owned server did not exit cleanly'
     assert record.get('probe_exit')==0 and not record.get('probe_forced_kill'),'owned Node did not exit cleanly'
     assert record['ports_released']and not record.get('browser_emergency_signal'),'owned resources required emergency cleanup'
     assert record['owned_tree']['released_without_reclamation'],'owned descendants survived clean shutdown'
     assert isinstance(result,dict)and any(row['pid']==result['ownership']['browserPid']and row['start']==result['ownership']['browserLinuxStart']for row in record['owned_descendants']),'actual browser child not observed by owned process tree'
     assert sha(binary)==report['binaries'][side]['sha256']and sha(script)==record['script_sha256']and sha(lock)==a.interop_lock_sha256 and sha(fixtures/'owned-browser-exit.mjs')==record['browser_helper_sha256'],'inputs drifted during actual campaign'
    except Exception as error:
     record['status']='FAIL';record['acceptance_failure']={'type':type(error).__name__,'message':str(error)[:1000]}
    save()
 assert len(report['runs'])==len(profiles)*len(sides)
 report['status']='PASS_SCOPED_ALL_REQUESTED_CAMPAIGNS'if all(r['status']=='PASS_SCOPED'for r in report['runs'])else'FAIL'
except BaseException as error:
 report['status']='FAIL';report['failure']={'type':type(error).__name__,'message':str(error)[:1000]}
finally:
 try:restore_netem()
 except BaseException as error:
  report['status']='FAIL';report['netem_cleanup_failure']={'type':type(error).__name__,'message':str(error)[:1000]}
 save()
raise SystemExit(0 if report['status']=='PASS_SCOPED_ALL_REQUESTED_CAMPAIGNS'else 1)
