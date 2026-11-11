#!/usr/bin/env python3
"""Real original Go observer + actual CLI12. Never starts/stops Redis/server.
Coordinator must pass a private exclusively owned runtime from prepare-runtime.py.
"""
from pathlib import Path
import argparse,base64,errno,hashlib,json,os,signal,socket,stat,subprocess,tempfile,time,uuid

def sha(path):return hashlib.sha256(Path(path).read_bytes()).hexdigest()
class Redis:
 def __init__(self,path,password):
  self.sock=socket.socket(socket.AF_UNIX);self.sock.settimeout(5);self.sock.connect(path);self.file=self.sock.makefile('rb');assert self.call(b'AUTH',password.encode())==b'OK';assert self.call(b'PING')==b'PONG'
 def read(self):
  line=self.file.readline(1<<20);assert line.endswith(b'\r\n'),'bounded RESP line';tag,data=line[:1],line[1:-2]
  if tag==b'+':return data
  if tag==b'-':raise RuntimeError('owned Redis error '+data.decode('utf8','replace'))
  if tag==b':':return int(data)
  if tag==b'$':
   n=int(data)
   if n==-1:return None
   assert 0<=n<=1<<20;value=self.file.read(n);assert len(value)==n and self.file.read(2)==b'\r\n';return value
  if tag==b'*':
   n=int(data);assert 0<=n<=65536;return [self.read()for _ in range(n)]
  raise RuntimeError('unexpected RESP type')
 def call(self,*values):
  self.sock.sendall(b'*'+str(len(values)).encode()+b'\r\n'+b''.join(b'$'+str(len(v)).encode()+b'\r\n'+v+b'\r\n'for v in values));return self.read()
 def close(self):self.file.close();self.sock.close()
def invoke(binary,args,env,timeout=8):
 p=subprocess.Popen([str(binary),*args],stdout=subprocess.PIPE,stderr=subprocess.PIPE,env=env,start_new_session=True)
 try:out,err=p.communicate(timeout=timeout);timed_out=False
 except subprocess.TimeoutExpired:os.killpg(p.pid,signal.SIGKILL);out,err=p.communicate();timed_out=True
 assert len(out)<=1<<20 and len(err)<=1<<20
 # Inputs only have validUTF8 controlled fixtures. Preserve exact output, never trim.
 return {'exit':p.returncode,'timed_out':timed_out,'stdout':out.decode('utf8'),'stderr':err.decode('utf8')}
def group_alive(pgid):
 try:os.killpg(pgid,0);return True
 except ProcessLookupError:return False

def stop_consumer_group(process):
 # Only this Popen's new session/process group; never broad-name/host cleanup.
 # Preserve actual exit receipt and do not report cleanup if a group survives.
 if group_alive(process.pid):
  try:os.killpg(process.pid,signal.SIGTERM)
  except ProcessLookupError:pass
  end=time.monotonic()+5
  while time.monotonic()<end and group_alive(process.pid):
   process.poll();time.sleep(.02)
 if group_alive(process.pid):
  try:os.killpg(process.pid,signal.SIGKILL)
  except ProcessLookupError:pass
 try:process.wait(timeout=5)
 except subprocess.TimeoutExpired:return False
 end=time.monotonic()+5
 while time.monotonic()<end and group_alive(process.pid):time.sleep(.02)
 return not group_alive(process.pid)

def invoke_consumer(command,env,timeout):
 # Inherit original stdout/stderr unchanged: no stream decoding/normalization.
 process=subprocess.Popen(command,env=env,start_new_session=True)
 timed_out=False;interrupted=False;cleanup_complete=False
 try:
  try:process.wait(timeout=timeout)
  except subprocess.TimeoutExpired:timed_out=True
  except BaseException:interrupted=True
 finally:
  cleanup_complete=stop_consumer_group(process)
 return {'exit':process.returncode,'timed_out':timed_out,'interrupted':interrupted,
         'cleanup_complete':cleanup_complete,'timeout_seconds':timeout,
         'process_group_alive_after_cleanup':group_alive(process.pid)}

def field(number,value):return bytes([number<<3|2,len(value)])+value

def main():
 p=argparse.ArgumentParser();p.add_argument('--official',type=Path,required=True);p.add_argument('--rust',type=Path,required=True);p.add_argument('--go-observer',type=Path,required=True);p.add_argument('--owned-dir',type=Path,required=True);p.add_argument('--owned-isolated-redis',action='store_true');p.add_argument('--output',type=Path,required=True);p.add_argument('--consumer-command-json',help='Optional coordinator Cargo argv executed while exact owned fixtures remain live');p.add_argument('--consumer-timeout-seconds',type=int,default=900,help='Execution-only Cargo/test child budget; does not alter business request timeouts');a=p.parse_args();assert a.owned_isolated_redis;assert a.consumer_timeout_seconds>0
 d=Path(__file__).resolve().parent;source=json.loads((d/'SOURCE.json').read_text())
 for n,v in source['assets_sha256'].items():assert sha(d/n)==v,'fixture asset drift '+n
 owned=a.owned_dir.resolve();proof=json.loads((owned/'OWNED.json').read_text());assert proof['uid']==os.getuid();assert stat.S_IMODE(owned.stat().st_mode)==0o700;unix=Path(proof['unix_socket']);assert unix.parent==owned and stat.S_ISSOCK(unix.stat().st_mode);assert unix.stat().st_uid==os.getuid();port=proof['port'];password=os.environ['NODE_ADDRESS_REDIS_PASSWORD'];assert password
 env={k:os.environ[k]for k in ['PATH','HOME','TMPDIR','LSOF']if k in os.environ};env.update({'TZ':'UTC','LANG':'C','NODE_ADDRESS_REDIS_PASSWORD':password})
 closed=socket.socket();closed.bind(('127.0.0.1',0));closed.listen(1);closed_addr='127.0.0.1:'+str(closed.getsockname()[1]);closed.close();user='owned_addr_'+uuid.uuid4().hex[:12]
 replacements={'$UNIX':str(unix),'$MISSING_UNIX':str(owned/'not-present.sock'),'$IPV4':'127.0.0.1:'+str(port),'$IPV6':'[::1]:'+str(port),'$CLOSED_TCP':closed_addr,'$ACL_USER':user}
 definitions=json.loads((d/'cases.json').read_text());assert len(definitions)==12
 runtime=[]
 for case in definitions:
  row={k:replacements.get(v,v)if isinstance(v,str)else v for k,v in case.items()};row['effective_address']=row.get('cli_redis_host',row['address']);runtime.append(row)
 expected_ids=[r['id']for r in runtime]
 report={'status':'RUNNING','scope':'original10 address/ENV plus2 additive DB/auth controls, real original library network and actual main processes; no resolver/performance100','source_package_sha256':sha(d/'SOURCE.json'),'cases_sha256':sha(d/'cases.json'),'binary_sha256':{k:sha(v)for k,v in [('official',a.official),('rust',a.rust),('go_observer',a.go_observer)]},'normalization':'NONE; stdout+exit functional parity separate from raw stderr exactness','stderr_scope':'OPEN operational Go pool retry timestamps; actual bytes retained without normalization or invented Rust log','refusal_witnesses':[],'cases':[],'cleanup_errors':[]}
 assert not a.output.exists(),'refuse overwriting prior actual evidence';a.output.parent.mkdir(parents=True,exist_ok=True)
 def save():a.output.write_text(json.dumps(report,indent=2)+'\n')
 def refusal_witness(phase):
  # Darwin a bound-but-not-listening socket can drop SYN rather than RST.
  # Select via real short listen->close; require genuine ECONNREFUSED each time.
  host,number=closed_addr.rsplit(':',1);number=int(number)
  try:
   peer=socket.create_connection((host,number),timeout=.5)
  except OSError as error:
   assert error.errno==errno.ECONNREFUSED,'FAILED_SETUP: endpoint did not produce actual ECONNREFUSED'
   observed_errno=error.errno
  else:
   peer.close();raise AssertionError('FAILED_SETUP: refusal endpoint has a listener')
  args=[os.environ.get('LSOF','lsof'),'-nP','-iTCP:'+str(number),'-sTCP:LISTEN','-Fpn']
  actual=subprocess.run(args,stdout=subprocess.PIPE,stderr=subprocess.PIPE,timeout=5,check=False)
  assert actual.returncode in (0,1) and actual.stdout==b'','FAILED_SETUP: refusal endpoint has LISTEN ownership or lsof failed'
  report['refusal_witnesses'].append({'phase':phase,'errno':observed_errno,'kind':'ECONNREFUSED','lsof_exit':actual.returncode,'lsof_stdout_sha256':hashlib.sha256(actual.stdout).hexdigest(),'lsof_stderr_sha256':hashlib.sha256(actual.stderr).hexdigest()});save()
 save();admin=None;acl=False;seeded=False
 try:
  admin=Redis(str(unix),password)
  for db in [0,1]:admin.call(b'SELECT',str(db).encode());assert admin.call(b'DBSIZE')==0,'exclusive fresh test DB required; do not overwrite application data'
  assert admin.call(b'ACL',b'SETUSER',user.encode(),b'on',('>'+password).encode(),b'~*',b'+@all')==b'OK';acl=True
  seeded=True
  for db in [0,1]:
   admin.call(b'SELECT',str(db).encode());region=('db'+str(db)).encode();node=field(1,b'ND_addr')+field(2,b'127.0.0.1')+b'\x22\x00\x30\x01'+field(7,region);assert admin.call(b'HSET',b'nodes',b'owned-node',node)==1
  seeded=True
  def snapshot():
   values=[]
   for db in [0,1]:admin.call(b'SELECT',str(db).encode());assert admin.call(b'DBSIZE')==1;values.append(admin.call(b'HGETALL',b'nodes'))
   return values
  original_state=snapshot()
  plan=owned/'runtime-cases.json';plan.write_text(json.dumps(runtime,indent=2)+'\n');plan.chmod(0o600)
  refusal_witness('before-source-observer');observed=invoke(a.go_observer,[str(plan)],env,timeout=120);report['observer_process']={k:v for k,v in observed.items()if k!='stdout'};assert observed['exit']==0 and not observed['timed_out'],'actual original observer setup/execute failure';observer=[json.loads(line)for line in observed['stdout'].splitlines()];assert [r['id']for r in observer]==expected_ids;report['observer_rows']=observer;refusal_witness('after-source-observer');assert snapshot()==original_state,'source observer mutated nodes'
  versions={name:invoke(binary,['--version'],env)for name,binary in [('official',a.official),('rust',a.rust)]};assert versions['official']['stdout']=='livekit-server version 1.13.10\n' and versions['official']['exit']==0 and not versions['official']['timed_out'];report['version_observations']=versions
  for row,source_row in zip(runtime,observer):
   expected_success=bool(row['expected_region']);assert source_row['accepted']is expected_success,'actual original source disposition mismatch '+row['id']
   if expected_success:assert source_row['regions']==[row['expected_region']],'real source DB/node selection'
   secret={'admin':password,'wrong':'owned-known-wrong-secret','empty':''}[row['secret_kind']]
   config=owned/(row['id']+'.yaml');config.write_text('keys: {}\nlogging: {level: error}\nrtc: {node_ip: 127.0.0.1, use_external_ip: false}\nredis:\n  address: '+json.dumps(row['address'])+'\n  db: '+str(row['db'])+'\n  username: '+json.dumps(row['username'])+'\n  password: '+json.dumps(secret)+'\n');config.chmod(0o600)
   argv=['--config',str(config)]
   if row.get('cli_redis_host')is not None:argv+=['--redis-host',row['cli_redis_host']]
   argv+=['list-nodes'];case_env=dict(env)
   if row.get('env_redis_host')is not None:case_env['REDIS_HOST']=row['env_redis_host']
   refusal_witness(row['id']+'-before-official');official=invoke(a.official,argv,case_env);refusal_witness(row['id']+'-after-official');rust=invoke(a.rust,argv,case_env);refusal_witness(row['id']+'-after-rust')
   source_valid=not official['timed_out']and(official['exit']==0 if expected_success else official['exit']==1)
   if expected_success:
    stdout=official['stdout'];source_valid=source_valid and 'IP Address'in stdout and any(cell.strip(' ')==row['expected_region']for line in stdout.splitlines()for cell in line.split('│'))
   else:source_valid=source_valid and official['stdout']==source_row['error']+'\n'
   assert snapshot()==original_state,'main CLI mutated owned nodes'
   report['cases'].append({'id':row['id'],'config_path':str(config),'config_sha256':sha(config),'cli_redis_host':row.get('cli_redis_host'),'source_setup_valid':source_valid,'official':official,'rust':rust,'exact_equal':official==rust,'stderr_exact_equal':official['stderr']==rust['stderr'],'functional_stdout_exit_equal':not official['timed_out'] and not rust['timed_out'] and official['exit']==rust['exit'] and official['stdout']==rust['stdout']});save();
   if official['timed_out']:report['failure_class']='FAILED_SETUP';save()
   assert source_valid,'actual original main/source disposition mismatch '+row['id']
  assert len(report['cases'])==12 and [r['id']for r in report['cases']]==expected_ids
  if a.consumer_command_json:
   command=json.loads(a.consumer_command_json);assert isinstance(command,list)and command and all(isinstance(v,str)for v in command);assert '--lib'in command,'actual public reader is in the library; bin zero-tests is not evidence';consumer_env=dict(os.environ);consumer_env['LIVEKIT_REDIS_ADDRESS_ACTUAL']=str(a.output.resolve());result=invoke_consumer(command,consumer_env,a.consumer_timeout_seconds);report['consumer_process']=result;report['old_public_consumer_exit']=result['exit'];save()
  same=all(row['functional_stdout_exit_equal']for row in report['cases']);reader_ok=report.get('old_public_consumer_exit',0)==0 and not report.get('consumer_process',{}).get('timed_out',False) and not report.get('consumer_process',{}).get('interrupted',False) and report.get('consumer_process',{}).get('cleanup_complete',True);report['functional_stdout_exit_status']='PASS'if same and reader_ok else'FAIL';report['whole_cli_stdio_status']='PASS'if all(row['exact_equal']for row in report['cases']) else'OPEN_STDERR_DIFFERENCE';report['status']='PASS_FUNCTIONAL_SCOPED_STDERR_OPEN'if same and reader_ok else'FAIL';save()
 finally:
  if admin:
   if seeded:
    for db in [0,1]:
     try:admin.call(b'SELECT',str(db).encode());admin.call(b'DEL',b'nodes')
     except Exception:report['cleanup_errors'].append('owned node cleanup failed')
   if acl:
    try:admin.call(b'ACL',b'DELUSER',user.encode())
    except Exception:report['cleanup_errors'].append('owned ACL cleanup failed')
   admin.close()
  closed.close()
  if report['status']=='RUNNING'or report['cleanup_errors']:report['status']='FAIL'
  save()
 if report['status']!='PASS_FUNCTIONAL_SCOPED_STDERR_OPEN':raise SystemExit(1)
if __name__=='__main__':main()
