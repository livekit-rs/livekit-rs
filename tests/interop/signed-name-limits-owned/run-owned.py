#!/usr/bin/env python3
"""Root-only real nine-profile source/paired campaign, original81 untouched."""
import argparse, hashlib, json, os, pathlib, shutil, signal, socket, subprocess, time, urllib.request
p=argparse.ArgumentParser()
p.add_argument('--root',type=pathlib.Path,required=True)
p.add_argument('--go',type=pathlib.Path,required=True);p.add_argument('--rust',type=pathlib.Path,required=True)
p.add_argument('--go-sha256',required=True);p.add_argument('--rust-sha256',required=True)
p.add_argument('--node',default='node');p.add_argument('--lsof',default='lsof')
p.add_argument('--output',type=pathlib.Path,required=True);p.add_argument('--reference-only',action='store_true')
a=p.parse_args();root=a.root.resolve();here=root/'tests/interop/signed-name-limits'
sha=lambda f:hashlib.sha256(pathlib.Path(f).read_bytes()).hexdigest()
package=pathlib.Path(__file__).resolve().parent;frozen=json.loads((package/'SOURCE.json').read_text())
for rel,want in frozen['fixture_guards'].items():assert sha(root/rel)==want,'immutable original fixture drift: '+rel
plan=json.loads((here/'PLAN.json').read_text())
for rel,want in plan['fixture_hashes'].items():assert sha(here/rel)==want,'PLAN source mismatch: '+rel
for conf in plan['configs']:assert sha(here/conf['config'])==conf['sha256']
assert len(plan['required_profiles'])==9 and plan['required_paired_observations']==81 and plan['required_actual_runs']==18
binaries={side:getattr(a,side).resolve()for side in ['go','rust']}
for side,binary in binaries.items():assert sha(binary)==getattr(a,side+'_sha256'),'qualified '+side+' binary mismatch'
node=pathlib.Path(shutil.which(a.node)or a.node).resolve();lsof=pathlib.Path(shutil.which(a.lsof)or a.lsof).resolve();assert node.is_file()and lsof.is_file()
# Fresh allowlist, not inherited generated/legacy config or runtime injections.
# No inherited LIVEKIT_*, REDIS_HOST, KEYS, DEV, NODE_IP, RUST_LOG, NODE_OPTIONS,
# GODEBUG, GOFLAGS, proxy variables or loader overrides reach servers/runners.
clean={key:os.environ[key]for key in ['PATH','HOME','TMPDIR','TMP','TEMP']if key in os.environ}
clean.update({'TZ':'UTC','LANG':'C'})
a.output.mkdir(parents=True,exist_ok=False)
manifest={'status':'INCOMPLETE','scope':'original unchanged nine profiles/81 HTTP+authenticated signal observations; no RTC/media claim','source_package_sha256':sha(package/'SOURCE.json'),'fixture_plan_sha256':sha(here/'PLAN.json'),'runs':[],'binaries':{side:{'path':str(b),'sha256':sha(b)}for side,b in binaries.items()},'environment':{'mode':'fresh allowlist','inherited_livekit_keys_forwarded':0,'inherited_livekit_key_count':sum(k.startswith('LIVEKIT_')for k in os.environ),'allowed_names':sorted(clean)},'node':{'path':str(node),'sha256':sha(node)},'lsof':{'path':str(lsof),'sha256':sha(lsof)},'cleanup_errors':[]}
def save():
 (a.output/'execution.json').write_text(json.dumps(manifest,indent=2)+'\n')
save()
def command(args,timeout=5,cwd=None):
 result=subprocess.run([str(x)for x in args],env=clean,capture_output=True,timeout=timeout,check=False,cwd=cwd)
 assert len(result.stdout)<1<<20 and len(result.stderr)<1<<20
 return result
version=command([node,'--version']);assert version.returncode==0;manifest['node']['version']=version.stdout.decode().strip();save()
# The original runner uses this unchanged ESM dependency from the same directory.
identity_program="""import{createHash}from'node:crypto';import{readFile,realpath}from'node:fs/promises';import{dirname,resolve}from'node:path';import{fileURLToPath}from'node:url';const sha=b=>createHash('sha256').update(b).digest('hex');const entry=await realpath(fileURLToPath(import.meta.resolve('@livekit/protocol')));let dir=dirname(entry);for(;;){try{const bytes=await readFile(resolve(dir,'package.json')),pkg=JSON.parse(bytes);if(pkg.name==='@livekit/protocol'){if(pkg.version!=='1.52.1')throw Error('protocol version mismatch');console.log(JSON.stringify({name:pkg.name,version:pkg.version,entry,entry_sha256:sha(await readFile(entry)),package_sha256:sha(bytes)}));break}}catch(e){if(e.code!=='ENOENT')throw e}const parent=dirname(dir);if(parent===dir)throw Error('resolved package missing');dir=parent}"""
identity=command([node,'--input-type=module','-e',identity_program],cwd=here);assert identity.returncode==0,'actual original protocol dependency setup failed';manifest['protocol']=json.loads(identity.stdout)
lock=here.parent/'package-lock.json';assert lock.is_file();manifest['interop_lock_sha256']=sha(lock);save()

# No proxy environment may route a local health request to somebody else.
opener=urllib.request.build_opener(urllib.request.ProxyHandler({}))
def rows_for_port(port,protocol,pid=None):
 args=[lsof,'-nP']
 if pid is not None:args+=['-a','-p',str(pid)]
 args+=['-i'+protocol+':'+str(port)]
 if protocol=='TCP':args+=['-sTCP:LISTEN']
 args+=['-Fpn']
 result=command(args);assert result.returncode in [0,1],'lsof setup failure'
 lines=result.stdout.decode().splitlines();pids={int(s[1:])for s in lines if s.startswith('p')}
 endpoints=[s[1:]for s in lines if s.startswith('n')]
 return {'pids':pids,'endpoints':endpoints,'sha256':hashlib.sha256(result.stdout).hexdigest(),'exit':result.returncode}
def reserve_probe(conf):
 held=[];observed=[]
 try:
  for field,kind,protocol in [('http_ws_port',socket.SOCK_STREAM,'TCP'),('rtc_tcp_port',socket.SOCK_STREAM,'TCP'),('rtc_udp_port',socket.SOCK_DGRAM,'UDP')]:
   port=conf[field];found=rows_for_port(port,protocol);assert not found['pids']and not found['endpoints'],('occupied',field,port)
   sock=socket.socket(socket.AF_INET,kind)
   try:
    if kind==socket.SOCK_STREAM:sock.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1)
    sock.bind(('127.0.0.1',port));held.append(sock)
   except BaseException:sock.close();raise
   observed.append({'field':field,'port':port,'protocol':protocol,'prior_listener_or_udp_owners':0,'actual_bind_probe':True})
  return observed
 finally:
  # Release only own probes immediately before actual spawn. Startup/current
  # PID proofs below catch the unavoidable bind gap instead of trusting health.
  for sock in held:sock.close()
def process_proof(server,binary,argv,config,conf):
 assert server.poll() is None,'tracked owned server exited'
 pid=server.pid;proc=pathlib.Path('/proc')/str(pid);config_sha=sha(config)
 assert config_sha==conf['sha256'],'startup YAML changed'
 assert sha(binary)==manifest['binaries'][conf['side']]['sha256'],'binary changed'
 if (proc/'exe').exists():
  executable=(proc/'exe').resolve();assert executable==binary
  assert sha(proc/'exe')==sha(binary);actual=(proc/'cmdline').read_bytes().split(b'\0');actual=[x.decode()for x in actual if x]
  assert proc.stat().st_uid==os.getuid()
 else:
  uid=command(['/bin/ps','-p',pid,'-o','uid=']);assert uid.returncode==0 and int(uid.stdout.strip())==os.getuid()
  cmd=command(['/bin/ps','-ww','-p',pid,'-o','command=']);assert cmd.returncode==0
  # Canonical task paths without whitespace make this fallback unambiguous.
  assert all(not any(ch.isspace()for ch in arg)for arg in argv)
  actual=cmd.stdout.decode().strip().split();executable=pathlib.Path(actual[0]).resolve()
  maps=command([lsof,'-a','-p',pid,'-d','txt','-Fn']);assert maps.returncode==0
  names=[line[1:]for line in maps.stdout.decode().splitlines()if line.startswith('n')]
  assert str(binary)in names,'actual mapped executable identity'
 assert actual==argv,'unexpected startup arguments; not persisting unsafe argv'
 listeners=[]
 for field,protocol in [('http_ws_port','TCP'),('rtc_tcp_port','TCP'),('rtc_udp_port','UDP')]:
  port=conf[field];own=rows_for_port(port,protocol,pid);all_owners=rows_for_port(port,protocol)
  assert own['pids']=={pid}and own['endpoints'],('tracked PID missing actual socket',field)
  assert all_owners['pids']=={pid},('competing socket owner',field)
  assert all(endpoint.endswith(':'+str(port))for endpoint in own['endpoints'])
  listeners.append({'field':field,'port':port,'protocol':protocol,'actual_owned_endpoints':own['endpoints'],'pid':pid,'owned_query_sha256':own['sha256'],'all_owner_query_sha256':all_owners['sha256']})
 return {'pid':pid,'uid':os.getuid(),'actual_executable':str(executable),'binary_sha256':sha(binary),'actual_argv':actual,'config_path':str(config),'startup_config_sha256':config_sha,'listeners':listeners,'environment_config_overrides_absent':True}
def kill_owned_group(child):
 try:os.killpg(child.pid,signal.SIGKILL)
 except ProcessLookupError:pass
 return child.wait(timeout=5)
def run_child(args,env,log,timeout):
 child=subprocess.Popen([str(x)for x in args],env=env,stdout=log,stderr=subprocess.STDOUT,start_new_session=True)
 try:return child.wait(timeout=timeout)
 except BaseException:
  kill_owned_group(child);raise
try:
 for profile in plan['required_profiles']:
  for side in (['go']if a.reference_only else['go','rust']):
   conf=next(c for c in plan['configs']if c['side']==side and c['profile']==profile)
   config=(here/conf['config']).resolve();assert sha(config)==conf['sha256']
   binary=binaries[side];url='http://127.0.0.1:'+str(conf['http_ws_port'])
   record={'side':side,'profile':profile,'config_sha256':conf['sha256'],'started_unix':time.time(),'preflight':reserve_probe(conf)}
   manifest['runs'].append(record);save()
   argv=[str(binary),'--dev','--config',str(config)]
   server_log=a.output/(side+'-'+profile+'.server.log');log=server_log.open('xb')
   server=None
   try:
    server=subprocess.Popen(argv,stdout=log,stderr=subprocess.STDOUT,env=clean,start_new_session=True);record['pid']=server.pid;save()
    deadline=time.monotonic()+30
    while True:
     assert server.poll()is None,('server exited',server.returncode)
     try:
      with opener.open(url+'/',timeout=1)as response:assert response.status==200
      break
     except(OSError,TimeoutError):
      if time.monotonic()>=deadline:raise
      time.sleep(.1)
    record['before_runner']=process_proof(server,binary,argv,config,conf);save()
    env=dict(clean,LIVEKIT_URL=url.replace('http:','ws:'),LIVEKIT_API_KEY='devkey',LIVEKIT_API_SECRET='secret',SIGNED_LIMIT_PROFILE=profile,SIGNED_LIMIT_OUTPUT=str((a.output/(side+'-'+profile+'.json')).resolve()))
    with (a.output/(side+'-'+profile+'.runner.log')).open('xb')as out:record['runner_exit']=run_child([node,here/'run.mjs'],env,out,180)
    record['after_runner']=process_proof(server,binary,argv,config,conf);save()
    assert record['runner_exit']==0,(side,profile,'original runner failed')
    result_path=a.output/(side+'-'+profile+'.json');result=json.loads(result_path.read_text())
    assert result['status']=='PASS_SCOPED'and result['profile']==profile and result['cleanupErrors']==[]
    assert len(result['observations'])==plan['expected_observations'][profile]
    assert len({x['case']for x in result['observations']})==len(result['observations'])
    record['actual_observations']=len(result['observations']);record['report_sha256']=sha(result_path)
   finally:
    if server is not None:
     if server.poll()is None:server.send_signal(signal.SIGINT)
     try:record['server_exit']=server.wait(timeout=30)
     except subprocess.TimeoutExpired:record['forced_kill']=True;record['server_exit']=kill_owned_group(server)
    log.close();record['server_log_sha256']=sha(server_log);record['finished_unix']=time.time();save()
   assert record.get('server_exit')==0 and not record.get('forced_kill',False),record
  if not a.reference_only:
   compare_log=a.output/('pair-'+profile+'.compare.log')
   with compare_log.open('xb')as out:
    code=run_child([node,here/'compare.mjs',a.output/('go-'+profile+'.json'),a.output/('rust-'+profile+'.json'),a.output/('pair-'+profile+'.json')],clean,out,30)
   assert code==0,('original paired comparator failed',profile)
 if a.reference_only:
  assert len(manifest['runs'])==9 and sum(r['actual_observations']for r in manifest['runs'])==81
  manifest['status']='REFERENCE_FIXTURE_ONLY_ALL9_COMPLETED_NOT_PAIRED'
 else:
  result=subprocess.run(['/usr/bin/python3',str(here/'check-reports.py'),str(a.output)],env=clean,check=False,capture_output=True,text=True,timeout=30)
  (a.output/'aggregate.stdout').write_text(result.stdout);(a.output/'aggregate.stderr').write_text(result.stderr)
  assert result.returncode==0,'original strict81 checker failed'
  aggregate=json.loads(result.stdout);assert aggregate['paired_observations']==81 and aggregate['actual_runs']==18
  (a.output/'aggregate.json').write_text(result.stdout);manifest['status']='PASS_SCOPED_ACTUAL_9_PAIRS'
except BaseException as error:
 manifest['failure']={'kind':type(error).__name__};raise
finally:save()
