#!/usr/bin/env python3
"""Coordinator-only actual binary/CLI/ENV -> live metrics HTTP consumer. NOT_RUN."""
import argparse,base64,hashlib,http.client,json,os,pathlib,signal,socket,subprocess,tempfile,time

def sha(path):return hashlib.sha256(pathlib.Path(path).read_bytes()).hexdigest()
def freeport():
 with socket.socket()as s:s.bind(('127.0.0.1',0));return s.getsockname()[1]
def request(port,values):
 c=http.client.HTTPConnection('127.0.0.1',port,timeout=2)
 try:
  c.putrequest('GET','/metrics');c.putheader('Connection','close')
  for value in values:c.putheader('Authorization',value)
  c.endheaders();r=c.getresponse();body=r.read(1<<20)
  assert len(body)<1<<20,'bounded metrics response'
  return {'status':r.status,'www_authenticate':r.getheader('WWW-Authenticate',''),'content_type':r.getheader('Content-Type',''),'body_length':len(body),'metrics_body':b'# HELP' in body,'body_sha256':hashlib.sha256(body).hexdigest()}
 finally:c.close()
def api_health(port):
 c=http.client.HTTPConnection('127.0.0.1',port,timeout=2)
 try:
  c.request('GET','/',headers={'Connection':'close'})
  response=c.getresponse();body=response.read(4096)
  return response.status==200 and body==b'OK'
 finally:c.close()

def startup_stability(child,api_port,metrics_port,deadline,record,save):
 # Both HTTP servers run before Go Start sets running=true. Keep the same
 # original startup deadline and record a stable post-listener observation.
 # This is a fixture premise, not a public IsRunning() observation.
 stable_since=None;first_both=None;attempts=0;resets=0;states=[]
 record['startup_observations']=states
 while True:
  if child.poll()is not None:raise RuntimeError('owned server exited during startup stability')
  observed=time.monotonic()
  if observed>=deadline:raise RuntimeError('original startup deadline during stability')
  healthy=False;main_ok=False;metrics_status=None;error_kind=None
  try:
   main_ok=api_health(api_port)
   metrics_status=request(metrics_port,[])['status']
   healthy=main_ok and metrics_status in [200,401]
  except (OSError,http.client.HTTPException)as error:error_kind=type(error).__name__
  completed=time.monotonic();attempts+=1
  states.append({'started_monotonic':observed,'completed_monotonic':completed,'process_alive':child.poll()is None,'main_health_200_OK':main_ok,'metrics_status':metrics_status,'healthy':healthy,'error_kind':error_kind})
  assert len(states)<=1000,'bounded startup observation history'
  save()
  if completed>=deadline:raise RuntimeError('original startup deadline during stability')
  if healthy:
   if stable_since is None:stable_since=completed
   if first_both is None:first_both=completed
   if completed-stable_since>=1.0:
    return {'scope':'stable dual HTTP listeners; not direct private running state',
      'minimum_stable_seconds':1.0,'observed_stable_seconds':completed-stable_since,
      'first_both_monotonic':first_both,'stable_since_monotonic':stable_since,
      'completed_monotonic':completed,'observations':attempts,'resets':resets,'states':states}
  else:
   if stable_since is not None:resets+=1
   stable_since=None
  time.sleep(min(.05,max(0,deadline-time.monotonic())))

def stop(child):
 result={'pid_owned':True,'forced':False}
 if child.poll()is None:
  child.send_signal(signal.SIGINT)
  try:child.wait(timeout=10)
  except subprocess.TimeoutExpired:
   result['forced']=True;os.killpg(child.pid,signal.SIGTERM)
   try:child.wait(timeout=5)
   except subprocess.TimeoutExpired:os.killpg(child.pid,signal.SIGKILL);child.wait(timeout=5)
 result['exit']=child.returncode;return result

def main():
 p=argparse.ArgumentParser();p.add_argument('--official',type=pathlib.Path,required=True);p.add_argument('--rust',type=pathlib.Path,required=True);p.add_argument('--go-results',type=pathlib.Path,required=True);p.add_argument('--cases',type=pathlib.Path,default=pathlib.Path(__file__).with_name('cases.json'));p.add_argument('--output',type=pathlib.Path,required=True);a=p.parse_args();assert not a.output.exists(),'fresh result only'
 cases=json.loads(a.cases.read_text());source={}
 for line in a.go_results.read_text().splitlines():
  row=json.loads(line);assert row['id']not in source;source[row['id']]=row
 assert len(cases)==21 and len({x['id']for x in cases})==21 and len(source)==21 and {x['id']for x in cases}==set(source)
 assert sum(bool(x['wire_http'])for x in cases)==17
 for row in cases:assert source[row['id']]['downstream_called']==(source[row['id']]['status']==200)
 logs_dir=a.output.with_name(a.output.name+'.owned-logs');logs_dir.mkdir(mode=0o700,parents=True,exist_ok=False)
 report={'status':'RUNNING','scope':'actual CLI/ENV consumer, live metrics listener; auth status/challenge/unauthorizedbody compared exactly, positive metrics bodies validated not equalized','cases_sha256':sha(a.cases),'source_actual_sha256':sha(a.go_results),'binary_sha256':{'official':sha(a.official),'rust':sha(a.rust)},'campaigns':[],'cleanup_errors':[]}
 report['owned_logs_directory']=str(logs_dir);report['log_policy']='retain actual combined process bytes except exact task-owned credential redactions, preserving all other bytes; raw and safe hashes recorded'
 a.output.parent.mkdir(parents=True,exist_ok=True)
 def save():a.output.write_text(json.dumps(report,indent=2)+'\n');a.output.chmod(0o600)
 save()
 expected_user='owned-user';expected_pass='owned-pass!'
 try:
  for profile in ['yaml','cli','env','cli-over-env']:
   for name,binary in [('official',a.official),('rust',a.rust)]:
    record={'server':name,'profile':profile,'rows':[]};report['campaigns'].append(record);child=None;log=None
    with tempfile.TemporaryDirectory(prefix='metrics-auth-owned-')as tmp:
     tmp=pathlib.Path(tmp);api_port=freeport();metrics_port=freeport()
     while metrics_port==api_port:metrics_port=freeport()
     base_user,base_pass=(expected_user,expected_pass)if profile=='yaml'else('unused-yaml-user','unused-yaml-pass')
     config=tmp/'config.yaml';config.write_text('port: '+str(api_port)+'\nbind_addresses: [127.0.0.1]\nkeys: {APImetrics: owned-metrics-synthetic-secret-at-least32}\nrtc: {node_ip: 127.0.0.1, use_external_ip: false, tcp_port: 0, udp_port: 0, port_range_start: 0, port_range_end: 0}\nlogging: {level: error}\nprometheus: {port: '+str(metrics_port)+', username: '+json.dumps(base_user)+', password: '+json.dumps(base_pass)+'}\n');config.chmod(0o600)
     env={k:os.environ[k]for k in ['PATH','HOME','TMPDIR']if k in os.environ};env.update({'TZ':'UTC','LANG':'C'});argv=[str(binary.resolve()),'--config',str(config)]
     if profile in ['env','cli-over-env']:
      env['LIVEKIT_PROMETHEUS_USERNAME']=expected_user if profile=='env'else'unused-env-user';env['LIVEKIT_PROMETHEUS_PASSWORD']=expected_pass if profile=='env'else'unused-env-pass'
     if profile in ['cli','cli-over-env']:argv+=['--prometheus.username',expected_user,'--prometheus.password',expected_pass]
     record['input']={'config_sha256':sha(config),'credential_source':profile,'argv_shape':['--config','<owned-config>']+(['--prometheus.username','<owned-credential>','--prometheus.password','<owned-credential>']if profile in ['cli','cli-over-env']else[]),'selected_env_names':sorted(k for k in env if k.startswith('LIVEKIT_'))}
     logpath=tmp/'process.log';log=logpath.open('wb')
     try:
      child=subprocess.Popen(argv,env=env,stdout=log,stderr=subprocess.STDOUT,start_new_session=True)
      deadline=time.monotonic()+30
      while True:
       if child.poll()is not None:raise RuntimeError('owned server exited before metrics readiness')
       try:
        ready=request(metrics_port,[])
        if ready['status']in [200,401]:break
       except (OSError,http.client.HTTPException):pass
       if time.monotonic()>=deadline:raise RuntimeError('metrics actual listener readiness deadline')
       time.sleep(.05)
      record['startup_stability']=startup_stability(child,api_port,metrics_port,deadline,record,save);save()
      assert child.poll()is None
      for case in cases:
       if not case['wire_http']:continue
       values=case.get('header_values',[])or([]if case['authorization']is None else[case['authorization']]);actual=request(metrics_port,values);expected=source[case['id']]
       equal=actual['status']==expected['status']and actual['www_authenticate']==expected['www_authenticate']
       if expected['status']==200:equal=equal and actual['metrics_body']
       else:equal=equal and actual['body_length']==expected['body_length']and actual['content_type']==expected['content_type']
       record['rows'].append({'id':case['id'],'matches_source_auth':equal,'actual':actual});save()
      # Real startup precedence, not parse-only: base YAML and superseded ENV
      # credentials must not authenticate when another configured source won.
      if profile!='yaml':
       for label,user,password in [('yaml','unused-yaml-user','unused-yaml-pass')]+([('env','unused-env-user','unused-env-pass')]if profile=='cli-over-env'else[]):
        token=base64.b64encode((user+':'+password).encode()).decode();actual=request(metrics_port,['Basic '+token]);record['rows'].append({'id':'superseded-'+label,'matches_source_auth':actual['status']==401,'actual':actual})
     finally:
      if child:
       try:record['process_cleanup']=stop(child)
       except Exception as cleanup_error:
        record['process_cleanup']={'exit':child.poll(),'forced':True,'failure_type':type(cleanup_error).__name__};report['cleanup_errors'].append(name+'-'+profile+'-stop-failed')
      if log:log.close()
      raw_log=logpath.read_bytes();safe_log=raw_log
      # This process receives only our synthetic config/env/argv. Preserve real
      # diagnostics, redacting exact known credential values and encoded tokens.
      sensitive=[expected_user,expected_pass,base_user,base_pass,'unused-env-user','unused-env-pass','APImetrics','owned-metrics-synthetic-secret-at-least32']
      sensitive += [base64.b64encode((user+':'+password).encode()).decode()for user,password in [(expected_user,expected_pass),(base_user,base_pass),('unused-env-user','unused-env-pass')]]
      sensitive += [str(value) for case in cases for value in ([case.get('authorization')]if case.get('authorization')else case.get('header_values',[]))]
      for value in sorted(set(sensitive),key=len,reverse=True):
       if value:safe_log=safe_log.replace(value.encode('utf8'),b'<owned-credential-redacted>')
      stem=name+'-'+profile;safe_path=logs_dir/(stem+'.process.log');fd=os.open(safe_path,os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o600)
      with os.fdopen(fd,'wb')as retained:retained.write(safe_log)
      record['process_log_sha256']=hashlib.sha256(raw_log).hexdigest();record['safe_process_log']={'path':str(safe_path),'sha256':sha(safe_path),'bytes':len(safe_log),'redactions_applied':safe_log!=raw_log}
      receipt={'server':name,'profile':profile,'binary_sha256':report['binary_sha256'][name],'input':record['input'],'process_cleanup':record.get('process_cleanup',{'spawn_failed':True}),'raw_log_sha256':record['process_log_sha256'],'safe_log':record['safe_process_log'],'rows_completed':len(record['rows'])}
      receipt_path=logs_dir/(stem+'.exit.json');fd=os.open(receipt_path,os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o600)
      with os.fdopen(fd,'w')as retained:json.dump(receipt,retained,indent=2);retained.write('\n')
      record['exit_receipt']={'path':str(receipt_path),'sha256':sha(receipt_path)}
      if child and(record['process_cleanup']['forced']or record['process_cleanup']['exit']!=0):report['cleanup_errors'].append(name+'-'+profile)
      save()
  required_ids={x['id']for x in cases if x['wire_http']}
  for campaign in report['campaigns']:
   actual_ids=[x['id']for x in campaign['rows']if not x['id'].startswith('superseded-')]
   assert len(actual_ids)==17 and len(set(actual_ids))==17 and set(actual_ids)==required_ids,'all unique actual HTTP cases required'
  assert {(c['server'],c['profile'])for c in report['campaigns']}=={(server,profile)for server in ['official','rust']for profile in ['yaml','cli','env','cli-over-env']}
  report['status']='PASS_SCOPED'if all(r['matches_source_auth']for c in report['campaigns']for r in c['rows'])and len(report['campaigns'])==8 and not report['cleanup_errors']else'FAIL'
 except Exception as error:report['status']='FAIL';report['failure']={'type':type(error).__name__};raise
 finally:
  if report['status']=='RUNNING':report['status']='FAIL'
  save()
 if report['status']!='PASS_SCOPED':raise SystemExit(1)
if __name__=='__main__':main()
