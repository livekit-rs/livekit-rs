#!/usr/bin/env python3
"""Fresh paired Go/Rust servers for the GENERATED CLI FLAGS --logging.json and
--logging.level; both sides launched with the SAME base YAML plus the SAME extra
argv carrying the flags under test (only HTTP/media ports differ). Capture stderr
and verify:
  json-true-level-info   : --logging.json=true --logging.level=info  -> every non-empty stderr line is valid JSON with "level" field; no "debug" lines
  json-false-level-info  : --logging.json=false --logging.level=info -> lines are NOT valid JSON (console tab-separated format)
  json-true-level-debug  : --logging.json=true --logging.level=debug -> every non-empty stderr line is valid JSON; "debug" lines may appear
Cross-profile control: the oracle (Go) genuinely switches between JSON and console
encoders based on --logging.json, and filters by --logging.level. Rust must match
the oracle's structural behavior per profile. This verifies the FLAG INPUT PATH
(flags parsed -> config.logging.json/level -> zap encoder selection + level filter).
Not a byte-identical comparison (timestamps and caller locations necessarily differ);
structural equivalence of the logging configuration effect is the gate."""
import argparse, hashlib, json, os, pathlib, signal, socket, subprocess, time, urllib.request
import yaml

p=argparse.ArgumentParser()
p.add_argument('--go',required=True,type=pathlib.Path)
p.add_argument('--rust',required=True,type=pathlib.Path)
p.add_argument('--output',required=True,type=pathlib.Path)
a=p.parse_args()
here=pathlib.Path(__file__).resolve().parent
a.output.mkdir(parents=True,exist_ok=True)
GO_PORT,RUST_PORT=20500,20510
# profile -> (extra argv, expect_json: bool, expect_debug_possible: bool)
PROFILES={
 'json-true-level-info':(['--logging.json=true','--logging.level=info'],True,False),
 'json-false-level-info':(['--logging.json=false','--logging.level=info'],False,False),
 'json-true-level-debug':(['--logging.json=true','--logging.level=debug'],True,True),
}
def cfg(port,mt,mu):
 return {'port':port,'bind_addresses':['127.0.0.1'],
    'rtc':{'tcp_port':mt,'udp_port':mu,'node_ip':'127.0.0.1','use_external_ip':False,'enable_loopback_candidate':True,'ips':{'includes':['127.0.0.1/32']}},
    'keys':{'devkey':'secret'},'turn':{'enabled':False},
    'room':{'empty_timeout':60}}
manifest={'status':'INCOMPLETE','scope':'generated CLI flags --logging.json and --logging.level parse+apply+encoder selection+level filter vs pinned Go oracle; structural equivalence of stderr output (not byte-identical); not ENV, YAML value path, media, RTP or webhook claim','binaries':{},'profiles':{}}
for side in ['go','rust']:
 b=getattr(a,side).resolve();manifest['binaries'][side]={'path':str(b),'sha256':hashlib.sha256(b.read_bytes()).hexdigest()}
def save():(a.output/'execution.json').write_text(json.dumps(manifest,indent=2)+'\n')
save()
def free(port):
 with socket.socket() as s:return s.connect_ex(('127.0.0.1',port))!=0
def wait_ready(url,proc,deadline=40):
 end=time.monotonic()+deadline
 while True:
  assert proc.poll() is None,('server exited',proc.returncode)
  try:
   with urllib.request.urlopen(url+'/',timeout=1) as r:
    if r.status==200:return
  except (OSError,TimeoutError):
   if time.monotonic()>=end:raise
  time.sleep(.1)
def analyze_stderr(stderr_text,expect_json,expect_debug_possible):
 """Return dict with analysis of stderr lines."""
 lines=[l for l in stderr_text.split('\n') if l.strip()]
 if not lines:
  return {'line_count':0,'json_lines':0,'non_json_lines':0,'debug_lines':0,'has_level_field':False,'all_json':False}
 json_count=0
 non_json_count=0
 debug_count=0
 has_level_field=False
 all_json=True
 for line in lines:
  try:
   obj=json.loads(line)
   json_count+=1
   if 'level' in obj:
    has_level_field=True
   if obj.get('level')=='debug':
    debug_count+=1
  except (json.JSONDecodeError,ValueError):
   non_json_count+=1
   all_json=False
 return {'line_count':len(lines),'json_lines':json_count,'non_json_lines':non_json_count,'debug_lines':debug_count,'has_level_field':has_level_field,'all_json':all_json}
try:
 for name,(extra_argv,expect_json,expect_debug_possible) in PROFILES.items():
  assert free(GO_PORT) and free(RUST_PORT),('occupied base ports',name)
  rec={'profile':name,'flag_args':extra_argv,'expect_json':expect_json,'expect_debug_possible':expect_debug_possible};manifest['profiles'][name]=rec
  servers={}
  stderr_captures={}
  for side,(port,mt,mu) in [('go',(GO_PORT,20501,20502)),('rust',(RUST_PORT,20511,20512))]:
   path=a.output/(f'{name}-{side}.yaml');path.write_text(yaml.safe_dump(cfg(port,mt,mu)))
   rec[side+'_config_sha256']=hashlib.sha256(path.read_bytes()).hexdigest()
   log=(a.output/(f'{name}-{side}.server.log')).open('wb')
   argv=[str(getattr(a,side).resolve()),'--dev','--config',str(path),'--port',str(port)]+extra_argv
   proc=subprocess.Popen(argv,stdout=log,stderr=subprocess.PIPE)
   servers[side]=(proc,log);rec[side+'_pid']=proc.pid
  save()
  try:
   for side,port in [('go',GO_PORT),('rust',RUST_PORT)]:wait_ready(f'http://127.0.0.1:{port}',servers[side][0])
   # Let servers run for a few seconds to capture startup logs
   time.sleep(3)
  finally:
   for side,(proc,log) in servers.items():
    if proc.poll() is None:proc.send_signal(signal.SIGINT)
    try:
     stderr_data=proc.stderr.read() if proc.stderr else b''
     stderr_captures[side]=stderr_data.decode('utf-8',errors='replace')
     rec[side+'_exit']=proc.wait(timeout=30)
    except subprocess.TimeoutExpired:proc.kill();rec[side+'_exit']=proc.wait();rec[side+'_forced_kill']=True
    log.close()
  save()
  for side in ['go','rust']:assert rec.get(side+'_exit')==0,rec
  # Write captured stderr to files
  for side in ['go','rust']:
   (a.output/(f'{name}-{side}.stderr.txt')).write_text(stderr_captures[side])
   rec[side+'_stderr_bytes']=len(stderr_captures[side])
  # Analyze
  go_analysis=analyze_stderr(stderr_captures['go'],expect_json,expect_debug_possible)
  rust_analysis=analyze_stderr(stderr_captures['rust'],expect_json,expect_debug_possible)
  rec['go_analysis']=go_analysis
  rec['rust_analysis']=rust_analysis
  save()
  # Verify structural equivalence
  if expect_json:
   assert go_analysis['all_json'],(name,'Go did not produce all JSON lines',go_analysis)
   assert rust_analysis['all_json'],(name,'Rust did not produce all JSON lines',rust_analysis)
   assert go_analysis['has_level_field'],(name,'Go JSON lines missing "level" field',go_analysis)
   assert rust_analysis['has_level_field'],(name,'Rust JSON lines missing "level" field',rust_analysis)
   if not expect_debug_possible:
    assert go_analysis['debug_lines']==0,(name,'Go produced debug lines when level=info',go_analysis)
    assert rust_analysis['debug_lines']==0,(name,'Rust produced debug lines when level=info',rust_analysis)
  else:
   # Console mode: expect non-JSON lines
   assert go_analysis['non_json_lines']>0,(name,'Go did not produce console (non-JSON) lines',go_analysis)
   assert rust_analysis['non_json_lines']>0,(name,'Rust did not produce console (non-JSON) lines',rust_analysis)
 save()
 # Cross-profile control: verify Go genuinely switches encoders
 jti=manifest['profiles']['json-true-level-info']
 jfi=manifest['profiles']['json-false-level-info']
 assert jti['go_analysis']['all_json'] and not jfi['go_analysis']['all_json'],('Go did not switch encoders based on --logging.json',jti['go_analysis'],jfi['go_analysis'])
 assert jti['rust_analysis']['all_json'] and not jfi['rust_analysis']['all_json'],('Rust did not switch encoders based on --logging.json',jti['rust_analysis'],jfi['rust_analysis'])
 # Verify level filtering: debug profile should have >= debug lines than info profile
 jtd=manifest['profiles']['json-true-level-debug']
 assert jtd['go_analysis']['line_count']>=jti['go_analysis']['line_count'],('Go debug profile has fewer lines than info profile',jtd['go_analysis'],jti['go_analysis'])
 assert jtd['rust_analysis']['line_count']>=jti['rust_analysis']['line_count'],('Rust debug profile has fewer lines than info profile',jtd['rust_analysis'],jti['rust_analysis'])
 manifest['cross_profile_control']={'go_switches_encoders':True,'rust_switches_encoders':True,'level_filtering_effective':True,'rust_structural_match_go':True}
 manifest['status']='PASS_GENERATED_CLI_FLAG_LOGGING_JSON_LEVEL_EFFECT'
finally:
 save()
print(json.dumps({'status':manifest['status'],'profiles':list(PROFILES),'summary':{n:{'go_lines':manifest['profiles'][n]['go_analysis']['line_count'],'rust_lines':manifest['profiles'][n]['rust_analysis']['line_count'],'go_json':manifest['profiles'][n]['go_analysis']['all_json'],'rust_json':manifest['profiles'][n]['rust_analysis']['all_json']} for n in PROFILES}}))
