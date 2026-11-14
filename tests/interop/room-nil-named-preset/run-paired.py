#!/usr/bin/env python3
"""OBSERVATION paired driver for CONFIG:NilNamedPresetPointer. One fresh Go server and
one fresh Rust server share a YAML whose room.room_configurations contains a NIL entry
(nilpreset: ~), a VALID entry (validpreset) and no 'absent-preset' key. The unchanged
node consumer issues public RoomService.CreateRoom for three cases against both sides:
  unknown-preset : room_preset='absent-preset' -> parity control, both 400 identical msg
  valid-preset   : room_preset='validpreset'   -> parity control, both 200 inherited fields
  nil-preset     : room_preset='nilpreset'     -> the divergence: Go dereferences a nil
                   *RoomConfiguration (roomallocator.go:219) and crashes (no psrpc
                   recovery); Rust returns a clean 500 internal "Internal service panic"
                   (manager.rs:672-674, the standalone local-owner path MAIN uses).
This driver RECORDS the real Go outcome (it does not invent a panic): the Go server log
is captured verbatim and the Go process exit code is recorded whether or not it crashed.
Rust MUST survive (exit 0) and return a structured error. The nil case is an R03 recorded
divergence, not a parity claim; the two controls prove the preset map is truly consumed.
Retain failures, never count partials."""
import argparse, hashlib, json, os, pathlib, signal, socket, subprocess, time, urllib.request
import yaml

p=argparse.ArgumentParser()
p.add_argument('--go',required=True,type=pathlib.Path)
p.add_argument('--rust',required=True,type=pathlib.Path)
p.add_argument('--node',default='node')
p.add_argument('--output',required=True,type=pathlib.Path)
a=p.parse_args()
here=pathlib.Path(__file__).resolve().parent
a.output.mkdir(parents=True,exist_ok=False)
GO_PORT,RUST_PORT=19620,19630
OVERLAY={'room':{'empty_timeout':300,'departure_timeout':20,
  'room_configurations':{'nilpreset':None,
    'validpreset':{'empty_timeout':30,'departure_timeout':20,'max_participants':3,'metadata':'valid-preset-md'}}}}
def cfg(port,mt,mu):
 c={'port':port,'bind_addresses':['127.0.0.1'],
    'rtc':{'tcp_port':mt,'udp_port':mu,'node_ip':'127.0.0.1','use_external_ip':False,'enable_loopback_candidate':True,'ips':{'includes':['127.0.0.1/32']}},
    'keys':{'devkey':'secret'},'turn':{'enabled':False},'logging':{'level':'error'}}
 c.update(OVERLAY);return c
manifest={'status':'INCOMPLETE','item':'CONFIG:NilNamedPresetPointer',
 'scope':'Observable behavior of a NIL named-preset pointer (room_configurations.nilpreset=~) on public RoomService.CreateRoom, fixed Go vs Rust, with unknown-preset and valid-preset parity controls. Records the Go nil-deref outcome verbatim (connection abort or recovered 5xx) and the Rust clean internal error; R03 recorded divergence on the nil case, not a parity claim. Not sync_streams/agents internal fields, CLI per-key flag, ENV, WS join, media or webhook.',
 'binaries':{}}
for side in ['go','rust']:
 b=getattr(a,side).resolve();manifest['binaries'][side]={'path':str(b),'sha256':hashlib.sha256(b.read_bytes()).hexdigest()}
def save():(a.output/'execution.json').write_text(json.dumps(manifest,indent=2)+'\n')
save()
def free(port):
 with socket.socket() as s:return s.connect_ex(('127.0.0.1',port))!=0
def wait_ready(url,proc,deadline=40):
 end=time.monotonic()+deadline
 while True:
  assert proc.poll() is None,('server exited before ready',proc.returncode)
  try:
   with urllib.request.urlopen(url+'/',timeout=1) as r:
    if r.status==200:return
  except (OSError,TimeoutError):
   if time.monotonic()>=end:raise
  time.sleep(.1)
try:
 assert free(GO_PORT) and free(RUST_PORT),'occupied base ports'
 rec={'overlay':OVERLAY};manifest['profile']=rec
 servers={}
 for side,(port,mt,mu) in [('go',(GO_PORT,19621,19622)),('rust',(RUST_PORT,19631,19632))]:
  path=a.output/(f'{side}.yaml');path.write_text(yaml.safe_dump(cfg(port,mt,mu)))
  rec[side+'_config_sha256']=hashlib.sha256(path.read_bytes()).hexdigest()
  log=(a.output/(f'{side}.server.log')).open('wb')
  proc=subprocess.Popen([str(getattr(a,side).resolve()),'--dev','--config',str(path)],stdout=log,stderr=subprocess.STDOUT)
  servers[side]=(proc,log);rec[side+'_pid']=proc.pid
 save()
 try:
  for side,port in [('go',GO_PORT),('rust',RUST_PORT)]:wait_ready(f'http://127.0.0.1:{port}',servers[side][0])
  env=dict(os.environ,REFERENCE_URL=f'http://127.0.0.1:{GO_PORT}',LIVEKIT_URL=f'http://127.0.0.1:{RUST_PORT}',
   NP_OUTPUT=str((a.output/'nil-named-preset.pair.json').resolve()))
  with (a.output/'runner.log').open('wb') as out:
   r=subprocess.run([a.node,str(here/'run.mjs')],env=env,stdout=out,stderr=subprocess.STDOUT,timeout=120)
  rec['runner_exit']=r.returncode
 finally:
  for side,(proc,log) in servers.items():
   rec[side+'_alive_after_run']=proc.poll() is None
   if proc.poll() is None:proc.send_signal(signal.SIGINT)
   try:rec[side+'_exit']=proc.wait(timeout=30)
   except subprocess.TimeoutExpired:proc.kill();rec[side+'_exit']=proc.wait();rec[side+'_forced_kill']=True
   log.close()
 save()
 # Rust MUST survive a nil preset (clean error, no crash). Go's exit is RECORDED, not
 # asserted: an unrecovered nil-deref panic may abort the connection (process lives) or
 # crash it; either is the observable upstream behavior this item documents.
 assert rec.get('rust_exit')==0,('Rust must survive a nil preset',rec)
 pair=json.loads((a.output/'nil-named-preset.pair.json').read_text())
 rec['pair_status']=pair['status'];rec['observations']=pair.get('observations');rec['nil_preset_divergence']=pair.get('nil_preset_divergence')
 assert pair['status']=='PASS_NIL_NAMED_PRESET_OBSERVED',('observation failed',pair.get('error'))
 assert rec['runner_exit']==0,('runner failed; see runner.log',rec['runner_exit'])
 # Capture the Go server log verbatim (panic stack / recovery / internal-error line).
 golog=(a.output/'go.server.log').read_text(errors='replace')
 (a.output/'go.server.log.txt').write_text(golog)
 rec['go_server_log_bytes']=len(golog.encode())
 rec['go_log_panic_markers']=[ln for ln in golog.splitlines() if 'panic' in ln.lower() or 'nil pointer' in ln.lower() or 'Caught server panic' in ln or 'invalid memory address' in ln.lower()][:40]
 manifest['cross_control']={'unknown_preset':'400 parity','valid_preset':'200 parity','nil_preset':'recorded divergence (Go deref vs Rust clean internal error)'}
 manifest['status']='PASS_NIL_NAMED_PRESET_OBSERVED'
finally:
 save()
print(json.dumps({'status':manifest['status'],'nil_divergence':manifest.get('profile',{}).get('nil_preset_divergence'),'go_exit':manifest.get('profile',{}).get('go_exit'),'rust_exit':manifest.get('profile',{}).get('rust_exit')}))