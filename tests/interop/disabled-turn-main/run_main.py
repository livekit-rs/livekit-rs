"""Owned fixed-Go/Rust main-process consumer; author has NOT RUN this fixture."""
from pathlib import Path
import argparse,json,hashlib,tempfile,socket,subprocess,os,time,signal,urllib.request,urllib.error,hmac,base64,secrets
p=argparse.ArgumentParser();p.add_argument('--binary',type=Path,required=True);p.add_argument('--result',type=Path,required=True);p.add_argument('--reference',type=Path);p.add_argument('--artifacts',type=Path,required=True);a=p.parse_args();d=Path(__file__).parent
KEY='APIownedDisabledTurn';SECRET='owned-disabled-turn-not-production-32-byte-secret';h=lambda b:hashlib.sha256(b).hexdigest()
def port(kind):
 with socket.socket(socket.AF_INET,kind)as s:s.bind(('127.0.0.1',0));return s.getsockname()[1]
def token(room):
 enc=lambda b:base64.urlsafe_b64encode(b).rstrip(b'=');head=enc(b'{"alg":"HS256","typ":"JWT"}');body=enc(json.dumps({'iss':KEY,'sub':'owned-admin','exp':int(time.time())+600,'video':{'roomCreate':True,'roomList':True,'roomAdmin':True,'room':room}},separators=(',',':')).encode());part=head+b'.'+body;return(part+b'.'+enc(hmac.new(SECRET.encode(),part,'sha256').digest())).decode()
def request(url,body=None,auth=None):
 headers={};data=None
 if body is not None:data=json.dumps(body).encode();headers['content-type']='application/json'
 if auth:headers['authorization']='Bearer '+auth
 try:
  with urllib.request.urlopen(urllib.request.Request(url,data=data,headers=headers),timeout=2)as r:return r.status,r.read()
 except urllib.error.HTTPError as e:return e.code,e.read()
def listener(pid,port,kind):
 r=subprocess.run(['/usr/sbin/lsof','-a','-nP','-p',str(pid),'-i'+kind+':'+str(port)]+(['-sTCP:LISTEN']if kind=='TCP'else[]),capture_output=True,timeout=2);return r.returncode==0 and str(pid).encode()in r.stdout
a.artifacts.mkdir(mode=0o700,parents=True,exist_ok=False)
report={'status':'RUNNING','binary_sha256':h(a.binary.read_bytes()),'cases_sha256':h((d/'source_cases.json').read_bytes()),'observations':[],'cleanup_errors':[]}
try:
 for case in json.loads((d/'source_cases.json').read_text()):
  http,rtc=port(socket.SOCK_STREAM),port(socket.SOCK_DGRAM);room='disabled-turn-'+secrets.token_hex(6);base=f'http://127.0.0.1:{http}';process=None
  with tempfile.TemporaryDirectory(prefix='owned-disabled-turn-')as temp:
   temp=Path(temp);cfg=temp/'config.yaml';log=temp/'main.log';config=f'port: {http}\nbind_addresses: [127.0.0.1]\nkeys: {{{KEY}: {SECRET}}}\nrtc:\n  node_ip: 127.0.0.1\n  udp_port: {rtc}\n  tcp_port: 0\n  enable_loopback_candidate: true\nturn:\n  enabled: false\n'+case['tail'];cfg.write_text(config);cfg.chmod(0o600)
   observed={'id':case['id'],'config_sha256':h(config.encode()),'http_timeout_s':2,'startup_budget_s':30,'sigint_budget_s':10};created=False
   try:
    env={k:v for k,v in os.environ.items()if not k.startswith(('LIVEKIT_','LIVECLOUD_'))}
    with log.open('wb')as f:process=subprocess.Popen([str(a.binary.resolve()),'--config',str(cfg)],env=env,stdout=f,stderr=subprocess.STDOUT)
    deadline=time.monotonic()+30;stable=None
    while time.monotonic()<deadline:
     assert process.poll()is None,'main exited before ready'
     try:status,_=request(base+'/');ready=status==200 and listener(process.pid,http,'TCP') and listener(process.pid,rtc,'UDP')
     except(Exception):ready=False
     now=time.monotonic()
     if ready:
      if stable is None:stable=now
      if now-stable>=1:break
     else:stable=None
     time.sleep(.05)
    else:raise AssertionError('original30s startup budget exhausted')
    observed['actual_pid_http_rtc_owned']=True;observed['stable_health_s']=time.monotonic()-stable
    auth=token(room);endpoint=base+'/twirp/livekit.RoomService/'
    status,body=request(endpoint+'CreateRoom',{'name':room,'metadata':'owned-disabled-turn-positive'},auth);assert status==200,(status,body[:120]);created=True;created_info=json.loads(body);sid=created_info['sid'];assert sid and created_info['name']==room and created_info['metadata']=='owned-disabled-turn-positive'
    status,body=request(endpoint+'ListRooms',{'names':[room]},auth);assert status==200;rooms=json.loads(body)['rooms'];assert len(rooms)==1 and rooms[0]['sid']==sid and rooms[0]['metadata']=='owned-disabled-turn-positive'
    status,_=request(endpoint+'DeleteRoom',{'room':room},auth);assert status==200;created=False
    status,body=request(endpoint+'ListRooms',{'names':[room]},auth);assert status==200 and not json.loads(body).get('rooms',[])
    observed['contract']={'health':200,'create':200,'same_sid_metadata':True,'delete':200,'absent_after_delete':True}
   except Exception as error:
    observed['failure']={'type':type(error).__name__,'message':str(error)}
    raise
   finally:
    if created and process and process.poll()is None:
     try:assert request(base+'/twirp/livekit.RoomService/DeleteRoom',{'room':room},token(room))[0]==200
     except Exception:report['cleanup_errors'].append('delete-owned-room')
    if process and process.poll()is None:
     process.send_signal(signal.SIGINT)
     try:process.wait(timeout=10);observed['forced']=False
     except subprocess.TimeoutExpired:
      observed['forced']=True;report['cleanup_errors'].append('first-sigint-timeout');process.terminate()
      try:process.wait(timeout=5)
      except subprocess.TimeoutExpired:process.kill();process.wait(timeout=5);report['cleanup_errors'].append('owned-process-kill')
    observed['exit_code']=process.returncode if process else None;observed['log_sha256']=h(log.read_bytes()) if log.exists() else None
    (a.artifacts/(str(case['id'])+'-config.yaml')).write_bytes(cfg.read_bytes())
    if log.exists():(a.artifacts/(str(case['id'])+'-main.log')).write_bytes(log.read_bytes())
    for owned in a.artifacts.iterdir():owned.chmod(0o600)
    report['observations'].append(observed)
    assert process is not None and process.returncode==0 and observed.get('forced')is False,'owned main must exit cleanly first SIGINT'
 assert not report['cleanup_errors']
 contract=[{'id':r['id'],**r['contract'],'exit_code':r['exit_code'],'forced':r['forced']}for r in report['observations']]
 report['contract']=contract
 if a.reference:
  ref=json.loads(a.reference.read_text());assert ref['status']=='REFERENCE_CAPTURED';assert ref['contract']==contract;report['status']='PASS'
 else:report['status']='REFERENCE_CAPTURED'
except Exception as e:report['status']='FAIL';report['failure']={'type':type(e).__name__,'message':str(e)}
a.result.write_text(json.dumps(report,indent=2)+'\n');a.result.chmod(0o600)
if report['status']=='FAIL':raise SystemExit(1)
