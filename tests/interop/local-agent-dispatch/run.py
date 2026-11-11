"""Coordinator-owned actual main/source capture and exact Rust comparison. NOT_RUN."""
from pathlib import Path
import argparse,json,hashlib,tempfile,socket,subprocess,os,time,signal,urllib.request,urllib.error,hmac,base64,secrets
p=argparse.ArgumentParser();p.add_argument('--binary',type=Path,required=True);p.add_argument('--result',type=Path,required=True);p.add_argument('--reference',type=Path);p.add_argument('--artifacts',type=Path,required=True);a=p.parse_args();here=Path(__file__).resolve().parent
KEY='APIownedLocalDispatch';SECRET='owned-local-dispatch-fixture-32-byte-secret';h=lambda b:hashlib.sha256(b).hexdigest();opener=urllib.request.build_opener(urllib.request.ProxyHandler({}))
def port(kind):
 with socket.socket(socket.AF_INET,kind)as s:s.bind(('127.0.0.1',0));return s.getsockname()[1]
def token(room):
 enc=lambda b:base64.urlsafe_b64encode(b).rstrip(b'=');head=enc(b'{"alg":"HS256","typ":"JWT"}');body=enc(json.dumps({'iss':KEY,'sub':'owned-admin','exp':int(time.time())+600,'video':{'roomCreate':True,'roomList':True,'roomAdmin':True,'room':room}},separators=(',',':')).encode());raw=head+b'.'+body;return(raw+b'.'+enc(hmac.new(SECRET.encode(),raw,hashlib.sha256).digest())).decode()
def request(url,body=None,auth=None,timeout=15):
 headers={};data=None
 if body is not None:data=json.dumps(body).encode();headers['content-type']='application/json'
 if auth:headers['authorization']='Bearer '+auth
 try:
  with opener.open(urllib.request.Request(url,data=data,headers=headers),timeout=timeout)as r:return r.status,r.read()
 except urllib.error.HTTPError as e:return e.code,e.read()
def listener(pid,n,kind):
 r=subprocess.run(['/usr/sbin/lsof','-a','-nP','-p',str(pid),'-i'+kind+':'+str(n)]+(['-sTCP:LISTEN']if kind=='TCP'else[]),capture_output=True,timeout=2);return r.returncode==0 and str(pid).encode()in r.stdout
assert a.binary.is_file()and not a.result.exists();a.artifacts.mkdir(mode=0o700,parents=True,exist_ok=False)
cases=json.loads((here/'cases.json').read_text());reference=json.loads(a.reference.read_text())if a.reference else None
if reference:assert reference['status']=='REFERENCE_CAPTURED'and reference['cases_sha256']==h((here/'cases.json').read_bytes())and not reference['cleanup_errors']
report={'status':'RUNNING','binary_sha256':h(a.binary.read_bytes()),'cases_sha256':h((here/'cases.json').read_bytes()),'observations':[],'cleanup_errors':[],'scope':'public actual main HTTP standalone AgentDispatch record/owner lifecycle; no registered worker/job assignment or cluster proof'}
try:
 for case in cases:
  http,rtc=port(socket.SOCK_STREAM),port(socket.SOCK_DGRAM);base=f'http://127.0.0.1:{http}';process=None;rooms=set();step='startup';obs={'id':case['id'],'startup_budget_s':30,'startup_stable_s':1,'health_timeout_s':2,'business_timeout_s':15,'sigint_budget_s':10,'contract':{}}
  with tempfile.TemporaryDirectory(prefix='owned-local-dispatch-')as tmp:
   tmp=Path(tmp);cfg=tmp/'main.yaml';log=tmp/'main.log'
   config=f'port: {http}\nbind_addresses: [127.0.0.1]\nkeys: {{{KEY}: {SECRET}}}\nrtc:\n  node_ip: 127.0.0.1\n  udp_port: {rtc}\n  tcp_port: 0\n  enable_loopback_candidate: true\nturn:\n  enabled: false\nroom:\n  auto_create: {str(case["auto_create"]).lower()}\n'+case['tail'];cfg.write_text(config);cfg.chmod(0o600);obs['config_sha256']=h(config.encode())
   def rpc(service,method,body,room):
    status,raw=request(base+'/twirp/livekit.'+service+'/'+method,body,token(room));return status,json.loads(raw)
   def ok(service,method,body,room):
    status,data=rpc(service,method,body,room);assert status==200,(service,method,status);return data
   def listroom(room):return ok('RoomService','ListRooms',{'names':[room]},room).get('rooms',[])
   def failure(status,data):
    assert status!=200 and isinstance(data.get('code'),str)and isinstance(data.get('msg'),str);return {'status':status,'code':data['code'],'msg':data['msg']}
   def record_fields(data,body):
    expected={k:v for k,v in body.items()if k!='room'}
    for k,v in expected.items():assert data.get(k)==v,(k,'actual returned business field mismatch')
    assert data.get('room')==body['room']and data.get('id');state=data.get('state',{});assert not state.get('jobs',[]);assert int(state.get('created_at',0))>0
    return {**expected,'room_matches':True,'id_nonempty':True,'created_at_positive':True,'actual_jobs':[]}
   try:
    env={k:v for k,v in os.environ.items()if not k.startswith(('LIVEKIT_','LIVECLOUD_','EGRESS_'))}
    with log.open('wb')as f:process=subprocess.Popen([str(a.binary.resolve()),'--config',str(cfg)],env=env,stdout=f,stderr=subprocess.STDOUT)
    deadline=time.monotonic()+30;stable=None
    while time.monotonic()<deadline:
     assert process.poll()is None,'main exited before ready'
     try:status,_=request(base+'/',timeout=2);ready=status==200 and listener(process.pid,http,'TCP')and listener(process.pid,rtc,'UDP')
     except Exception:ready=False
     now=time.monotonic()
     if ready:
      if stable is None:stable=now
      if now-stable>=1:break
     else:stable=None
     time.sleep(.05)
    else:raise AssertionError('original30s startup budget exhausted')
    obs['actual_pid_http_rtc_owned']=True
    step='missing-owner-and-order';missing='local-dispatch-missing-'+secrets.token_hex(6);rooms.add(missing)
    body={'room':missing,'agent_name':'controlled-agent','metadata':'real metadata π','restart_policy':'JRP_NEVER','deployment':'production-1','attributes':{'controlled':'actual-http','unicode':'東京'}}
    status,data=rpc('AgentDispatchService','CreateDispatch',{**body,'deployment':'bad_name'},missing)
    assert status==400 and data['code']=='invalid_argument'and data['msg']=="deployment contains reserved character '_'";assert not listroom(missing)
    obs['contract']['validation_before_creation']={'status':status,'code':data['code'],'msg':data['msg'],'room_absent':True}
    status,data=rpc('AgentDispatchService','CreateDispatch',body,missing)
    if case['auto_create']:
     assert status==200;obs['contract']['missing_owner']=record_fields(data,body);assert len(listroom(missing))==1
    else:
     obs['contract']['missing_owner']=failure(status,data);assert not listroom(missing)
    step='explicit-room-crud';room='local-dispatch-explicit-'+secrets.token_hex(6);rooms.add(room)
    info=ok('RoomService','CreateRoom',{'name':room,'metadata':'explicit-owner'},room);sid=info['sid'];assert sid
    body={**body,'room':room};first=ok('AgentDispatchService','CreateDispatch',body,room);first_fields=record_fields(first,body)
    second_body={**body,'metadata':'second full payload','attributes':{'second':'record','empty':''}};second=ok('AgentDispatchService','CreateDispatch',second_body,room);second_fields=record_fields(second,second_body);assert first['id']!=second['id']
    listed=ok('AgentDispatchService','ListDispatch',{'room':room},room).get('agent_dispatches',[]);assert len(listed)==3;assert sum(not x.get('agent_name')for x in listed)==1;assert {first['id'],second['id']}<={x['id']for x in listed}
    filtered=ok('AgentDispatchService','ListDispatch',{'room':room,'dispatch_id':first['id']},room).get('agent_dispatches',[]);assert filtered==[first]
    removed=ok('AgentDispatchService','DeleteDispatch',{'room':room,'dispatch_id':first['id']},room);assert removed['id']==first['id'];record_fields(removed,body)
    assert not ok('AgentDispatchService','ListDispatch',{'room':room,'dispatch_id':first['id']},room).get('agent_dispatches',[])
    status,data=rpc('AgentDispatchService','DeleteDispatch',{'room':room,'dispatch_id':first['id']},room);repeat_error=failure(status,data)
    assert listroom(room)[0]['sid']==sid
    obs['contract']['explicit_owner']={'first':first_fields,'second':second_fields,'default_records':1,'same_room_sid':True,'filtered_full_message':True,'removed_full_fields':True,'repeat_delete':repeat_error}
    step='delete-recreate-owner';ok('RoomService','DeleteRoom',{'room':room},room);assert not listroom(room)
    new=ok('RoomService','CreateRoom',{'name':room,'metadata':'replacement-owner'},room);assert new['sid']!=sid
    for old in [first['id'],second['id']]:assert not ok('AgentDispatchService','ListDispatch',{'room':room,'dispatch_id':old},room).get('agent_dispatches',[])
    status,data=rpc('AgentDispatchService','DeleteDispatch',{'room':room,'dispatch_id':second['id']},room);old_error=failure(status,data)
    replacement=ok('AgentDispatchService','CreateDispatch',body,room);record_fields(replacement,body);assert replacement['id']not in[first['id'],second['id']];assert listroom(room)[0]['sid']==new['sid']
    obs['contract']['owner_recreation']={'new_sid':True,'old_ids_absent':True,'old_delete':old_error,'new_record_fields':record_fields(replacement,body),'replacement_room_survives_old_id':True}
   except Exception as e:obs['failure']={'step':step,'type':type(e).__name__};raise
   finally:
    if process and process.poll()is None:
     for room in rooms:
      try:
       if listroom(room):ok('RoomService','DeleteRoom',{'room':room},room)
       assert not listroom(room)
      except Exception:report['cleanup_errors'].append(case['id']+':owned-room-cleanup')
     process.send_signal(signal.SIGINT)
     try:process.wait(timeout=10);obs['forced']=False
     except subprocess.TimeoutExpired:
      obs['forced']=True;report['cleanup_errors'].append(case['id']+':first-sigint-timeout');process.terminate()
      try:process.wait(timeout=5)
      except subprocess.TimeoutExpired:process.kill();process.wait(timeout=5);report['cleanup_errors'].append(case['id']+':owned-process-kill')
    obs['exit_code']=process.returncode if process else None;obs['log_sha256']=h(log.read_bytes())if log.exists()else None
    (a.artifacts/(case['id']+'-config.yaml')).write_bytes(cfg.read_bytes())
    if log.exists():(a.artifacts/(case['id']+'-main.log')).write_bytes(log.read_bytes())
    for f in a.artifacts.iterdir():f.chmod(0o600)
    report['observations'].append(obs)
    assert process is not None and process.returncode==0 and obs.get('forced')is False,'owned main must exit0 on first SIGINT'
  assert not report['cleanup_errors']
 report['contract']=[{'id':x['id'],'contract':x['contract'],'exit_code':x['exit_code'],'forced':x['forced'],'actual_pid_http_rtc_owned':x['actual_pid_http_rtc_owned']}for x in report['observations']]
 if reference:assert report['contract']==reference['contract'],'strict source-observed main business effects differ';report['status']='PASS'
 else:report['status']='REFERENCE_CAPTURED'
except Exception as e:report['status']='FAIL';report['failure']={'type':type(e).__name__,'message':str(e)if isinstance(e,AssertionError)else'owned fixture operation failed'}
a.result.write_text(json.dumps(report,indent=2,ensure_ascii=False)+'\n');a.result.chmod(0o600)
if report['status']=='FAIL':raise SystemExit(1)
