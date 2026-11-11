#!/usr/bin/env python3
"""Actual Go/Rust IOInfo and RedisStore cross-client oracle. No service mocks.
The coordinator starts dedicated Redis brokers and invokes this runner. It owns
only child oracle processes, synthetic store keys and its private temp directory.
"""
import argparse,base64,concurrent.futures,hashlib,json,os,pathlib,signal,socket,subprocess,tempfile,time,urllib.parse
HERE=pathlib.Path(__file__).resolve().parent
PROCESS_AUDIT=[]

def sha(data):return hashlib.sha256(data).hexdigest()
class Redis:
 def __init__(self,url):
  p=urllib.parse.urlparse(url)
  if p.scheme!='redis' or p.hostname not in ('127.0.0.1','localhost','::1'):raise RuntimeError('dedicated loopback Redis only')
  self.endpoint=(p.hostname,p.port or 6379);self.user=urllib.parse.unquote(p.username or '');self.password=urllib.parse.unquote(p.password or '');self.db=int(p.path.strip('/') or 0)
 def execute(self,*args):
  with socket.create_connection(self.endpoint,timeout=5) as s:
   f=s.makefile('rb')
   def command(args):
    raw=[x if isinstance(x,bytes) else str(x).encode() for x in args]
    s.sendall(b'*'+str(len(raw)).encode()+b'\r\n'+b''.join(b'$'+str(len(x)).encode()+b'\r\n'+x+b'\r\n' for x in raw))
    return self.read(f)
   if self.password:command(('AUTH',self.user,self.password) if self.user else ('AUTH',self.password))
   if self.db:command(('SELECT',self.db))
   return command(args)
 def read(self,f,depth=0):
  if depth>100:raise RuntimeError('Redis nesting bound')
  line=f.readline(1<<20)
  if not line.endswith(b'\r\n'):raise RuntimeError('Redis framing failure')
  kind,data=line[:1],line[1:-2]
  if kind==b'+':return data
  if kind==b'-':raise RuntimeError('actual Redis command error')
  if kind==b':':return int(data)
  n=int(data)
  if n==-1:return None
  if n<0 or n>64<<20:raise RuntimeError('Redis response bound')
  if kind==b'$':
   result=f.read(n)
   if len(result)!=n or f.read(2)!=b'\r\n':raise RuntimeError('Redis payload failure')
   return result
  if kind==b'*':return [self.read(f,depth+1) for _ in range(n)]
  raise RuntimeError('Redis response type')

def rows(path):return [json.loads(x) for x in path.read_text().splitlines() if x.strip()]
def communicate(binary,mode,url,inputs,timeout=40):
 env={**os.environ,'IO_REDIS_URL':url}
 # URLs/credentials remain in environment; never in command/evidence text.
 result=subprocess.run([binary,'--mode',mode],input=''.join(json.dumps(x,separators=(',',':'))+'\n' for x in inputs),text=True,stdout=subprocess.PIPE,stderr=subprocess.PIPE,env=env,timeout=timeout)
 PROCESS_AUDIT.append({'ordinal':len(PROCESS_AUDIT),'mode':mode,'actual_returncode':result.returncode,'input_rows':len(inputs),'stdout_sha256':sha(result.stdout.encode()),'stderr_sha256':sha(result.stderr.encode())})
 if result.returncode:raise RuntimeError('actual oracle process failed')
 output=[json.loads(x) for x in result.stdout.splitlines() if x.strip()]
 if len(output)!=len(inputs):raise RuntimeError('actual response count mismatch')
 if any(out.get('case')!=command.get('case') for out,command in zip(output,inputs)):raise RuntimeError('actual response case identity mismatch')
 return output

def normalize(go,url,output):
 good=[r for r in output if r['status']=='OK'];decoded=communicate(go,'decode',url,good) if good else []
 it=iter(decoded);result=[]
 for r in output:
  if r['status']=='OK':
   d=next(it);result.append({'case':r['case'],'status':'OK','proto_name':r['proto_name'],'known_json':d['known_json'],'unknown_tree':d['unknown_tree']})
  else:result.append({'case':r['case'],'status':'ERROR','message':r.get('message',''),'code':r.get('code','unknown')})
 return result

def differences(a,b,path='$'):
 if type(a)!=type(b):return [path]
 if isinstance(a,dict):
  result=[]
  for key in sorted(set(a)|set(b)):
   if key not in a or key not in b:result.append(path+'.'+key)
   else:result+=differences(a[key],b[key],path+'.'+key)
  return result
 if isinstance(a,list):
  if len(a)!=len(b):return [path+'.length']
  return sum((differences(x,y,path+'['+str(i)+']') for i,(x,y) in enumerate(zip(a,b))),[])
 return [] if a==b else [path]

def controlled_error(row):
 # Only the fixed 0xff state parser path: never serialize arbitrary server or
 # transport messages. Preserve exact Unicode (and hex) for source diagnosis.
 if row.get('case')!='fault-corrupt-ingress-state' or row.get('status')!='ERROR':raise RuntimeError('controlled error source invalid')
 message=row.get('message','')
 if len(message)>512 or any(marker in message for marker in ('redis://','rediss://','127.0.0.1','Bearer ','IN_F06_','EG_F06_')):raise RuntimeError('controlled error contains unexpected sensitive context')
 return {'case':row['case'],'code':row.get('code','unknown'),'message':message,'message_utf8_hex':message.encode().hex()}

CORRUPT_STATE_ERROR_FORMS=('proto: cannot parse invalid wire-format data','proto:\u00a0cannot parse invalid wire-format data')
def canonical_comparison_rows(name,values):
 # Exactly one source-proven upstream nondeterministic family, not whitespace
 # normalization. Every other property and every wrapped/suffixed error is exact.
 result=[];records=[]
 for index,row in enumerate(values):
  if name.endswith('-faults') and row.get('case')=='fault-corrupt-ingress-state' and row.get('status')=='ERROR' and row.get('message') in CORRUPT_STATE_ERROR_FORMS:
   result.append({**row,'message':CORRUPT_STATE_ERROR_FORMS[0]})
   records.append({'index':index,'family':'pinned-protobuf-invalid-wire-prefix','original_message':row['message'],'original_utf8_hex':row['message'].encode().hex(),'canonical_message':CORRUPT_STATE_ERROR_FORMS[0]})
  else:result.append(row)
 return result,records

def safe_comparison(name,expected,actual):
 canonical_expected,expected_records=canonical_comparison_rows(name,expected)
 canonical_actual,actual_records=canonical_comparison_rows(name,actual)
 diff=differences(canonical_expected,canonical_actual)
 result={'name':name,'equal':not diff,'observations':len(actual),'difference_paths':diff[:64],'expected_sha256':sha(json.dumps(expected,sort_keys=True,separators=(',',':')).encode()),'actual_sha256':sha(json.dumps(actual,sort_keys=True,separators=(',',':')).encode()),'actual_statuses':[{'case':r['case'],'status':r['status'],'code':r.get('code')} for r in actual]}
 if expected_records or actual_records:
  result['upstream_representation_canonicalization']={'contract':'SPEC-0018 exact corrupt-state invalid-wire family only','expected':expected_records,'actual':actual_records,'raw_equal':expected==actual}
 if name.endswith('-faults'):
  result['controlled_corrupt_state_errors']=[{'side':side,**controlled_error(row)} for side,values in [('expected',expected),('actual',actual)] for row in values if row.get('case')=='fault-corrupt-ingress-state']
 return result

def private_write(path,value):path.write_text(json.dumps(value,indent=2)+'\n');path.chmod(0o600)
def stop(child):
 if child.poll() is None:
  child.send_signal(signal.SIGINT)
  try:child.wait(timeout=20)
  except subprocess.TimeoutExpired:
   child.kill();child.wait(timeout=5);raise RuntimeError('owned server forced termination')
 if child.returncode!=0:raise RuntimeError('owned server nonzero exit')

def clean(redis):
 # Fixed synthetic names only. Never FLUSH or delete unexpected keys.
 for field in ('EG_F06_probe','EG_F06_unknown','EG_F06_fault'):
  redis.execute('HDEL','egress',field);redis.execute('HDEL','ended_egress',field)
  for room in ('F06_room_A','F06_room_U','F06_room_F'):redis.execute('SREM','egress:room:'+room,field)
 for field in ('IN_F06_probe','IN_F06_unknown'):
  redis.execute('HDEL','ingress',field);redis.execute('DEL','{ingress}_state:'+field)
  for room in ('F06_room_A','F06_room_B','F06_room_U'):redis.execute('SREM','room_{ingress}:'+room,field)
 redis.execute('HDEL','{ingress}_stream_key','public-synthetic-F06-stream','public-synthetic-F06-unknown')
 redis.execute('DEL','livekit_version')
 # Queue SETNX entries are actual five-second leases. Let them expire rather
 # than broadly deleting bus keys that cannot be attributed to one caller.
 until=time.monotonic()+7
 while redis.execute('DBSIZE') and time.monotonic()<until:time.sleep(.1)
 if redis.execute('DBSIZE'):raise RuntimeError('unexpected Redis keys remain; no unsafe deletion')

def varint(value):
 out=bytearray()
 while value>127:out.append((value&127)|128);value>>=7
 out.append(value);return bytes(out)
def extras(go,caller,reader,url,redis):
 def calls(inputs):return normalize(go,url,communicate(caller,'call',url,inputs))
 def encode(name,message):
  row=communicate(go,'encode',url,[{'case':'independent-generated-go-input','proto_name':name,'message':message}])[0]
  return base64.b64decode(row['wire_base64'])
 suffix=bytes.fromhex('b00603a00681001007')
 egress=encode('livekit.EgressInfo',{'egressId':'EG_F06_unknown','roomName':'F06_room_U'})+suffix
 ingress=encode('livekit.IngressInfo',{'ingressId':'IN_F06_unknown','streamKey':'public-synthetic-F06-unknown','roomName':'F06_room_U'})+suffix
 state=encode('livekit.IngressState',{'status':'ENDPOINT_PUBLISHING','startedAt':'600','updatedAt':'600'})+suffix
 request=encode('rpc.UpdateIngressStateRequest',{'ingressId':'IN_F06_unknown'})+b'\x12'+varint(len(state))+state
 unknown=calls([
  {'case':'unknown-egress-create','op':'CreateEgress','wire_base64':base64.b64encode(egress).decode()},
  {'case':'unknown-egress-get','op':'GetEgress','id':'EG_F06_unknown'},
  {'case':'unknown-ingress-create','op':'CreateIngress','wire_base64':base64.b64encode(ingress).decode()},
  {'case':'unknown-state-update','op':'UpdateIngressState','wire_base64':base64.b64encode(request).decode()},
  {'case':'unknown-ingress-get','op':'GetIngressInfo','message':{'ingressId':'IN_F06_unknown'}},
 ])
 unknown_store=normalize(go,url,communicate(reader,'store',url,[{'case':'unknown-egress-cross-store','op':'GetEgress','id':'EG_F06_unknown'},{'case':'unknown-ingress-cross-store','op':'GetIngress','id':'IN_F06_unknown'}]))
 retained=[node['unknown_hex'] for row in unknown+unknown_store for node in row.get('unknown_tree',[])]
 if not retained or any(value!=suffix.hex() for value in retained):raise RuntimeError('actual unknown value/order preservation failed')
 if any(row['status']!='OK' for row in unknown+unknown_store):raise RuntimeError('actual unknown callback/store failed')
 fault=[]
 def wrong_type(key,commands):
  dump=redis.execute('DUMP',key);ttl=redis.execute('PTTL',key)
  try:
   redis.execute('DEL',key);redis.execute('SET',key,'public-synthetic-wrongtype')
   fault.extend(calls(commands))
  finally:
   redis.execute('DEL',key)
   if dump is not None:redis.execute('RESTORE',key,max(ttl,0),dump)
 wrong_type('egress',[{'case':'fault-egress-get-wrongtype','op':'GetEgress','id':'EG_F06_probe'},{'case':'fault-egress-update-still-emits-event','op':'UpdateEgress','message':{'egressId':'EG_F06_fault','roomName':'F06_room_F','status':'EGRESS_ACTIVE'}}])
 wrong_type('ingress',[{'case':'fault-ingress-get-wrongtype','op':'GetIngressInfo','message':{'ingressId':'IN_F06_probe'}}])
 key='{ingress}_state:IN_F06_probe';previous=redis.execute('GET',key)
 try:
  redis.execute('SET',key,b'\xff')
  fault+=calls([{'case':'fault-corrupt-ingress-state','op':'GetIngressInfo','message':{'ingressId':'IN_F06_probe'}}])
 finally:
  if previous is not None:redis.execute('SET',key,previous)
  else:redis.execute('DEL',key)
 if any(row['status']!='ERROR' for row in fault):raise RuntimeError('actual seeded fault did not return an error')
 # Actual missing external state, not a fabricated Store return and not proof
 # that the deletion happened specifically between the service's two reads.
 redis.execute('DEL',key)
 missing=calls([{'case':'actual-missing-external-state-callback','op':'UpdateIngressState','message':{'ingressId':'IN_F06_probe','state':{'status':'ENDPOINT_BUFFERING','startedAt':'350','updatedAt':'350'}}},{'case':'actual-state-after-missing-key','op':'GetIngressInfo','message':{'ingressId':'IN_F06_probe'}}])
 if any(row['status']!='OK' for row in missing):raise RuntimeError('real missing-state callback failed')
 if missing[1]['known_json']['info']['state']['started_at']!='350':raise RuntimeError('real missing-state callback did not persist replacement')
 requests=[{'case':'concurrent-generation-'+str(g),'op':'UpdateIngressState','message':{'ingressId':'IN_F06_probe','state':{'status':'ENDPOINT_PUBLISHING','startedAt':str(g),'updatedAt':str(g)}}} for g in (400,500)]
 with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
  future=[pool.submit(calls,[request]) for request in requests]
  race=[item for f in future for item in f.result()]
 if race[1]['status']!='OK' or not (race[0]['status']=='OK' or race[0].get('code')=='failed_precondition'):raise RuntimeError('actual concurrent callback outcomes invalid')
 current=calls([{'case':'concurrent-final-state','op':'GetIngressInfo','message':{'ingressId':'IN_F06_probe'}}])
 if current[0]['status']!='OK' or current[0]['known_json']['info']['state']['started_at']!='500':raise RuntimeError('actual concurrent generation protection failed')
 return {'unknown':unknown+unknown_store,'faults':fault,'missing_state':missing,'race_results':race,'race_final':current,'unknown_nodes_preserved':len(retained)}

def main():
 ap=argparse.ArgumentParser();ap.add_argument('--go',required=True);ap.add_argument('--rust',required=True);ap.add_argument('--output',required=True);ap.add_argument('--dedicated-brokers',action='store_true');args=ap.parse_args()
 if not args.dedicated_brokers:raise RuntimeError('explicit dedicated brokers required')
 urls=[os.environ['IO_REFERENCE_REDIS_URL'],os.environ['IO_RUST_REDIS_URL']]
 report={'status':'RUNNING','scope':'actual IOInfo callbacks, Go/Rust Redis PSRPC and persisted stores; external media workers NOT_RUN','comparisons':[],'campaigns':[],'cleanup_errors':[],'progress':[]};stage='preflight';active=[];owned=[];children=[]
 with tempfile.TemporaryDirectory(prefix='livekit-rs-io-cross-') as directory:
  temp=pathlib.Path(directory)
  try:
   for url in urls:
    redis=Redis(url)
    if redis.execute('DBSIZE')!=0 or redis.execute('PUBSUB','CHANNELS','*IOInfo*'):raise RuntimeError('dedicated Redis preflight not empty')
   baseline=None
   profiles=[('go-to-go',args.go,args.go,args.go,urls[0]),('rust-to-rust',args.rust,args.rust,args.go,urls[1]),('go-to-rust',args.go,args.rust,args.rust,urls[1]),('rust-to-go',args.rust,args.go,args.rust,urls[0])]
   for label,caller,server,reader,url in profiles:
    stage=label;redis=Redis(url);ready=temp/(label+'.ready');events=temp/(label+'.events');log=open(temp/(label+'.log'),'wb');owned.append(log)
    child=subprocess.Popen([server,'--mode','serve','--ready',str(ready),'--events',str(events)],env={**os.environ,'IO_REDIS_URL':url},stdout=log,stderr=log);active.append(child);children.append((label,child))
    until=time.monotonic()+20
    while not ready.exists():
     if child.poll() is not None or time.monotonic()>until:raise RuntimeError('actual IOInfo ACK readiness failed')
     time.sleep(.02)
    if ready.read_text()!='ACK_READY\n':raise RuntimeError('invalid ACK marker')
    callback=normalize(args.go,url,communicate(caller,'call',url,rows(HERE/'callback-cases.jsonl')))
    report['progress'].append({'name':label,'phase':'callbacks','statuses':[{'case':r['case'],'status':r['status'],'code':r.get('code')} for r in callback]})
    expected_status=['ERROR' if i in (0,12,18) else 'OK' for i in range(21)]
    if [r['status'] for r in callback]!=expected_status:raise RuntimeError('source callback success/error expectations not satisfied')
    if callback[3]['known_json']['status']!='EGRESS_STARTING':raise RuntimeError('duplicate Egress Create overwrote actual state')
    for index,started,updated in [(16,'200','210'),(20,'300','300')]:
     actual_state=callback[index]['known_json']['info']['state']
     if actual_state['started_at']!=started or actual_state['updated_at']!=updated or actual_state['status']!='ENDPOINT_PUBLISHING':raise RuntimeError('actual ordered state generation mismatch')
    extra=extras(args.go,caller,reader,url,redis)
    store=normalize(args.go,url,communicate(reader,'store',url,rows(HERE/'store-cases.jsonl')))
    report['progress'].append({'name':label,'phase':'store','statuses':[{'case':r['case'],'status':r['status'],'code':r.get('code')} for r in store]})
    if [r['status'] for r in store]!=['ERROR' if i in (2,8) else 'OK' for i in range(9)]:raise RuntimeError('source cross-store success/error expectations not satisfied')
    stop(child);active.remove(child)
    actual_events=json.loads(events.read_text());campaign={'name':label,'ready_ack':True,'server_exit_zero':True,'callback_count':len(callback),'store_count':len(store),'callback_statuses':[r['status'] for r in callback],'events':actual_events,'unknown_nodes_preserved':extra['unknown_nodes_preserved'],'fault_error_count':len(extra['faults']),'actual_missing_state_callback':True,'concurrent_generation_500_retained':True,'race_results':[{'case':r['case'],'status':r['status'],'code':r.get('code')} for r in extra['race_results']],'race_scope':'real concurrent RPC delivery; WATCH collision not forced'};report['campaigns'].append(campaign)
    ordered_events=[e for e in actual_events if e['started_at'] not in (400,500)]
    if baseline is None:baseline=(callback,store,ordered_events,extra)
    for name,expected,actual in [('callbacks',baseline[0],callback),('store',baseline[1],store)]:report['comparisons'].append(safe_comparison(label+'-'+name,expected,actual))
    event_equal=baseline[2]==ordered_events;report['comparisons'].append({'name':label+'-ordered-events-excluding-declared-concurrent-phase','equal':event_equal,'observations':len(ordered_events),'difference_paths':differences(baseline[2],ordered_events)[:64]})
    for name in ('unknown','faults','missing_state','race_final'):report['comparisons'].append(safe_comparison(label+'-'+name,baseline[3][name],extra[name]))
    clean(redis)
   if not all(c['equal'] for c in report['comparisons']):raise RuntimeError('actual cross-client comparison mismatch')
   report['status']='PASS'
  except Exception:report['status']='FAIL';report['failure_stage']=stage
  finally:
   for child in active:
    try:stop(child)
    except Exception:report['cleanup_errors'].append('owned server cleanup failed')
   for url in urls:
    try:clean(Redis(url))
    except Exception:report['cleanup_errors'].append('dedicated owned state cleanup incomplete')
   for log in owned:log.close()
   if report['cleanup_errors']:report['status']='FAIL'
   report['oracle_processes']=PROCESS_AUDIT
   report['processes']=[{'name':label,'actual_returncode':child.poll()} for label,child in children]
   report['input_sha256']={p.name:sha(p.read_bytes()) for p in (HERE/'callback-cases.jsonl',HERE/'store-cases.jsonl',HERE/'main.go',pathlib.Path(__file__))}
   report['binary_sha256']={'go':sha(pathlib.Path(args.go).read_bytes()),'rust':sha(pathlib.Path(args.rust).read_bytes())}
   private_write(pathlib.Path(args.output),report)
 print(report['status']+' real IOInfo cross-client/store/events')
 return 0 if report['status']=='PASS' else 1
if __name__=='__main__':raise SystemExit(main())
