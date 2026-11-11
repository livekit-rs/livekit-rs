"""Owned transparent Redis wire fault, never production or success emulation.
Gate actual first Join response stream for one controlled room; preserve all
other RESP bytes verbatim, including stream setup/ACK/close and registry.
"""
import socket,threading,hashlib,gzip,json,time,io

def fields(raw):
 out={};i=0
 def varint():
  nonlocal i
  n=0;shift=0
  while i<len(raw) and shift<70:
   b=raw[i];i+=1;n|=(b&127)<<shift
   if b<128:return n
   shift+=7
  raise ValueError('invalid bounded protobuf varint')
 while i<len(raw):
  key=varint();tag,wire=key>>3,key&7
  if not tag:raise ValueError('zero protobuf tag')
  if wire==0:value=varint()
  elif wire==1:value=raw[i:i+8];i+=8
  elif wire==2:
   n=varint();assert n<=4*1024*1024 and i+n<=len(raw);value=raw[i:i+n];i+=n
  elif wire==5:value=raw[i:i+4];i+=4
  else:raise ValueError('unsupported protobuf wire')
  out.setdefault(tag,[]).append(value)
 return out

def one(f,n,default=b''):return f.get(n,[default])[-1]
def stream_info(payload):
 f=fields(payload)
 if one(f,1).rsplit(b'/',1)[-1]!=b'internal.Stream':return None
 raw=one(f,2);compression=one(f,4,0)
 if compression==1:
  with gzip.GzipFile(fileobj=io.BytesIO(raw)) as zipped:raw=zipped.read(4*1024*1024+1)
  assert len(raw)<=4*1024*1024
 elif compression!=0:raise ValueError('unknown real compression')
 stream=fields(raw);result={'stream_id':one(stream,1).decode(),'request_id':one(stream,2).decode(),'body':'other','payload_sha256':hashlib.sha256(payload).hexdigest()}
 if 9 in stream:result['body']='close';return result
 if 7 not in stream:return result
 message=fields(one(stream,7));any_message=fields(one(message,1)) if 1 in message else {}
 data=one(message,2) if 2 in message else one(any_message,2)
 result['body']='message';business=fields(data)
 if 1 in business:
  try:
   start=fields(one(business,1));room=one(start,1).decode();connection=one(start,3).decode()
   if room and connection.startswith('CO_'):result.update(kind='start',room=room,connection_id=connection)
  except (ValueError,UnicodeDecodeError,AssertionError):pass
 for response in business.get(2,[]):
  try:
   signal=fields(response)
   if 1 not in signal:continue
   join=fields(one(signal,1));room=fields(one(join,1));name=one(room,2).decode();sid=one(room,1).decode()
   if name and sid.startswith('RM_'):result.update(kind='join',room=name,room_sid=sid)
  except (ValueError,UnicodeDecodeError,AssertionError):pass
 return result

def read_resp(file):
 line=file.readline(4*1024*1024+1)
 if not line:raise EOFError()
 assert len(line)<=4*1024*1024 and line.endswith(b'\r\n'),'RESP framing'
 kind,value=line[:1],line[1:-2];raw=line
 if kind in [b'+',b'-',b':',b',',b'(',b'#',b'_']:return value,raw
 if kind in [b'$',b'!',b'=']:
  size=int(value)
  if size<0:return None,raw
  assert size<=4*1024*1024
  data=file.read(size+2);assert len(data)==size+2 and data[-2:]==b'\r\n';return data[:-2],raw+data
 if kind in [b'*',b'>',b'~',b'%',b'|']:
  size=int(value)
  if size<0:return None,raw
  assert size<=65536
  vals=[]
  for _ in range(size*(2 if kind in [b'%',b'|']else 1)):
   item,encoded=read_resp(file);vals.append(item);raw+=encoded
  return vals,raw
 raise ValueError('unsupported RESP framing '+repr(kind))

class RedisSignalGate:
 def __init__(self,port,upstream,control):
  self.port=port;self.upstream=upstream;self.control=control;self.stop=threading.Event();self.lock=threading.Lock();self.rows=[];self.sockets=[];self.threads=[];self.blocked=None;self.errors=[]
  self.listener=socket.socket();self.listener.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1);self.listener.bind(('127.0.0.1',port));self.listener.listen();self.listener.settimeout(.2)
  self.acceptor=threading.Thread(target=self.accept,daemon=True);self.acceptor.start()
 def record(self,row):
  with self.lock:self.rows.append({'monotonic':time.monotonic(),**row})
 def accept(self):
  while not self.stop.is_set():
   try:client,_=self.listener.accept()
   except socket.timeout:continue
   except OSError:break
   try:up=socket.create_connection(('127.0.0.1',self.upstream),timeout=3);up.settimeout(None)
   except OSError as error:self.errors.append(str(error));client.close();continue
   with self.lock:self.sockets.extend([client,up])
   for source,target,direction in [(client,up,'request'),(up,client,'response')]:
    thread=threading.Thread(target=self.pipe,args=(source,target,direction),daemon=True);self.threads.append(thread);thread.start()
 def target_room(self):
  try:return json.loads(self.control.read_text()).get('room')
  except (OSError,ValueError):return None
 def fault_enabled(self):
  try:return json.loads(self.control.read_text()).get('first_join_stream_fault',True)is True
  except (OSError,ValueError):return False
 def inspect(self,value,direction):
  payload=None;channel=None
  if isinstance(value,list) and value:
   command=value[0].lower()if isinstance(value[0],bytes)else b''
   if direction=='request'and command in [b'publish',b'spublish']and len(value)>=3:channel,payload=value[1:3]
   if direction=='response'and command in [b'message',b'smessage']and len(value)>=3:channel,payload=value[1:3]
   if direction=='response'and command==b'pmessage'and len(value)>=4:channel,payload=value[2:4]
  if not isinstance(channel,bytes)or b'Signal'not in channel:return False
  if not isinstance(payload,bytes):return False
  info=stream_info(payload)
  if info is None:return False
  target=self.target_room()
  if info.get('room')==target and info.get('kind')in ['join','start']:self.record({'direction':direction,**info})
  if direction=='response':
   with self.lock:
    if self.blocked is None and target and self.fault_enabled()and info.get('kind')=='join'and info.get('room')==target:
     self.blocked=info['stream_id'];self.rows.append({'monotonic':time.monotonic(),'action':'gate-first-actual-join-stream','direction':direction,**info})
    blocked=self.blocked
   if blocked==info['stream_id'] and info['body']=='message':
    self.record({'action':'discard-blocked-stream-application-response',**info});return True
   if blocked==info['stream_id'] and info['body']=='close':self.record({'action':'actual-blocked-stream-close-forwarded',**info})
  return False
 def pipe(self,source,target,direction):
  file=source.makefile('rb')
  try:
   while not self.stop.is_set():
    value,raw=read_resp(file)
    if not self.inspect(value,direction):target.sendall(raw)
  except (EOFError,ConnectionError,OSError):pass
  except BaseException as error:self.errors.append(type(error).__name__+': '+str(error)[:300])
  finally:
   file.close()
   try:target.shutdown(socket.SHUT_WR)
   except OSError:pass
 def snapshot(self):
  with self.lock:return {'fault':'discard application responses only on first actual Join stream; no fabricated replies','blocked_stream':self.blocked,'rows':list(self.rows),'errors':list(self.errors)}
 def close(self):
  self.stop.set();self.listener.close();self.acceptor.join(3)
  with self.lock:sockets=list(self.sockets)
  for sock in sockets:
   try:sock.shutdown(socket.SHUT_RDWR)
   except OSError:pass
   sock.close()
  for thread in self.threads:thread.join(3)
  assert not self.acceptor.is_alive()and not any(t.is_alive()for t in self.threads),'owned proxy thread retained'
