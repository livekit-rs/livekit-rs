#!/usr/bin/env python3
# Owned fixture metadata only; never a fake worker callback or successful job.
import argparse,base64,os,socket
class Redis:
 def __init__(self,host,port,db,password):
  self.sock=socket.create_connection((host,port),timeout=3);self.sock.settimeout(3);self.file=self.sock.makefile('rb')
  if password:self.call(b'AUTH',password.encode())
  self.call(b'SELECT',str(db).encode());assert self.call(b'PING')==b'PONG'
 def read(self):
  line=self.file.readline(1<<20);assert line.endswith(b'\r\n'),'bounded RESP line'
  tag,data=line[:1],line[1:-2]
  if tag==b'+':return data
  if tag==b'-':raise RuntimeError('owned Redis returned error: '+data.decode('utf8','replace'))
  if tag==b':':return int(data)
  if tag==b'$':
   n=int(data);assert 0<=n<=1<<20;v=self.file.read(n);assert len(v)==n and self.file.read(2)==b'\r\n';return v
  raise RuntimeError('unexpected RESP type')
 def call(self,*args):
  self.sock.sendall(b'*'+str(len(args)).encode()+b'\r\n'+b''.join(b'$'+str(len(v)).encode()+b'\r\n'+v+b'\r\n'for v in args));return self.read()
 def close(self):self.file.close();self.sock.close()

p=argparse.ArgumentParser();p.add_argument('--mode',choices=['write','cleanup'],required=True);p.add_argument('--port',type=int,required=True);p.add_argument('--db',type=int,default=2);p.add_argument('--egress-id',required=True);p.add_argument('--room',required=True);p.add_argument('--wire-base64');a=p.parse_args()
assert os.environ.get('LAYOUT_REDIS_ISOLATED')=='true','exclusive owned Redis only'
assert a.egress_id.startswith('EG_layout_')and a.room=='layout-preflight-owned'
c=Redis('127.0.0.1',a.port,a.db,os.environ.get('LAYOUT_REDIS_PASSWORD',''))
try:
 if a.mode=='write':
  raw=base64.b64decode(a.wire_base64,validate=True);assert len(raw)<1<<20
  c.call(b'HSET',b'egress',a.egress_id.encode(),raw);c.call(b'SADD',('egress:room:'+a.room).encode(),a.egress_id.encode())
 else:
  c.call(b'HDEL',b'egress',a.egress_id.encode());c.call(b'SREM',('egress:room:'+a.room).encode(),a.egress_id.encode())
finally:c.close()
