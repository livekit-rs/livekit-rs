#!/usr/bin/env python3
"""Exact real Redis -> actual CLI process comparison. Starts no server/broker.
Requires a coordinator-owned exclusive Redis and independently actual Go decoder rows.
"""
import argparse,base64,hashlib,json,os,pathlib,socket,subprocess,tempfile,signal,sys

def sha(p):return hashlib.sha256(pathlib.Path(p).read_bytes()).hexdigest()
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
def invoke(binary,argv,env):
 p=subprocess.Popen([str(binary),*argv],stdout=subprocess.PIPE,stderr=subprocess.PIPE,env=env,start_new_session=True)
 try:o,e=p.communicate(timeout=8);timeout=False
 except subprocess.TimeoutExpired:os.killpg(p.pid,signal.SIGKILL);o,e=p.communicate();timeout=True
 assert len(o)<=1<<20 and len(e)<=1<<20
 return {'exit':p.returncode,'timed_out':timeout,'stdout_b64':base64.b64encode(o).decode(),'stderr_b64':base64.b64encode(e).decode()}
def main():
 p=argparse.ArgumentParser();p.add_argument('--official',type=pathlib.Path,required=True);p.add_argument('--rust',type=pathlib.Path,required=True);p.add_argument('--source-results',type=pathlib.Path,required=True);p.add_argument('--redis-port',type=int,required=True);p.add_argument('--redis-db',type=int,default=2);p.add_argument('--owned-isolated-redis',action='store_true');p.add_argument('--cases',type=pathlib.Path,default=pathlib.Path(__file__).resolve().parents[2]/'oracles/list-nodes-wire/cases.jsonl');p.add_argument('--output',type=pathlib.Path,required=True);a=p.parse_args();assert a.owned_isolated_redis,'exclusive owned broker required; never point at application Redis';assert 0<=a.redis_db<16
 cases=[json.loads(x)for x in a.cases.read_text().splitlines()];source={}
 for line in a.source_results.read_text().splitlines():
  row=json.loads(line);assert row['id']not in source;source[row['id']]=row
 assert len(cases)==16 and len(source)==16 and {c['id']for c in cases}==set(source)
 assert source['known-id-wrong-varint-after']['accepted']is True,'actual source must demonstrate the legal wrong-wire positive challenge'
 env={k:os.environ[k]for k in ['PATH','HOME','TMPDIR']if k in os.environ};env.update({'TZ':'UTC','LANG':'C'})
 report={'status':'RUNNING','scope':'exact16 stored Node protobuf cases; real HVALS and actual list-nodes process; no runtime node/stats/cluster claim','cases_sha256':sha(a.cases),'source_actual_sha256':sha(a.source_results),'binary_sha256':{'official':sha(a.official),'rust':sha(a.rust)},'normalization':'NONE','cases':[]}
 a.output.parent.mkdir(parents=True,exist_ok=True)
 def save():a.output.write_text(json.dumps(report,indent=2)+'\n')
 save();conn=None
 try:
  password=os.environ.get('NODE_WIRE_REDIS_PASSWORD','')
  conn=Redis('127.0.0.1',a.redis_port,a.redis_db,password)
  with tempfile.TemporaryDirectory(prefix='list-nodes-owned-')as tmp:
   config=pathlib.Path(tmp)/'config.yaml';config.write_text('keys: {}\nlogging: {level: error}\nrtc: {node_ip: 127.0.0.1, use_external_ip: false}\nredis:\n  address: '+json.dumps('127.0.0.1:'+str(a.redis_port))+'\n  db: '+str(a.redis_db)+'\n  password: '+json.dumps(password)+'\n');config.chmod(0o600)
   versions={name:invoke(binary,['--version'],env)for name,binary in [('official',a.official),('rust',a.rust)]};report['version_observations']=versions;assert base64.b64decode(versions['official']['stdout_b64'])==b'livekit-server version 1.13.10\n' and versions['official']['exit']==0 and not versions['official']['timed_out'],'wrong pinned official binary'
   for c in cases:
    conn.call(b'DEL',b'nodes');assert conn.call(b'HSET',b'nodes',b'ND_owned_fixture',bytes.fromhex(c['hex']))==1
    official=invoke(a.official,['--config',str(config),'list-nodes'],env);rust=invoke(a.rust,['--config',str(config),'list-nodes'],env)
    accepted=source[c['id']]['accepted'];same=official==rust;healthy=(official['exit']==0 and not official['timed_out'])if accepted else(official['exit']!=0 and not official['timed_out'])
    if accepted:
     observed_stdout=base64.b64decode(official['stdout_b64'])
     # Positive setup proof only; full stdout/stderr/exit comparison above stays exact.
     # Every accepted source vector retains region='owned', even when ID is empty.
     assert source[c['id']]['known_json'].get('region')=='owned'
     seeded_row=any(cell.strip(b' ')==b'owned' for line in observed_stdout.splitlines() for cell in line.split('│'.encode('utf8')))
     healthy=healthy and b'IP Address'in observed_stdout and seeded_row
    report['cases'].append({'id':c['id'],'actual_go_proto_accepted':accepted,'source_cli_admission_matches':healthy,'exact_equal':same,'official':official,'rust':rust});save()
   assert len(report['cases'])==16
   report['status']='PASS_SCOPED'if all(x['exact_equal']and x['source_cli_admission_matches']for x in report['cases'])else'FAIL'
 finally:
  if conn:
   try:conn.call(b'DEL',b'nodes')
   finally:conn.close()
  if report['status']=='RUNNING':report['status']='FAIL'
  save()
 if report['status']!='PASS_SCOPED':raise SystemExit(1)
if __name__=='__main__':main()
