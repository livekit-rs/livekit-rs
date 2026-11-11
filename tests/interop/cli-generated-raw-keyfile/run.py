#!/usr/bin/env python3
"""Root-only actual binary/SDK acceptance. Does not execute when imported."""
import argparse,ast,hashlib,json,os,pathlib,platform,signal,socket,subprocess,tempfile,threading,time
KEY='APIrawowned'
SECRETS=['owned-only-selected-first-file-secret-more-than32','owned-only-selected-second-file-secret-more-than32']

def sha(path):
 h=hashlib.sha256()
 with open(path,'rb') as f:
  for block in iter(lambda:f.read(131072),b''):h.update(block)
 return h.hexdigest()
def port(kind):
 with socket.socket(socket.AF_INET,kind) as sock:sock.bind(('127.0.0.1',0));return sock.getsockname()[1]
def private_file(path,body):
 fd=os.open(path,os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o600)
 with os.fdopen(fd,'wb') as f:f.write(body)
 assert os.stat(path).st_mode&0o777==0o600
class Drain:
 def __init__(self,pipe):
  self.count=0;self.hash=hashlib.sha256();self.over=False;self.private=bytearray();self.thread=threading.Thread(target=self.read,args=(pipe,),daemon=True);self.thread.start()
 def read(self,pipe):
  with pipe:
   for block in iter(lambda:pipe.read(8192),b''):
    self.count+=len(block);self.hash.update(block);self.over|=self.count>1048576
    if not self.over:self.private.extend(block)
 def result(self):
  self.thread.join(2);return {'bytes':self.count,'sha256':self.hash.hexdigest(),'overflow':self.over,'drain_finished':not self.thread.is_alive()}
def scenarios(paths):
 raw0=b'~/../absent-\xfe/../first.yaml';raw1=b'~/../absent-\xfd/../second.yaml'
 return [
  ('generated-cli-cleaned-raw', [b'--key_file',raw0],{},None,0),
  ('generated-env-cleaned-raw', [],{b'LIVEKIT_KEY_FILE':raw0},None,0),
  ('generated-cli-over-raw-env', [b'--key_file',raw0],{b'LIVEKIT_KEY_FILE':raw1},None,0),
  ('generated-unicode-cli-masks-raw-env',[b'--key_file',paths[1]],{b'LIVEKIT_KEY_FILE':raw0},None,1),
  ('generated-repeated-last-raw',[b'--key_file',paths[1],b'--key_file',raw0],{},None,0),
  ('generated-repeated-last-unicode',[b'--key_file',raw0,b'--key_file',paths[1]],{},None,1),
  ('base-unicode-over-generated-raw',[b'--key_file',raw0,b'--key-file',paths[1]],{},None,1),
  ('base-raw-over-generated-raw',[b'--key_file',raw0,b'--key-file',raw1],{},None,1),
  ('base-empty-suppresses-generated-raw',[b'--key_file',raw0,b'--key-file='],{},None,None),
  ('generated-raw-over-yaml',[b'--key_file',raw0],{},1,0),
  ('base-empty-suppresses-generated-env',[b'--key-file='],{b'LIVEKIT_KEY_FILE':raw0},None,None),
  ('generated-cli-empty-masks-raw-env',[b'--key_file='],{b'LIVEKIT_KEY_FILE':raw0},None,None),
  ('generated-env-empty-clears-yaml',[],{b'LIVEKIT_KEY_FILE':b''},1,None),
 ]
def main():
 a=argparse.ArgumentParser();a.add_argument('--official',required=True);a.add_argument('--rust',required=True);a.add_argument('--node',required=True);a.add_argument('--interop-package',type=pathlib.Path,required=True);a.add_argument('--out',type=pathlib.Path,required=True);a.add_argument('--case',action='append');v=a.parse_args()
 assert platform.system() in ['Darwin','Linux'],'explicit POSIX platform only'
 package=json.loads(v.interop_package.read_text());assert package['dependencies']['livekit-server-sdk']=='2.19.2'
 report={'status':'RUNNING','scope':'new generated-key_file Run actual file read and unchanged official ServerSDK JWT/auth/CRUD only; not original POSIX32/37/initialized37','platform':platform.system(),'binary_sha256':{'official':sha(v.official),'rust':sha(v.rust)},'rows':[],'cleanup_errors':[],'precondition':'actual config keys:{} supplies zero fallback credentials; only selected mode0600 ASCII file has key secret; raw byte components must be removed by actual home Join/Clean before file read'}
 script=pathlib.Path(__file__).with_name('sdk-auth.mjs');base={k:val for k,val in os.environb.items() if not k.startswith((b'LIVEKIT_',b'LIVECLOUD_'))}
 identity_probe=subprocess.run([v.node,str(script),'--identity-only'],input=json.dumps({'interop_package':str(v.interop_package.resolve())}).encode(),stdout=subprocess.PIPE,stderr=subprocess.PIPE,timeout=35)
 assert identity_probe.returncode==0 and len(identity_probe.stdout)<65536 and len(identity_probe.stderr)<65536,'actual SDK/Node identity preflight required'
 identity=json.loads(identity_probe.stdout);assert identity['status']=='PASS_IDENTITY_ONLY' and identity['sdk_version']=='2.19.2'
 report['sdk_identity']=identity['sdk_identity'];report['identity_probe_exit']=identity_probe.returncode
 report['identity_probe_scope']='Actual resolver/import and package/entry/lock/Node identity; no API or credentials in preflight; negative no-key cases do not claim SDK calls.'
 try:
  with tempfile.TemporaryDirectory(prefix='raw-generated-keyfile-addon-') as temp:
   cwd=os.fsencode(temp);paths=[cwd+b'/first.yaml',cwd+b'/second.yaml']
   for path,secret in zip(paths,SECRETS):private_file(path,(KEY+': '+secret+'\n').encode())
   cases=scenarios(paths)
   if v.case:assert set(v.case)<=set(row[0] for row in cases);cases=[row for row in cases if row[0] in v.case]
   for name,extra_args,extra_env,yaml_index,expected in cases:
    for kind,binary in [('official',v.official),('rust',v.rust)]:
     row={'case':name,'kind':kind,'expected_selected_file_ordinal':None if expected is None else expected+1,'raw_byte_filename_created':False,'status':'FAIL'};report['rows'].append(row);child=None;stdout=None;stderr=None
     try:
      http=port(socket.SOCK_STREAM);udp=port(socket.SOCK_DGRAM)
      config=f'port: {http}\nbind_addresses: [127.0.0.1]\nrtc:\n  tcp_port: 0\n  udp_port: {udp}\n  node_ip: 127.0.0.1\n  use_external_ip: false\n  enable_loopback_candidate: true\nkeys: {{}}\nlogging:\n  level: error\n'
      if yaml_index is not None:config+='key_file: '+json.dumps(os.fsdecode(paths[yaml_index]))+'\n'
      argv=[b'livekit-server',b'--config-body',config.encode(),*extra_args]
      env=base.copy();env[b'HOME']=cwd+b'/missing-home-\xff';env.update(extra_env)
      row['argv_projection']={'extra_hex':[arg.hex() for arg in extra_args],'home_has_non_utf8':True,'env_value_hex':{key.decode():val.hex() for key,val in extra_env.items()},'yaml_key_file_ordinal':None if yaml_index is None else yaml_index+1,'http_port':http,'udp_port':udp}
      child=subprocess.Popen(argv,executable=os.fsencode(os.path.abspath(binary)),cwd=cwd,env=env,stdin=subprocess.DEVNULL,stdout=subprocess.PIPE,stderr=subprocess.PIPE,start_new_session=True)
      stdout=Drain(child.stdout);stderr=Drain(child.stderr)
      if expected is None:
       child.wait(timeout=10);stdout.thread.join(2);stderr.thread.join(2)
       assert child.returncode==1,'explicit empty source key_file must fail without fallback keys'
       assert b'one of key-file or keys must be provided' in stdout.private+stderr.private,'must fail at real no-credentials consumer, not UTF8 parser'
       row['expected_no_credentials_failure']=True
      else:
       payload={'interop_package':str(v.interop_package.resolve()),'url':'http://127.0.0.1:'+str(http),'key':KEY,'secret':SECRETS[expected],'other_secret':SECRETS[1-expected]}
       probe=subprocess.run([v.node,str(script)],input=json.dumps(payload).encode(),stdout=subprocess.PIPE,stderr=subprocess.PIPE,timeout=35)
       assert len(probe.stdout)<65536 and len(probe.stderr)<65536,'bounded SDK result'
       observed=json.loads(probe.stdout);row['sdk']=observed;row['sdk_exit']=probe.returncode
       assert probe.returncode==0 and observed['status']=='PASS','actual SDK file-secret authentication/CRUD failed'
       assert observed['sdk_version']=='2.19.2' and observed['sdk_identity']==report['sdk_identity'],'same actual SDK/Node package and entry identity for every positive probe'
       assert child.poll() is None,'server must remain alive through actual SDK cleanup'
      row['status']='PASS'
     except Exception as error:row['failure_type']=type(error).__name__
     finally:
      if child is not None:
       if child.poll() is None:
        child.send_signal(signal.SIGINT)
        try:child.wait(timeout=20)
        except subprocess.TimeoutExpired:child.kill();child.wait(timeout=5);row['forced_stop']=True;row['status']='FAIL';report['cleanup_errors'].append('owned server required kill')
       row['actual_exit']=child.returncode
       if expected is not None and child.returncode!=0:row['status']='FAIL'
       row['stdout']=stdout.result();row['stderr']=stderr.result()
       if any(row[key]['overflow'] or not row[key]['drain_finished'] for key in ['stdout','stderr']):row['status']='FAIL'
   report['expected_rows']=len(cases)*2;report['complete']=len(report['rows'])==report['expected_rows']
  report['private_files_removed']=True
 except Exception as error:report['failure_type']=type(error).__name__
 finally:
  report['status']='PASS' if report.get('complete') and report.get('private_files_removed') and not report['cleanup_errors'] and all(row['status']=='PASS' for row in report['rows']) else 'FAIL'
  v.out.write_text(json.dumps(report,indent=2)+'\n')
 return int(report['status']!='PASS')
if __name__=='__main__':raise SystemExit(main())
