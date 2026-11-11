#!/usr/bin/env python3
"""Actual fixed official/Rust command consumer. Never persists a token or secret."""
import argparse, base64, hashlib, hmac, json, os, pathlib, subprocess, tempfile
p=argparse.ArgumentParser();p.add_argument('--official',required=True);p.add_argument('--rust');p.add_argument('--out',required=True);a=p.parse_args()
KEY='APIownedcliboundary';SECRET='owned-cli-not-production-synthetic-secret';ROOM='owned-cli-room';IDENTITY='owned-cli-person'
EMPTY_ERROR='missing API key or secret key\n'
def sha_file(path):
 h=hashlib.sha256()
 with open(path,'rb') as f:
  for b in iter(lambda:f.read(1024*1024),b''):h.update(b)
 return h.hexdigest()
def decode_part(part):return base64.urlsafe_b64decode(part+b'='*((-len(part))%4))
def checked_token(stdout,key,secret,recorder):
 if not stdout.startswith(b'Token: ') or not stdout.endswith(b'\n') or stdout.count(b'\n')!=1:return None
 try:
  token=stdout[7:-1];parts=token.split(b'.')
  if len(parts)!=3:return None
  head=json.loads(decode_part(parts[0]));claims=json.loads(decode_part(parts[1]))
  sig=decode_part(parts[2]);actual=hmac.new(secret.encode(),parts[0]+b'.'+parts[1],hashlib.sha256).digest()
  video={'roomJoin':True,'room':ROOM}
  if recorder:video.update(hidden=True,recorder=True,canPublish=False,canPublishData=False)
  checks={'signature_valid':hmac.compare_digest(sig,actual),'hs256_jwt_header':head=={'alg':'HS256','typ':'JWT'},'issuer':claims.get('iss')==key,'identity':claims.get('sub')==IDENTITY,'video_grant':claims.get('video')==video,'issued_notbefore_equal':claims.get('iat')==claims.get('nbf'),'thirty_day_lifetime':claims.get('exp',0)-claims.get('nbf',0)==30*24*3600}
  business={k:v for k,v in claims.items() if k not in ('iat','nbf','exp')}
  # Hash only the fixed synthetic semantic cohort, never token/signature bytes.
  checks['business_claims_sha256']=hashlib.sha256(json.dumps(business,sort_keys=True,separators=(',',':'),ensure_ascii=False).encode()).hexdigest()
  return checks
 except (ValueError,TypeError,KeyError):return None
report={'status':'RUNNING','scope':'actual create-join-token command credential boundary; no server/media/SDK issuer claim','source_expectation':'fixed commands.go selects configured pair; auth.AccessToken.ToJWT rejects empty API key OR secret','all_inputs_owned_synthetic':True,'rows':[],'binaries':{}}
env={k:v for k,v in os.environ.items() if not k.startswith('LIVEKIT_') and k not in ('NODE_IP','UDP_PORT','REDIS_HOST','REDIS_PASSWORD')}
cases=[('empty-key','',SECRET,False),('empty-secret',KEY,'',False),('both-empty','','',False),('one-valid',KEY,SECRET,False),('valid-recorder',KEY,SECRET,True)]
case_inputs=[(name,key,secret,recorder,False) for name,key,secret,recorder in cases]+[(name+'-keyfile',key,secret,recorder,True) for name,key,secret,recorder in cases if name!='valid-recorder']
try:
 with tempfile.TemporaryDirectory(prefix='livekit-cli-token-boundary-') as td:
  for mode,binary in [('official',a.official)]+([('rust',a.rust)] if a.rust else []):
   report['binaries'][mode]={'sha256':sha_file(binary)}
   for ordinal,(name,key,secret,recorder,keyfile) in enumerate(case_inputs):
    mapping=json.dumps({key:secret},ensure_ascii=False)
    argv=['--node-ip','127.0.0.1','--config-body','keys: {}' if keyfile else 'keys: '+mapping]
    if keyfile:
     path=pathlib.Path(td)/('owned-%d.yaml'%ordinal);path.write_text(mapping+'\n');path.chmod(0o600);argv+=['--key-file',str(path)]
    argv+=['create-join-token','--room',ROOM,'--identity',IDENTITY]
    if recorder:argv+=['--recorder']
    row={'binary':mode,'case':name,'profile':'initialized-empty-map-keyfile' if keyfile else 'single-inline-key','keyfile_mode_0600':keyfile,'command':'create-join-token','argv_contains_only_owned_synthetic_credentials':True}
    try:
     result=subprocess.run([binary]+argv,env=env,stdin=subprocess.DEVNULL,stdout=subprocess.PIPE,stderr=subprocess.PIPE,timeout=30,check=False)
     row.update(exit_code=result.returncode,stdout_bytes=len(result.stdout),stderr_bytes=len(result.stderr))
     if len(result.stdout)>65536 or len(result.stderr)>65536:row.update(passed=False,error='bounded CLI output exceeded')
     elif not key or not secret:
      row['exact_missing_credentials_stdout']=result.stdout==EMPTY_ERROR.encode()
      # Only complete recognized non-sensitive diagnostics may leave this process.
      known=[EMPTY_ERROR.encode(),b'no API keys configured\n',b'no API key configured\n',b'no singing key or secret was provided\n']
      if result.stdout in known:row['controlled_error']=result.stdout.decode()
      row['passed']=result.returncode==1 and result.stderr==b'' and row['exact_missing_credentials_stdout']
     else:
      checks=checked_token(result.stdout,key,secret,recorder);row['claims_checks']=checks
      row['passed']=result.returncode==0 and result.stderr==b'' and checks is not None and all(v for k,v in checks.items() if k!='business_claims_sha256')
    except subprocess.TimeoutExpired:row.update(passed=False,error='CLI child timed out and was killed/waited by subprocess.run')
    except OSError:row.update(passed=False,error='CLI child could not be spawned')
    report['rows'].append(row)
  if a.rust:
   for name,*_ in case_inputs:
    rows=[r for r in report['rows'] if r['case']==name]
    if all(r.get('claims_checks') for r in rows):
     same=rows[0]['claims_checks']['business_claims_sha256']==rows[1]['claims_checks']['business_claims_sha256']
     for r in rows:r['exact_business_claims_match_reference']=same;r['passed']&=same
 report['status']=('PASS' if a.rust else 'REFERENCE_PASS') if all(r['passed'] for r in report['rows']) else 'FAIL'
except Exception as error:
 report['status']='FAIL';report['harness_error_type']=type(error).__name__
finally:
 path=pathlib.Path(a.out);path.parent.mkdir(parents=True,exist_ok=True)
 with open(path,'x',encoding='utf-8') as f:json.dump(report,f,ensure_ascii=False,indent=2);f.write('\n')
 path.chmod(0o600)
raise SystemExit(0 if report['status'] in ('PASS','REFERENCE_PASS') else 1)
