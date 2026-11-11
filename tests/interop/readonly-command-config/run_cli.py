"""Actual source/main CLI effects, no server started. Coordinator executes only."""
from pathlib import Path
import argparse,json,hashlib,subprocess,os,time,hmac,base64
p=argparse.ArgumentParser();p.add_argument('--binary',type=Path,required=True);p.add_argument('--result',type=Path,required=True);p.add_argument('--reference',type=Path);p.add_argument('--artifacts',type=Path,required=True);a=p.parse_args();d=Path(__file__).parent;a.artifacts.mkdir(mode=0o700,parents=True,exist_ok=False);h=lambda b:hashlib.sha256(b).hexdigest();unb64=lambda b:base64.urlsafe_b64decode(b+b'='*(-len(b)%4))
report={'status':'RUNNING','binary_sha256':h(a.binary.read_bytes()),'cases_sha256':h((d/'cases.json').read_bytes()),'rows':[]}
try:
 for row in json.loads((d/'cases.json').read_text()):
  try:
   argv=[str(a.binary.resolve()),'--config-body',row['body'],row['command']]
   if row['command']=='create-join-token':argv+=['--room','owned-command-room','--identity','owned-command-person']+(['--recorder']if row['recorder']else[])
   before=int(time.time());process=subprocess.run(argv,env={k:v for k,v in os.environ.items()if not k.startswith(('LIVEKIT_','LIVECLOUD_'))},capture_output=True,timeout=10);after=int(time.time())
   for suffix,body in [('stdout',process.stdout),('stderr',process.stderr)]:f=a.artifacts/(row['id']+'.'+suffix);f.write_bytes(body);f.chmod(0o600)
   observation={'id':row['id'],'exit_code':process.returncode,'stdout_sha256':h(process.stdout),'stderr_sha256':h(process.stderr)}
   report['rows'].append(observation)
   if row['negative_control']:
    assert process.returncode!=0,'source negative pacer validation must remain'
    observation['contract']={'id':row['id'],'exit_code':process.returncode,'stdout_hex':process.stdout.hex(),'stderr_hex':process.stderr.hex()}
   elif row['command']=='ports':
    assert process.returncode==0,('positive command rejected',row['id']);observation['contract']={'id':row['id'],'exit_code':0,'stdout_hex':process.stdout.hex(),'stderr_hex':process.stderr.hex()}
   else:
    assert process.returncode==0;assert process.stdout.startswith(b'Token: ')and process.stdout.endswith(b'\n');token=process.stdout[len(b'Token: '):].strip();parts=token.split(b'.');assert len(parts)==3
    assert hmac.compare_digest(unb64(parts[2]),hmac.new(b'owned-command-secret-not-production-32-bytes',parts[0]+b'.'+parts[1],hashlib.sha256).digest())
    header=json.loads(unb64(parts[0]));claims=json.loads(unb64(parts[1]));assert header['alg']=='HS256';assert claims['iss']=='APIownedcommand'and claims['sub']=='owned-command-person';assert before<=claims['iat']<=after and claims['nbf']==claims['iat']and claims['exp']-claims['iat']==30*24*3600
    semantic={k:v for k,v in claims.items()if k not in ['iat','nbf','exp']};assert semantic['video']['room']=='owned-command-room'and semantic['video']['roomJoin']is True
    if row['recorder']:assert semantic['video']['hidden']is True and semantic['video']['recorder']is True and semantic['video']['canPublish']is False and semantic['video']['canPublishData']is False
    observation['contract']={'id':row['id'],'exit_code':0,'header':header,'verified_hs256':True,'duration_s':30*24*3600,'exact_nontemporal_claims':semantic,'stderr_hex':process.stderr.hex()}
  except Exception as case_error:
   if not report['rows'] or report['rows'][-1]['id']!=row['id']:report['rows'].append({'id':row['id']})
   report['rows'][-1]['failure']={'type':type(case_error).__name__,'message':str(case_error)}
 assert len(report['rows'])==12 and not any('failure'in r for r in report['rows']),'all12 actual cases must succeed'
 contract=[r['contract']for r in report['rows']];report['contract']=contract
 if a.reference:
  ref=json.loads(a.reference.read_text());assert ref['status']=='REFERENCE_CAPTURED';assert ref['cases_sha256']==report['cases_sha256'];assert ref['contract']==contract;report['status']='PASS'
 else:report['status']='REFERENCE_CAPTURED'
except Exception as e:report['status']='FAIL';report['failure']={'type':type(e).__name__,'message':str(e)}
a.result.write_text(json.dumps(report,indent=2)+'\n');a.result.chmod(0o600)
if report['status']=='FAIL':raise SystemExit(1)
