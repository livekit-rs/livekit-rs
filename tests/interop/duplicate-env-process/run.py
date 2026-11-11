#!/usr/bin/env python3
"""Coordinator-only: fixed Go readers + genuine Go/Rust CLI via duplicate envp.
No compiler/server/browser. Exactly ten complete fresh-process source/consumer rows.
"""
import argparse,base64,hashlib,hmac,json,os,platform,tempfile,time
from pathlib import Path
from execve_bytes import execute

def sha(b):return hashlib.sha256(b).hexdigest()
def summary(r):return {'exit':r['exit'],'timed_out':r['timed_out'],'stdout_bytes':len(r['stdout']),'stdout_sha256':sha(r['stdout']),'stderr_bytes':len(r['stderr']),'stderr_sha256':sha(r['stderr'])}
def token(stdout,index):
    assert stdout.startswith(b'Token: ')and stdout.endswith(b'\n'),'token framing'
    parts=stdout[7:].strip().split(b'.');assert len(parts)==3
    decode=lambda v:base64.urlsafe_b64decode(v+b'='*(-len(v)%4))
    header=json.loads(decode(parts[0]));claims=json.loads(decode(parts[1]));secret=f'f92-owned-{index}-source-secret'.encode()
    assert header.get('alg')=='HS256'and hmac.compare_digest(decode(parts[2]),hmac.new(secret,b'.'.join(parts[:2]),hashlib.sha256).digest()),'actual selected owned keyfile signature'
    assert claims.get('iss')==f'APIf92{index}'and claims.get('sub')=='f92-identity'and claims.get('video')=={'roomJoin':True,'room':'f92-room'}
    now=int(time.time());nbf=claims.get('nbf');exp=claims.get('exp')
    assert isinstance(nbf,int)and isinstance(exp,int)and now-30<=nbf<=now+2 and exp-nbf==30*24*3600
    if 'iat'in claims:assert isinstance(claims['iat'],int)and abs(claims['iat']-nbf)<=1
    return {'signature_valid':True,'selected_owned_key':index,'lifetime':exp-nbf,'claims_without_generated_times':{k:v for k,v in claims.items()if k not in ('iat','nbf','exp')}}

def main():
    p=argparse.ArgumentParser();p.add_argument('--go-oracle',required=True);p.add_argument('--official',required=True);p.add_argument('--rust',required=True);p.add_argument('--output',required=True);a=p.parse_args()
    profile={'Darwin':'darwin','Linux':'linux'}.get(platform.system());assert os.name=='posix'and profile,'actual POSIX Darwin/Linux host only'
    bins={k:Path(v).resolve()for k,v in [('go_oracle',a.go_oracle),('official',a.official),('rust',a.rust)]}
    report={'status':'RUNNING','profile':profile,'scope':'Actual raw envp first duplicate source readers and real keyfile/JWT + official ENV/argv precedence consumers. No HOME fallback proof.','binaries':{k:{'path':str(v),'sha256':sha(v.read_bytes())}for k,v in bins.items()},'rows':[]}
    with tempfile.TemporaryDirectory(prefix='f92-owned-')as owned:
        cwd=os.fsencode(owned)
        for name,index in [(b'',0),(b'first',1),(b'second',2),(b'third',3)]:
            directory=cwd+(b'/'+name if name else b'');os.makedirs(directory,exist_ok=True)
            fd=os.open(directory+b'/keys.yaml',os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o600)
            with os.fdopen(fd,'wb')as f:f.write(f'APIf92{index}: f92-owned-{index}-source-secret\n'.encode())
        cases=[
          ('ascii-first-valued',b'F92_DUP',[(b'F92_DUP',b'first'),(b'F92_DUP',b'second')],1,None),
          ('ascii-first-empty',b'F92_DUP',[(b'F92_DUP',b''),(b'F92_DUP',b'second')],0,None),
          ('ascii-later-empty',b'F92_DUP',[(b'F92_DUP',b'first'),(b'F92_DUP',b'')],1,None),
          ('ascii-three-later-overrides',b'F92_DUP',[(b'F92_DUP',b'first'),(b'F92_DUP',b'second'),(b'F92_DUP',b'third')],1,None),
          ('raw-name-first-valued',b'F92_DUP\xff',[(b'F92_DUP\xff',b'first'),(b'F92_DUP\xff',b'second')],1,None),
          ('raw-name-first-empty',b'F92_DUP\xff',[(b'F92_DUP\xff',b''),(b'F92_DUP\xff',b'second')],0,None),
          ('distinct-ascii-case',b'F92_DUP',[(b'F92_DUP',b'first'),(b'f92_dup',b'second')],1,None),
          ('raw-known-looking-name',b'LIVEKIT_PORT\xff',[(b'LIVEKIT_PORT\xff',b'first'),(b'LIVEKIT_PORT\xff',b'second')],1,None),
          ('declared-env-first',b'LIVEKIT_CONFIG',[(b'LIVEKIT_CONFIG',b'port: 17881\n'),(b'LIVEKIT_CONFIG',b'port: 17882\n')],None,17881),
          ('explicit-argv-over-first-env',b'LIVEKIT_CONFIG',[(b'LIVEKIT_CONFIG',b'port: 17881\n'),(b'LIVEKIT_CONFIG',b'port: 17882\n')],None,17883),
        ]
        for label,key,pairs,index,port in cases:
            row={'id':label,'status':'RUNNING','source':{},'consumers':{}}
            entries=[b'PATH=/usr/bin:/bin',b'LC_ALL=C',b'TZ=UTC',b'HOME='+cwd]+[k+b'='+v for k,v in pairs]
            keys=list(dict.fromkeys(k for k,v in pairs));first={}
            for k,v in pairs:first.setdefault(k,v)
            try:
                actual=execute(bins['go_oracle'],[k.hex().encode()for k in keys],entries,cwd)
                row['source']['process']=summary(actual);assert actual['exit']==0and not actual['timed_out']and not actual['stderr']
                observations=json.loads(actual['stdout']);assert len(observations)==len(keys)
                for q,k in zip(observations,keys):
                    expected={'key_hex':k.hex(),'getenv_hex':first[k].hex(),'lookup_hex':first[k].hex(),'found':True,'environ_hex':[(k+b'='+first[k]).hex()]}
                    assert q==expected,('actual fixed Go first occurrence observation',q,expected)
                row['source']['observations']=observations
                if index is not None:
                    path=cwd+b'/${'+key+b'}/keys.yaml'
                    args=[b'--node-ip=127.0.0.1',b'--key-file',path,b'--config-body',b'keys: {}',b'create-join-token',b'--room=f92-room',b'--identity=f92-identity']
                else:
                    args=[b'--node-ip=127.0.0.1']+([b'--config-body',b'port: 17883\n']if label=='explicit-argv-over-first-env'else[])+[b'ports']
                outputs={}
                for side in ['official','rust']:
                    result=execute(bins[side],args,entries,cwd);entry=summary(result);row['consumers'][side]=entry
                    assert result['exit']==0and not result['timed_out']and not result['stderr'],(side,'consumer process failed')
                    if index is not None:entry['semantic']=token(result['stdout'],index)
                    else:
                        assert f'{port} - HTTP service\n'.encode()in result['stdout'];entry['http_port']=port
                    outputs[side]=result
                if index is not None:assert row['consumers']['official']['semantic']==row['consumers']['rust']['semantic']
                else:assert outputs['official']['stdout']==outputs['rust']['stdout'],'whole ports output bytes differ'
                row['status']='PASS_SCOPED'
            except Exception as error:row['status']='FAIL';row['failure']=str(error)
            report['rows'].append(row)
    report['owned_cleanup']=not Path(owned).exists();report['status']='PASS_SCOPED'if len(report['rows'])==10and all(r['status']=='PASS_SCOPED'for r in report['rows'])and report['owned_cleanup']else'FAIL'
    Path(a.output).write_text(json.dumps(report,indent=2)+'\n');return 0if report['status']=='PASS_SCOPED'else 1
if __name__=='__main__':raise SystemExit(main())
