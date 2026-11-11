#!/usr/bin/env python3
"""Coordinator-run differential; source acceptance expected only after actual Go execution."""
import argparse,hashlib,json,os,subprocess,sys

def digest(path):
 h=hashlib.sha256()
 with open(path,'rb') as f:
  for b in iter(lambda:f.read(131072),b''):h.update(b)
 return h.hexdigest()
def main():
 a=argparse.ArgumentParser();a.add_argument('--official',required=True);a.add_argument('--rust',required=True);a.add_argument('--out',required=True);v=a.parse_args()
 rows=[];base={k:val for k,val in os.environb.items() if not k.startswith((b'LIVEKIT_',b'LIVECLOUD_'))}
 cases=[('cli-root-generated-unused-ports',[b'--log_level',b'\xff',b'ports'],{}),('env-generated-unused-ports',[b'ports'],{b'LIVEKIT_LOG_LEVEL':b'\xff'}),('cli-overrides-raw-env',[b'--log_level=error',b'ports'],{b'LIVEKIT_LOG_LEVEL':b'\xff'}),('root-help-preempts-generated-raw-env',[b'--help'],{b'LIVEKIT_LOG_LEVEL':b'\xff'})]
 for name,argv,extra in cases:
  results={};raw={}
  for kind,binary in [('official',v.official),('rust',v.rust)]:
   env=base.copy();env.update(extra)
   r=subprocess.run([b'livekit-server',*argv],executable=os.fsencode(os.path.abspath(binary)),env=env,stdout=subprocess.PIPE,stderr=subprocess.PIPE,timeout=10)
   if len(r.stdout)+len(r.stderr)>1048576:raise RuntimeError('output bound exceeded')
   raw[kind]=(r.returncode,r.stdout,r.stderr)
   results[kind]={'exit':r.returncode,'stdout_bytes':len(r.stdout),'stdout_sha256':hashlib.sha256(r.stdout).hexdigest(),'stderr_bytes':len(r.stderr),'stderr_sha256':hashlib.sha256(r.stderr).hexdigest()}
  # Last help case contains approved product identity difference: don't introduce
  # another normalizer; capture only, require original accepted runner for it.
  match=raw['official']==raw['rust'] if name!='root-help-preempts-generated-raw-env' else None
  rows.append({'case':name,'argv_hex':[x.hex() for x in argv],'env_hex':{k.decode():val.hex() for k,val in extra.items()},'actual':results,'exact_equal':match,'status':'OBSERVED_ONLY' if match is None else 'PASS' if match else 'FAIL'})
 report={'status':'FAIL' if any(x['status']=='FAIL' for x in rows) else 'COMPARISONS_MATCH_HELP_SEPARATE','scope':'new4 only, not original POSIX32/37 and not Run/keyfile service proof','binary_sha256':{'official':digest(v.official),'rust':digest(v.rust)},'rows':rows}
 with open(v.out,'w') as f:json.dump(report,f,indent=2);f.write('\n')
 return int(report['status']=='FAIL')
if __name__=='__main__':sys.exit(main())
