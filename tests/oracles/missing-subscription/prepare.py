from pathlib import Path
import argparse,hashlib,json,shutil
p=argparse.ArgumentParser();p.add_argument('--root',type=Path,required=True);p.add_argument('--out',type=Path,required=True);a=p.parse_args();here=Path(__file__).resolve().parent;m=json.loads((here/'SOURCE.json').read_text());h=lambda p:hashlib.sha256(p.read_bytes()).hexdigest()
for rel,sha in m['fixed_inputs_sha256'].items():assert h(a.root/rel)==sha,rel
for rel,sha in m['assets_sha256'].items():assert h(here/rel)==sha,rel
assert not a.out.exists();shutil.copytree(a.root/'references/livekit-stable',a.out,ignore=shutil.ignore_patterns('.git'))
q=a.out/'pkg/rtc/subscriptionmanager.go';s=q.read_text();counts={'now':s.count('time.Now()'),'since':s.count('time.Since('),'worker':s.count('go m.reconcileWorker()')};assert counts==m['declared_overlay_occurrences'],counts
s=s.replace('time.Now()','sourceOracleNow()').replace('time.Since(','sourceOracleSince(').replace('go m.reconcileWorker()','// Original queue is synchronously driven by the source observer; no background test clock races.')
q.write_text(s)
shutil.copyfile(here/'source_missing_test.go',a.out/'pkg/rtc/zz_source_missing_test.go');shutil.copyfile(here/'clock.go',a.out/'pkg/rtc/zz_source_missing_clock.go');shutil.copyfile(here/'cases.json',a.out/'source-missing-cases.json')
q=a.out/'go.mod';mod=q.read_text()
for name,path in [('github.com/livekit/protocol','references/protocol-stable'),('github.com/livekit/psrpc','references/psrpc'),('github.com/livekit/mediatransportutil','references/go-modules/mediatransportutil-f234b534b095')]:mod+='\nreplace '+name+' => '+json.dumps(str((a.root/path).resolve()))+'\n'
q.write_text(mod);print(a.out)
