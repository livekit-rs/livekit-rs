from pathlib import Path
import argparse,json,hashlib,shutil
p=argparse.ArgumentParser();p.add_argument('--fixed-source-root',type=Path,required=True);p.add_argument('--out',type=Path,required=True);a=p.parse_args();here=Path(__file__).resolve().parent.parent;m=json.loads((here/'PROVENANCE.json').read_text())
for n,h in m['assets_sha256'].items():assert hashlib.sha256((here/n).read_bytes()).hexdigest()==h,'asset drift '+n
for n,h in m['source_sha256'].items():assert hashlib.sha256((a.fixed_source_root/n).read_bytes()).hexdigest()==h,'source drift '+n
assert not a.out.exists();a.out.mkdir();shutil.copyfile(here/'oracle/main.go',a.out/'main.go');shutil.copyfile(here/'cases.json',a.out/'cases.json')
mod=(a.fixed_source_root/'references/livekit-stable/go.mod').read_text().replace('module github.com/livekit/livekit-server','module client-browser-capability-observer',1)
for name,rel in [('github.com/livekit/livekit-server','references/livekit-stable'),('github.com/livekit/protocol','references/protocol-stable'),('github.com/livekit/psrpc','references/psrpc'),('github.com/livekit/mediatransportutil','references/go-modules/mediatransportutil-f234b534b095')]:
 mod+='\nreplace '+name+' => '+json.dumps(str((a.fixed_source_root/rel).resolve()))+'\n'
mod+='\nrequire github.com/livekit/livekit-server v0.0.0\n';(a.out/'go.mod').write_text(mod);shutil.copyfile(a.fixed_source_root/'references/livekit-stable/go.sum',a.out/'go.sum');print(a.out)
