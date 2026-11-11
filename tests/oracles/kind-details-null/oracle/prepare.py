from pathlib import Path
import argparse,json,hashlib,re,shutil
p=argparse.ArgumentParser();p.add_argument('--root',type=Path,required=True);p.add_argument('--out',type=Path,required=True);a=p.parse_args();here=Path(__file__).resolve().parent.parent;m=json.loads((here/'SOURCE.json').read_text());h=lambda p:hashlib.sha256(p.read_bytes()).hexdigest()
for rel,w in m['fixed_source_sha256'].items():assert h(a.root/rel)==w,rel
for rel in ['oracle/main.go','oracle/cases.json']:assert h(here/rel)==m['assets_sha256'][rel],rel
assert not a.out.exists();a.out.mkdir(parents=True);mod=(a.root/'references/protocol-stable/go.mod').read_text();mod=re.sub(r'^module [^\n]+','module livekit-rs-tests/open-source-numeric',mod,count=1);mod+='\nrequire github.com/livekit/protocol v0.0.0\nreplace github.com/livekit/protocol => '+json.dumps(str((a.root/'references/protocol-stable').resolve()))+'\n';(a.out/'go.mod').write_text(mod);shutil.copyfile(a.root/'references/protocol-stable/go.sum',a.out/'go.sum')
for name in ['main.go','cases.json']:shutil.copyfile(here/'oracle'/name,a.out/name)
print('PREPARED_ONLY_NO_GO_EXECUTION',a.out)
