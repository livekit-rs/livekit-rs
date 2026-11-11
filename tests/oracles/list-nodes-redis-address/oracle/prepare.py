from pathlib import Path
import argparse,hashlib,json,re,shutil
p=argparse.ArgumentParser();p.add_argument('--root',type=Path,required=True);p.add_argument('--out',type=Path,required=True);a=p.parse_args();d=Path(__file__).resolve().parent;s=json.loads((d.parent/'SOURCE.json').read_text());h=lambda p:hashlib.sha256(p.read_bytes()).hexdigest()
for n,v in s['fixed_source_sha256'].items():assert h(a.root/n)==v,'fixed source drift '+n
assert not a.out.exists();a.out.mkdir();mod=(a.root/'references/protocol-stable/go.mod').read_text();mod=re.sub(r'^module [^\n]+','module livekit-rs-tests/redis-address',mod,count=1);mod+='\nrequire github.com/livekit/protocol v0.0.0\nreplace github.com/livekit/protocol => '+json.dumps(str((a.root/'references/protocol-stable').resolve()))+'\n';(a.out/'go.mod').write_text(mod);shutil.copyfile(a.root/'references/protocol-stable/go.sum',a.out/'go.sum');shutil.copyfile(d/'main.go',a.out/'main.go');shutil.copytree(d/'ports',a.out/'ports');print(a.out)
