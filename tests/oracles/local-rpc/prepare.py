from pathlib import Path
import argparse,json,hashlib
p=argparse.ArgumentParser();p.add_argument('--fixed-source-root',type=Path,required=True);p.add_argument('--out',type=Path,required=True);p.add_argument('--kind',choices=['phase','constructor'],required=True);a=p.parse_args();here=Path(__file__).resolve().parent
for rel,sha in json.loads((here/'fixed-source.json').read_text()).items():assert hashlib.sha256((a.fixed_source_root/rel).read_bytes()).hexdigest()==sha,rel
assert not a.out.exists();a.out.mkdir()
for name in ['main.go','extra-v3.go','extra-v4.go','extra-v5.go']:
 text=(here/name).read_text()
 if a.kind=='constructor'and name=='main.go':
  assert text.count('func main(){')==1;text=text.replace('func main(){','func originalSource12Entry(){')
 (a.out/name).write_text(text)
if a.kind=='constructor':(a.out/'constructor.go').write_bytes((here/'constructor.go').read_bytes())
mod=(a.fixed_source_root/'references/livekit-stable/go.mod').read_text();sums=set()
for name,rel in [('github.com/livekit/protocol','references/protocol-stable'),('github.com/livekit/psrpc','references/psrpc'),('github.com/livekit/mediatransportutil','references/go-modules/mediatransportutil-f234b534b095')]:mod+='\nreplace '+name+' => '+json.dumps(str((a.fixed_source_root/rel).resolve()))+'\n'
for rel in ['references/livekit-stable','references/protocol-stable','references/psrpc','references/go-modules/mediatransportutil-f234b534b095']:
 f=a.fixed_source_root/rel/'go.sum'
 if f.exists():sums.update(f.read_text().splitlines())
(a.out/'go.mod').write_text(mod);(a.out/'go.sum').write_text('\n'.join(sorted(sums))+'\n')
