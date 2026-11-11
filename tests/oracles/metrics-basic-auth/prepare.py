from pathlib import Path
import argparse,json,hashlib,re,shutil
p=argparse.ArgumentParser();p.add_argument('--root',type=Path,required=True);p.add_argument('--out',type=Path,required=True);a=p.parse_args();d=Path(__file__).resolve().parent;s=json.loads((d/'SOURCE.json').read_text());h=lambda p:hashlib.sha256(p.read_bytes()).hexdigest()
for n,v in s['fixed_inputs_sha256'].items():assert h(a.root/n)==v,n
for n,v in s['local_toolchain_source_sha256'].items():assert h(Path(n))==v,n
assert h(d/'main.go')==s['observer_sha256'];assert h(d/'cases.json')==s['cases_sha256'];assert not a.out.exists();a.out.mkdir()
m=(a.root/'references/livekit-stable/go.mod').read_text();m=re.sub(r'^module [^\n]+','module livekit-rs-tests/metrics-basic-auth',m,count=1);m+='\nrequire github.com/livekit/livekit-server v1.13.10\n'
for name,path in [('github.com/livekit/livekit-server','references/livekit-stable'),('github.com/livekit/protocol','references/protocol-stable'),('github.com/livekit/psrpc','references/psrpc'),('github.com/livekit/mediatransportutil','references/go-modules/mediatransportutil-f234b534b095')]:m+='\nreplace '+name+' => '+json.dumps(str((a.root/path).resolve()))+'\n'
(a.out/'go.mod').write_text(m);sums=set()
for n in ['references/livekit-stable/go.sum','references/protocol-stable/go.sum','references/psrpc/go.sum']:sums.update((a.root/n).read_text().splitlines())
(a.out/'go.sum').write_text('\n'.join(sorted(sums))+'\n')
for n in ['main.go','cases.json']:shutil.copyfile(d/n,a.out/n)
print(a.out)
