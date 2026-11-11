from pathlib import Path
import argparse,json,hashlib,shutil
p=argparse.ArgumentParser();p.add_argument('--root',type=Path,required=True);p.add_argument('--out',type=Path,required=True);a=p.parse_args();d=Path(__file__).parent;s=json.loads((d/'SOURCE.json').read_text());h=lambda p:hashlib.sha256(p.read_bytes()).hexdigest()
for n,v in s['fixed_inputs_sha256'].items():assert h(a.root/n)==v,n
for n,v in s['assets_sha256'].items():assert h(d/n)==v,n
assert not a.out.exists();a.out.mkdir(mode=0o700)
for n in ['main.go','cases.jsonl']:shutil.copyfile(d/n,a.out/n)
mod=(a.root/'references/livekit-stable/go.mod').read_text().replace('module github.com/livekit/livekit-server','module local.test/create-room-policy-source',1)+'\nrequire github.com/livekit/livekit-server v0.0.0\n'
for module,path in [('github.com/livekit/livekit-server','references/livekit-stable'),('github.com/livekit/protocol','references/protocol-stable'),('github.com/livekit/psrpc','references/psrpc'),('github.com/livekit/mediatransportutil','references/go-modules/mediatransportutil-f234b534b095')]:mod+='\nreplace '+module+' => '+json.dumps(str((a.root/path).resolve()))+'\n'
(a.out/'go.mod').write_text(mod);shutil.copyfile(a.root/'references/livekit-stable/go.sum',a.out/'go.sum');print(a.out)
