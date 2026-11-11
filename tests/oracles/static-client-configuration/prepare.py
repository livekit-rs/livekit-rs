from pathlib import Path
import argparse,json,hashlib,shutil
p=argparse.ArgumentParser();p.add_argument('--root',type=Path,required=True);p.add_argument('--out',type=Path,required=True);a=p.parse_args();here=Path(__file__).resolve().parent
for n,v in json.loads((here/'fixed-source.json').read_text()).items():assert hashlib.sha256((a.root/n).read_bytes()).hexdigest()==v,n
assert not a.out.exists();a.out.mkdir()
for n in ['main.go','cases.json']:shutil.copy2(here/n,a.out/n)
mod=(a.root/'references/livekit-stable/go.mod').read_text().replace('module github.com/livekit/livekit-server','module static-client-config-observer',1)
for name,rel in [('github.com/livekit/livekit-server','references/livekit-stable'),('github.com/livekit/protocol','references/protocol-stable'),('github.com/livekit/psrpc','references/psrpc')]:mod+='\nreplace '+name+' => '+json.dumps(str((a.root/rel).resolve()))+'\n'
mod+='\nrequire github.com/livekit/livekit-server v0.0.0\n';(a.out/'go.mod').write_text(mod);shutil.copy2(a.root/'references/livekit-stable/go.sum',a.out/'go.sum')
