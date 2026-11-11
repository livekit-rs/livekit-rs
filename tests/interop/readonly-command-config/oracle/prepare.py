from pathlib import Path
import argparse,shutil,json,hashlib
p=argparse.ArgumentParser();p.add_argument('--root',type=Path,required=True);p.add_argument('--out',type=Path,required=True);a=p.parse_args();d=Path(__file__).parent;s=json.loads((d/'SOURCE.json').read_text());h=lambda p:hashlib.sha256(p.read_bytes()).hexdigest()
for path,v in s['fixed_inputs_sha256'].items():assert h(a.root/path)==v,path
for path,v in s['assets_sha256'].items():assert h(d/path)==v,path
assert not a.out.exists();shutil.copytree(a.root/'references/livekit-stable',a.out,ignore=shutil.ignore_patterns('.git'));shutil.copyfile(d/'source_test.go',a.out/'pkg/config/source_readonly_getconfig_test.go');f=a.out/'go.mod';v=f.read_text()
for module,path in [('github.com/livekit/protocol','references/protocol-stable'),('github.com/livekit/mediatransportutil','references/go-modules/mediatransportutil-f234b534b095')]:v+='\nreplace '+module+' => '+json.dumps(str((a.root/path).resolve()))+'\n'
f.write_text(v)
