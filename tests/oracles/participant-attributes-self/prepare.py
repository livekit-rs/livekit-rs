from pathlib import Path
import argparse,json,hashlib,shutil
p=argparse.ArgumentParser();p.add_argument('--root',type=Path,required=True);p.add_argument('--out',type=Path,required=True);a=p.parse_args();d=Path(__file__).parent;s=json.loads((d/'SOURCE.json').read_text());h=lambda p:hashlib.sha256(p.read_bytes()).hexdigest()
for name,v in s['fixed_inputs_sha256'].items():assert h(a.root/name)==v,name
for name,v in s['assets_sha256'].items():assert h(d/name)==v,name
assert not a.out.exists();shutil.copytree(a.root/'references/livekit-stable',a.out,ignore=shutil.ignore_patterns('.git'));shutil.copyfile(d/'source_self_attributes_test.go',a.out/'pkg/rtc/source_self_nonempty_attributes_observer_test.go')
f=a.out/'go.mod';mod=f.read_text()
for module,path in [('github.com/livekit/protocol',a.root/'references/protocol-stable'),('github.com/livekit/mediatransportutil',a.root/'references/go-modules/mediatransportutil-f234b534b095')]:mod+='\nreplace '+module+' => '+json.dumps(str(path.resolve()))+'\n'
f.write_text(mod);print(a.out)
