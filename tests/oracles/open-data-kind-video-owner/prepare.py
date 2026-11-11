from pathlib import Path
import argparse,json,hashlib,shutil
p=argparse.ArgumentParser();p.add_argument('--root',type=Path,required=True);p.add_argument('--out',type=Path,required=True);a=p.parse_args();here=Path(__file__).resolve().parent
for n,v in json.loads((here/'fixed-source.json').read_text()).items():assert hashlib.sha256((a.root/n).read_bytes()).hexdigest()==v,n
assert not a.out.exists();shutil.copytree(a.root/'references/livekit-stable',a.out,ignore=shutil.ignore_patterns('.git'))
for n in ['source_open_data_kind_observer_test.go','update_video_owner_observer_test.go','source_functional_observer_init_test.go']:shutil.copy2(here/n,a.out/'pkg/rtc'/n)
f=a.out/'go.mod';mod=f.read_text()
for name,rel in [('github.com/livekit/protocol','references/protocol-stable'),('github.com/livekit/mediatransportutil','references/go-modules/mediatransportutil-f234b534b095'),('github.com/livekit/psrpc','references/psrpc')]:mod+='\nreplace '+name+' => '+json.dumps(str((a.root/rel).resolve()))+'\n'
f.write_text(mod);sums=set()
for d in [a.out,a.root/'references/protocol-stable',a.root/'references/psrpc',a.root/'references/go-modules/mediatransportutil-f234b534b095']:sums.update((d/'go.sum').read_text().splitlines())
(a.out/'go.sum').write_text('\n'.join(sorted(sums))+'\n')
