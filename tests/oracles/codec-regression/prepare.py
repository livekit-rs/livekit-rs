#!/usr/bin/env python3
"""Stage an unmodified fixed LiveKit source copy plus two observer tests. No build/run."""
from pathlib import Path
import argparse,hashlib,json,shutil
p=argparse.ArgumentParser(description=__doc__);p.add_argument('--with-window-controls',action='store_true');p.add_argument('--root',required=True,type=Path);p.add_argument('--out',required=True,type=Path);a=p.parse_args()
root=a.root.resolve();ref=root/'references/livekit-stable';here=Path(__file__).resolve().parent;out=a.out.resolve()
sha=lambda f:hashlib.sha256(f.read_bytes()).hexdigest()
lock=json.loads((here/'original-inputs.json').read_text())
for name,digest in lock['files'].items():assert sha(ref/name)==digest,'fixed source drift: '+name
assert not out.exists(),'new isolated stage required';out.mkdir(parents=True)
shutil.copytree(ref,out/'source',ignore=shutil.ignore_patterns('.git'))
for package in ['rtc','service']:
 shutil.copyfile(here/(package+'_observer_test.go'),out/'source/pkg'/package/'source_codec_observer_test.go')
if a.with_window_controls:
 for fixture,destination in [('rtc_window_observer_test.go','pkg/rtc/source_codec_window_observer_test.go'),('source_connection_snapshot.go','pkg/sfu/connectionquality/source_codec_snapshot.go'),('source_receiver_snapshot.go','pkg/sfu/source_codec_snapshot.go')]:
  shutil.copyfile(here/fixture,out/'source'/destination)
mod=out/'source/go.mod';mod.write_text(mod.read_text()+'\nreplace github.com/livekit/protocol => '+json.dumps(str(root/'references/protocol-stable'))+'\nreplace github.com/livekit/mediatransportutil => '+json.dumps(str(root/'references/go-modules/mediatransportutil-f234b534b095'))+'\n')
(out/'STAGED.json').write_text(json.dumps({'status':'STAGED_NOT_RUN','upstream_commit':lock['commit'],'original_inputs':lock['files'],'algorithm_substitutions':[],'window_control_observers':{name:sha(here/name) for name in ['rtc_window_observer_test.go','source_connection_snapshot.go','source_receiver_snapshot.go']} if a.with_window_controls else {},'added_observers':{package:sha(here/(package+'_observer_test.go')) for package in ['rtc','service']},'go_mod_original_sha256':lock['files']['go.mod'],'go_mod_staged_sha256':sha(mod)},indent=2)+'\n')
print(out)
