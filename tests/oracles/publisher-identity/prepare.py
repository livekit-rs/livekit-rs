#!/usr/bin/env python3
"""Stage the fixed original Go and observer without running any program."""
import argparse,hashlib,json,shutil
from pathlib import Path
p=argparse.ArgumentParser();p.add_argument('--root',type=Path,required=True);p.add_argument('--out',type=Path,required=True);a=p.parse_args()
here=Path(__file__).resolve().parent;ref=a.root/'references/livekit-stable';manifest=json.loads((here/'SOURCE.json').read_text());sha=lambda p:hashlib.sha256(p.read_bytes()).hexdigest()
for name,h in manifest['original_inputs'].items():assert sha(ref/name)==h,name
assert sha(here/'publisher_identity_observer_test.go')==manifest['observer_sha256']
assert not a.out.exists();a.out.mkdir(parents=True);shutil.copytree(ref,a.out/'source',ignore=shutil.ignore_patterns('.git'));shutil.copyfile(here/'publisher_identity_observer_test.go',a.out/'source/pkg/rtc/source_publisher_identity_observer_test.go')
p=a.out/'source/go.mod';p.write_text(p.read_text()+'\nreplace github.com/livekit/protocol => '+json.dumps(str((a.root/'references/protocol-stable').resolve()))+'\nreplace github.com/livekit/mediatransportutil => '+json.dumps(str((a.root/'references/go-modules/mediatransportutil-f234b534b095').resolve()))+'\n')
(a.out/'STAGED.json').write_text(json.dumps({'status':'STAGED_NOT_RUN','source':manifest,'go_mod_sha256':sha(p)},indent=2)+'\n')
