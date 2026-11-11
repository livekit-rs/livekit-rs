#!/usr/bin/env python3
"""Stage original locked server+actual DownTrack/Pion spatial admission observer; no build/run."""
import argparse,hashlib,json,shutil
from pathlib import Path
p=argparse.ArgumentParser();p.add_argument('--root',required=True,type=Path);p.add_argument('--out',required=True,type=Path);a=p.parse_args();root=a.root.resolve();out=a.out.resolve();here=Path(__file__).resolve().parent;ref=root/'references/livekit-stable'
manifest=json.loads((here/'SOURCE.json').read_text());sha=lambda f:hashlib.sha256(f.read_bytes()).hexdigest()
for name,digest in manifest['original_inputs'].items():assert sha(ref/name)==digest,'original input drift: '+name

assert sha(root/'references/go-modules/mediatransportutil-f234b534b095/pkg/codec/keyframe.go')==manifest['codec_keyframe_sha256'],'fixed codec classifier drift'
assert sha(here/'high_only_test.go')==manifest['high_only_observer_sha256'],'observer drift'
assert sha(here/'av1_targets_test.go')==manifest['av1_observer_sha256'],'AV1 observer drift'
assert not out.exists(),'fresh isolated stage required';out.mkdir(parents=True)
shutil.copytree(ref,out/'source',ignore=shutil.ignore_patterns('.git'))
shutil.copyfile(here/'observer_test.go',out/'source/pkg/sfu/source_svc_admission_observer_test.go')
shutil.copyfile(here/'high_only_test.go',out/'source/pkg/sfu/source_high_only_svc_observer_test.go')
shutil.copyfile(here/'av1_targets_test.go',out/'source/pkg/sfu/source_av1_targets_observer_test.go')
mod=out/'source/go.mod';mod.write_text(mod.read_text()+'\nreplace github.com/livekit/protocol => '+json.dumps(str(root/'references/protocol-stable'))+'\nreplace github.com/livekit/mediatransportutil => '+json.dumps(str(root/'references/go-modules/mediatransportutil-f234b534b095'))+'\n')
(out/'STAGED.json').write_text(json.dumps({'status':'STAGED_NOT_RUN','source':manifest,'observer_sha256':sha(here/'observer_test.go'),'algorithm_substitutions':[],'go_mod_staged_sha256':sha(mod)},indent=2)+'\n');print(out)
