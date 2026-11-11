#!/usr/bin/env python3
"""Stage original locked server+one original RTPStatsSender observer; no build/run."""
import argparse,hashlib,json,shutil
from pathlib import Path
p=argparse.ArgumentParser();p.add_argument('--root',required=True,type=Path);p.add_argument('--out',required=True,type=Path);a=p.parse_args();root=a.root.resolve();out=a.out.resolve();here=Path(__file__).resolve().parent;ref=root/'references/livekit-stable'
manifest=json.loads((here/'SOURCE.json').read_text());sha=lambda f:hashlib.sha256(f.read_bytes()).hexdigest()
for name,digest in manifest['original_inputs'].items():assert sha(ref/name)==digest,'original input drift: '+name
assert sha(root/'references/go-modules/mediatransportutil-f234b534b095/helpers.go')==manifest['helper_sha256'],'original RTT helper drift'
assert not out.exists(),'fresh isolated stage required';out.mkdir(parents=True)
shutil.copytree(ref,out/'source',ignore=shutil.ignore_patterns('.git'))
shutil.copyfile(here/'observer_test.go',out/'source/pkg/sfu/rtpstats/source_rpc_rtt_observer_test.go')
mod=out/'source/go.mod';mod.write_text(mod.read_text()+'\nreplace github.com/livekit/protocol => '+json.dumps(str(root/'references/protocol-stable'))+'\nreplace github.com/livekit/mediatransportutil => '+json.dumps(str(root/'references/go-modules/mediatransportutil-f234b534b095'))+'\n')
(out/'STAGED.json').write_text(json.dumps({'status':'STAGED_NOT_RUN','source':manifest,'observer_sha256':sha(here/'observer_test.go'),'algorithm_substitutions':[],'go_mod_staged_sha256':sha(mod)},indent=2)+'\n');print(out)
