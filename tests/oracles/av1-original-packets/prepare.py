#!/usr/bin/env python3
"""Stage exact original source and controlled source-bound observers; does not run Go."""
import argparse,hashlib,json,shutil
from pathlib import Path
p=argparse.ArgumentParser();p.add_argument('--root',required=True,type=Path);p.add_argument('--out',required=True,type=Path);a=p.parse_args();here=Path(__file__).resolve().parent;ref=a.root/'references/livekit-stable';assert not a.out.exists();a.out.mkdir(parents=True);sha=lambda p:hashlib.sha256(p.read_bytes()).hexdigest();manifest=json.loads((here/'SOURCE.json').read_text())
for name,digest in manifest['original_inputs'].items():assert sha(ref/name)==digest,name
for name,digest in manifest['helpers'].items():assert sha(here/name)==digest,name
shutil.copytree(ref,a.out/'source',ignore=shutil.ignore_patterns('.git'))
for name,target in {'munger_observer_test.go':'pkg/sfu/source_original_packet_munger_test.go','wrapper_observer_test.go':'pkg/sfu/videolayerselector/source_original_packet_wrapper_test.go','chain_observer_test.go':'pkg/sfu/videolayerselector/source_original_packet_chain_test.go','wire_shapes_observer_test.go':'pkg/sfu/videolayerselector/source_original_dd_wire_shapes_test.go','physical_helpers_test.go':'pkg/sfu/source_original_packet_physical_helpers_test.go','physical_observer_test.go':'pkg/sfu/source_original_packet_physical_observer_test.go'}.items():shutil.copyfile(here/name,a.out/'source'/target)
p=a.out/'source/go.mod';p.write_text(p.read_text()+'\nreplace github.com/livekit/protocol => '+json.dumps(str((a.root/'references/protocol-stable').resolve()))+'\nreplace github.com/livekit/mediatransportutil => '+json.dumps(str((a.root/'references/go-modules/mediatransportutil-f234b534b095').resolve()))+'\n');(a.out/'STAGED.json').write_text(json.dumps({'status':'STAGED_NOT_RUN','source':manifest,'go_mod_sha256':sha(p)},indent=2)+'\n')
