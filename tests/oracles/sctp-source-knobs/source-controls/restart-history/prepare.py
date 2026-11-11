#!/usr/bin/env python3
"""Stage byte-identical Pion source/tests plus one restart observer; no execution."""
from pathlib import Path
import argparse,hashlib,json,shutil
p=argparse.ArgumentParser(description=__doc__)
p.add_argument('--root',type=Path,required=True)
p.add_argument('--source-reference',type=Path,required=True)
p.add_argument('--out',type=Path,required=True)
a=p.parse_args();here=Path(__file__).resolve().parent;root=a.root.resolve();ref=a.source_reference.resolve();out=a.out.resolve()
lock=json.loads((here/'pion-source-inputs.json').read_text());sha=lambda f:hashlib.sha256(f.read_bytes()).hexdigest()
for name,value in lock['file_sha256'].items():assert sha(ref/name)==value,'ORIGINAL_PION_SOURCE_DRIFT '+name
assert not out.exists(),'new isolated stage required';out.mkdir(parents=True,mode=0o700);(out/'source').mkdir()
# Include original tests solely for original UDP fixture helpers. None of the
# original source or tests is changed, and only the two named observer tests run.
for name in lock['file_sha256']:
 if name.endswith('.go') or name=='LICENSE' or name.startswith('testdata/'):
  dest=out/'source'/name;dest.parent.mkdir(parents=True,exist_ok=True);shutil.copyfile(ref/name,dest)
fixture=here/'restart_history_test.go';shutil.copyfile(fixture,out/'source/source_restart_history_test.go')
mod=(root/'references/livekit-stable/go.mod').read_text().replace('module github.com/livekit/livekit-server','module local.test/livekit-sctp-restart-observer',1)
mod+='\nreplace github.com/livekit/protocol => '+json.dumps(str(root/'references/protocol-stable'))+'\n'
mod+='replace github.com/livekit/mediatransportutil => '+json.dumps(str(root/'references/go-modules/mediatransportutil-f234b534b095'))+'\n'
(out/'go.mod').write_text(mod);shutil.copyfile(root/'references/livekit-stable/go.sum',out/'go.sum')
(out/'STAGED.json').write_text(json.dumps({'status':'STAGED_NOT_RUN','original_pion_source_zip_sha256':lock['zip_sha256'],'algorithm_substitutions':[],'fixed_server_go_mod_sha256':sha(root/'references/livekit-stable/go.mod'),'observer_source_sha256':sha(fixture),'files':{str(f.relative_to(out)):sha(f)for f in out.rglob('*')if f.is_file()}},indent=2)+'\n')
print(out)
