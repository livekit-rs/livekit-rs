#!/usr/bin/env python3
from pathlib import Path
import argparse,json,hashlib,shutil
p=argparse.ArgumentParser();p.add_argument('--root',type=Path,required=True);p.add_argument('--sctp-source',type=Path,required=True);p.add_argument('--out',type=Path,required=True);a=p.parse_args();here=Path(__file__).resolve().parent;s=json.loads((here/'pion-source-inputs.json').read_text());sha=lambda p:hashlib.sha256(p.read_bytes()).hexdigest()
for path,h in s['file_sha256'].items():assert sha(a.sctp_source/path)==h,'SCTP_SOURCE_DRIFT '+path
lock=json.loads((here/'oracle-source-lock.json').read_text())
for path,h in lock['reference_inputs'].items():assert sha(a.root/path)==h,'REFERENCE_DRIFT '+path
for path,h in lock['oracle_inputs'].items():assert sha(here/path)==h,'ORACLE_INPUT_DRIFT '+path
assert not a.out.exists();a.out.mkdir(mode=0o700);(a.out/'source').mkdir();(a.out/'settings').mkdir()
# Original algorithm package under the same locked fixed-server MVS. Source has
# no imports of its own module/internal packages, so no package-path rewrite.
for f in a.sctp_source.rglob('*.go'):
 if not f.name.endswith('_test.go'):
  dst=a.out/'source'/f.relative_to(a.sctp_source);dst.parent.mkdir(parents=True,exist_ok=True);shutil.copyfile(f,dst)
shutil.copyfile(here/'oracle/knobs_test.go',a.out/'source/source_knobs_test.go');shutil.copyfile(here/'oracle/settings/main.go',a.out/'settings/main.go');shutil.copyfile(here/'oracle/cases.jsonl',a.out/'cases.jsonl');shutil.copyfile(a.sctp_source/'LICENSE',a.out/'source/LICENSE')
mod=(a.root/'references/livekit-stable/go.mod').read_text().replace('module github.com/livekit/livekit-server','module local.test/livekit-sctp-source-knobs',1)
mod+='\nreplace github.com/livekit/protocol => '+json.dumps(str((a.root/'references/protocol-stable').resolve()))+'\n'
mod+='replace github.com/livekit/mediatransportutil => '+json.dumps(str((a.root/'references/go-modules/mediatransportutil-f234b534b095').resolve()))+'\n'
(a.out/'go.mod').write_text(mod);shutil.copyfile(a.root/'references/livekit-stable/go.sum',a.out/'go.sum')
(a.out/'STAGED.json').write_text(json.dumps({'status':'STAGED_NOT_RUN','sctp_version':s['version'],'source_zip_sha256':s['zip_sha256'],'files':{str(f.relative_to(a.out)):sha(f) for f in a.out.rglob('*') if f.is_file()}},indent=2)+'\n');print(a.out)
