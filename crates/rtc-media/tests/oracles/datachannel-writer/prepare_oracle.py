#!/usr/bin/env python3
import argparse,hashlib,json,shutil
from pathlib import Path
p=argparse.ArgumentParser();p.add_argument('--root',type=Path,required=True);p.add_argument('--out',type=Path,required=True);a=p.parse_args();here=Path(__file__).resolve().parent
s=json.loads((here/'SOURCE.json').read_text());sha=lambda p:hashlib.sha256(p.read_bytes()).hexdigest()
for name,h in s['source_sha256'].items():assert sha(a.root/name)==h,'SOURCE_DRIFT '+name
for name,h in s['oracle_inputs_sha256'].items():assert sha(here/name)==h,'INPUT_DRIFT '+name
assert not a.out.exists();a.out.mkdir(mode=0o700);(a.out/'source').mkdir()
for name in ['datachannel_writer.go','bitrate.go']:
 text=(a.root/'references/livekit-stable/pkg/sfu/datachannel'/name).read_text()
 text=text.replace('"github.com/livekit/protocol/utils/mono"','')
 text=text.replace('mono.Now()','sourceNow()').replace('time.Now()','sourceNow()')
 (a.out/'source'/name).write_text(text)
shutil.copyfile(here/'oracle/driver.go',a.out/'source/driver.go');shutil.copyfile(here/'oracle/main.go',a.out/'main.go');shutil.copyfile(here/'oracle/cases.jsonl',a.out/'cases.jsonl')
mod=(a.root/'references/livekit-stable/go.mod').read_text().replace('module github.com/livekit/livekit-server','module local.test/livekit-data-writer-oracle',1)
mod+='\nreplace github.com/livekit/protocol => '+json.dumps(str((a.root/'references/protocol-stable').resolve()))+'\n'
(a.out/'go.mod').write_text(mod);shutil.copyfile(a.root/'references/livekit-stable/go.sum',a.out/'go.sum')
(a.out/'STAGED.json').write_text(json.dumps({'status':'STAGED_NOT_EXECUTED','source_manifest':sha(here/'SOURCE.json'),'changes':'clock calls only replaced with explicit supplied Instant-equivalent clock; package source logic unchanged','files':{str(f.relative_to(a.out)):sha(f) for f in a.out.rglob('*') if f.is_file()}},indent=2)+'\n')
print(a.out)
