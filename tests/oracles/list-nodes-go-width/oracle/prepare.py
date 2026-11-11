from pathlib import Path
import argparse, hashlib, json, shutil, re
p=argparse.ArgumentParser();p.add_argument('--root',type=Path,required=True);p.add_argument('--displaywidth',type=Path,required=True);p.add_argument('--stage',type=Path,required=True);a=p.parse_args();here=Path(__file__).resolve().parent.parent;m=json.loads((here/'SOURCE-INPUTS.json').read_text());h=lambda f:hashlib.sha256(f.read_bytes()).hexdigest()
for q,want in m['repository'].items():assert h(a.root/q)==want,q
for q,want in m['displaywidth'].items():assert h(a.displaywidth/q)==want,q
assert not a.stage.exists();a.stage.mkdir(parents=True)
mod=(a.root/'references/livekit-stable/go.mod').read_text();mod=re.sub(r'^module [^\n]+','module list-nodes-width-source',mod,count=1);mod+='\nreplace github.com/clipperhouse/displaywidth => '+json.dumps(str(a.displaywidth.resolve()))+'\n';mod+='\nreplace github.com/olekukonko/tablewriter => '+json.dumps(str((a.root/'references/go-cli/github.com/olekukonko/tablewriter@v1.1.4').resolve()))+'\n';(a.stage/'go.mod').write_text(mod);shutil.copyfile(a.root/'references/livekit-stable/go.sum',a.stage/'go.sum');shutil.copyfile(here/'oracle/main.go',a.stage/'main.go');shutil.copyfile(here/'oracle/cases.json',a.stage/'cases.json');shutil.copyfile(here/'oracle/scalar-main.go',a.stage/'scalar-main.go');print('PREPARED_ONLY_NO_GO_COMPILATION_OR_EXECUTION')
