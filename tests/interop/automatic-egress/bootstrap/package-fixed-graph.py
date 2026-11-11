#!/usr/bin/env python3
"""Fresh original fixed server/module source inputs; no source rewriting or build."""
import argparse,pathlib,tarfile,json,hashlib,os
p=argparse.ArgumentParser();p.add_argument('--repo',type=pathlib.Path,required=True);p.add_argument('--out',type=pathlib.Path,required=True);a=p.parse_args();os.umask(0o077);assert not a.out.exists() and not a.out.is_symlink() and not pathlib.Path(str(a.out)+'.json').exists() and not pathlib.Path(str(a.out)+'.json').is_symlink();assert a.out.parent.is_dir()
roots=['references/livekit-stable','references/protocol-stable','references/psrpc','references/go-modules/mediatransportutil-f234b534b095'];items=[]
for name in roots:
 assert (a.repo/name).is_dir(),name
 for f in (a.repo/name).rglob('*'):
  rel=f.relative_to(a.repo)
  if any(x in {'.git','node_modules','target','output','__pycache__'} for x in rel.parts):continue
  assert not f.is_symlink(),str(rel)
  if f.is_file():items.append((str(rel),f))
h=lambda f:hashlib.sha256(f.read_bytes()).hexdigest()
with tarfile.open(a.out,'w:gz') as t:
 for rel,f in sorted(items):t.add(f,arcname=rel,recursive=False)
pathlib.Path(str(a.out)+'.json').write_text(json.dumps({'archive_sha256':h(a.out),'files_sha256':{rel:h(f) for rel,f in sorted(items)},'module_policy':'Original server go.mod/go.sum unchanged; original Go1.26.3 graph, no guessed replacements'},indent=2)+'\n')
