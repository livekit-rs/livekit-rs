#!/usr/bin/env python3
import argparse,pathlib,json,hashlib
p=argparse.ArgumentParser();p.add_argument('--root',type=pathlib.Path,required=True);p.add_argument('--manifest',type=pathlib.Path,required=True);a=p.parse_args();a.root=a.root.resolve();m=json.loads(a.manifest.read_text())
for rel,want in m['files_sha256'].items():
 name=pathlib.PurePosixPath(rel);assert not name.is_absolute() and '..' not in name.parts,rel
 f=a.root/rel;assert not f.is_symlink() and f.is_file(),rel
 for ancestor in f.parents:
  if ancestor==a.root.resolve():break
  assert not ancestor.is_symlink(),rel
 assert hashlib.sha256(f.read_bytes()).hexdigest()==want,rel
print('verified',len(m['files_sha256']),'files')
