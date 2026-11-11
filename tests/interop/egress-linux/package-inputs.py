#!/usr/bin/python3
"""Coordinator-run source archive, no Git/compilers, no target/node_modules/cache."""
import argparse,hashlib,json,pathlib,tarfile
p=argparse.ArgumentParser();p.add_argument('--repo',required=True);p.add_argument('--out',required=True);a=p.parse_args()
r=pathlib.Path(a.repo).resolve();out=pathlib.Path(a.out).resolve();assert not out.exists()
roots=['Cargo.toml','Cargo.lock','rust-toolchain.toml','.cargo','crates','vendor','tests','examples','references/egress'];skip={'.git','target','node_modules','output','.DS_Store','__pycache__'}
files=[]
for root in roots:
 source=r/root
 for f in ([source] if source.is_file() else source.rglob('*')):
  if any(x in skip for x in f.relative_to(r).parts):continue
  if f.is_symlink():raise SystemExit('source symlink requires explicit review: '+str(f.relative_to(r)))
  if f.is_file():files.append(f)
files.sort(key=lambda f:str(f.relative_to(r)))
def streaming_sha256(path):
 h=hashlib.sha256()
 with path.open('rb') as source:
  for chunk in iter(lambda:source.read(1024*1024),b''):h.update(chunk)
 return h.hexdigest()
manifest={str(f.relative_to(r)):streaming_sha256(f) for f in files}
with tarfile.open(out,'w:gz')as t:
 for f in files:t.add(f,arcname=str(f.relative_to(r)),recursive=False)
pathlib.Path(str(out)+'.json').write_text(json.dumps({'archive_sha256':streaming_sha256(out),'files_sha256':manifest},indent=2)+'\n')
print('prepared exact source files:',len(files))
