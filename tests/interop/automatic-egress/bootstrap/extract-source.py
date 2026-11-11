#!/usr/bin/env python3
"""Coordinator-run exact manifest extraction; no owner restoration or overwrite."""
import argparse,hashlib,json,os,pathlib,tarfile
p=argparse.ArgumentParser();p.add_argument('--archive',type=pathlib.Path,required=True);p.add_argument('--manifest',type=pathlib.Path,required=True);p.add_argument('--root',type=pathlib.Path,required=True);a=p.parse_args();m=json.loads(a.manifest.read_text());root=a.root.resolve()
assert os.getuid()!=0,'extract as the owned fixture user, never sudo'
assert a.archive.is_file() and not a.archive.is_symlink();assert root.is_dir() and root.stat().st_uid==os.getuid()
expected=m['files_sha256'];assert isinstance(expected,dict) and expected
for name in expected:
 rel=pathlib.PurePosixPath(name);assert not rel.is_absolute() and '..' not in rel.parts
 dest=root/name;assert not dest.exists() and not dest.is_symlink(),('refuse overwrite',name)
 for ancestor in dest.parents:
  if ancestor==root:break
  assert not ancestor.is_symlink(),name
with a.archive.open('rb') as file:
 digest=hashlib.sha256()
 for chunk in iter(lambda:file.read(1024*1024),b''):digest.update(chunk)
 assert digest.hexdigest()==m['archive_sha256'],'archive bytes drift'
 file.seek(0)
 with tarfile.open(fileobj=file,mode='r:gz') as archive:
  members=archive.getmembers();assert len(members)==len(expected) and len({x.name for x in members})==len(expected)
  assert {x.name for x in members}==set(expected)
  for member in members:assert member.isfile(),member.name
  for member in members:
   dest=root/member.name;dest.parent.mkdir(parents=True,exist_ok=True,mode=0o700)
   fd=os.open(dest,os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o600);digest=hashlib.sha256()
   with os.fdopen(fd,'wb') as out,archive.extractfile(member) as source:
    for chunk in iter(lambda:source.read(1024*1024),b''):digest.update(chunk);out.write(chunk)
   assert digest.hexdigest()==expected[member.name],('extracted file drift',member.name)
   dest.chmod((member.mode & 0o700)|0o400)
print('ACTUAL_ARCHIVE_AND_EXTRACTED_FILES_VERIFIED',len(expected))
