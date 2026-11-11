#!/usr/bin/python3
"""Python3.9-compatible streaming verification; no network or image execution."""
import argparse,hashlib,json,pathlib,sys
FIXED={'noble-server-cloudimg-arm64.img':'50e24e1fb79e89116e939d90aa7738f7af48c442fa1bc2510131e0b39c50348e','noble-server-cloudimg-arm64.manifest':'820cdc3ef86f90ed996308913718960b6dbd1bccf8cd7cfad4032d0344f60711'}
def digest(path):
 h=hashlib.sha256()
 with path.open('rb')as source:
  for chunk in iter(lambda:source.read(1024*1024),b''):h.update(chunk)
 return h.hexdigest()
def atomic_json(path,value):
 temporary=path.with_name(path.name+'.new');temporary.write_text(json.dumps(value,indent=2)+'\n');temporary.replace(path)
p=argparse.ArgumentParser();p.add_argument('mode',choices=['metadata','file','final','checkpoint']);p.add_argument('directory');p.add_argument('--name');p.add_argument('--path');p.add_argument('--state');a=p.parse_args();r=pathlib.Path(a.directory)
if a.mode=='checkpoint':
 assert a.state;atomic_json(r/'fetch-checkpoint.json',{'state':a.state,'status':'NOT_BOOTED','profile':'noble-20261017-arm64'});sys.exit(0)
if a.mode in ('metadata','final'):
 sums=dict((line.split()[1].lstrip('*'),line.split()[0])for line in(r/'SHA256SUMS').read_text().splitlines())
 for name,expected in FIXED.items():assert sums.get(name)==expected,('signed metadata differs from pinned input',name)
if a.mode=='file':
 assert a.name in FIXED and a.path
 path=pathlib.Path(a.path)
 if not path.is_file()or path.is_symlink()or digest(path)!=FIXED[a.name]:sys.exit(1)
elif a.mode=='final':
 for name,expected in FIXED.items():
  assert not(r/name).is_symlink();assert digest(r/name)==expected,('download mismatch',name)
 atomic_json(r/'verified-image.json',{'status':'SIGNATURE_AND_SHA256_VERIFIED','files':FIXED})
