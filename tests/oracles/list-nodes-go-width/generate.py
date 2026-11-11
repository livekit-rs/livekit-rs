#!/usr/bin/env python3
"""Hash-bound source table extraction. No Go/Rust compiler, network or shared writes."""
from pathlib import Path
import argparse, hashlib, json, re
p=argparse.ArgumentParser();p.add_argument('--root',type=Path,required=True);p.add_argument('--displaywidth',type=Path,required=True);p.add_argument('--output',type=Path,required=True);a=p.parse_args();here=Path(__file__).resolve().parent
manifest=json.loads((here/'SOURCE-INPUTS.json').read_text());sha=lambda f:hashlib.sha256(f.read_bytes()).hexdigest()
for rel,want in manifest['repository'].items():assert sha(a.root/rel)==want,rel
for rel,want in manifest['displaywidth'].items():assert sha(a.displaywidth/rel)==want,rel
s=(a.displaywidth/'trie.go').read_text();arrays={}
for name,count in [('stringWidthValues',15744),('stringWidthIndex',1920)]:
 body=s.split('var '+name+' = ',1)[1].split('{',1)[1].split('}',1)[0];body=re.sub(r'//[^\n]*','',body);values=[0]*count
 tokens=[x.strip() for x in body.split(',') if x.strip()];at=0
 for token in tokens:
  if ':'in token:k,v=token.split(':');at=int(k.strip(),0)
  else:v=token
  assert 0<=at<count;values[at]=int(v.strip(),0);assert 0<=values[at]<=255;at+=1
 arrays[name]=values
out=['// Source-derived fixed displaywidth v0.11.0 property trie. See NOTICE and SOURCE-INPUTS.json.\n']
for name,values in arrays.items():
 rust='VALUES' if name=='stringWidthValues' else 'INDEX';out.append(f'pub(super) static {rust}: [u8; {len(values)}] = [\n')
 for n in range(0,len(values),32):out.append('    '+', '.join(str(x) for x in values[n:n+32])+',\n')
 out.append('];\n')
a.output.mkdir(parents=True,exist_ok=True);target=a.output/'go_width_data.rs';target.write_text(''.join(out));print(json.dumps({'output':str(target),'sha256':sha(target),'data_bytes':17664,'source_only':True}))
