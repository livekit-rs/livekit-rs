#!/usr/bin/env python3
"""Pure byte-exact published SCTP source + patch replay; never Git/Cargo/network."""
from pathlib import Path
import argparse,hashlib,importlib.util,json

def main():
    server=Path(__file__).resolve().parent.parent
    a=argparse.ArgumentParser(description=__doc__)
    a.add_argument('--reference-dir',type=Path,default=server/'references/crates/sctp-proto-0.10.7')
    a.add_argument('--vendor-dir',type=Path,default=server/'vendor/sctp-proto')
    a.add_argument('--patch-parser',type=Path,default=server/'scripts/check-dimpl-reconstruction.py')
    args=a.parse_args();sha=lambda b:hashlib.sha256(b).hexdigest()
    v=args.vendor_dir;r=args.reference_dir;s=json.loads((v/'SOURCE.json').read_text())
    files={str(p.relative_to(r)):p.read_bytes() for p in r.rglob('*') if p.is_file() and p.name not in {'.cargo-ok','.cargo-checksum.json'}}
    assert set(files)==set(s['upstream_files_sha256']) and len(files)==s['upstream_file_count']
    for name,data in files.items():assert sha(data)==s['upstream_files_sha256'][name],name
    spec=importlib.util.spec_from_file_location('exact_patch_parser',args.patch_parser);m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)
    patches=s.get('sequential_patches',[{'patch':s['patch'],'sha256':s['patch_sha256']}])
    touched=set();patch_hashes={}
    for item in patches:
        assert Path(item['patch']).name==item['patch']
        patch=(v/item['patch']).read_bytes();assert sha(patch)==item['sha256']
        patch_hashes[item['patch']]=sha(patch)
        touched.update(m.apply_patch(files,patch))
    assert touched==set(s['modified_native_files'])
    assert set(files)==set(s['exact_files_sha256'])
    for name,data in files.items():assert data==(v/name).read_bytes() and sha(data)==s['exact_files_sha256'][name],name
    for name,h in s['license_sha256'].items():assert sha((v/name).read_bytes())==h,name
    actual={str(p.relative_to(v)) for p in v.rglob('*') if p.is_file() and p.name not in {'.cargo-ok','.cargo-checksum.json'}}
    assert actual==set(files)|set(s['metadata_files']),'undeclared extra/missing vendor input'
    print(json.dumps({'status':'PASS_SOURCE_RECONSTRUCTION_ONLY','original_files':s['upstream_file_count'],'result_files':len(files),'modified_source_files':len(touched),'sequential_patch_sha256':patch_hashes}))
if __name__=='__main__':main()
