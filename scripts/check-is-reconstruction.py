#!/usr/bin/env python3
"""Exact published is0.11.1 -> mapped-host patch -> entire vendored tree.
No Cargo/Git/network/subprocess/temp writes. Restore reference inputs first.
"""
import argparse
import hashlib
import json
from pathlib import Path
import runpy


def main():
    server = Path(__file__).resolve().parent.parent
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--reference-dir', type=Path, default=server/'references/crates/is-0.11.1')
    parser.add_argument('--vendor-dir', type=Path, default=server/'vendor/is')
    parser.add_argument('--patch-parser', type=Path, default=server/'scripts/check-dimpl-reconstruction.py')
    args = parser.parse_args()
    vendor = args.vendor_dir.resolve()
    source_bytes = (vendor/'SOURCE.json').read_bytes()
    source = json.loads(source_bytes)
    digest = lambda b: hashlib.sha256(b).hexdigest()
    reference = args.reference_dir.resolve()
    original = source['original_files_sha256']
    excluded = {'.cargo-ok', '.cargo-checksum.json'}
    paths = {p.relative_to(reference).as_posix(): p for p in reference.rglob('*') if p.is_file() and p.name not in excluded}
    assert set(paths) == set(original), 'original published file set mismatch (restore references first)'
    tree = {name: path.read_bytes() for name, path in paths.items()}
    assert all(digest(tree[name]) == value for name, value in original.items()), 'original published bytes mismatch'
    apply = runpy.run_path(str(args.patch_parser.resolve()))['apply_patch']
    patch_hashes={};touched=set()
    for name in source.get('sequential_patches',['mapped-host.patch']):
        patch=(vendor/name).read_bytes()
        assert digest(patch)==source['metadata_sha256'][name], 'patch checksum mismatch: '+name
        patch_hashes[name]=digest(patch)
        touched.update(apply(tree,patch))
    expected_modified=set(source.get('modified_native_files',['src/candidate.rs','src/agent.rs']))
    assert touched==expected_modified,'unexpected modified paths'
    assert set(tree)==set(source['expected_files_sha256']),'reconstruction file set mismatch'
    for name, data in tree.items():
        assert digest(data) == source['expected_files_sha256'][name], 'declared reconstructed hash mismatch: '+name
        assert data == (vendor/name).read_bytes(), 'vendored bytes differ: '+name
    for name, value in source['metadata_sha256'].items():
        assert digest((vendor/name).read_bytes()) == value, 'metadata/license mismatch: '+name
    actual = {p.relative_to(vendor).as_posix() for p in vendor.rglob('*') if p.is_file() and p.name not in excluded}
    assert actual == set(tree)|set(source['metadata_files']), 'extra/missing vendored file'
    print(json.dumps({'status':'PASS_SOURCE_RECONSTRUCTION_ONLY','package':'is','version':source['upstream_version'],
        'registry_checksum':source['registry_checksum'],'source_sha256':digest(source_bytes),
        'original_file_count':len(original),'sequential_patch_sha256':patch_hashes,'patched_file_count':len(touched),
        'whole_reconstructed_file_count':len(tree),'files':{n:digest(b) for n,b in sorted(tree.items())},
        'scope':'Exact source/patch/license reconstruction only; no ICE/network/SDK/Cargo/runtime claim.'},indent=2))


if __name__ == '__main__':
    main()
