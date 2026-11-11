#!/usr/bin/env python3
"""Byte-exact Axum/h3-quinn published input + native patch replay; no Cargo/network."""
from pathlib import Path
import hashlib
import importlib.util
import json


def main():
    server = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location('exact_patch_parser', server / 'scripts/check-dimpl-reconstruction.py')
    parser = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(parser)
    sha = lambda value: hashlib.sha256(value).hexdigest()
    results = {}
    for package in ['axum', 'h3-quinn']:
        vendor = server / 'vendor' / package
        source = json.loads((vendor / 'SOURCE.json').read_text())
        reference = server / source['reference']
        files = {str(path.relative_to(reference)): path.read_bytes() for path in reference.rglob('*') if path.is_file()}
        assert len(files) == source['original_file_count']
        assert {name: sha(data) for name, data in files.items()} == source['original_files_sha256']
        patch = (vendor / source['patch']).read_bytes()
        assert sha(patch) == source['patch_sha256']
        touched = parser.apply_patch(files, patch)
        assert {name: sha(data) for name, data in files.items()} == source['exact_files_sha256']
        for name, data in files.items():
            assert data == (vendor / name).read_bytes(), name
        expected = set(files) | {'SOURCE.json', source['patch']}
        actual = {str(path.relative_to(vendor)) for path in vendor.rglob('*') if path.is_file()}
        assert actual == expected, f'{package}: undeclared vendor inputs'
        results[package] = {'status': 'PASS', 'version': source['version'], 'original_files': len(files),
                            'modified_files': sorted(touched), 'patch_sha256': source['patch_sha256']}
    print(json.dumps({'status': 'PASS_SOURCE_RECONSTRUCTION_ONLY', 'packages': results,
                      'scope': 'Pure byte-exact source reconstruction; no build, services, runtime or network.'}, indent=2))


if __name__ == '__main__':
    main()
