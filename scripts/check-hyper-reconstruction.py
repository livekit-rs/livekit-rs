#!/usr/bin/env python3
"""Verify the exact published Hyper archive and the complete vendored patch chain."""
import argparse
import hashlib
import json
from pathlib import Path
import runpy
import tarfile


def digest(value):
    return hashlib.sha256(value).hexdigest()


def main():
    server = Path(__file__).resolve().parent.parent
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--archive", required=True, type=Path, help="original hyper-1.12.0.crate")
    args = parser.parse_args()
    vendor = server / "vendor/hyper"
    source = json.loads((vendor / "SOURCE.json").read_bytes())
    assert digest(args.archive.read_bytes()) == source["published_archive_sha256"]
    expected = source["original_files_sha256"]
    prefix = f'{source["package"]}-{source["version"]}/'
    tree = {}
    with tarfile.open(args.archive, "r:gz") as archive:
        for item in archive:
            assert item.isfile() and item.name.startswith(prefix), item.name
            name = item.name.removeprefix(prefix)
            assert name in expected and name not in tree, name
            data = archive.extractfile(item).read()
            assert digest(data) == expected[name], name
            tree[name] = data
    assert len(tree) == source["original_file_count"] and set(tree) == set(expected)
    apply = runpy.run_path(str(server / "scripts/check-dimpl-reconstruction.py"))["apply_patch"]
    touched = set()
    patch_names = set()
    for record in source["patches"]:
        name = record["name"]
        assert name not in patch_names
        patch_names.add(name)
        patch = (vendor / name).read_bytes()
        assert digest(patch) == record["sha256"], name
        touched |= apply(tree, patch)
    assert touched == set(source["patched_files_sha256"])
    assert set(tree) - set(expected) == set(source["new_files"])
    assert len(tree) == source["patched_file_count"]
    for name, data in tree.items():
        declared = source["patched_files_sha256"].get(name, expected.get(name))
        assert digest(data) == declared and data == (vendor / name).read_bytes(), name
    actual = {path.relative_to(vendor).as_posix() for path in vendor.rglob("*") if path.is_file()}
    assert actual == set(tree) | patch_names | {"SOURCE.json"}
    print(f"Verified {len(tree)} Hyper files from {len(patch_names)} exact patches.")


if __name__ == "__main__":
    main()
