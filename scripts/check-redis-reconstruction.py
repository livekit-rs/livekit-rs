#!/usr/bin/env python3
"""Offline exact redis archive plus patch reconstruction; no Cargo or services."""
import hashlib
import importlib.util
import io
import json
import tarfile
from pathlib import Path, PurePosixPath

SERVER = Path(__file__).resolve().parents[1]
VENDOR = SERVER / "vendor/redis"
sha = lambda value: hashlib.sha256(value).hexdigest()

def main():
    source = json.loads((VENDOR / "SOURCE.json").read_text())
    archive = (VENDOR / source["archive"]).read_bytes()
    assert sha(archive) == source["archive_sha256"]
    tree = {}
    with tarfile.open(fileobj=io.BytesIO(archive), mode="r:gz") as tar:
        for member in tar:
            path = PurePosixPath(member.name)
            assert not path.is_absolute() and ".." not in path.parts
            assert path.parts[0] == source["package"] + "-" + source["version"]
            if member.isdir():
                continue
            assert member.isfile() and len(path.parts) > 1
            relative = PurePosixPath(*path.parts[1:]).as_posix()
            assert relative not in tree
            tree[relative] = tar.extractfile(member).read()
    assert len(tree) == source["published_file_count"]
    patch = (VENDOR / source["patch"]).read_bytes()
    assert sha(patch) == source["patch_sha256"]
    spec = importlib.util.spec_from_file_location("exact_patch", SERVER / "scripts/check-dimpl-reconstruction.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    assert module.apply_patch(tree, patch) == set(source["modified_files"])
    assert {path: sha(data) for path, data in tree.items()} == source["exact_files_sha256"]
    actual = {str(path.relative_to(VENDOR)): path.read_bytes() for path in VENDOR.rglob("*")
              if path.is_file() and str(path.relative_to(VENDOR)) not in source["metadata_files"]}
    assert actual == tree
    print(json.dumps({"package":"redis", "status":"EXACT_BYTES_ONLY_PASS", "files":len(tree)}))

if __name__ == "__main__":
    main()
