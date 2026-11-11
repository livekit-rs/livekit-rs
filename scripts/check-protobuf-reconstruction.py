#!/usr/bin/env python3
"""Offline byte-exact Protobuf patches from reproducibly restored references."""
import argparse
import hashlib
import importlib.util
import json
import io
import tarfile
import tempfile
import urllib.request
from pathlib import Path, PurePosixPath

SERVER = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location("exact_patch", SERVER / "scripts/check-dimpl-reconstruction.py")
patches = importlib.util.module_from_spec(spec)
spec.loader.exec_module(patches)
sha = lambda data: hashlib.sha256(data).hexdigest()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--reference-root", type=Path, default=SERVER / "references")
    parser.add_argument("--restore-reference", action="store_true", help="Restore only missing exact original Protobuf packages; never overwrite")
    args = parser.parse_args()
    lock = json.loads((SERVER / "references/0001-references-lock.json").read_text())
    for name in ("prost-reflect", "prost-derive"):
        vendor = SERVER / "vendor" / name
        source = json.loads((vendor / "SOURCE.json").read_text())
        rows = [v for v in lock["published_package_reviews"] if v["crate"] == name and v["version"] == source["version"]]
        assert len(rows) == 1, "exact original package review is required"
        row = rows[0]
        relative = Path(row["path"]).relative_to("references")
        reference = (args.reference_root / relative).resolve()
        assert args.reference_root.resolve() in reference.parents
        if not reference.exists() and args.restore_reference:
            prefix = name + "-" + source["version"]
            url = "https://static.crates.io/crates/" + name + "/" + prefix + ".crate"
            with urllib.request.urlopen(url, timeout=60) as response:
                raw = response.read(4 * 1024 * 1024 + 1)
            assert len(raw) <= 4 * 1024 * 1024 and sha(raw) == source["archive_sha256"]
            expected = {v["path"]: v["sha256"] for v in row["files"]}
            reference.parent.mkdir(parents=True, exist_ok=True)
            with tempfile.TemporaryDirectory(prefix=".protobuf-restore-", dir=reference.parent) as temp:
                stage = Path(temp) / "package"
                stage.mkdir()
                seen = set()
                total = 0
                with tarfile.open(fileobj=io.BytesIO(raw), mode="r:gz") as archive:
                    for member in archive:
                        path = PurePosixPath(member.name)
                        assert not path.is_absolute() and ".." not in path.parts and path.parts[0] == prefix
                        if member.isdir():
                            continue
                        assert member.isfile() and len(path.parts) > 1
                        relative = PurePosixPath(*path.parts[1:]).as_posix()
                        assert relative in expected and relative not in seen
                        total += member.size
                        assert total <= 16 * 1024 * 1024
                        data = archive.extractfile(member).read()
                        assert len(data) == member.size and sha(data) == expected[relative]
                        dest = stage / relative
                        dest.parent.mkdir(parents=True, exist_ok=True)
                        dest.write_bytes(data)
                        seen.add(relative)
                assert seen == set(expected)
                assert not reference.exists(), "refusing to overwrite existing reference"
                stage.rename(reference)
        tree = {}
        for item in row["files"]:
            path = Path(patches.safe_path(item["path"]))
            original = reference / path
            assert original.is_file() and not original.is_symlink()
            data = original.read_bytes()
            assert sha(data) == item["sha256"], str(path)
            tree[str(path)] = data
        assert len(tree) == source["published_file_count"]
        # A restored source tree is sufficient: references/fetch.py restores
        # these exact files on a clean clone without retaining the .crate zip.
        archive = args.reference_root / "crates" / Path(source["archive"]).name
        if archive.exists():
            assert sha(archive.read_bytes()) == source["archive_sha256"]
        patch = (vendor / source["patch"]).read_bytes()
        assert sha(patch) == source["patch_sha256"]
        touched = patches.apply_patch(tree, patch)
        assert touched == set(source["modified_files"])
        assert {p: sha(v) for p, v in tree.items()} == source["exact_files_sha256"]
        actual = {str(p.relative_to(vendor)): p.read_bytes() for p in vendor.rglob("*")
                  if p.is_file() and str(p.relative_to(vendor)) not in source["metadata_files"]}
        assert actual == tree, "vendored contents differ from exact reconstruction"
        print(json.dumps({"package": name, "reconstructed": len(tree), "modified": len(touched), "status": "EXACT_BYTES_ONLY_PASS"}))


if __name__ == "__main__":
    main()
