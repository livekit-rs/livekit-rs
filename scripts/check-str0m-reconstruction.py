#!/usr/bin/env python3
"""Rebuild vendored str0m from local published inputs; never invokes Cargo.

Only a self-cleaning temporary directory is written. JSON evidence goes to stdout.
"""
import argparse
import hashlib
import json
from pathlib import Path
import re
import shutil
import subprocess
import tempfile


def digest(data):
    return hashlib.sha256(data).hexdigest()


def check(condition, message):
    if not condition:
        raise ValueError(message)


def main():
    server = Path(__file__).resolve().parent.parent
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--reference-dir", type=Path,
                        default=server / "references/crates/str0m-0.24.1")
    args = parser.parse_args()
    vendor = server / "vendor/str0m"
    reference = args.reference_dir.resolve()
    source_bytes = (vendor / "SOURCE.json").read_bytes()
    source = json.loads(source_bytes)
    reconstruction = max(
        (value["reconstruction"] for value in source.values()
         if isinstance(value, dict) and "reconstruction" in value),
        key=lambda value: len(value["sequential_patches"]),
    )
    patch_names = reconstruction["sequential_patches"]
    files = sorted(p for p in reference.rglob("*") if p.is_file()
                   and p.name not in {".cargo-ok", ".cargo-checksum.json"})
    manifest = b"".join(str(p.relative_to(reference)).encode() + b"\0"
                        + digest(p.read_bytes()).encode() + b"\n" for p in files)
    check(len(files) == source["upstream_file_count"], "original input file count differs")
    check(digest(manifest) == source["upstream_tree_manifest_sha256"],
          "original input tree manifest differs")
    expected_patch_hashes = {}
    for value in source.values():
        if isinstance(value, dict) and "patch" in value and "patch_sha256" in value:
            expected_patch_hashes[value["patch"]] = value["patch_sha256"]
    patch_hashes = {}
    touched = set()
    for name in patch_names:
        check(Path(name).name == name, "patch path must stay in vendor root")
        data = (vendor / name).read_bytes()
        patch_hashes[name] = digest(data)
        if name in expected_patch_hashes:
            check(digest(data) == expected_patch_hashes[name], f"patch hash differs: {name}")
        touched.update(re.findall(r"^\+\+\+ b/(.+)$", data.decode(), re.MULTILINE))
    check(touched == set(reconstruction["exact_files"]), "declared files differ from patch paths")
    check(len(touched) == reconstruction["count"], "declared reconstruction count differs")
    results = {}
    with tempfile.TemporaryDirectory(prefix="livekit-rs-str0m-reconstruction-") as temporary:
        rebuilt = Path(temporary) / "str0m"
        shutil.copytree(reference, rebuilt)
        for name in patch_names:
            subprocess.run(["git", "apply", "--check", str(vendor / name)],
                           cwd=rebuilt, check=True, capture_output=True)
            subprocess.run(["git", "apply", str(vendor / name)],
                           cwd=rebuilt, check=True, capture_output=True)
        for name in sorted(touched):
            actual = (rebuilt / name).read_bytes()
            expected = (vendor / name).read_bytes()
            check(actual == expected, f"reconstructed file differs: {name}")
            results[name] = {"reconstructed_sha256": digest(actual),
                             "vendored_sha256": digest(expected), "identical": True}
    print(json.dumps({"status": "PASS", "package": source["package"],
                      "version": source["version"], "source_sha256": digest(source_bytes),
                      "original_file_count": len(files),
                      "original_tree_manifest_sha256": digest(manifest),
                      "sequential_patch_sha256": patch_hashes,
                      "exact_file_count": len(results), "files": results,
                      "temporary_directory_removed": True,
                      "scope": "Byte-exact native source reconstruction only; no Cargo, network, browser or runtime acceptance."},
                     indent=2))


if __name__ == "__main__":
    main()
