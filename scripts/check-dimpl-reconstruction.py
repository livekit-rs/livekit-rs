#!/usr/bin/env python3
"""Byte-exact dimpl provenance check; no Cargo, Git, network or temporary writes."""
import argparse
import hashlib
import json
from pathlib import Path, PurePosixPath
import re


def digest(data):
    return hashlib.sha256(data).hexdigest()


def check(condition, message):
    if not condition:
        raise ValueError(message)


def safe_path(path):
    check(path and not PurePosixPath(path).is_absolute(), "absolute/empty patch path")
    check(".." not in PurePosixPath(path).parts, "patch path traverses parent")
    return path


def apply_patch(tree, patch):
    """Apply textual unified hunks with exact context/counts; never use fuzz."""
    lines = patch.decode("utf-8").splitlines(keepends=True)
    touched = set()
    i = 0
    while i < len(lines):
        if not lines[i].startswith("--- "):
            check(not lines[i].startswith("GIT binary patch"), "binary patch unsupported")
            i += 1
            continue
        old_path = lines[i][4:].rstrip("\n").split("\t", 1)[0]
        i += 1
        check(i < len(lines) and lines[i].startswith("+++ "), "missing new patch path")
        new_path = lines[i][4:].rstrip("\n").split("\t", 1)[0]
        i += 1
        check(new_path.startswith("b/"), "patch must produce b/ relative file")
        name = safe_path(new_path[2:])
        if old_path == "/dev/null":
            check(name not in tree, "new file already exists: " + name)
            original = []
        else:
            check(old_path == "a/" + name and name in tree, "old file identity mismatch: " + name)
            original = tree[name].decode("utf-8").splitlines(keepends=True)
        result = []
        position = 0
        hunks = 0
        while i < len(lines) and lines[i].startswith("@@ "):
            match = re.fullmatch(r"@@ -(\d+)(?:,(\d+))? \+(\d+)(?:,(\d+))? @@[^\n]*\n?", lines[i])
            check(match is not None, "invalid hunk header")
            old_start, old_count, new_start, new_count = match.groups()
            old_count = int(old_count) if old_count is not None else 1
            new_count = int(new_count) if new_count is not None else 1
            target = int(old_start) - 1 if int(old_start) else 0
            check(position <= target <= len(original), "overlapping/out-of-range hunk")
            result.extend(original[position:target])
            position = target
            check(len(result) == max(0, int(new_start) - 1), "new hunk offset mismatch")
            i += 1
            body = []
            old_seen = new_seen = 0
            while i < len(lines) and (lines[i][:1] in {" ", "+", "-"} or lines[i].startswith("\\ No newline")):
                if lines[i].startswith("--- ") and old_seen == old_count and new_seen == new_count:
                    break
                if lines[i].startswith("\\ No newline"):
                    check(bool(body), "newline marker without body")
                    body[-1] = (body[-1][0], body[-1][1].removesuffix("\n"))
                else:
                    tag, value = lines[i][0], lines[i][1:]
                    old_seen += tag != "+"
                    new_seen += tag != "-"
                    check(old_seen <= old_count and new_seen <= new_count, "hunk count overrun")
                    body.append((tag, value))
                i += 1
                if old_seen == old_count and new_seen == new_count:
                    if i < len(lines) and lines[i].startswith("\\ No newline"):
                        continue
                    break
            check(old_seen == old_count and new_seen == new_count, "truncated hunk")
            for tag, value in body:
                if tag != "+":
                    check(position < len(original) and original[position] == value,
                          "exact patch context mismatch: " + name)
                    position += 1
                if tag != "-":
                    result.append(value)
            hunks += 1
        check(hunks > 0, "file has no text hunks: " + name)
        result.extend(original[position:])
        tree[name] = "".join(result).encode("utf-8")
        touched.add(name)
    check(bool(touched), "empty patch")
    return touched


def parser_self_check():
    """Deterministic parser regressions, independent of a real vendor manifest."""
    tree = {"sample.rs": b"alpha\nbeta\n"}
    patch = b"--- a/sample.rs\n+++ b/sample.rs\n@@ -1,2 +1,3 @@\n alpha\n-beta\n+BETA\n+gamma\n"
    check(apply_patch(tree, patch) == {"sample.rs"}, "self-check touched paths")
    check(tree["sample.rs"] == b"alpha\nBETA\ngamma\n", "self-check exact replacement")
    created = {}
    apply_patch(created, b"--- /dev/null\n+++ b/new.rs\n@@ -0,0 +1 @@\n+new\n")
    check(created == {"new.rs": b"new\n"}, "self-check new file")
    no_newline = {"sample.rs": b"old"}
    apply_patch(no_newline, b"--- a/sample.rs\n+++ b/sample.rs\n@@ -1 +1 @@\n-old\n\\ No newline at end of file\n+new\n\\ No newline at end of file\n")
    check(no_newline["sample.rs"] == b"new", "self-check newline preservation")
    for invalid in [
        patch.replace(b" alpha\n", b" wrong\n"),
        patch.replace(b"+gamma\n", b""),
        patch.replace(b"b/sample.rs", b"b/../escape.rs"),
        b"GIT binary patch\n",
    ]:
        try:
            apply_patch({"sample.rs": b"alpha\nbeta\n"}, invalid)
        except ValueError:
            pass
        else:
            raise ValueError("self-check accepted invalid/fuzzy/traversal patch")


def main():
    server = Path(__file__).resolve().parent.parent
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--reference-dir", type=Path,
                        default=server / "references/crates/dimpl-0.7.6")
    args = parser.parse_args()
    parser_self_check()
    vendor = server / "vendor/dimpl"
    source_bytes = (vendor / "SOURCE.json").read_bytes()
    source = json.loads(source_bytes)
    reference = args.reference_dir.resolve()
    files = sorted(p for p in reference.rglob("*") if p.is_file()
                   and p.name not in {".cargo-ok", ".cargo-checksum.json"})
    tree = {p.relative_to(reference).as_posix(): p.read_bytes() for p in files}
    manifest = b"".join(digest(data).encode() + b"  " + name.encode() + b"\n"
                        for name, data in sorted(tree.items()))
    check(len(tree) == source["upstream_file_count"], "upstream file count differs")
    check(digest(manifest) == source["upstream_tree_manifest_sha256"], "upstream tree differs")
    recon = source["reconstruction"]
    patch_names = recon["sequential_patches"]
    declared_hashes = source["patch_sha256"]
    touched = set()
    patch_hashes = {}
    for name in patch_names:
        check(Path(name).name == name, "patch must remain in vendor root")
        data = (vendor / name).read_bytes()
        patch_hashes[name] = digest(data)
        check(patch_hashes[name] == declared_hashes[name], "patch checksum differs: " + name)
        touched.update(apply_patch(tree, data))
    check(touched == set(source["modified_native_files"]), "declared patched paths differ")
    check(set(tree) == set(recon["exact_files"]), "declared reconstructed tree paths differ")
    check(len(tree) == recon["count"], "reconstructed file count differs")
    check(set(tree) == set(source["exact_files_sha256"]), "declared file hashes differ")
    for name in sorted(tree):
        check(digest(tree[name]) == source["exact_files_sha256"][name], "declared output hash differs: " + name)
    for name, data in tree.items():
        check((vendor / name).is_file() and (vendor / name).read_bytes() == data,
              "reconstructed vendor file differs: " + name)
    metadata = set(source.get("metadata_files", ["SOURCE.json", "PATCHES.md"])) | set(patch_names)
    actual_files = {p.relative_to(vendor).as_posix() for p in vendor.rglob("*") if p.is_file()
                    and p.name not in {".cargo-ok", ".cargo-checksum.json"}}
    check(actual_files == set(tree) | metadata, "undeclared/missing vendor tree files")
    licenses = source["license_sha256"]
    check(bool(licenses), "license identity required")
    for name, expected in licenses.items():
        check(name in tree and digest(tree[name]) == expected, "license bytes differ: " + name)
    print(json.dumps({"status": "PASS", "package": source["package"], "version": source["version"],
                      "source_sha256": digest(source_bytes), "upstream_file_count": len(files),
                      "upstream_tree_manifest_sha256": digest(manifest),
                      "sequential_patch_sha256": patch_hashes, "exact_file_count": len(tree), "patched_file_count": len(touched),
                      "whole_reconstructed_file_count": len(tree),
                      "patched_files_sha256": {name: digest(tree[name]) for name in sorted(touched)},
                      "license_sha256": licenses, "temporary_files_created": False, "strict_parser_self_checks": "PASS",
                      "scope": "Strict in-memory byte reconstruction of published source, manifests and licenses; no compiler/runtime/security-timing acceptance."}, indent=2))


if __name__ == "__main__":
    main()
