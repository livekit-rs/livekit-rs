#!/usr/bin/env python3
"""Verify pinned schema hashes and, optionally, the local upstream checkout.

This verifies only the consumed .proto snapshots. It does not establish SDK
interoperability; generated wire/JSON behavior has separate Rust tests.
"""
import argparse
import hashlib
import json
from pathlib import Path
import subprocess
import sys


def main():
    server = Path(__file__).resolve().parents[1]
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--reference", type=Path, help="optional protocol Git checkout; verify its commit and protobufs")
    parser.add_argument("--psrpc-reference", type=Path, help="optional fixed psrpc checkout")
    args = parser.parse_args()
    protos = server / "crates/lk-protocol/protos"
    lock = json.loads((protos / "schema-lock.json").read_text())
    failures = []
    if args.reference:
        revision = subprocess.check_output(["git", "-C", str(args.reference), "rev-parse", "HEAD"], text=True).strip()
        if revision != lock["commit"]:
            failures.append(f"reference commit {revision} != {lock['commit']}")
    files = {file.relative_to(protos).as_posix() for file in protos.rglob("*.proto")
             if "psrpc" not in file.relative_to(protos).parts}
    if files != set(lock["sha256"]):
        failures.append("schema file inventory does not match schema-lock.json")
    for name, expected in lock["sha256"].items():
        paths = [protos / name]
        if args.reference:
            paths.append(args.reference / "protobufs" / name)
        for path in paths:
            if not path.is_file() or hashlib.sha256(path.read_bytes()).hexdigest() != expected:
                failures.append(f"schema hash mismatch: {path}")
    rpc_root = protos / "psrpc"
    rpc_lock = json.loads((rpc_root / "schema-lock.json").read_text())
    rpc_files = {file.relative_to(rpc_root).as_posix() for file in rpc_root.rglob("*.proto")}
    if rpc_files != set(rpc_lock["sha256"]):
        failures.append("psrpc schema inventory differs")
    if args.psrpc_reference:
        revision = subprocess.check_output(["git", "-C", str(args.psrpc_reference), "rev-parse", "HEAD"], text=True).strip()
        if revision != rpc_lock["commit"]:
            failures.append("psrpc reference commit differs")
    for name, expected in rpc_lock["sha256"].items():
        paths = [rpc_root / name]
        if args.psrpc_reference:
            reference_path = rpc_lock.get("reference_paths", {}).get(
                name, str(Path(rpc_lock["reference_subdirectory"]) / name))
            paths.append(args.psrpc_reference / reference_path)
        for path in paths:
            if not path.is_file() or hashlib.sha256(path.read_bytes()).hexdigest() != expected:
                failures.append(f"psrpc schema hash mismatch: {path}")
    licenses = [rpc_root / "LICENSE"]
    if args.psrpc_reference:
        licenses.append(args.psrpc_reference / "LICENSE")
    for license_path in licenses:
        if not license_path.is_file() or hashlib.sha256(license_path.read_bytes()).hexdigest() != rpc_lock["license_sha256"]:
            failures.append(f"psrpc license hash mismatch: {license_path}")
    if failures:
        print("\n".join(failures), file=sys.stderr)
        return 1
    print(f"PASS: {len(files)} public schema files + {len(rpc_files)} psrpc schema files, protocol commit {lock['commit']}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
