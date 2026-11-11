#!/usr/bin/env python3
"""Check resolved architecture and production dependency graphs without compiling.

Cargo metadata can contain inactive optional edges. Cargo tree selects actual
per-root normal/build edges. This is a dependency graph
check, not a source-language or final executable/link audit. OS FFI crates such
as libc are permitted; known native crypto implementations are checked by name.
"""
from __future__ import annotations

import argparse
from collections import deque
from datetime import datetime, timezone
import hashlib
import json
import os
import re
from pathlib import Path
import subprocess
import sys
import tempfile

DEFAULT_TARGETS = ["x86_64-unknown-linux-gnu", "aarch64-unknown-linux-gnu"]
ARCHITECTURE_ROOTS = ("rtc-core", "rtc-media", "rtc-runtime")
BANNED_PRODUCTION = {
    "ring", "aws-lc-rs", "aws-lc-sys", "openssl", "openssl-sys", "openssl-src", "rcgen",
}


def production_edges(metadata: dict) -> tuple[dict, dict]:
    packages = {package["id"]: package for package in metadata["packages"]}
    if metadata.get("resolve") is None:
        raise ValueError("cargo metadata returned no resolved graph")
    edges = {}
    for node in metadata["resolve"]["nodes"]:
        children = set()
        for dep in node["deps"]:
            # Cargo's normal dependency kind is null; build scripts and their
            # dependency trees also belong to the production build closure.
            if any(kind["kind"] in (None, "normal", "build") for kind in dep["dep_kinds"]):
                children.add(dep["pkg"])
        edges[node["id"]] = sorted(children)
    return packages, edges


def activated_tree_edges(metadata: dict, output: str) -> dict:
    """Parse Cargo's per-root normal/build graph, not metadata's optional superset."""
    identities = {}
    for package in metadata["packages"]:
        identities.setdefault((package["name"], package["version"]), []).append(package["id"])
    edges, stack = {}, []
    for line in output.splitlines():
        match = re.fullmatch(r"(\d+)([^ ]+) v([^ ]+)(?: .*)?", line)
        if not match:
            raise ValueError(f"unexpected cargo tree line: {line!r}")
        depth, name, version = int(match[1]), match[2], match[3]
        ids = identities.get((name, version), [])
        if len(ids) != 1:
            raise ValueError(f"ambiguous or unknown Cargo package {name}@{version}: {ids}")
        pid = ids[0]
        if depth > len(stack) or (depth == 0 and stack):
            raise ValueError("invalid cargo tree depth/root")
        stack = stack[:depth]
        edges.setdefault(pid, set())
        if stack:
            edges[stack[-1]].add(pid)
        stack.append(pid)
    if not stack:
        raise ValueError("empty cargo tree")
    return {pid: sorted(children) for pid, children in edges.items()}


def paths_from(edges: dict, root: str) -> dict[str, str | None]:
    parents: dict[str, str | None] = {root: None}
    queue = deque([root])
    while queue:
        parent = queue.popleft()
        for child in edges.get(parent, []):
            if child not in parents:
                parents[child] = parent
                queue.append(child)
    return parents


def check_metadata(metadata: dict, production_root: str = "livecloud", active_graphs: dict | None = None) -> dict:
    packages, edges = production_edges(metadata)
    members = set(metadata["workspace_members"])

    def workspace_root(name: str) -> str:
        matches = [pid for pid in members if packages[pid]["name"] == name]
        if len(matches) != 1:
            raise ValueError(f"expected exactly one workspace package {name!r}, got {len(matches)}")
        return matches[0]

    def label(pid: str) -> str:
        package = packages[pid]
        return f"{package['name']}@{package['version']}"

    def path(parents: dict, pid: str) -> list[str]:
        result = []
        while pid is not None:
            result.append(label(pid))
            pid = parents[pid]
        return list(reversed(result))

    checks = []
    for name in ARCHITECTURE_ROOTS:
        parents = paths_from(active_graphs[name] if active_graphs is not None else edges, workspace_root(name))
        violations = [
            {"package": label(pid), "path": path(parents, pid)}
            for pid in sorted(parents) if packages[pid]["name"].startswith("lk-")
        ]
        checks.append({"rule": "A01", "root": name, "passed": not violations,
                       "package_count": len(parents), "violations": violations})
    parents = paths_from(active_graphs[production_root] if active_graphs is not None else edges, workspace_root(production_root))
    violations = [
        {"package": label(pid), "path": path(parents, pid)}
        for pid in sorted(parents)
        if packages[pid]["name"] in BANNED_PRODUCTION or packages[pid]["name"].startswith("aws-lc-")
    ]
    checks.append({"rule": "STRICT_RUST_DEPENDENCIES", "root": production_root,
                   "passed": not violations, "package_count": len(parents), "violations": violations})
    return {"passed": all(check["passed"] for check in checks), "checks": checks,
            "production_packages": [
                {"id": pid, "name": packages[pid]["name"], "version": packages[pid]["version"],
                 "source": packages[pid].get("source"),
                 "features": next((node["features"] for node in metadata["resolve"]["nodes"] if node["id"] == pid), [])}
                for pid in sorted(parents)
            ]}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    server = Path(__file__).resolve().parents[1]
    parser.add_argument("--manifest-path", type=Path, default=server / "Cargo.toml")
    parser.add_argument("--target", action="append", help="target triple; repeatable; defaults to Linux x86_64 + aarch64")
    parser.add_argument("--root", default="livecloud", help="production workspace package")
    parser.add_argument("--output", type=Path, default=server / "target/evidence/dependency-graph.json")
    args = parser.parse_args()
    manifest = args.manifest_path.resolve()
    lock = manifest.parent / "Cargo.lock"
    evidence = {
        "schema_version": 1, "generated_at": datetime.now(timezone.utc).isoformat(),
        "manifest_path": str(manifest), "cargo_lock_sha256": hashlib.sha256(lock.read_bytes()).hexdigest() if lock.exists() else None,
        "scope": "Per-root cargo tree activated normal/build edges; metadata supplies package identities; dev edges excluded",
        "limitations": ["Does not inspect native link inputs, machine code, inline assembly, or algorithm source languages",
                        "Package features reported from metadata may include workspace feature unification; graph reachability uses per-root cargo tree; final production link inputs require a separate audit"],
        "banned_production_packages": sorted(BANNED_PRODUCTION), "targets": [],
    }
    for target in args.target or DEFAULT_TARGETS:
        command = ["cargo", "metadata", "--locked", "--format-version", "1", "--manifest-path", str(manifest), "--filter-platform", target]
        result = {"target": target, "command": command}
        try:
            process = subprocess.run(command, text=True, capture_output=True, env={**os.environ, "CARGO_TERM_COLOR": "never"}, check=False)
            if process.returncode:
                raise RuntimeError(process.stderr.strip() or f"cargo metadata exited {process.returncode}")
            metadata = json.loads(process.stdout)
            graphs = {}
            result["activated_graphs"] = []
            for name in dict.fromkeys((*ARCHITECTURE_ROOTS, args.root)):
                tree_command = ["cargo", "tree", "--locked", "--manifest-path", str(manifest),
                                "--target", target, "-p", name, "--edges", "normal,build",
                                "--prefix", "depth", "--format", "{p}", "--no-dedupe"]
                tree = subprocess.run(tree_command, text=True, capture_output=True,
                                      env={**os.environ, "CARGO_TERM_COLOR": "never"}, check=False)
                if tree.returncode:
                    raise RuntimeError(tree.stderr.strip() or "cargo tree failed")
                graphs[name] = activated_tree_edges(metadata, tree.stdout)
                result["activated_graphs"].append({"root": name, "command": tree_command,
                    "output_sha256": hashlib.sha256(tree.stdout.encode()).hexdigest(),
                    "edges": graphs[name]})
            result.update(check_metadata(metadata, args.root, graphs))
        except (OSError, RuntimeError, ValueError, KeyError) as error:
            result.update(passed=False, error=str(error))
        evidence["targets"].append(result)
    evidence["passed"] = all(target["passed"] for target in evidence["targets"])
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile("w", encoding="utf-8", dir=args.output.parent, delete=False) as stream:
        json.dump(evidence, stream, indent=2, ensure_ascii=False, sort_keys=True)
        stream.write("\n")
        temporary = Path(stream.name)
    temporary.replace(args.output)
    for target in evidence["targets"]:
        print(f"{target['target']}: {'PASS' if target['passed'] else 'FAIL'}")
        if "error" in target:
            print(target["error"], file=sys.stderr)
        for check in target.get("checks", []):
            for violation in check["violations"]:
                print(f"  {check['rule']}: {' -> '.join(violation['path'])}")
    print(f"Evidence: {args.output.resolve()}")
    return 0 if evidence["passed"] else 1


if __name__ == "__main__":
    sys.exit(main())
