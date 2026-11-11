#!/usr/bin/env python3
"""Stage a tiny external module that drives the REAL upstream fanout entry point.

Never executes Go, never writes inside references/, and refuses a non-fresh
output directory. Every manifest sha256 is verified BEFORE anything is written.

An external module (rather than a whole-tree pkg/rtc test stage) is used because
rtc.BroadcastDataPacketForRoom is exported and takes the types.Room interface:
the pinned upstream typesfakes supply the room inventory and the recipients, so
no in-package access, no pkg/rtc test binary and no Docker TestMain are needed.
"""
import argparse
import hashlib
import json
from pathlib import Path

REPLACES = [
    ("github.com/livekit/livekit-server", "references/livekit-stable"),
    ("github.com/livekit/protocol", "references/protocol-stable"),
    ("github.com/livekit/mediatransportutil", "references/go-modules/mediatransportutil-f234b534b095"),
    ("github.com/livekit/psrpc", "references/psrpc"),
]
SUM_MODULES = [
    "references/livekit-stable",
    "references/protocol-stable",
    "references/psrpc",
    "references/go-modules/mediatransportutil-f234b534b095",
]
MODULE_NAME = "local.test/ordinary-fanout"


def sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--root", required=True, type=Path)
    p.add_argument("--out", required=True, type=Path)
    a = p.parse_args()
    root = a.root.resolve()
    out = a.out.resolve()
    here = Path(__file__).resolve().parent

    manifest = json.loads((here / "SOURCE.json").read_text())
    for rel, want in manifest["source_sha256"].items():
        if sha(root / rel) != want:
            raise SystemExit("source drift: " + rel)
    for name, want in manifest["oracle_sha256"].items():
        if sha(here / name) != want:
            raise SystemExit("observer drift: " + name)

    if out.exists():
        raise SystemExit("fresh isolated stage required: " + str(out))
    out.mkdir(parents=True)

    mod = (root / "references/livekit-stable/go.mod").read_text()
    needle = "module github.com/livekit/livekit-server\n"
    if mod.count(needle) != 1:
        raise SystemExit("unexpected pinned go.mod module line")
    mod = mod.replace(needle, "module " + MODULE_NAME + "\n", 1)
    mod += "\nrequire github.com/livekit/livekit-server v0.0.0\n"
    for name, rel in REPLACES:
        mod += "replace " + name + " => " + json.dumps(str((root / rel).resolve())) + "\n"
    (out / "go.mod").write_text(mod)

    sums = set()
    for rel in SUM_MODULES:
        sums.update((root / rel / "go.sum").read_text().splitlines())
    (out / "go.sum").write_text("\n".join(sorted(sums)) + "\n")

    (out / "main.go").write_bytes((here / "main.go").read_bytes())

    staged = {
        "status": "STAGED_NOT_RUN",
        "oracle": "ordinary-fanout",
        "label": "NEWLY AUTHORED",
        "module": MODULE_NAME,
        "source_manifest_sha256": sha(here / "SOURCE.json"),
        "source_sha256": manifest["source_sha256"],
        "oracle_sha256": manifest["oracle_sha256"],
        "inputs": {n: sha(out / n) for n in ["go.mod", "go.sum", "main.go"]},
        "staged_go_sum_lines": len(sums),
        "algorithm_substitutions": [],
        "go_executed": False,
    }
    (out / "STAGED.json").write_text(json.dumps(staged, indent=2) + "\n")
    print(out)


if __name__ == "__main__":
    main()