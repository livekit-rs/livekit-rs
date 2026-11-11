#!/usr/bin/env python3
"""Stage the pinned livekit-stable tree plus the NEWLY AUTHORED in-package
joining-reliable-queue observers.

Never executes Go, never writes inside references/, and refuses a non-fresh
output directory. Every manifest sha256 is verified BEFORE anything is copied.
"""
import argparse
import hashlib
import json
import shutil
from pathlib import Path

OBSERVERS = [
    "joining_reliable_observer_test.go",
    "source_functional_observer_init_test.go",
]
REPLACES = [
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
        got = sha(root / rel)
        if got != want:
            raise SystemExit("source drift: " + rel)
    for name in OBSERVERS:
        want = manifest["oracle_sha256"][name]
        got = sha(here / name)
        if got != want:
            raise SystemExit("observer drift: " + name)

    if out.exists():
        raise SystemExit("fresh isolated stage required: " + str(out))
    ref = root / "references/livekit-stable"
    if not ref.is_dir():
        raise SystemExit("missing pinned server tree: " + str(ref))
    shutil.copytree(ref, out, ignore=shutil.ignore_patterns(".git"))
    for name in OBSERVERS:
        shutil.copy2(here / name, out / "pkg/rtc" / name)

    mod = out / "go.mod"
    text = mod.read_text()
    for name, rel in REPLACES:
        text += "\nreplace " + name + " => " + json.dumps(str((root / rel).resolve())) + "\n"
    mod.write_text(text)

    sums = set()
    for rel in SUM_MODULES:
        sums.update((root / rel / "go.sum").read_text().splitlines())
    (out / "go.sum").write_text("\n".join(sorted(sums)) + "\n")

    staged = {
        "status": "STAGED_NOT_RUN",
        "oracle": "joining-reliable-queue",
        "label": "NEWLY AUTHORED",
        "source_manifest_sha256": sha(here / "SOURCE.json"),
        "source_sha256": manifest["source_sha256"],
        "observer_sha256": {n: sha(out / "pkg/rtc" / n) for n in OBSERVERS},
        "staged_go_mod_sha256": sha(mod),
        "staged_go_sum_sha256": sha(out / "go.sum"),
        "staged_go_sum_lines": len(sums),
        "algorithm_substitutions": [],
        "go_executed": False,
    }
    (out / "STAGED.json").write_text(json.dumps(staged, indent=2) + "\n")
    print(out)


if __name__ == "__main__":
    main()